#pragma once

#include <any>
#include <atomic>
#include <cassert>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <functional>
#include <future>
#include <iostream>
#include <memory>
#include <mutex>
#include <queue>
#include <shared_mutex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <typeindex>
#include <typeinfo>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

namespace minidag {

// ==========================================
// 0. Observability: Logger & Metrics
// ==========================================
enum class LogLevel { kDebug, kInfo, kWarn, kError };

using LogFn = std::function<void(LogLevel, const std::string&)>;

inline LogFn StderrLogger(LogLevel min = LogLevel::kInfo) {
  return [min](LogLevel level, const std::string& msg) {
    if (level < min) return;
    static const char* tags[] = {"DEBUG", "INFO", "WARN", "ERROR"};
    std::cerr << "[" << tags[static_cast<int>(level)] << "] " << msg << "\n";
  };
}

struct NodeMetric {
  std::string name;
  int64_t duration_us = 0;
  bool skipped = false;    // optional node failed
  bool timed_out = false;  // node exceeded timeout_ms
};

class ConfigNode;
class Operator;
class Registry;
class GraphTemplate;
class GraphExecutor;

// ==========================================
// 1. Infrastructure: Thread Pool
// ==========================================
class ThreadPool {
 public:
  explicit ThreadPool(size_t threads = std::thread::hardware_concurrency())
      : stop_(false) {
    for (size_t i = 0; i < threads; ++i)
      workers_.emplace_back([this] {
        while (true) {
          std::function<void()> task;
          {
            std::unique_lock<std::mutex> lock(this->queue_mutex_);
            this->condition_.wait(
                lock, [this] { return this->stop_ || !this->tasks_.empty(); });
            if (this->stop_ && this->tasks_.empty()) {
              return;
            }
            task = std::move(this->tasks_.front());
            this->tasks_.pop();
          }
          task();
        }
      });
  }

  template <class F>
  void Enqueue(F&& f) {
    {
      std::unique_lock<std::mutex> lock(queue_mutex_);
      if (stop_) {
        throw std::runtime_error("Enqueue on stopped ThreadPool");
      }

      tasks_.emplace(std::forward<F>(f));
    }
    condition_.notify_one();
  }

  ~ThreadPool() {
    {
      std::unique_lock<std::mutex> lock(queue_mutex_);
      stop_ = true;
    }
    condition_.notify_all();
    for (std::thread& worker : workers_) {
      worker.join();
    }
  }

 private:
  std::vector<std::thread> workers_;
  std::queue<std::function<void()>> tasks_;
  std::mutex queue_mutex_;
  std::condition_variable condition_;
  std::atomic<bool> stop_;
};

// ==========================================
// 2. Type System: Token & Context
// ==========================================

// Strongly-typed token; avoids raw string/int keys
template <typename T>
struct DataToken {
  int index = -1;
  std::string name;
  bool IsValid() const { return index != -1; }
};

// Cache-line-padded wrapper to avoid false sharing.
// Context slots are read/written very frequently; alignment is critical.
struct alignas(64) PaddedSlot {
  std::any value;
};

class Context {
 public:
  explicit Context(size_t size) : data_(size) {}

  // Write data (wait-free)
  template <typename T>
  void Set(const DataToken<T>& token, T&& value) {
    if (!token.IsValid()) return;
    data_[token.index].value = std::forward<T>(value);
  }

  // Read data (wait-free, immutable)
  template <typename T>
  const T& Get(const DataToken<T>& token) const {
    if (!token.IsValid()) {
      throw std::runtime_error("Invalid token access: " + token.name);
    }

    try {
      return std::any_cast<const T&>(data_[token.index].value);
    } catch (const std::bad_any_cast&) {
      throw std::runtime_error("Type mismatch reading data: " + token.name);
    }
  }

  // Move-extract data (transfer ownership to the next stage or final output)
  template <typename T>
  T Move(const DataToken<T>& token) {
    if (!token.IsValid()) {
      throw std::runtime_error("Invalid token move");
    }

    return std::any_cast<T>(std::move(data_[token.index].value));
  }

  // Check whether a slot has been populated (safe for optional inputs)
  template <typename T>
  bool Has(const DataToken<T>& token) const {
    if (!token.IsValid()) return false;
    return data_[token.index].value.has_value();
  }

  // Cancellation
  void SetCancelFlag(std::atomic<bool>* flag) { cancel_ = flag; }
  bool IsCancelled() const {
    return cancel_ && cancel_->load(std::memory_order_acquire);
  }

 private:
  std::vector<PaddedSlot> data_;
  std::atomic<bool>* cancel_ = nullptr;
};

// ==========================================
// 3. Registry & Operator Base
// ==========================================

class Registry {
 public:
  struct Meta {
    int index;
    std::string name;
    std::type_index type;
  };

  void SetCurrentNode(int node_id) { current_node_ = node_id; }

  template <typename T>
  DataToken<T> Input(const std::string& name) {
    auto tok = RegisterSlot<T>(name);
    consumers_[tok.index].insert(current_node_);
    return tok;
  }

  template <typename T>
  DataToken<T> Output(const std::string& name) {
    auto tok = RegisterSlot<T>(name);
    producers_[tok.index].insert(current_node_);
    return tok;
  }

  template <typename T>
  DataToken<T> Lookup(const std::string& name) const {
    auto it = name_map_.find(name);
    if (it == name_map_.end()) {
      return {-1, name};
    }
    if (slots_[it->second].type != std::type_index(typeid(T))) {
      throw std::runtime_error("Type mismatch looking up data: " + name);
    }
    return {it->second, name};
  }

  size_t SlotCount() const { return slots_.size(); }
  const std::vector<Meta>& Slots() const { return slots_; }

  const std::unordered_map<int, std::unordered_set<int>>& Producers() const {
    return producers_;
  }
  const std::unordered_map<int, std::unordered_set<int>>& Consumers() const {
    return consumers_;
  }

  // Slots consumed but never produced — must be injected externally
  std::unordered_set<int> InputSlots() const {
    std::unordered_set<int> result;
    for (const auto& [slot, _] : consumers_) {
      if (producers_.find(slot) == producers_.end()) result.insert(slot);
    }
    return result;
  }

  // Slots produced but never consumed — available as graph outputs
  std::unordered_set<int> OutputSlots() const {
    std::unordered_set<int> result;
    for (const auto& [slot, _] : producers_) {
      if (consumers_.find(slot) == consumers_.end()) result.insert(slot);
    }
    return result;
  }

  void Dump(LogFn log = {}) const {
    auto emit = [&](const std::string& msg) {
      if (log)
        log(LogLevel::kDebug, msg);
      else
        std::cout << msg << "\n";
    };
    emit("[Registry] Total Slots: " + std::to_string(slots_.size()));
    for (const auto& s : slots_)
      emit("  Slot " + std::to_string(s.index) + ": " + s.name + " [" +
           s.type.name() + "]");
  }

 private:
  template <typename T>
  DataToken<T> RegisterSlot(const std::string& name) {
    if (auto it = name_map_.find(name); it != name_map_.end()) {
      if (slots_[it->second].type != std::type_index(typeid(T))) {
        throw std::runtime_error("Type conflict for data: " + name);
      }
      return {it->second, name};
    }
    int id = static_cast<int>(slots_.size());
    slots_.push_back({id, name, std::type_index(typeid(T))});
    name_map_[name] = id;
    return {id, name};
  }

  int current_node_ = -1;
  std::vector<Meta> slots_;
  std::unordered_map<std::string, int> name_map_;
  std::unordered_map<int, std::unordered_set<int>> producers_;  // slot → nodes
  std::unordered_map<int, std::unordered_set<int>> consumers_;  // slot → nodes
};

// Operator interface
class Operator {
 public:
  virtual ~Operator() = default;
  virtual void Configure(const ConfigNode& config) {}  // optional configuration
  virtual void Init(Registry& reg) = 0;  // build phase: declare data deps
  virtual void Run(Context& ctx) = 0;    // runtime: pure computation
  virtual std::string Name() const = 0;
};

// Operator factory
using OpCreator = std::function<std::shared_ptr<Operator>()>;

class OpFactory {
 public:
  static OpFactory& Get() {
    static OpFactory f;
    return f;
  }

  void RegisterCreator(const std::string& type, OpCreator c) {
    creators_[type] = c;
  }

  std::shared_ptr<Operator> Create(const std::string& type) {
    if (creators_.find(type) == creators_.end()) {
      throw std::runtime_error("Unknown Op: " + type);
    }

    return creators_[type]();
  }

 private:
  std::unordered_map<std::string, OpCreator> creators_;
};

#define REGISTER_OP(Type)                                    \
  static struct Reg##Type {                                  \
    Reg##Type() {                                            \
      minidag::OpFactory::Get().RegisterCreator(             \
          #Type, []() { return std::make_shared<Type>(); }); \
    }                                                        \
  } reg_##Type;

// ==========================================
// 4. Graph Engine (Template & Executor)
// ==========================================

class ConfigNode {
 public:
  enum Type { kNull, kScalar, kSequence, kMap };
  ConfigNode() : type_(kNull) {}
  explicit ConfigNode(std::string value)
      : type_(kScalar), scalar_(std::move(value)) {}

  bool IsNull() const { return type_ == kNull; }
  bool IsScalar() const { return type_ == kScalar; }
  bool IsSequence() const { return type_ == kSequence; }
  bool IsMap() const { return type_ == kMap; }

  const ConfigNode& operator[](const std::string& key) const {
    if (type_ != kMap) {
      return NullNode();
    }
    auto it = map_.find(key);
    if (it == map_.end()) {
      return NullNode();
    }
    return it->second;
  }

  const ConfigNode& operator[](size_t index) const {
    if (type_ != kSequence || index >= sequence_.size()) {
      return NullNode();
    }
    return sequence_[index];
  }

  std::vector<ConfigNode>::const_iterator begin() const {
    return sequence_.begin();
  }
  std::vector<ConfigNode>::const_iterator end() const {
    return sequence_.end();
  }

  size_t size() const {
    if (type_ == kSequence) {
      return sequence_.size();
    } else if (type_ == kMap) {
      return map_.size();
    }
    return 0;
  }

  template <typename T>
  T As(const T& default_value = T{}) const {
    if (type_ != kScalar) {
      return default_value;
    }
    return Parse<T>(scalar_);
  }

  template <typename T>
  T AsRequired() const {
    if (type_ != kScalar) {
      throw std::runtime_error("ConfigNode is not a scalar");
    }
    return Parse<T>(scalar_);
  }

  void SetScalar(std::string value) {
    type_ = kScalar;
    scalar_ = std::move(value);
  }

  ConfigNode& AddSequenceItem() {
    type_ = kSequence;
    sequence_.emplace_back();
    return sequence_.back();
  }

  ConfigNode& AddMapItem(const std::string& key) {
    type_ = kMap;
    return map_[key];
  }

 private:
  Type type_;
  std::string scalar_;
  std::vector<ConfigNode> sequence_;
  std::unordered_map<std::string, ConfigNode> map_;

  static const ConfigNode& NullNode() {
    static ConfigNode null;
    return null;
  }

  template <typename T>
  T Parse(const std::string& value) const {
    if constexpr (std::is_same_v<T, std::string>) {
      return value;
    }
    if constexpr (std::is_same_v<T, bool>) {
      return value == "true" || value == "1";
    }

    T res{};
    std::stringstream ss(value);
    ss >> res;
    return res;
  }
};

struct NodeConfig {
  std::string id;
  std::string op_type;
  std::vector<std::string> dependencies;

  ConfigNode params;      // optional per-operator config
  bool optional = false;  // framework-level, NOT inside params
  int timeout_ms = 0;     // 0 = no timeout check
};

// Immutable graph template (read-only, reusable across requests)
class GraphTemplate {
 public:
  struct NodeDef {
    int id;
    std::string name;
    std::shared_ptr<Operator> op;
    std::vector<int> children;
    std::vector<int> parents;
    int initial_indegree = 0;
    bool optional = false;
    int timeout_ms = 0;
  };

  void Build(const std::vector<NodeConfig>& configs, LogFn log = {}) {
    log_ = log;
    nodes_.resize(configs.size());
    std::unordered_map<std::string, int> id_map;
    registry_ = Registry{};

    // Pass 1: Instantiate & Init
    for (size_t i = 0; i < configs.size(); ++i) {
      id_map[configs[i].id] = i;
      nodes_[i].id = i;
      nodes_[i].name = configs[i].id;
      nodes_[i].optional = configs[i].optional;
      nodes_[i].timeout_ms = configs[i].timeout_ms;
      nodes_[i].op = OpFactory::Get().Create(configs[i].op_type);
      nodes_[i].op->Configure(configs[i].params);
      registry_.SetCurrentNode(static_cast<int>(i));
      nodes_[i].op->Init(registry_);
    }

    // Pass 2: Topology
    for (size_t i = 0; i < configs.size(); ++i) {
      for (const auto& dep : configs[i].dependencies) {
        auto it = id_map.find(dep);
        if (it == id_map.end()) {
          throw std::runtime_error("Unknown dependency: " + dep);
        }
        int parent_id = it->second;
        nodes_[parent_id].children.push_back(i);
        nodes_[i].parents.push_back(parent_id);
        nodes_[i].initial_indegree++;
      }
    }

    // Pass 3: Validation
    ValidateCycle();
    ValidateDataFlow();

    slot_count_ = registry_.SlotCount();
    LogMsg(LogLevel::kInfo, "[GraphTemplate] Built successfully. Nodes: " +
                                std::to_string(nodes_.size()) +
                                ", DataSlots: " + std::to_string(slot_count_));
    registry_.Dump(log_);
  }

  const std::vector<NodeDef>& Nodes() const { return nodes_; }
  size_t SlotCount() const { return slot_count_; }
  const Registry& Reg() const { return registry_; }

  template <typename T>
  DataToken<T> Token(const std::string& name) const {
    return registry_.Lookup<T>(name);
  }

 private:
  std::vector<NodeDef> nodes_;
  size_t slot_count_ = 0;
  Registry registry_;
  LogFn log_;

  void LogMsg(LogLevel level, const std::string& msg) const {
    if (log_)
      log_(level, msg);
    else
      std::cout << msg << "\n";
  }

  // Kahn's algorithm cycle detection
  void ValidateCycle() const {
    size_t n = nodes_.size();
    std::vector<int> in(n);
    for (size_t i = 0; i < n; ++i) in[i] = nodes_[i].initial_indegree;

    std::queue<int> q;
    for (size_t i = 0; i < n; ++i)
      if (in[i] == 0) q.push(static_cast<int>(i));

    int processed = 0;
    while (!q.empty()) {
      int cur = q.front();
      q.pop();
      ++processed;
      for (int child : nodes_[cur].children)
        if (--in[child] == 0) q.push(child);
    }

    if (processed != static_cast<int>(n)) {
      std::string cycle_nodes;
      for (size_t i = 0; i < n; ++i) {
        if (in[i] > 0) {
          if (!cycle_nodes.empty()) cycle_nodes += ", ";
          cycle_nodes += nodes_[i].name;
        }
      }
      throw std::runtime_error("Cycle detected involving nodes: " +
                               cycle_nodes);
    }
  }

  // Check that every node's input slots are reachable from an ancestor or are
  // graph inputs (externally injected)
  void ValidateDataFlow() const {
    const auto& producers = registry_.Producers();
    auto graph_inputs = registry_.InputSlots();

    for (size_t i = 0; i < nodes_.size(); ++i) {
      const auto& consumers = registry_.Consumers();
      // Find all slots this node consumes
      for (const auto& [slot, node_set] : consumers) {
        if (node_set.find(static_cast<int>(i)) == node_set.end()) continue;
        // This node consumes this slot — check reachability
        if (graph_inputs.count(slot)) continue;  // Externally injected, OK

        // BFS backwards through parents to find a producer
        auto pit = producers.find(slot);
        if (pit == producers.end()) {
          const auto& meta = registry_.Slots()[slot];
          throw std::runtime_error("Node '" + nodes_[i].name +
                                   "' reads slot '" + meta.name +
                                   "' but no operator produces it");
        }

        const auto& producer_nodes = pit->second;
        std::unordered_set<int> visited;
        std::queue<int> bfs;
        for (int p : nodes_[i].parents) {
          bfs.push(p);
          visited.insert(p);
        }

        bool found = false;
        while (!bfs.empty() && !found) {
          int cur = bfs.front();
          bfs.pop();
          if (producer_nodes.count(cur)) {
            found = true;
            break;
          }
          for (int gp : nodes_[cur].parents) {
            if (visited.insert(gp).second) bfs.push(gp);
          }
        }

        if (!found) {
          const auto& meta = registry_.Slots()[slot];
          throw std::runtime_error("Node '" + nodes_[i].name +
                                   "' reads slot '" + meta.name +
                                   "' but no upstream dependency produces it");
        }
      }
    }
  }
};

// Per-request executor
class GraphExecutor : public std::enable_shared_from_this<GraphExecutor> {
 public:
  GraphExecutor(std::shared_ptr<const GraphTemplate> tmpl, ThreadPool& pool,
                LogFn log = {})
      : tmpl_(std::move(tmpl)),
        pool_(pool),
        context_(tmpl_->SlotCount()),
        indegrees_(tmpl_->Nodes().size()),
        log_(std::move(log)),
        metrics_(tmpl_->Nodes().size()) {
    const auto& nodes = tmpl_->Nodes();
    for (size_t i = 0; i < nodes.size(); ++i) {
      indegrees_[i].store(nodes[i].initial_indegree, std::memory_order_relaxed);
      metrics_[i].name = nodes[i].name;
    }
    remaining_tasks_.store(nodes.size(), std::memory_order_relaxed);
    context_.SetCancelFlag(&cancelled_);
  }

  Context& Ctx() { return context_; }
  const GraphTemplate& Template() const { return *tmpl_; }

  void Cancel() { cancelled_.store(true, std::memory_order_release); }

  const std::vector<NodeMetric>& Metrics() const { return metrics_; }

  std::future<void> Run() {
    promise_ = std::make_shared<std::promise<void>>();
    bool has_start = false;
    for (const auto& node : tmpl_->Nodes()) {
      if (node.initial_indegree == 0) {
        Schedule(node.id);
        has_start = true;
      }
    }
    if (!has_start && !tmpl_->Nodes().empty()) {
      promise_->set_exception(std::make_exception_ptr(
          std::runtime_error("Cycle detected or no entry!")));
    }

    return promise_->get_future();
  }

 private:
  std::shared_ptr<const GraphTemplate> tmpl_;
  ThreadPool& pool_;
  Context context_;
  std::vector<std::atomic<int>> indegrees_;
  std::atomic<int> remaining_tasks_;
  std::shared_ptr<std::promise<void>> promise_;
  std::atomic<bool> finished_{false};
  std::atomic<bool> cancelled_{false};
  LogFn log_;
  std::vector<NodeMetric> metrics_;

  void LogMsg(LogLevel level, const std::string& msg) const {
    if (log_) log_(level, msg);
  }

  void TrySetException(std::exception_ptr ep) {
    if (!finished_.exchange(true, std::memory_order_acq_rel)) {
      promise_->set_exception(ep);
    }
  }

  void TrySetValue() {
    if (!finished_.exchange(true, std::memory_order_acq_rel)) {
      promise_->set_value();
    }
  }

  void DrainChildren(int node_id) {
    for (int child_id : tmpl_->Nodes()[node_id].children) {
      if (indegrees_[child_id].fetch_sub(1, std::memory_order_acq_rel) == 1) {
        // This child is now runnable but cancelled — drain it too
        if (remaining_tasks_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
          TrySetValue();
          return;
        }
        DrainChildren(child_id);
      }
    }
  }

  void PropagateSuccess(int node_id) {
    for (int child_id : tmpl_->Nodes()[node_id].children) {
      if (indegrees_[child_id].fetch_sub(1, std::memory_order_acq_rel) == 1) {
        Schedule(child_id);
      }
    }
    if (remaining_tasks_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
      TrySetValue();
    }
  }

  void Schedule(int node_id) {
    auto self = shared_from_this();
    pool_.Enqueue([self, node_id] {
      // Check cancellation before running
      if (self->cancelled_.load(std::memory_order_acquire)) {
        self->DrainChildren(node_id);
        if (self->remaining_tasks_.fetch_sub(1, std::memory_order_acq_rel) ==
            1) {
          self->TrySetValue();
        }
        return;
      }

      const auto& node = self->tmpl_->Nodes()[node_id];
      self->LogMsg(LogLevel::kDebug, "[Executor] Running: " + node.name);

      auto t0 = std::chrono::steady_clock::now();
      try {
        node.op->Run(self->context_);
      } catch (const std::exception& e) {
        auto t1 = std::chrono::steady_clock::now();
        self->metrics_[node_id].duration_us =
            std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0)
                .count();

        if (node.optional) {
          self->metrics_[node_id].skipped = true;
          self->LogMsg(LogLevel::kWarn,
                       "[Executor] Optional node failed (skipped): " +
                           node.name + " — " + e.what());
          self->PropagateSuccess(node_id);
        } else {
          self->cancelled_.store(true, std::memory_order_release);
          self->LogMsg(LogLevel::kError, "[Executor] Exception in node: " +
                                             node.name + " — " + e.what());
          self->DrainChildren(node_id);
          if (self->remaining_tasks_.fetch_sub(1, std::memory_order_acq_rel) ==
              1) {
          }
          self->TrySetException(std::current_exception());
        }
        return;
      } catch (...) {
        self->cancelled_.store(true, std::memory_order_release);
        self->LogMsg(LogLevel::kError,
                     "[Executor] Exception in node: " + node.name);
        self->DrainChildren(node_id);
        if (self->remaining_tasks_.fetch_sub(1, std::memory_order_acq_rel) ==
            1) {
        }
        self->TrySetException(std::current_exception());
        return;
      }
      auto t1 = std::chrono::steady_clock::now();
      self->metrics_[node_id].duration_us =
          std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0)
              .count();

      self->LogMsg(LogLevel::kDebug,
                   "[Executor] Done: " + node.name + " (" +
                       std::to_string(self->metrics_[node_id].duration_us) +
                       "us)");

      // Post-hoc timeout check
      if (node.timeout_ms > 0 &&
          self->metrics_[node_id].duration_us >
              static_cast<int64_t>(node.timeout_ms) * 1000) {
        self->metrics_[node_id].timed_out = true;
        if (node.optional) {
          self->LogMsg(LogLevel::kWarn,
                       "[Executor] Optional node timed out: " + node.name);
          self->PropagateSuccess(node_id);
          return;
        } else {
          self->cancelled_.store(true, std::memory_order_release);
          self->LogMsg(LogLevel::kError,
                       "[Executor] Required node timed out: " + node.name);
          self->DrainChildren(node_id);
          if (self->remaining_tasks_.fetch_sub(1, std::memory_order_acq_rel) ==
              1) {
          }
          self->TrySetException(std::make_exception_ptr(
              std::runtime_error("timeout: " + node.name)));
          return;
        }
      }

      self->PropagateSuccess(node_id);
    });
  }
};

// ==========================================
// 5. DAG Manager (Multi-DAG)
// ==========================================
class DagManager {
 public:
  explicit DagManager(size_t threads = std::thread::hardware_concurrency(),
                      LogFn log = {})
      : pool_(std::make_unique<ThreadPool>(threads)), log_(std::move(log)) {}

  void BuildDag(const std::string& name,
                const std::vector<NodeConfig>& configs) {
    auto tmpl = std::make_shared<GraphTemplate>();
    tmpl->Build(configs, log_);
    std::unique_lock<std::shared_mutex> lock(mu_);
    if (dags_.count(name)) {
      throw std::runtime_error("DAG already registered: " + name);
    }
    dags_[name] = std::move(tmpl);
  }

  void ReplaceDag(const std::string& name,
                  const std::vector<NodeConfig>& configs) {
    auto tmpl = std::make_shared<GraphTemplate>();
    tmpl->Build(configs, log_);
    std::unique_lock<std::shared_mutex> lock(mu_);
    if (!dags_.count(name)) {
      throw std::runtime_error("Cannot replace unknown DAG: " + name);
    }
    dags_[name] = std::move(tmpl);
  }

  std::shared_ptr<GraphExecutor> CreateExecutor(const std::string& name) {
    std::shared_lock<std::shared_mutex> lock(mu_);
    auto it = dags_.find(name);
    if (it == dags_.end()) {
      throw std::runtime_error("Unknown DAG: " + name);
    }
    return std::make_shared<GraphExecutor>(it->second, *pool_, log_);
  }

  std::shared_ptr<const GraphTemplate> Dag(const std::string& name) const {
    std::shared_lock<std::shared_mutex> lock(mu_);
    auto it = dags_.find(name);
    if (it == dags_.end()) {
      throw std::runtime_error("Unknown DAG: " + name);
    }
    return it->second;
  }

  bool HasDag(const std::string& name) const {
    std::shared_lock<std::shared_mutex> lock(mu_);
    return dags_.count(name) > 0;
  }

  std::vector<std::string> ListDags() const {
    std::shared_lock<std::shared_mutex> lock(mu_);
    std::vector<std::string> names;
    names.reserve(dags_.size());
    for (const auto& [k, _] : dags_) {
      names.push_back(k);
    }
    return names;
  }

 private:
  std::unique_ptr<ThreadPool> pool_;
  std::unordered_map<std::string, std::shared_ptr<GraphTemplate>> dags_;
  mutable std::shared_mutex mu_;
  LogFn log_;
};

}  // namespace minidag
