#include <atomic>
#include <chrono>
#include <future>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "mini_dag.hpp"

#include <gtest/gtest.h>

using namespace minidag;

// Silent logger — suppress stdout/stderr noise in tests
static LogFn silent = [](LogLevel, const std::string&) {};

// ============================================================
// Test Helper Operators
// ============================================================

class AddOp : public Operator {
 public:
  void Init(Registry& reg) override {
    a_ = reg.Input<int>("a");
    b_ = reg.Input<int>("b");
    sum_ = reg.Output<int>("sum");
  }
  void Run(Context& ctx) override {
    int result = ctx.Get(a_) + ctx.Get(b_);
    ctx.Set(sum_, std::move(result));
  }
  std::string Name() const override { return "AddOp"; }

 private:
  DataToken<int> a_, b_;
  DataToken<int> sum_;
};
REGISTER_OP(AddOp);

class PassthroughOp : public Operator {
 public:
  void Init(Registry& reg) override {
    input_ = reg.Input<std::string>("input");
    output_ = reg.Output<std::string>("output");
  }
  void Run(Context& ctx) override {
    std::string val = ctx.Get(input_);
    ctx.Set(output_, std::move(val));
  }
  std::string Name() const override { return "PassthroughOp"; }

 private:
  DataToken<std::string> input_, output_;
};
REGISTER_OP(PassthroughOp);

class FailOp : public Operator {
 public:
  void Init(Registry& reg) override {
    input_ = reg.Input<std::string>("input");
  }
  void Run(Context& ctx) override {
    (void)ctx;
    throw std::runtime_error("FailOp always fails");
  }
  std::string Name() const override { return "FailOp"; }

 private:
  DataToken<std::string> input_;
};
REGISTER_OP(FailOp);

class SlowOp : public Operator {
 public:
  void Configure(const ConfigNode& conf) override {
    sleep_ms_ = conf["sleep_ms"].As<int>(50);
  }
  void Init(Registry& reg) override {
    input_ = reg.Input<std::string>("input");
    output_ = reg.Output<std::string>("slow_output");
  }
  void Run(Context& ctx) override {
    (void)ctx.Get(input_);
    std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms_));
    ctx.Set(output_, std::string("slow_done"));
  }
  std::string Name() const override { return "SlowOp"; }

 private:
  DataToken<std::string> input_, output_;
  int sleep_ms_ = 50;
};
REGISTER_OP(SlowOp);

class ConfigEchoOp : public Operator {
 public:
  void Configure(const ConfigNode& conf) override {
    echo_val_ = conf["echo"].As<std::string>("default_echo");
  }
  void Init(Registry& reg) override {
    output_ = reg.Output<std::string>("config_echo_out");
  }
  void Run(Context& ctx) override {
    ctx.Set(output_, std::string(echo_val_));
  }
  std::string Name() const override { return "ConfigEchoOp"; }

  const std::string& GetEchoVal() const { return echo_val_; }

 private:
  DataToken<std::string> output_;
  std::string echo_val_;
};
REGISTER_OP(ConfigEchoOp);

class ProducerOp : public Operator {
 public:
  void Init(Registry& reg) override {
    output_ = reg.Output<std::string>("input");
  }
  void Run(Context& ctx) override {
    ctx.Set(output_, std::string("produced_value"));
  }
  std::string Name() const override { return "ProducerOp"; }

 private:
  DataToken<std::string> output_;
};
REGISTER_OP(ProducerOp);

class IntProducerOp : public Operator {
 public:
  void Configure(const ConfigNode& conf) override {
    a_val_ = conf["a"].As<int>(1);
    b_val_ = conf["b"].As<int>(2);
  }
  void Init(Registry& reg) override {
    a_ = reg.Output<int>("a");
    b_ = reg.Output<int>("b");
  }
  void Run(Context& ctx) override {
    ctx.Set(a_, int(a_val_));
    ctx.Set(b_, int(b_val_));
  }
  std::string Name() const override { return "IntProducerOp"; }

 private:
  DataToken<int> a_, b_;
  int a_val_ = 1, b_val_ = 2;
};
REGISTER_OP(IntProducerOp);

class CancelAwareOp : public Operator {
 public:
  void Init(Registry& reg) override {
    input_ = reg.Input<std::string>("input");
    output_ = reg.Output<std::string>("cancel_output");
  }
  void Run(Context& ctx) override {
    for (int i = 0; i < 100; ++i) {
      if (ctx.IsCancelled()) {
        ctx.Set(output_, std::string("cancelled"));
        return;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ctx.Set(output_, std::string("completed"));
  }
  std::string Name() const override { return "CancelAwareOp"; }

 private:
  DataToken<std::string> input_, output_;
};
REGISTER_OP(CancelAwareOp);

class StringSinkOp : public Operator {
 public:
  void Init(Registry& reg) override {
    in1_ = reg.Input<std::string>("output");
    in2_ = reg.Input<std::string>("slow_output");
    merged_ = reg.Output<std::string>("merged_string");
  }
  void Run(Context& ctx) override {
    std::string result;
    if (ctx.Has(in1_)) result += ctx.Get(in1_);
    if (ctx.Has(in2_)) {
      if (!result.empty()) result += "|";
      result += ctx.Get(in2_);
    }
    ctx.Set(merged_, std::move(result));
  }
  std::string Name() const override { return "StringSinkOp"; }

 private:
  DataToken<std::string> in1_, in2_, merged_;
};
REGISTER_OP(StringSinkOp);

// Helper: build a GraphTemplate and create a shared executor
static std::shared_ptr<GraphExecutor> MakeExecutor(
    const std::vector<NodeConfig>& configs, ThreadPool& pool,
    LogFn log = silent) {
  auto tmpl = std::make_shared<GraphTemplate>();
  tmpl->Build(configs, log);
  return std::make_shared<GraphExecutor>(tmpl, pool, log);
}

// ============================================================
// A. ThreadPool Tests
// ============================================================

TEST(ThreadPool, BasicEnqueueAndComplete) {
  std::atomic<int> counter{0};
  {
    ThreadPool pool(4);
    for (int i = 0; i < 10; ++i) {
      pool.Enqueue([&counter] { counter.fetch_add(1); });
    }
  }  // destructor joins
  EXPECT_EQ(counter.load(), 10);
}

TEST(ThreadPool, ConcurrentManyTasks) {
  std::atomic<int> counter{0};
  {
    ThreadPool pool(4);
    for (int i = 0; i < 1000; ++i) {
      pool.Enqueue([&counter] { counter.fetch_add(1); });
    }
  }
  EXPECT_EQ(counter.load(), 1000);
}

TEST(ThreadPool, DestructorJoinsAllThreads) {
  std::atomic<int> counter{0};
  {
    ThreadPool pool(4);
    for (int i = 0; i < 100; ++i) {
      pool.Enqueue([&counter] {
        std::this_thread::sleep_for(std::chrono::microseconds(100));
        counter.fetch_add(1);
      });
    }
  }
  EXPECT_EQ(counter.load(), 100);
}

TEST(ThreadPool, SingleThreadSerializesExecution) {
  std::vector<int> order;
  std::mutex mu;
  {
    ThreadPool pool(1);
    for (int i = 0; i < 10; ++i) {
      pool.Enqueue([&order, &mu, i] {
        std::lock_guard<std::mutex> lock(mu);
        order.push_back(i);
      });
    }
  }
  ASSERT_EQ(order.size(), 10u);
  for (int i = 0; i < 10; ++i) {
    EXPECT_EQ(order[i], i);
  }
}

// ============================================================
// B. Context Tests
// ============================================================

TEST(Context, SetGetRoundTripInt) {
  Registry reg;
  auto tok = reg.Output<int>("val");
  Context ctx(reg.SlotCount());
  ctx.Set(tok, int(42));
  EXPECT_EQ(ctx.Get(tok), 42);
}

TEST(Context, SetGetRoundTripString) {
  Registry reg;
  auto tok = reg.Output<std::string>("val");
  Context ctx(reg.SlotCount());
  ctx.Set(tok, std::string("hello"));
  EXPECT_EQ(ctx.Get(tok), "hello");
}

TEST(Context, SetGetRoundTripVector) {
  Registry reg;
  auto tok = reg.Output<std::vector<int>>("val");
  Context ctx(reg.SlotCount());
  ctx.Set(tok, std::vector<int>{1, 2, 3});
  EXPECT_EQ(ctx.Get(tok), (std::vector<int>{1, 2, 3}));
}

struct Point {
  int x, y;
  bool operator==(const Point& o) const { return x == o.x && y == o.y; }
};

TEST(Context, SetGetRoundTripCustomStruct) {
  Registry reg;
  auto tok = reg.Output<Point>("pt");
  Context ctx(reg.SlotCount());
  ctx.Set(tok, Point{3, 4});
  EXPECT_EQ(ctx.Get(tok), (Point{3, 4}));
}

TEST(Context, GetInvalidTokenThrows) {
  Context ctx(1);
  DataToken<int> bad{-1, "bad"};
  EXPECT_THROW(ctx.Get(bad), std::runtime_error);
}

TEST(Context, GetTypeMismatchThrows) {
  Registry reg;
  auto int_tok = reg.Output<int>("val");
  Context ctx(reg.SlotCount());
  ctx.Set(int_tok, int(42));
  DataToken<std::string> str_tok{int_tok.index, "val"};
  EXPECT_THROW(ctx.Get(str_tok), std::runtime_error);
}

TEST(Context, MoveSemantics) {
  Registry reg;
  auto tok = reg.Output<std::string>("val");
  Context ctx(reg.SlotCount());
  ctx.Set(tok, std::string("moveme"));
  std::string moved = ctx.Move(tok);
  EXPECT_EQ(moved, "moveme");
  // After std::any_cast with move, the any is in a valid-but-unspecified state.
  // The value has been extracted — a second Move or Get would fail or return
  // a moved-from value. We verify the extraction succeeded above.
}

TEST(Context, MoveInvalidTokenThrows) {
  Context ctx(1);
  DataToken<int> bad{-1, "bad"};
  EXPECT_THROW(ctx.Move(bad), std::runtime_error);
}

TEST(Context, HasReturnsFalseForUnset) {
  Registry reg;
  auto tok = reg.Output<int>("val");
  Context ctx(reg.SlotCount());
  EXPECT_FALSE(ctx.Has(tok));
  ctx.Set(tok, int(1));
  EXPECT_TRUE(ctx.Has(tok));
}

TEST(Context, HasReturnsFalseForInvalidToken) {
  Context ctx(1);
  DataToken<int> bad{-1, "bad"};
  EXPECT_FALSE(ctx.Has(bad));
}

TEST(Context, CancelFlagPropagation) {
  Context ctx(0);
  EXPECT_FALSE(ctx.IsCancelled());
  std::atomic<bool> flag{false};
  ctx.SetCancelFlag(&flag);
  EXPECT_FALSE(ctx.IsCancelled());
  flag.store(true, std::memory_order_release);
  EXPECT_TRUE(ctx.IsCancelled());
}

// ============================================================
// C. Registry Tests
// ============================================================

TEST(Registry, InputOutputRegisterSlots) {
  Registry reg;
  reg.SetCurrentNode(0);
  auto in = reg.Input<int>("x");
  auto out = reg.Output<std::string>("y");
  EXPECT_TRUE(in.IsValid());
  EXPECT_TRUE(out.IsValid());
  EXPECT_NE(in.index, out.index);
  EXPECT_EQ(reg.SlotCount(), 2u);
}

TEST(Registry, SameNameSameTypeReturnsSameToken) {
  Registry reg;
  reg.SetCurrentNode(0);
  auto t1 = reg.Input<int>("x");
  reg.SetCurrentNode(1);
  auto t2 = reg.Output<int>("x");
  EXPECT_EQ(t1.index, t2.index);
  EXPECT_EQ(reg.SlotCount(), 1u);
}

TEST(Registry, SameNameDifferentTypeThrows) {
  Registry reg;
  reg.SetCurrentNode(0);
  reg.Input<int>("x");
  reg.SetCurrentNode(1);
  EXPECT_THROW(reg.Output<std::string>("x"), std::runtime_error);
}

TEST(Registry, LookupAfterRegistration) {
  Registry reg;
  reg.SetCurrentNode(0);
  reg.Output<int>("x");
  auto tok = reg.Lookup<int>("x");
  EXPECT_TRUE(tok.IsValid());
  EXPECT_EQ(tok.index, 0);
}

TEST(Registry, LookupUnregisteredReturnsInvalid) {
  Registry reg;
  auto tok = reg.Lookup<int>("nonexistent");
  EXPECT_FALSE(tok.IsValid());
  EXPECT_EQ(tok.index, -1);
}

TEST(Registry, LookupTypeMismatchThrows) {
  Registry reg;
  reg.SetCurrentNode(0);
  reg.Output<int>("x");
  EXPECT_THROW(reg.Lookup<std::string>("x"), std::runtime_error);
}

TEST(Registry, ProducersConsumersTracking) {
  Registry reg;
  reg.SetCurrentNode(0);
  auto tok = reg.Output<int>("x");
  reg.SetCurrentNode(1);
  reg.Input<int>("x");

  const auto& producers = reg.Producers();
  const auto& consumers = reg.Consumers();
  EXPECT_EQ(producers.at(tok.index).count(0), 1u);
  EXPECT_EQ(consumers.at(tok.index).count(1), 1u);
}

TEST(Registry, InputSlots) {
  Registry reg;
  reg.SetCurrentNode(0);
  reg.Input<int>("external");  // consumed but not produced
  reg.Output<int>("internal");
  auto inputs = reg.InputSlots();
  auto ext_tok = reg.Lookup<int>("external");
  EXPECT_EQ(inputs.count(ext_tok.index), 1u);
}

TEST(Registry, OutputSlots) {
  Registry reg;
  reg.SetCurrentNode(0);
  reg.Output<int>("orphan");  // produced but not consumed
  reg.Input<int>("external");
  auto outputs = reg.OutputSlots();
  auto orph_tok = reg.Lookup<int>("orphan");
  EXPECT_EQ(outputs.count(orph_tok.index), 1u);
}

// ============================================================
// D. ConfigNode Tests
// ============================================================

TEST(ConfigNode, DefaultIsNull) {
  ConfigNode cn;
  EXPECT_TRUE(cn.IsNull());
  EXPECT_EQ(cn.size(), 0u);
}

TEST(ConfigNode, ScalarAsInt) {
  ConfigNode cn("42");
  EXPECT_EQ(cn.As<int>(), 42);
}

TEST(ConfigNode, ScalarAsString) {
  ConfigNode cn("hello");
  EXPECT_EQ(cn.As<std::string>(), "hello");
}

TEST(ConfigNode, ScalarAsBoolTrue) {
  EXPECT_TRUE(ConfigNode("true").As<bool>());
  EXPECT_TRUE(ConfigNode("1").As<bool>());
}

TEST(ConfigNode, ScalarAsBoolFalse) {
  EXPECT_FALSE(ConfigNode("false").As<bool>());
  EXPECT_FALSE(ConfigNode("0").As<bool>());
}

TEST(ConfigNode, ScalarAsFloat) {
  ConfigNode cn("3.14");
  EXPECT_NEAR(cn.As<float>(), 3.14f, 0.001f);
}

TEST(ConfigNode, AsWithDefaultOnNull) {
  ConfigNode cn;
  EXPECT_EQ(cn.As<int>(99), 99);
  EXPECT_EQ(cn.As<std::string>("fallback"), "fallback");
}

TEST(ConfigNode, AsRequiredOnNullThrows) {
  ConfigNode cn;
  EXPECT_THROW(cn.AsRequired<int>(), std::runtime_error);
}

TEST(ConfigNode, AsRequiredOnScalar) {
  ConfigNode cn("7");
  EXPECT_EQ(cn.AsRequired<int>(), 7);
}

TEST(ConfigNode, MapAccess) {
  ConfigNode cn;
  cn.AddMapItem("key").SetScalar("value");
  EXPECT_EQ(cn["key"].As<std::string>(), "value");
  EXPECT_TRUE(cn["missing"].IsNull());
}

TEST(ConfigNode, MapNestedAccess) {
  ConfigNode cn;
  cn.AddMapItem("outer").AddMapItem("inner").SetScalar("deep");
  EXPECT_EQ(cn["outer"]["inner"].As<std::string>(), "deep");
}

TEST(ConfigNode, SequenceAccess) {
  ConfigNode cn;
  cn.AddSequenceItem().SetScalar("a");
  cn.AddSequenceItem().SetScalar("b");
  EXPECT_EQ(cn[0].As<std::string>(), "a");
  EXPECT_EQ(cn[1].As<std::string>(), "b");
  EXPECT_TRUE(cn[99].IsNull());
}

TEST(ConfigNode, SequenceIteration) {
  ConfigNode cn;
  cn.AddSequenceItem().SetScalar("x");
  cn.AddSequenceItem().SetScalar("y");
  cn.AddSequenceItem().SetScalar("z");
  std::vector<std::string> vals;
  for (const auto& item : cn) {
    vals.push_back(item.As<std::string>());
  }
  EXPECT_EQ(vals, (std::vector<std::string>{"x", "y", "z"}));
}

TEST(ConfigNode, SubscriptOnWrongTypeReturnsNull) {
  ConfigNode scalar("value");
  EXPECT_TRUE(scalar["key"].IsNull());
  EXPECT_TRUE(scalar[0].IsNull());
}

// ============================================================
// E. OpFactory Tests
// ============================================================

TEST(OpFactory, CreateRegisteredOpSucceeds) {
  auto op = OpFactory::Get().Create("AddOp");
  ASSERT_NE(op, nullptr);
  EXPECT_EQ(op->Name(), "AddOp");
}

TEST(OpFactory, CreateUnregisteredOpThrows) {
  EXPECT_THROW(OpFactory::Get().Create("NoSuchOp"), std::runtime_error);
}

TEST(OpFactory, MultipleRegisteredOpsAccessible) {
  std::vector<std::string> names = {"AddOp",       "PassthroughOp",
                                    "FailOp",      "SlowOp",
                                    "ProducerOp",  "IntProducerOp",
                                    "StringSinkOp"};
  for (const auto& name : names) {
    auto op = OpFactory::Get().Create(name);
    ASSERT_NE(op, nullptr) << "Failed to create: " << name;
    EXPECT_EQ(op->Name(), name);
  }
}

// ============================================================
// F. GraphTemplate Tests
// ============================================================

TEST(GraphTemplate, LinearDagBuildsCorrectly) {
  // ProducerOp -> PassthroughOp
  std::vector<NodeConfig> configs = {
      {"producer", "ProducerOp", {}, {}, false, 0},
      {"passthrough", "PassthroughOp", {"producer"}, {}, false, 0},
  };
  GraphTemplate tmpl;
  tmpl.Build(configs, silent);
  const auto& nodes = tmpl.Nodes();
  ASSERT_EQ(nodes.size(), 2u);

  // producer: no parents, one child
  EXPECT_EQ(nodes[0].initial_indegree, 0);
  EXPECT_EQ(nodes[0].children.size(), 1u);
  EXPECT_EQ(nodes[0].parents.size(), 0u);

  // passthrough: one parent, no children
  EXPECT_EQ(nodes[1].initial_indegree, 1);
  EXPECT_EQ(nodes[1].children.size(), 0u);
  EXPECT_EQ(nodes[1].parents.size(), 1u);
}

TEST(GraphTemplate, DiamondDagBuildsCorrectly) {
  // ProducerOp -> PassthroughOp \
  //            -> SlowOp        -> StringSinkOp
  std::vector<NodeConfig> configs = {
      {"producer", "ProducerOp", {}, {}, false, 0},
      {"passthrough", "PassthroughOp", {"producer"}, {}, false, 0},
      {"slow", "SlowOp", {"producer"}, {}, false, 0},
      {"sink", "StringSinkOp", {"passthrough", "slow"}, {}, false, 0},
  };
  // Configure SlowOp with minimal sleep
  configs[2].params.AddMapItem("sleep_ms").SetScalar("1");

  GraphTemplate tmpl;
  tmpl.Build(configs, silent);
  const auto& nodes = tmpl.Nodes();
  ASSERT_EQ(nodes.size(), 4u);

  // producer fans out to 2 children
  EXPECT_EQ(nodes[0].children.size(), 2u);
  EXPECT_EQ(nodes[0].initial_indegree, 0);

  // sink has 2 parents
  EXPECT_EQ(nodes[3].parents.size(), 2u);
  EXPECT_EQ(nodes[3].initial_indegree, 2);
}

TEST(GraphTemplate, CycleDetectionThrows) {
  // A depends on B, B depends on A — but we can't express this directly
  // since the dependency must refer to existing node ids.
  // Instead: A -> B -> C -> A  (using ops that don't cause data-flow issues)
  // We need a simpler approach: use ConfigEchoOp which has no inputs.
  // Actually, cycle detection is purely topological—data flow is checked later.
  // We need mutual dependencies. Let's use ProducerOp for all nodes since
  // cycle detection runs before data-flow validation.

  // Trick: register a no-input/no-output op for cycle testing
  // Use ConfigEchoOp which has output only, no input.
  std::vector<NodeConfig> configs = {
      {"A", "ConfigEchoOp", {"B"}, {}, false, 0},
      {"B", "ConfigEchoOp", {"A"}, {}, false, 0},
  };
  GraphTemplate tmpl;
  EXPECT_THROW(tmpl.Build(configs, silent), std::runtime_error);
}

TEST(GraphTemplate, UnknownDependencyThrows) {
  std::vector<NodeConfig> configs = {
      {"A", "ProducerOp", {"ghost"}, {}, false, 0},
  };
  GraphTemplate tmpl;
  EXPECT_THROW(tmpl.Build(configs, silent), std::runtime_error);
}

TEST(GraphTemplate, DataFlowNoUpstreamProducerThrows) {
  // PassthroughOp reads "input" but IntProducerOp produces "a" and "b".
  // No dependency edge from producer to passthrough, so data flow fails.
  std::vector<NodeConfig> configs = {
      {"producer", "IntProducerOp", {}, {}, false, 0},
      {"consumer", "PassthroughOp", {}, {}, false, 0},
  };
  // "input" is consumed by PassthroughOp but produced by no one with an
  // upstream dependency edge, and also not produced by anyone at all here.
  // Actually, ProducerOp produces "input" — let's use that but skip the dep edge.
  // consumer reads "input", producer (ProducerOp) produces "input" but no dep edge.
  std::vector<NodeConfig> configs2 = {
      {"producer", "ProducerOp", {}, {}, false, 0},
      {"consumer", "PassthroughOp", {}, {}, false, 0},
  };
  GraphTemplate tmpl;
  EXPECT_THROW(tmpl.Build(configs2, silent), std::runtime_error);
}

TEST(GraphTemplate, TokenLookupPostBuild) {
  std::vector<NodeConfig> configs = {
      {"producer", "IntProducerOp", {}, {}, false, 0},
      {"adder", "AddOp", {"producer"}, {}, false, 0},
  };
  GraphTemplate tmpl;
  tmpl.Build(configs, silent);
  auto tok = tmpl.Token<int>("sum");
  EXPECT_TRUE(tok.IsValid());
}

TEST(GraphTemplate, TokenLookupUnknownReturnsInvalid) {
  std::vector<NodeConfig> configs = {
      {"producer", "IntProducerOp", {}, {}, false, 0},
  };
  GraphTemplate tmpl;
  tmpl.Build(configs, silent);
  auto tok = tmpl.Token<int>("nonexistent");
  EXPECT_FALSE(tok.IsValid());
}

TEST(GraphTemplate, TokenLookupTypeMismatchThrows) {
  std::vector<NodeConfig> configs = {
      {"producer", "IntProducerOp", {}, {}, false, 0},
  };
  GraphTemplate tmpl;
  tmpl.Build(configs, silent);
  EXPECT_THROW(tmpl.Token<std::string>("a"), std::runtime_error);
}

// ============================================================
// G. GraphExecutor Tests
// ============================================================

class GraphExecutorTest : public ::testing::Test {
 protected:
  ThreadPool pool_{4};
};

TEST_F(GraphExecutorTest, LinearDagExecutesCorrectResult) {
  // IntProducerOp(3,7) -> AddOp -> sum == 10
  std::vector<NodeConfig> configs = {
      {"producer", "IntProducerOp", {}, {}, false, 0},
      {"adder", "AddOp", {"producer"}, {}, false, 0},
  };
  configs[0].params.AddMapItem("a").SetScalar("3");
  configs[0].params.AddMapItem("b").SetScalar("7");

  auto exec = MakeExecutor(configs, pool_);
  auto future = exec->Run();
  future.get();

  auto tok = exec->Template().Token<int>("sum");
  EXPECT_EQ(exec->Ctx().Get(tok), 10);
}

TEST_F(GraphExecutorTest, DiamondDagExecutesCorrectResult) {
  // ProducerOp -> PassthroughOp \
  //           -> SlowOp(1ms)   -> StringSinkOp -> "produced_value|slow_done"
  std::vector<NodeConfig> configs = {
      {"producer", "ProducerOp", {}, {}, false, 0},
      {"passthrough", "PassthroughOp", {"producer"}, {}, false, 0},
      {"slow", "SlowOp", {"producer"}, {}, false, 0},
      {"sink", "StringSinkOp", {"passthrough", "slow"}, {}, false, 0},
  };
  configs[2].params.AddMapItem("sleep_ms").SetScalar("1");

  auto exec = MakeExecutor(configs, pool_);
  auto future = exec->Run();
  future.get();

  auto tok = exec->Template().Token<std::string>("merged_string");
  auto result = exec->Ctx().Get(tok);
  // Both branches produce values; order may vary but both present
  EXPECT_TRUE(result.find("produced_value") != std::string::npos);
  EXPECT_TRUE(result.find("slow_done") != std::string::npos);
}

TEST_F(GraphExecutorTest, OptionalNodeFailureSkipsAndCompletes) {
  // ProducerOp -> FailOp (optional)
  std::vector<NodeConfig> configs = {
      {"producer", "ProducerOp", {}, {}, false, 0},
      {"failer", "FailOp", {"producer"}, {}, true, 0},  // optional=true
  };
  auto exec = MakeExecutor(configs, pool_);
  auto future = exec->Run();
  EXPECT_NO_THROW(future.get());

  // Check metrics
  bool found_skipped = false;
  for (const auto& m : exec->Metrics()) {
    if (m.name == "failer") {
      EXPECT_TRUE(m.skipped);
      found_skipped = true;
    }
  }
  EXPECT_TRUE(found_skipped);
}

TEST_F(GraphExecutorTest, RequiredNodeFailureCancelsGraph) {
  // ProducerOp -> FailOp (required, default)
  std::vector<NodeConfig> configs = {
      {"producer", "ProducerOp", {}, {}, false, 0},
      {"failer", "FailOp", {"producer"}, {}, false, 0},  // required
  };
  auto exec = MakeExecutor(configs, pool_);
  auto future = exec->Run();
  EXPECT_THROW(future.get(), std::exception);
}

TEST_F(GraphExecutorTest, CancelMidFlight) {
  // ProducerOp -> CancelAwareOp (will loop 100ms)
  std::vector<NodeConfig> configs = {
      {"producer", "ProducerOp", {}, {}, false, 0},
      {"cancel_aware", "CancelAwareOp", {"producer"}, {}, false, 0},
  };
  auto exec = MakeExecutor(configs, pool_);
  auto future = exec->Run();
  std::this_thread::sleep_for(std::chrono::milliseconds(5));
  exec->Cancel();
  // Cancel resolves via TrySetValue, no exception
  EXPECT_NO_THROW(future.get());
}

TEST_F(GraphExecutorTest, MetricsPopulated) {
  std::vector<NodeConfig> configs = {
      {"producer", "IntProducerOp", {}, {}, false, 0},
      {"adder", "AddOp", {"producer"}, {}, false, 0},
  };
  auto exec = MakeExecutor(configs, pool_);
  auto future = exec->Run();
  future.get();

  const auto& metrics = exec->Metrics();
  ASSERT_EQ(metrics.size(), 2u);
  EXPECT_EQ(metrics[0].name, "producer");
  EXPECT_EQ(metrics[1].name, "adder");
  EXPECT_GE(metrics[0].duration_us, 0);
  EXPECT_GE(metrics[1].duration_us, 0);
  EXPECT_FALSE(metrics[0].skipped);
  EXPECT_FALSE(metrics[0].timed_out);
}

TEST_F(GraphExecutorTest, TimeoutOptionalNodeMarkedTimedOut) {
  // SlowOp sleeps 100ms, timeout_ms=1 → timed_out=true
  std::vector<NodeConfig> configs = {
      {"producer", "ProducerOp", {}, {}, false, 0},
      {"slow", "SlowOp", {"producer"}, {}, true, 1},  // optional, timeout=1ms
  };
  configs[1].params.AddMapItem("sleep_ms").SetScalar("100");

  auto exec = MakeExecutor(configs, pool_);
  auto future = exec->Run();
  EXPECT_NO_THROW(future.get());

  bool found = false;
  for (const auto& m : exec->Metrics()) {
    if (m.name == "slow") {
      EXPECT_TRUE(m.timed_out);
      found = true;
    }
  }
  EXPECT_TRUE(found);
}

TEST_F(GraphExecutorTest, TimeoutRequiredNodeThrows) {
  std::vector<NodeConfig> configs = {
      {"producer", "ProducerOp", {}, {}, false, 0},
      {"slow", "SlowOp", {"producer"}, {}, false, 1},  // required, timeout=1ms
  };
  configs[1].params.AddMapItem("sleep_ms").SetScalar("100");

  auto exec = MakeExecutor(configs, pool_);
  auto future = exec->Run();
  EXPECT_THROW(future.get(), std::exception);
}

TEST_F(GraphExecutorTest, TemplateReturnsSameTemplate) {
  std::vector<NodeConfig> configs = {
      {"producer", "ProducerOp", {}, {}, false, 0},
  };
  auto tmpl = std::make_shared<GraphTemplate>();
  tmpl->Build(configs, silent);
  auto exec = std::make_shared<GraphExecutor>(tmpl, pool_, silent);
  EXPECT_EQ(&exec->Template(), tmpl.get());
}

TEST_F(GraphExecutorTest, ExternalInputInjection) {
  // PassthroughOp reads "input", we inject it externally (no ProducerOp)
  std::vector<NodeConfig> configs = {
      {"pt", "PassthroughOp", {}, {}, false, 0},
  };
  auto tmpl = std::make_shared<GraphTemplate>();
  tmpl->Build(configs, silent);

  auto input_tok = tmpl->Token<std::string>("input");
  ASSERT_TRUE(input_tok.IsValid());

  auto exec = std::make_shared<GraphExecutor>(tmpl, pool_, silent);
  exec->Ctx().Set(input_tok, std::string("injected"));

  auto future = exec->Run();
  future.get();

  auto output_tok = tmpl->Token<std::string>("output");
  EXPECT_EQ(exec->Ctx().Get(output_tok), "injected");
}

// ============================================================
// H. DagManager Tests
// ============================================================

TEST(DagManager, BuildAndHasAndDagRoundTrip) {
  DagManager mgr(2, silent);
  std::vector<NodeConfig> configs = {
      {"producer", "ProducerOp", {}, {}, false, 0},
  };
  mgr.BuildDag("test", configs);
  EXPECT_TRUE(mgr.HasDag("test"));
  auto tmpl = mgr.Dag("test");
  ASSERT_NE(tmpl, nullptr);
  EXPECT_EQ(tmpl->Nodes().size(), 1u);
}

TEST(DagManager, BuildDagDuplicateNameThrows) {
  DagManager mgr(2, silent);
  std::vector<NodeConfig> configs = {
      {"producer", "ProducerOp", {}, {}, false, 0},
  };
  mgr.BuildDag("dup", configs);
  EXPECT_THROW(mgr.BuildDag("dup", configs), std::runtime_error);
}

TEST(DagManager, CreateExecutorUnknownNameThrows) {
  DagManager mgr(2, silent);
  EXPECT_THROW(mgr.CreateExecutor("ghost"), std::runtime_error);
}

TEST(DagManager, DagUnknownNameThrows) {
  DagManager mgr(2, silent);
  EXPECT_THROW(mgr.Dag("ghost"), std::runtime_error);
}

TEST(DagManager, HasDagReturnsFalseForUnknown) {
  DagManager mgr(2, silent);
  EXPECT_FALSE(mgr.HasDag("nope"));
}

TEST(DagManager, ListDagsReturnsAllRegistered) {
  DagManager mgr(2, silent);
  mgr.BuildDag("alpha", {{"p", "ProducerOp", {}, {}, false, 0}});
  mgr.BuildDag("beta", {{"p", "ProducerOp", {}, {}, false, 0}});
  auto names = mgr.ListDags();
  std::set<std::string> name_set(names.begin(), names.end());
  EXPECT_EQ(name_set, (std::set<std::string>{"alpha", "beta"}));
}

TEST(DagManager, ReplaceDagSwapsTemplate) {
  DagManager mgr(2, silent);
  // Original: produces a=1, b=2 -> sum=3
  std::vector<NodeConfig> v1 = {
      {"producer", "IntProducerOp", {}, {}, false, 0},
      {"adder", "AddOp", {"producer"}, {}, false, 0},
  };
  v1[0].params.AddMapItem("a").SetScalar("1");
  v1[0].params.AddMapItem("b").SetScalar("2");
  mgr.BuildDag("math", v1);

  auto exec1 = mgr.CreateExecutor("math");
  exec1->Run().get();
  EXPECT_EQ(exec1->Ctx().Get(exec1->Template().Token<int>("sum")), 3);

  // Replace: produces a=10, b=20 -> sum=30
  std::vector<NodeConfig> v2 = {
      {"producer", "IntProducerOp", {}, {}, false, 0},
      {"adder", "AddOp", {"producer"}, {}, false, 0},
  };
  v2[0].params.AddMapItem("a").SetScalar("10");
  v2[0].params.AddMapItem("b").SetScalar("20");
  mgr.ReplaceDag("math", v2);

  auto exec2 = mgr.CreateExecutor("math");
  exec2->Run().get();
  EXPECT_EQ(exec2->Ctx().Get(exec2->Template().Token<int>("sum")), 30);
}

TEST(DagManager, ReplaceDagUnknownNameThrows) {
  DagManager mgr(2, silent);
  EXPECT_THROW(
      mgr.ReplaceDag("nonexistent",
                      {{"p", "ProducerOp", {}, {}, false, 0}}),
      std::runtime_error);
}

TEST(DagManager, HotReloadOldExecutorKeepsOldTemplate) {
  DagManager mgr(2, silent);

  std::vector<NodeConfig> v1 = {
      {"producer", "IntProducerOp", {}, {}, false, 0},
      {"adder", "AddOp", {"producer"}, {}, false, 0},
  };
  v1[0].params.AddMapItem("a").SetScalar("5");
  v1[0].params.AddMapItem("b").SetScalar("5");
  mgr.BuildDag("hr", v1);

  // Create executor before replace
  auto old_exec = mgr.CreateExecutor("hr");

  // Replace with new config
  std::vector<NodeConfig> v2 = {
      {"producer", "IntProducerOp", {}, {}, false, 0},
      {"adder", "AddOp", {"producer"}, {}, false, 0},
  };
  v2[0].params.AddMapItem("a").SetScalar("100");
  v2[0].params.AddMapItem("b").SetScalar("200");
  mgr.ReplaceDag("hr", v2);

  // Old executor still runs with old template
  old_exec->Run().get();
  EXPECT_EQ(
      old_exec->Ctx().Get(old_exec->Template().Token<int>("sum")),
      10);

  // New executor gets new result
  auto new_exec = mgr.CreateExecutor("hr");
  new_exec->Run().get();
  EXPECT_EQ(
      new_exec->Ctx().Get(new_exec->Template().Token<int>("sum")),
      300);
}

// ============================================================
// I. Integration Tests
// ============================================================

TEST(Integration, MultiOperatorPipelineEndToEnd) {
  ThreadPool pool(4);
  // Full diamond: ProducerOp -> PassthroughOp + SlowOp -> StringSinkOp
  std::vector<NodeConfig> configs = {
      {"producer", "ProducerOp", {}, {}, false, 0},
      {"passthrough", "PassthroughOp", {"producer"}, {}, false, 0},
      {"slow", "SlowOp", {"producer"}, {}, false, 0},
      {"sink", "StringSinkOp", {"passthrough", "slow"}, {}, false, 0},
  };
  configs[2].params.AddMapItem("sleep_ms").SetScalar("1");

  auto exec = MakeExecutor(configs, pool);
  auto future = exec->Run();
  future.get();

  auto tok = exec->Template().Token<std::string>("merged_string");
  auto result = exec->Ctx().Get(tok);
  EXPECT_FALSE(result.empty());
  EXPECT_TRUE(result.find("produced_value") != std::string::npos);
  EXPECT_TRUE(result.find("slow_done") != std::string::npos);
}

TEST(Integration, MultipleDAGsInSameDagManager) {
  DagManager mgr(4, silent);

  // DAG 1: IntProducer(2,3) -> Add -> sum=5
  std::vector<NodeConfig> d1 = {
      {"producer", "IntProducerOp", {}, {}, false, 0},
      {"adder", "AddOp", {"producer"}, {}, false, 0},
  };
  d1[0].params.AddMapItem("a").SetScalar("2");
  d1[0].params.AddMapItem("b").SetScalar("3");
  mgr.BuildDag("dag1", d1);

  // DAG 2: IntProducer(100,200) -> Add -> sum=300
  std::vector<NodeConfig> d2 = {
      {"producer", "IntProducerOp", {}, {}, false, 0},
      {"adder", "AddOp", {"producer"}, {}, false, 0},
  };
  d2[0].params.AddMapItem("a").SetScalar("100");
  d2[0].params.AddMapItem("b").SetScalar("200");
  mgr.BuildDag("dag2", d2);

  // Run concurrently
  auto e1 = mgr.CreateExecutor("dag1");
  auto e2 = mgr.CreateExecutor("dag2");
  auto f1 = e1->Run();
  auto f2 = e2->Run();
  f1.get();
  f2.get();

  EXPECT_EQ(e1->Ctx().Get(e1->Template().Token<int>("sum")), 5);
  EXPECT_EQ(e2->Ctx().Get(e2->Template().Token<int>("sum")), 300);
}

// ============================================================
// J. Concurrency Stress Tests
// ============================================================

TEST(ConcurrencyStress, ConcurrentExecutions) {
  DagManager mgr(4, silent);
  std::vector<NodeConfig> configs = {
      {"producer", "IntProducerOp", {}, {}, false, 0},
      {"adder", "AddOp", {"producer"}, {}, false, 0},
  };
  configs[0].params.AddMapItem("a").SetScalar("7");
  configs[0].params.AddMapItem("b").SetScalar("3");
  mgr.BuildDag("stress", configs);

  std::atomic<int> success{0};
  std::vector<std::thread> threads;
  for (int i = 0; i < 50; ++i) {
    threads.emplace_back([&mgr, &success] {
      auto exec = mgr.CreateExecutor("stress");
      auto future = exec->Run();
      future.get();
      auto tok = exec->Template().Token<int>("sum");
      if (exec->Ctx().Get(tok) == 10) {
        success.fetch_add(1);
      }
    });
  }
  for (auto& t : threads) t.join();
  EXPECT_EQ(success.load(), 50);
}

TEST(ConcurrencyStress, ConcurrentCreateAndReplace) {
  DagManager mgr(4, silent);
  std::vector<NodeConfig> v1 = {
      {"producer", "IntProducerOp", {}, {}, false, 0},
      {"adder", "AddOp", {"producer"}, {}, false, 0},
  };
  v1[0].params.AddMapItem("a").SetScalar("1");
  v1[0].params.AddMapItem("b").SetScalar("1");
  mgr.BuildDag("live", v1);

  std::atomic<bool> done{false};
  std::atomic<int> reads_ok{0};

  // Reader threads: create executors and run them
  std::vector<std::thread> readers;
  for (int i = 0; i < 10; ++i) {
    readers.emplace_back([&mgr, &done, &reads_ok] {
      while (!done.load(std::memory_order_acquire)) {
        try {
          auto exec = mgr.CreateExecutor("live");
          auto future = exec->Run();
          future.get();
          auto tok = exec->Template().Token<int>("sum");
          int val = exec->Ctx().Get(tok);
          // Value should be either 2 (original) or 1000 (replaced)
          if (val == 2 || val == 1000) {
            reads_ok.fetch_add(1);
          }
        } catch (...) {
          // Acceptable during replace transitions
        }
      }
    });
  }

  // Writer thread: replace the DAG a few times
  std::thread writer([&mgr, &done] {
    for (int j = 0; j < 5; ++j) {
      std::vector<NodeConfig> v = {
          {"producer", "IntProducerOp", {}, {}, false, 0},
          {"adder", "AddOp", {"producer"}, {}, false, 0},
      };
      v[0].params.AddMapItem("a").SetScalar("500");
      v[0].params.AddMapItem("b").SetScalar("500");
      try {
        mgr.ReplaceDag("live", v);
      } catch (...) {
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    done.store(true, std::memory_order_release);
  });

  writer.join();
  for (auto& r : readers) r.join();

  // At least some reads should have succeeded
  EXPECT_GT(reads_ok.load(), 0);
}
