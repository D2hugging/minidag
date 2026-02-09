#pragma once

#include <algorithm>
#include <chrono>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "../include/mini_dag.hpp"
#include "data_types.hpp"

using namespace minidag;

// Helper: simulate latency
inline void SleepMs(int ms) {
  std::this_thread::sleep_for(std::chrono::milliseconds(ms));
}

// ==========================
// Operator Definitions
// ==========================

// 1. Query parsing operator
class QueryParseOp : public Operator {
 public:
  void Init(Registry& reg) override {
    input_ = reg.Input<UserRequest>("request");
    output_ = reg.Output<std::string>("parsed_query");
    blacklist_ = reg.Output<std::vector<int>>("blacklist_ids");
  }

  void Run(Context& ctx) override {
    const auto& req = ctx.Get(input_);

    std::string parsed = "KeyWord[" + req.query + "]";
    ctx.Set(output_, std::move(parsed));
    ctx.Set(blacklist_, std::vector<int>{104, 105});  // example blacklist

    std::cout << "[" << Name() << "]User: " << req.uid
              << ", Query: " << req.query
              << ", Parsed: " << ctx.Get<std::string>(output_) << '\n';
  }

  std::string Name() const override { return "QueryParse"; }

 private:
  DataToken<UserRequest> input_;
  DataToken<std::string> output_;
  DataToken<std::vector<int>> blacklist_;
};

REGISTER_OP(QueryParseOp);

// 2. Vector recall operator (I/O intensive)
class VectorRecallOp : public Operator {
 public:
  void Configure(const ConfigNode& conf) override {
    top_k_ = conf["top_k"].As<int>(15);
    timeout_ms_ = conf["timeout_ms"].As<int>(100);
    model_name_ = conf["model_name"].As<std::string>("default_vec_model");
    enable_filter_ = conf["enable_filter"].As<bool>(false);

    std::cout << "[" << Name() << "]"
              << "\n top_k = " << top_k_ << "\n timeout_ms=" << timeout_ms_
              << "\n model_name=" << model_name_
              << "\n enable_filter=" << (enable_filter_ ? "true" : "false")
              << '\n';
  }

  void Init(Registry& reg) override {
    input_ = reg.Input<std::string>("parsed_query");
    output_ = reg.Output<RecallResult>("vec_items");

    if (enable_filter_) {
      blacklist_ = reg.Input<std::vector<int>>("blacklist_ids");
    }
  }

  void Run(Context& ctx) override {
    const auto& query = ctx.Get(input_);
    // Simulate vector database query
    SleepMs(timeout_ms_);
    RecallResult res;
    for (int i = 0; i < top_k_; ++i) {
      if (enable_filter_) {
        const auto& blist = ctx.Get(blacklist_);
        if (std::find(blist.begin(), blist.end(), i + 100) != blist.end()) {
          continue;  // filtered out
        }
      }
      res.items.emplace_back(i + 100, 0.5f + i * 0.01f);
    }
    std::cout << "[" << Name() << "] Found " << res.items.size()
              << " items for query: " << query << '\n';

    res.source = "VectorDB";
    ctx.Set(output_, std::move(res));
  }

  std::string Name() const override { return "VectorRecall"; }

 private:
  DataToken<std::string> input_;
  DataToken<RecallResult> output_;
  DataToken<std::vector<int>> blacklist_;

  // Configuration parameters
  int top_k_ = 10;
  int timeout_ms_ = 30;
  std::string model_name_ = "default_vec_model";
  bool enable_filter_ = false;
};
REGISTER_OP(VectorRecallOp);

// 3. Inverted-index recall operator (I/O intensive)
class IndexRecallOp : public Operator {
 public:
  void Init(Registry& reg) override {
    input_ = reg.Input<std::string>("parsed_query");
    output_ = reg.Output<RecallResult>("index_items");
  }
  void Run(Context& ctx) override {
    SleepMs(15);
    RecallResult res;
    res.source = "ElasticSearch";
    res.items = {{102, 0.6f}, {103, 0.9f}};  // note: 102 may duplicate
    ctx.Set(output_, std::move(res));

    std::cout << "[" << Name() << "]Found 2 items\n";
  }

  std::string Name() const override { return "IndexRecall"; }

 private:
  DataToken<std::string> input_;
  DataToken<RecallResult> output_;
};
REGISTER_OP(IndexRecallOp);

// 4. Hot-item recall operator (I/O intensive)
class HotRecallOp : public Operator {
 public:
  void Init(Registry& reg) override {
    input_ = reg.Input<std::string>("parsed_query");
    output_ = reg.Output<RecallResult>("hot_items");
  }
  void Run(Context& ctx) override {
    SleepMs(35);
    RecallResult res;
    res.source = "RedisHot";
    res.items = {{112, 0.8f}, {113, 0.9f}, {114, 0.7f}};
    ctx.Set(output_, std::move(res));

    std::cout << "[" << Name() << "] Found 3 items\n";
  }

  std::string Name() const override { return "HotRecall"; }

 private:
  DataToken<std::string> input_;
  DataToken<RecallResult> output_;
};

REGISTER_OP(HotRecallOp);

// 5. Merge operator (CPU intensive)
class MergeOp : public Operator {
 public:
  void Init(Registry& reg) override {
    in_vec_ = reg.Input<RecallResult>("vec_items");
    in_idx_ = reg.Input<RecallResult>("index_items");
    in_hot_ = reg.Input<RecallResult>("hot_items");
    output_ = reg.Output<std::vector<Item>>("merged_list");
  }

  void Run(Context& ctx) override {
    std::vector<Item> merged;
    if (ctx.Has(in_vec_)) {
      const auto& r1 = ctx.Get(in_vec_);
      merged.insert(merged.end(), r1.items.begin(), r1.items.end());
    }
    if (ctx.Has(in_idx_)) {
      const auto& r2 = ctx.Get(in_idx_);
      merged.insert(merged.end(), r2.items.begin(), r2.items.end());
    }
    if (ctx.Has(in_hot_)) {
      const auto& r3 = ctx.Get(in_hot_);
      merged.insert(merged.end(), r3.items.begin(), r3.items.end());
    }
    if (merged.empty()) {
      throw std::runtime_error("All recall sources failed — nothing to merge");
    }
    // Simple deduplication
    std::sort(merged.begin(), merged.end(),
              [](const Item& a, const Item& b) { return a.id < b.id; });
    auto last =
        std::unique(merged.begin(), merged.end(),
                    [](const Item& a, const Item& b) { return a.id == b.id; });
    merged.erase(last, merged.end());

    size_t count = merged.size();
    ctx.Set(output_, std::move(merged));
    std::cout << "[Merge] Merged total " << count << " unique items\n";
  }
  std::string Name() const override { return "Merge"; }

 private:
  DataToken<RecallResult> in_vec_, in_idx_, in_hot_;
  DataToken<std::vector<Item>> output_;
};

REGISTER_OP(MergeOp);

// 6. Ranking operator (deep learning model)
class RankOp : public Operator {
 public:
  void Init(Registry& reg) override {
    input_ = reg.Input<std::vector<Item>>("merged_list");
    output_ = reg.Output<RankResult>("ranked_list");
  }
  void Run(Context& ctx) override {
    auto items = ctx.Get(input_);  // Copy
    // Simulate ranking model scoring
    SleepMs(30);
    for (auto& item : items) item.score += 0.1f;  // simulate score boost

    // Sort by score descending
    std::sort(items.begin(), items.end(),
              [](const Item& a, const Item& b) { return a.score > b.score; });

    RankResult res;
    res.final_items = items;
    ctx.Set(output_, std::move(res));
    printf("[Rank] Ranking finished. Top1 ID: %d\n", items[0].id);
  }
  std::string Name() const override { return "Rank"; }

 private:
  DataToken<std::vector<Item>> input_;
  DataToken<RankResult> output_;
};

REGISTER_OP(RankOp);

class DeepRankOp : public Operator {
 public:
  void Configure(const ConfigNode& conf) override {
    model_path_ = conf["model_path"].As<std::string>("./model");
    ctr_threshold_ = conf["threshold"]["ctr"].As<float>(0.5f);
    cvr_threshold_ = conf["threshold"]["cvr"].As<float>(0.5f);

    std::cout << "[" << Name() << "]\n path = " << model_path_
              << "\n ctr_threshold = " << ctr_threshold_
              << "\n cvr_threshold = " << cvr_threshold_ << std::endl;

    const auto& features = conf["features"];
    for (size_t i = 0; i < features.size(); ++i) {
      features_.push_back(features[i]["name"].As<std::string>());
      std::string type = features[i]["type"].As<std::string>();

      std::cout << " feature[" << i << "] name = " << features_.back()
                << ", type = " << type << '\n';
    }
  }

  void Init(Registry& reg) override {
    input_ = reg.Input<std::vector<Item>>("merged_list");
    output_ = reg.Output<RankResult>("ranked_list");
  }

  void Run(Context& ctx) override {
    auto items = ctx.Get(input_);  // Copy
    // Simulate ranking model scoring
    SleepMs(30);
    for (auto& item : items) item.score += 0.1f;  // simulate score boost

    // Sort by score descending
    std::sort(items.begin(), items.end(),
              [](const Item& a, const Item& b) { return a.score > b.score; });

    RankResult res;
    res.final_items = items;
    ctx.Set(output_, std::move(res));
    printf("[DeepRank] Ranking finished. Top1 ID: %d\n", items[0].id);
  }
  std::string Name() const override { return "DeepRank"; }

 private:
  DataToken<std::vector<Item>> input_;
  DataToken<RankResult> output_;

  std::string model_path_;
  float ctr_threshold_;
  float cvr_threshold_;
  std::vector<std::string> features_;
};

REGISTER_OP(DeepRankOp);

// 7. Reranking operator (post-ranking refinement)
class ReRankOp : public Operator {
 public:
  void Configure(const ConfigNode& conf) override {
    top_n_ = conf["top_n"].As<int>(10);
  }

  void Init(Registry& reg) override {
    input_ = reg.Input<RankResult>("ranked_list");
    output_ = reg.Output<RankResult>("final_result");
  }

  void Run(Context& ctx) override {
    auto ranked = ctx.Get(input_);
    SleepMs(20);
    // Simulate reranking: apply diversity/business-rule adjustments
    for (auto& item : ranked.final_items) {
      item.score *= 1.05f;
    }
    // Truncate to top_n
    if (static_cast<int>(ranked.final_items.size()) > top_n_) {
      ranked.final_items.resize(top_n_);
    }
    printf("[ReRank] Reranked to top %d items\n", top_n_);
    ctx.Set(output_, std::move(ranked));
  }

  std::string Name() const override { return "ReRank"; }

 private:
  DataToken<RankResult> input_;
  DataToken<RankResult> output_;
  int top_n_ = 10;
};

REGISTER_OP(ReRankOp);
