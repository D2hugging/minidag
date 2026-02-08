#pragma once

#include <nlohmann/json.hpp>
#include <string>
#include <vector>

struct UserRequest {
  long uid = 0;
  std::string query;
};

inline void to_json(nlohmann::json& j, const UserRequest& r) {
  j = nlohmann::json{{"uid", r.uid}, {"query", r.query}};
}

inline void from_json(const nlohmann::json& j, UserRequest& r) {
  j.at("uid").get_to(r.uid);
  j.at("query").get_to(r.query);
}

struct Item {
  int id = 0;
  float score = 0.0f;
  Item() = default;
  Item(int id, float score) : id(id), score(score) {}
};

inline void to_json(nlohmann::json& j, const Item& i) {
  j = nlohmann::json{{"id", i.id}, {"score", i.score}};
}

inline void from_json(const nlohmann::json& j, Item& i) {
  j.at("id").get_to(i.id);
  j.at("score").get_to(i.score);
}

struct RecallResult {
  std::vector<Item> items;
  std::string source;
};

inline void to_json(nlohmann::json& j, const RecallResult& r) {
  j = nlohmann::json{{"items", r.items}, {"source", r.source}};
}

inline void from_json(const nlohmann::json& j, RecallResult& r) {
  j.at("items").get_to(r.items);
  j.at("source").get_to(r.source);
}

struct RankResult {
  std::vector<Item> final_items;
};

inline void to_json(nlohmann::json& j, const RankResult& r) {
  j = nlohmann::json{{"final_items", r.final_items}};
}

inline void from_json(const nlohmann::json& j, RankResult& r) {
  j.at("final_items").get_to(r.final_items);
}
