#pragma once

#include <fstream>
#include <nlohmann/json.hpp>
#include <sstream>
#include <stdexcept>
#include <string>

class Loader {
 public:
  struct DagConfig {
    std::string name;
    std::vector<minidag::NodeConfig> nodes;
  };

  struct AppConfig {
    int thread_pool_size = 0;
    std::vector<DagConfig> dags;
  };

  static nlohmann::json Load(const std::string& filename) {
    std::ifstream ifs(filename);
    if (!ifs.is_open()) {
      throw std::runtime_error("Cannot open config file: " + filename);
    }
    return nlohmann::json::parse(ifs);
  }

  static minidag::ConfigNode ConvertJson(const nlohmann::json& j) {
    minidag::ConfigNode node;

    if (j.is_string()) {
      node.SetScalar(j.get<std::string>());
    } else if (j.is_number_integer()) {
      node.SetScalar(std::to_string(j.get<int64_t>()));
    } else if (j.is_number_float()) {
      std::ostringstream oss;
      oss << j.get<double>();
      node.SetScalar(oss.str());
    } else if (j.is_boolean()) {
      node.SetScalar(j.get<bool>() ? "true" : "false");
    } else if (j.is_array()) {
      for (const auto& item : j) {
        node.AddSequenceItem() = ConvertJson(item);
      }
    } else if (j.is_object()) {
      for (auto it = j.begin(); it != j.end(); ++it) {
        node.AddMapItem(it.key()) = ConvertJson(it.value());
      }
    }

    return node;
  }

  static AppConfig ParseAppConfig(const std::string& filename) {
    auto root = Load(filename);
    AppConfig app;
    app.thread_pool_size = root.value("thread_pool_size", 0);

    if (!root.contains("dags") || !root["dags"].is_object()) {
      throw std::runtime_error("Config must contain a 'dags' object");
    }

    for (auto& [dag_name, dag_json] : root["dags"].items()) {
      DagConfig dc;
      dc.name = dag_name;

      if (!dag_json.contains("nodes") || !dag_json["nodes"].is_array()) {
        throw std::runtime_error("DAG '" + dag_name +
                                 "' must contain a 'nodes' array");
      }

      for (const auto& node_json : dag_json["nodes"]) {
        minidag::NodeConfig nc;
        nc.id = node_json.at("id").get<std::string>();
        nc.op_type = node_json.at("op_type").get<std::string>();

        if (node_json.contains("dependencies")) {
          for (const auto& dep : node_json["dependencies"]) {
            nc.dependencies.push_back(dep.get<std::string>());
          }
        }

        if (node_json.contains("params")) {
          nc.params = ConvertJson(node_json["params"]);
        }

        if (node_json.contains("optional")) {
          nc.optional = node_json["optional"].get<bool>();
        }
        if (node_json.contains("timeout_ms")) {
          nc.timeout_ms = node_json["timeout_ms"].get<int>();
        }

        dc.nodes.push_back(std::move(nc));
      }

      app.dags.push_back(std::move(dc));
    }

    return app;
  }
};
