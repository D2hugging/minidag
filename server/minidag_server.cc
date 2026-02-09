#include <httplib.h>

#include <chrono>
#include <iostream>
#include <nlohmann/json.hpp>
#include <string>

#include "../example/loader.hpp"
#include "../example/operators.hpp"

using json = nlohmann::json;
using namespace minidag;

static std::string ParseArg(int argc, char** argv, const std::string& flag,
                            const std::string& default_val) {
  for (int i = 1; i + 1 < argc; ++i) {
    if (flag == argv[i]) return argv[i + 1];
  }
  return default_val;
}

int main(int argc, char** argv) {
  std::string config_path =
      ParseArg(argc, argv, "--config", "example/example_conf.json");
  std::string host = ParseArg(argc, argv, "--host", "0.0.0.0");
  int port = std::stoi(ParseArg(argc, argv, "--port", "8080"));

  // 1. Parse config & build DAGs
  Loader::AppConfig app_config;
  try {
    app_config = Loader::ParseAppConfig(config_path);
  } catch (const std::exception& e) {
    std::cerr << "Error loading config: " << e.what() << std::endl;
    return 1;
  }

  size_t pool_size = app_config.thread_pool_size > 0
                         ? static_cast<size_t>(app_config.thread_pool_size)
                         : std::thread::hardware_concurrency();
  DagManager manager(pool_size, StderrLogger(LogLevel::kInfo));

  for (const auto& dag_conf : app_config.dags) {
    try {
      std::cout << "Building DAG: " << dag_conf.name << std::endl;
      manager.BuildDag(dag_conf.name, dag_conf.nodes);
    } catch (const std::exception& e) {
      std::cerr << "Failed to build DAG '" << dag_conf.name << "': " << e.what()
                << std::endl;
      return 1;
    }
  }

  // 2. Set up HTTP server
  httplib::Server svr;

  // Health check
  svr.Get("/api/v1/health",
          [](const httplib::Request&, httplib::Response& res) {
            res.set_content(json{{"status", "ok"}}.dump(), "application/json");
          });

  // List DAGs
  svr.Get("/api/v1/dags",
          [&manager](const httplib::Request&, httplib::Response& res) {
            res.set_content(json{{"dags", manager.ListDags()}}.dump(),
                            "application/json");
          });

  // Run DAG
  svr.Post(R"(/api/v1/dags/([^/]+)/run)", [&manager](
                                              const httplib::Request& req,
                                              httplib::Response& res) {
    std::string dag_name = req.matches[1];

    // Check DAG exists
    if (!manager.HasDag(dag_name)) {
      res.status = 404;
      res.set_content(json{{"error", "Unknown DAG: " + dag_name}}.dump(),
                      "application/json");
      return;
    }

    // Parse request body
    json body;
    try {
      body = json::parse(req.body);
    } catch (const std::exception& e) {
      res.status = 400;
      res.set_content(
          json{{"error", std::string("Invalid JSON: ") + e.what()}}.dump(),
          "application/json");
      return;
    }

    if (!body.contains("request") || !body["request"].is_object()) {
      res.status = 400;
      res.set_content(
          json{{"error", "Missing 'request' object in body"}}.dump(),
          "application/json");
      return;
    }

    int timeout_ms = body.value("timeout_ms", 2000);

    // Deserialize UserRequest
    UserRequest user_req;
    try {
      user_req = body["request"].get<UserRequest>();
    } catch (const std::exception& e) {
      res.status = 400;
      res.set_content(
          json{{"error", std::string("Invalid request fields: ") + e.what()}}
              .dump(),
          "application/json");
      return;
    }

    // Execute DAG
    auto t_start = std::chrono::high_resolution_clock::now();
    try {
      auto executor = manager.CreateExecutor(dag_name);

      auto req_token = executor->Template().Token<UserRequest>("request");
      executor->Ctx().Set(req_token, std::move(user_req));

      auto future = executor->Run();
      if (future.wait_for(std::chrono::milliseconds(timeout_ms)) ==
          std::future_status::timeout) {
        executor->Cancel();
        auto t_end = std::chrono::high_resolution_clock::now();
        auto total_us = std::chrono::duration_cast<std::chrono::microseconds>(
                            t_end - t_start)
                            .count();
        res.status = 504;
        res.set_content(json{{"error", "DAG execution timed out"},
                             {"dag", dag_name},
                             {"total_us", total_us}}
                            .dump(),
                        "application/json");
        return;
      }

      future.get();

      auto t_end = std::chrono::high_resolution_clock::now();
      auto total_us =
          std::chrono::duration_cast<std::chrono::microseconds>(t_end - t_start)
              .count();

      // Build metrics array
      json metrics_arr = json::array();
      for (const auto& m : executor->Metrics()) {
        metrics_arr.push_back(json{{"name", m.name},
                                   {"duration_us", m.duration_us},
                                   {"skipped", m.skipped},
                                   {"timed_out", m.timed_out}});
      }

      // Extract result
      json result_json = nullptr;
      auto result_token =
          executor->Template().Token<RankResult>("final_result");
      if (result_token.IsValid() && executor->Ctx().Has(result_token)) {
        const auto& rank_result = executor->Ctx().Get(result_token);
        result_json = json{{"final_result", rank_result}};
      }

      res.set_content(json{{"dag", dag_name},
                           {"result", result_json},
                           {"total_us", total_us},
                           {"metrics", metrics_arr}}
                          .dump(),
                      "application/json");

    } catch (const std::exception& e) {
      auto t_end = std::chrono::high_resolution_clock::now();
      auto total_us =
          std::chrono::duration_cast<std::chrono::microseconds>(t_end - t_start)
              .count();
      res.status = 500;
      res.set_content(
          json{{"error", std::string("DAG execution failed: ") + e.what()},
               {"dag", dag_name},
               {"total_us", total_us}}
              .dump(),
          "application/json");
    }
  });

  std::cout << "minidag_server listening on " << host << ":" << port
            << std::endl;
  svr.listen(host, port);
  return 0;
}
