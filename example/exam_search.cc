#include <chrono>
#include <iostream>
#include <string>

#include "operators.hpp"
#include "loader.hpp"

int main(int argc, char** argv) {
  std::string config_path = "../example/example_conf.json";
  if (argc > 1) {
    config_path = argv[1];
  }

  // 1. Parse config
  Loader::AppConfig app_config;
  try {
    app_config = Loader::ParseAppConfig(config_path);
  } catch (const std::exception& e) {
    std::cerr << "Error loading config: " << e.what() << std::endl;
    return -1;
  }

  // 2. Create DagManager (with stderr logger)
  size_t pool_size = app_config.thread_pool_size > 0
                         ? static_cast<size_t>(app_config.thread_pool_size)
                         : std::thread::hardware_concurrency();
  DagManager manager(pool_size, StderrLogger(LogLevel::kInfo));

  // 3. Build all DAGs
  for (const auto& dag_conf : app_config.dags) {
    try {
      std::cout << "\n=== Building DAG: " << dag_conf.name
                << " ===" << std::endl;
      manager.BuildDag(dag_conf.name, dag_conf.nodes);
    } catch (const std::exception& e) {
      std::cerr << "Failed to build DAG '" << dag_conf.name << "': " << e.what()
                << std::endl;
      return -1;
    }
  }

  // 4. Simulate traffic for each DAG
  for (const auto& dag_name : manager.DagNames()) {
    std::cout << "\n====== Running DAG: " << dag_name << " ======" << std::endl;

    for (int i = 0; i < 3; ++i) {
      std::cout << "\n--- " << dag_name << " Request " << i + 1 << " ---"
                << std::endl;
      auto t_start = std::chrono::high_resolution_clock::now();

      auto executor = manager.CreateExecutor(dag_name);

      // Look up token via Registry (no hard-coded index)
      auto req_token = executor->GetTemplate().Token<UserRequest>("request");
      executor->GetContext().Set(req_token,
                                 UserRequest{1000L + i, "iPhone 16"});

      try {
        auto future = executor->Run();
        if (future.wait_for(std::chrono::milliseconds(500)) ==
            std::future_status::timeout) {
          executor->Cancel();
          std::cerr << "Request Timed Out! (cancelled)" << std::endl;
        } else {
          future.get();

          auto result_token =
              executor->GetTemplate().Token<RankResult>("final_result");
          if (result_token.IsValid()) {
            const auto& result = executor->GetContext().Get(result_token);
            if (!result.final_items.empty()) {
              std::cout << "Top result ID: " << result.final_items[0].id
                        << " score: " << result.final_items[0].score
                        << std::endl;
            }
          }
        }
      } catch (const std::exception& e) {
        std::cerr << "Request Error: " << e.what() << std::endl;
      }

      auto t_end = std::chrono::high_resolution_clock::now();
      std::cout << "Latency: "
                << std::chrono::duration_cast<std::chrono::milliseconds>(
                       t_end - t_start)
                       .count()
                << "ms" << std::endl;

      // Print per-node metrics
      std::cout << "Node metrics:" << std::endl;
      for (const auto& m : executor->Metrics()) {
        std::cout << "  " << m.name << ": " << m.duration_us << "us";
        if (m.skipped) std::cout << " [SKIPPED]";
        if (m.timed_out) std::cout << " [TIMED_OUT]";
        std::cout << std::endl;
      }
    }
  }

  return 0;
}
