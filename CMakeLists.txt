cmake_minimum_required(VERSION 3.20)
project(mini_dag)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include_directories(include)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

set(CMAKE_CXX_FLAGS_RELEASE "-O2 -DNDEBUG -pthread -march=native")
set(CMAKE_CXX_FLAGS_DEBUG "-g -O0 -pthread -Wall -Wextra")

include(FetchContent)

FetchContent_Declare(
    json
    GIT_REPOSITORY https://github.com/nlohmann/json.git
    GIT_TAG        v3.11.3
)
FetchContent_MakeAvailable(json)

add_executable(exam_search example/exam_search.cc)

target_link_libraries(exam_search nlohmann_json::nlohmann_json)

target_compile_features(exam_search PRIVATE cxx_std_17)

# ---- HTTP Server ----
option(MINIDAG_BUILD_SERVER "Build HTTP server" ON)

if(MINIDAG_BUILD_SERVER)
  FetchContent_Declare(
      httplib
      GIT_REPOSITORY https://github.com/yhirose/cpp-httplib.git
      GIT_TAG        v0.18.3
  )
  set(HTTPLIB_COMPILE OFF CACHE BOOL "" FORCE)
  FetchContent_MakeAvailable(httplib)

  add_executable(minidag_server server/minidag_server.cc)
  target_link_libraries(minidag_server nlohmann_json::nlohmann_json httplib::httplib)
  target_compile_features(minidag_server PRIVATE cxx_std_17)
endif()

# ---- Testing ----
option(MINIDAG_BUILD_TESTS "Build unit tests" ON)

if(MINIDAG_BUILD_TESTS)
  enable_testing()

  FetchContent_Declare(
      googletest
      GIT_REPOSITORY https://github.com/google/googletest.git
      GIT_TAG        v1.14.0
  )
  set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
  FetchContent_MakeAvailable(googletest)

  add_executable(minidag_test tests/minidag_test.cc)
  target_link_libraries(minidag_test GTest::gtest_main)
  target_compile_features(minidag_test PRIVATE cxx_std_17)

  include(GoogleTest)
  gtest_discover_tests(minidag_test)
endif()
