set(CMAKE_CXX_FLAGS "-O0 -g")

include_directories(include)
add_executable(test_agent agent_test.cpp agent.cpp)
target_link_libraries(test_agent PUBLIC smart)
target_link_libraries(test_agent PUBLIC rt)

include_directories(include)
add_executable(fusa_rpc_agent breakdown_fusa_rpc_test_agent.cpp breakdown_fusa_rpc_agent.cpp)
target_link_libraries(fusa_rpc_agent PUBLIC smart)
target_link_libraries(fusa_rpc_agent PUBLIC rt)