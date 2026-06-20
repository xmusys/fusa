# set(CMAKE_CXX_FLAGS "-O0 -g")
set(CMAKE_CXX_FLAGS "-O3 -g")

include_directories(include)
add_executable(test_rdma main.cpp basic_client.cpp server.cpp)
target_link_libraries(test_rdma PUBLIC smart)
target_link_libraries(test_rdma PUBLIC rt)

add_executable(test_ycsb test_ycsb.cpp basic_client.cpp server.cpp)
target_link_libraries(test_ycsb PUBLIC smart)
target_link_libraries(test_ycsb PUBLIC rt)

add_executable(test_stride test_stride.cpp basic_client.cpp server.cpp)
target_link_libraries(test_stride PUBLIC smart)
target_link_libraries(test_stride PUBLIC rt)

add_executable(test_latency test_stride_latency.cpp basic_client.cpp server.cpp)
target_link_libraries(test_latency PUBLIC smart)
target_link_libraries(test_latency PUBLIC rt)

add_executable(ycsb_exp ycsb_exp.cpp basic_client.cpp server.cpp)
target_link_libraries(ycsb_exp PUBLIC smart)
target_link_libraries(ycsb_exp PUBLIC rt)

target_compile_options(ycsb_exp PUBLIC -O3)

add_executable(test_herd test_herd.cpp basic_client.cpp herd_server.cpp)
target_link_libraries(test_herd PUBLIC smart)
target_link_libraries(test_herd PUBLIC rt)

add_executable(test_static test_static.cpp basic_client.cpp herd_server.cpp)
target_link_libraries(test_static PUBLIC smart)
target_link_libraries(test_static PUBLIC rt)

add_executable(test_herd_faa test_herd_faa.cpp basic_client.cpp herd_server.cpp)
target_link_libraries(test_herd_faa PUBLIC smart)
target_link_libraries(test_herd_faa PUBLIC rt)

add_executable(test_faa test_faa.cpp basic_client.cpp herd_server.cpp)
target_link_libraries(test_faa PUBLIC smart)
target_link_libraries(test_faa PUBLIC rt)

add_executable(faa_stride test_faa_stride.cpp basic_client.cpp herd_server.cpp)
target_link_libraries(faa_stride PUBLIC smart)
target_link_libraries(faa_stride PUBLIC rt)

add_executable(faa_fusa test_faa_fusa.cpp basic_client.cpp server.cpp)
target_link_libraries(faa_fusa PUBLIC smart)
target_link_libraries(faa_fusa PUBLIC rt)

add_executable(smart_retry test_smart_retry.cpp basic_client.cpp server.cpp)
target_link_libraries(smart_retry PUBLIC smart)
target_link_libraries(smart_retry PUBLIC rt)

add_executable(test_ht test_ht.cpp coroutine_scheduler.cpp basic_client.cpp ht_server.cpp)
target_link_libraries(test_ht PUBLIC smart)
target_link_libraries(test_ht PUBLIC rt)

# find Boost library
find_package(Boost REQUIRED COMPONENTS coroutine context)

add_executable(test_adaption test_adaption.cpp coroutine_scheduler.cpp basic_client.cpp server.cpp)
target_link_libraries(test_adaption PUBLIC Boost::coroutine Boost::context smart rt)

add_executable(sequencer test_sequencer.cpp coroutine_scheduler.cpp basic_client.cpp sequencer_server.cpp)
target_link_libraries(sequencer PUBLIC Boost::coroutine Boost::context smart rt)

# add executable
add_executable(cas_retry test_cas_retry.cpp coroutine_scheduler.cpp basic_client.cpp server.cpp)

# link Boost.Coroutine and Boost.Context
target_link_libraries(cas_retry PUBLIC Boost::coroutine Boost::context smart rt)

add_executable(test_drtm test_drtm.cpp coroutine_scheduler.cpp basic_client.cpp server.cpp)
target_link_libraries(test_drtm PUBLIC Boost::coroutine Boost::context smart rt)

target_link_libraries(test_ht PUBLIC Boost::coroutine Boost::context)

# add O3 optimization for test_herd target
target_compile_options(test_herd PUBLIC -O3)
target_compile_options(cas_retry PUBLIC -O0 -g)
target_compile_options(test_ht PUBLIC -O0 -g)
target_compile_options(sequencer PUBLIC -O0 -g)
# link libmlx5.so to test_rdma target
# target_link_libraries(test_rdma PRIVATE mlx5) 

# if libmlx5.so is in a custom path, specify it here
# link_directories(/home/dell/sqs/rdma-core/providers/mlx5/fusioncas/)