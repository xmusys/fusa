#include <iostream>
#include <vector>

#include "server.h"
#include "stride_latency_client.hpp"

#define NUMA_NODE 1

int main(int argc, char **argv)
{
	JsonConfig config = JsonConfig::load_file("../config/test_rdma.json");
	size_t machine_num = (size_t)config.get("machine_num").get_uint64();
	size_t port = (size_t)config.get("port").get_uint64();
	std::string node_type = argc < 2 ? "unkown" : argv[1];
	if (node_type == "server") {
		JsonConfig server_config = config.get("memory_node");
		// GB
		size_t mem_pool_size =
			(size_t)server_config.get("mem_pool_size").get_uint64() *
			sds::kGigaBytes;
		// KB
		size_t rpc_region_size =
			(size_t)server_config.get("rpc_region_size")
				.get_uint64() *
			1024ul;

		bool rpc_enabled =
			(bool)server_config.get("rpc_enabled").get_bool();
		// memory node
		Server server(port, NUMA_NODE, mem_pool_size, rpc_region_size,
			      machine_num, rpc_enabled);

		JsonConfig rpc_config = server_config.get("rpc_param");

		if (rpc_enabled)
			server.init_rpc(rpc_config);

		server.start();

		while (getchar() != 'q') {
		}
	} else if (node_type == "client") {
		std::vector<std::string> server_list;
		JsonConfig client_config = config.get("compute_node");
		JsonConfig servers = client_config.get("servers");
		size_t block_size =
			client_config.get("block_size").get_uint64();
		std::string type = client_config.get("type").get_str();
		size_t machine_id =
			client_config.get("machine_id").get_uint64();
		// size_t stride = client_config.get("stride").get_uint64();
		uint64_t attempts_num =
			client_config.get("attempts_num").get_uint64();
		std::string ycsb_path =
			client_config.get("ycsb_path").get_str();

		// std::shared_ptr<sds::Initiator> nodes[sds::kMaxMemoryNodes];
		sds::Initiator *nodes[sds::kMaxMemoryNodes];

		size_t nr_threads = argc < 3 ? 1 : atoi(argv[2]);
		size_t depth = argc < 4 ? 1 : atoi(argv[3]);
		size_t stride = argc < 5 ? 8 : atoi(argv[4]);

		// one QP per thread
		size_t qp_count = nr_threads;
		for (int i = 0; i < servers.size(); i++) {
			server_list.push_back(servers.get(i).get_str());
		}
		assert(!server_list.empty());
		attempts_num = 300000000;
		StrideLatencyClient client(servers.size(), machine_num,
					   machine_id, depth, attempts_num);

		client.set_stride(stride);
		client.alloc_latencies();
		// one conn per server
		for (int i = 0; i < servers.size(); i++) {
			// nodes[i] = std::make_shared<sds::Initiator>();
			nodes[i] = new sds::Initiator();
			nodes[i]->disable_inline_write();
			// connect to servers[i], create qp_count QPs
			int rc = nodes[i]->connect(i, server_list[i].c_str(),
						   port, qp_count);
			// configure per client
			client.set_conns(nodes[i], i, 0);
			client.machine_id =
				nodes[i]->manager_.get_peer_node_id(0);
			std::cout << "client machine id: " << client.machine_id
				  << std::endl;
		}

		client.set_shm();
		client.muliti_node_sync(0);
		client.start(nr_threads);
		client.report_latencies();
	} else if (node_type == "unknown") {
		SDS_PERROR(
			"Unknown node type, please set 'compute' or 'memory'.");
	}
}