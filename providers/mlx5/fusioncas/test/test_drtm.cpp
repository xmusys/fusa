#include <iostream>
#include <vector>
#include <random>
#include <cstring>

#include "server.h"
#include "drtm_client.hpp"

#define NUMA_NODE 1

int main(int argc, char **argv)
{
	JsonConfig config = JsonConfig::load_file("../config/test_drtm.json");
	size_t machine_num = (size_t)config.get("machine_num").get_uint64();
	size_t port = (size_t)config.get("port").get_uint64();
	std::string node_type = argc < 2 ? "unkown" : argv[1];

	if (node_type == "server") {
		JsonConfig server_config = config.get("memory_node");
		size_t mem_pool_size =
			(size_t)server_config.get("mem_pool_size").get_uint64() *
			sds::kGigaBytes;
		size_t strategy_shm_size =
			(size_t)server_config.get("strategy_shm_size")
				.get_uint64() *
			sds::kMegaBytes;
		size_t rpc_region_size =
			(size_t)server_config.get("rpc_region_size")
				.get_uint64() *
			1024ul;
		bool rpc_enabled =
			(bool)server_config.get("rpc_enabled").get_bool();

		Server server(port, NUMA_NODE, mem_pool_size, rpc_region_size,
			      machine_num, rpc_enabled);
		server.set_strategy_shm_size(strategy_shm_size);
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
		size_t machine_id =
			client_config.get("machine_id").get_uint64();
		uint64_t attempts_num =
			client_config.get("attempts_num").get_uint64();

		sds::Initiator *nodes[sds::kMaxMemoryNodes];
		size_t nr_threads = argc < 3 ? 1 : atoi(argv[2]);
		size_t depth = argc < 4 ? 1 : atoi(argv[3]);
		// ShiftLock-like MicroZipf workload parameters (optional CLI): count theta read_ratio
		// Usage: ./test_drtm client <nr_threads> <depth> [count] [theta] [read_ratio]
		bool use_mz = false;
		uint64_t mz_count = 0;
		double mz_theta = 1.1;
		double mz_read_ratio = 0.8;
		// Optional trace input (minimal change):
		// Usage A: ./test_drtm client <nr_threads> <depth> trace <path_to_csv>
		// Usage B: ./test_drtm client <nr_threads> <depth> <path_to_csv>
		bool use_trace = false;
		std::string trace_path;
		if (argc >= 6 && std::strcmp(argv[4], "trace") == 0) {
			use_trace = true;
			trace_path = argv[5];
		} else if (argc >= 5 &&
			   std::strstr(argv[4], ".csv") != nullptr) {
			use_trace = true;
			trace_path = argv[4];
		}
		if (argc >= 7) {
			use_mz = true;
			mz_count = strtoull(argv[4], nullptr, 10);
			mz_theta = atof(argv[5]);
			mz_read_ratio = atof(argv[6]);
		}
		// If CLI did not request trace, check config for compute_node.trace_path
		if (!use_trace) {
			std::string t =
				client_config.get("trace_path").get_str();
			use_trace = true;
			trace_path = t;
		}
		size_t qp_count = nr_threads;

		for (int i = 0; i < servers.size(); i++) {
			server_list.push_back(servers.get(i).get_str());
		}
		if (server_list.empty()) {
			SDS_PERROR("no servers in config");
			return 1;
		}

		auto *client =
			new drtm_fusioncas::DrtmClient(servers.size(),
						       machine_num, machine_id,
						       depth, attempts_num);
		if (use_trace) {
			bool ok = client->set_trace(trace_path.c_str());
			if (!ok) {
				SDS_PERROR("failed to load trace file");
				return 1;
			}
		} else if (use_mz) {
			client->set_microzipf(mz_count, mz_theta,
					      mz_read_ratio);
		} else {
			// Fallback simple workload if MicroZipf not specified
			std::vector<char> methods;
			std::vector<uint64_t> keys;
			methods.reserve(attempts_num);
			keys.reserve(attempts_num);
			const uint64_t nlocks = 4096;
			std::mt19937_64 rng(12345);
			std::uniform_real_distribution<double> uni(0.0, 1.0);
			for (uint64_t i = 0; i < attempts_num; ++i) {
				bool is_read = uni(rng) < 0.8;
				methods.push_back(is_read ? 'r' : 'w');
				keys.push_back(i % nlocks);
			}
			client->load_workload(methods, keys);
		}

		// Each server has one connection
		for (int i = 0; i < servers.size(); i++) {
			nodes[i] = new sds::Initiator();
			nodes[i]->disable_inline_write();
			int rc = nodes[i]->connect(i, server_list[i].c_str(),
						   port, qp_count);
			if (rc) {
				SDS_PERROR("connect failed");
				return 1;
			}
			client->set_conns(nodes[i], i, 0);
			client->machine_id =
				nodes[i]->manager_.get_peer_node_id(0);
			std::cout << "client machine id: " << client->machine_id
				  << std::endl;
		}

		client->set_shm();
		client->muliti_node_sync(0);
		client->start(nr_threads);

	} else if (node_type == "unknown") {
		SDS_PERROR(
			"Unknown node type, please set 'compute' or 'memory'.");
	}
}
