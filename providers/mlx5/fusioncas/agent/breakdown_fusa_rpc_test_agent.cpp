#include "breakdown_fusa_rpc_agent.h"

int main(int argc, char **argv)
{
	JsonConfig config = JsonConfig::load_file("../config/agent.json");
	size_t machine_num = (size_t)config.get("machine_num").get_uint64();
	size_t port = (size_t)config.get("port").get_uint64();
	std::string agent_type = argc < 2 ? "unknown" : argv[1];
	Agent agent;
	if (agent_type == "client") {
		std::vector<std::string> server_list;
		JsonConfig client_config = config.get("compute_node");
		JsonConfig servers = client_config.get("servers");
		size_t machine_id =
			(size_t)client_config.get("machine_id").get_uint64();
		size_t mem_size =
			(size_t)client_config.get("mem_size").get_uint64() *
			sds::kMegaBytes;

		agent.set_type(agent_type);
		std::shared_ptr<sds::Initiator> conn =
			std::make_shared<sds::Initiator>();

		for (int i = 0; i < servers.size(); i++) {
			server_list.push_back(servers.get(i).get_str());
		}
		assert(!server_list.empty());

		// void * client_mem_addr = sds::mmap_huge_page(mem_size);
		// client.register_memory(client_mem_addr, mem_size);

		for (int i = 0; i < servers.size(); i++) {
			// set qp_size to 1 for now
			int rc = conn->connect(i, server_list[i].c_str(), port,
					       1);
		}
		agent.set_conn(machine_num, machine_id, conn, mem_size);

		agent.start_client(machine_num, port);
		// agent.muliti_node_sync();

		while (1) {
			// std::cout << agent.get_local_addr() << " " << *(uint64_t *)agent.get_local_addr() << std::endl;
		}

	} else if (agent_type == "server") {
		JsonConfig server_config = config.get("memory_node");
		size_t mem_size =
			(size_t)server_config.get("mem_size").get_uint64() *
			sds::kMegaBytes;
		// allocate extra 64B since target uses memory header as metadata
		void *local_addr = sds::mmap_huge_page(mem_size + local_offset);
		memset(local_addr, 0, mem_size + local_offset);

		size_t threshold = argc < 3 ? 100000 : atoi(argv[2]);
		if (argc < 3) {
			SDS_WARN(
				"Threshold doesn't specified, using default %llu",
				threshold);
		}

		std::string enable_flag = argc < 4 ? "enable" : argv[3];
		bool enable = true;
		if (enable_flag.compare("enable") == 0) {
			SDS_INFO("Agent server is enabled");
			enable = true;
		} else {
			SDS_INFO("Agent server is disabled");
			enable = false;
		}
		agent.set_threshold(threshold);
		SDS_INFO("Agent server threshold: %llu", threshold);
		agent.set_type(agent_type);
		agent.register_memory(local_addr, mem_size + local_offset);
		agent.start_server(machine_num, port, enable);
		while (getchar() != 'q')
			;
		agent.stop_server();
	} else if (agent_type == "unknown") {
		SDS_PERROR(
			"Unknown node type, please set 'client' or 'server'.");
		return 0;
	}
}