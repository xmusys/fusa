#include "agent.h"
#include <thread>

Agent::~Agent()
{
	if (onload_nums != NULL)
		free(onload_nums);

	// unmap
	if (ptr != MAP_FAILED) {
		munmap(ptr, shm_size);
	}

	// close fd
	if (shm_fd != -1) {
		close(shm_fd);
	}

	// remove shared memory object
	shm_unlink(shm_name);
}

void Agent::set_conn(uint64_t machine_num, uint64_t machine_idx,
		     std::shared_ptr<sds::Initiator> agent_conn,
		     uint64_t mem_size)
{
	nr_machine = machine_num;
	machine_id = machine_idx;
	conn = agent_conn;
	// take a region from conn's registered memory as buffer
	local_addr = agent_conn->get_cache();
	agent_conn->set_cache(mem_size);
	return;
}

void Agent::set_type(std::string type)
{
	agent_type = type;
	if (agent_type == "server") {
		mem_handler = std::make_shared<sds::Target>();
	} else {
		mem_handler = NULL;
	}
	return;
}

void Agent::post_send()
{
	;
}

void Agent::register_memory(void *addr, size_t length)
{
	// skip superchunk in target
	local_addr = addr + local_offset;
	if (agent_type == "server") {
		mem_handler->register_main_memory(addr, length);
	} else {
		SDS_PERROR("unknown type, can not register main memory...");
		exit(-1);
	}
}

void Agent::register_shm_memory()
{
	local_addr = server_strategy_shm_ptr + local_offset;
	SDS_INFO("%p %d", local_addr, strategy_shm_size);
	if (agent_type == "server") {
		mem_handler->register_main_memory(server_strategy_shm_ptr,
						  strategy_shm_size);
	} else {
		SDS_PERROR("unknown type, can not register main memory...");
		exit(-1);
	}
}

void Agent::make_shm()
{
	SDS_INFO("Register shared memory...");
	// create shared memory
	shm_fd = shm_open(shm_name, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
	if (shm_fd == -1) {
		throw std::runtime_error("Error opening shared memory: " +
					 std::string(strerror(errno)));
	}

	// set shared memory size
	if (ftruncate(shm_fd, shm_size) == -1) {
		close(shm_fd);
		throw std::runtime_error(
			"Error setting size of shared memory: " +
			std::string(strerror(errno)));
	}

	// map shared memory
	ptr = mmap(0, shm_size, PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);
	if (ptr == MAP_FAILED) {
		close(shm_fd);
		throw std::runtime_error("Error mapping shared memory: " +
					 std::string(strerror(errno)));
	}

	memset(ptr, 0, shm_size);
	SDS_INFO("Register shared memory completed...");
}

void Agent::collect()
{
	// recorder * collector = (recorder *)ptr;
	recorder *collector =
		(recorder *)&(((sh_local_region *)ptr)->records[0]);
	client_layout *local_region = (client_layout *)local_addr;
	uint64_t recorder_cnt = 0;
	for (int i = 0; i < __MAX_RECORDER_NUM; i++) {
		// record exists
		// local_region->mesg_arr[0].crc = __CRC_COMPLETE;
		if ((collector + i)->qpn != 0) {
			for (int j = 0;
			     j < __MAX_COUNTER_NUM * __SLOT_GRANULARITY; j++) {
				local_region->mesg_arr[j / __COUNTER_PER_MESG]
					.crc = __CRC_COMPLETE;
				// local_region->mesg_arr[j / 7].counters[j % 7] += (collector + i)->counters[j];
				long long incre = 0;
				long long c_incre = 0;
				FAA(&incre, (collector + i)->counters[j]);
				FAA(&c_incre, (collector + i)->c_counters[j]);
				// if(incre > 0)
				//     std::cout << i << " " << j << local_region->mesg_arr[j / 7].counters[j % 7] << std::endl;
				// overwrite the previous phase's value
				if (recorder_cnt == 0) {
					local_region
						->mesg_arr[j /
							   __COUNTER_PER_MESG]
						.counters[j %
							  __COUNTER_PER_MESG] =
						incre;
					local_region
						->c_mesg_arr[j /
							     __COUNTER_PER_MESG]
						.counters[j %
							  __COUNTER_PER_MESG] =
						c_incre;
				} else {
					local_region
						->mesg_arr[j /
							   __COUNTER_PER_MESG]
						.counters[j %
							  __COUNTER_PER_MESG] +=
						incre;
					local_region
						->c_mesg_arr[j /
							     __COUNTER_PER_MESG]
						.counters[j %
							  __COUNTER_PER_MESG] +=
						c_incre;
				}
				tt_count += incre;
				// decay counter, otherwise it accumulates forever
				FAA(&((collector + i)->counters[j]), 0 - incre);
				FAA(&((collector + i)->c_counters[j]),
				    0 - c_incre);
			}
			// std::cout << std::endl;
			recorder_cnt++;
		}
	}
	std::cout << "tt count: " << tt_count << std::endl;
	// std::cout << "mesg region" << std::endl;
	// for(int i = 0; i < __MAX_COUNTER_NUM; i ++ )
	// {
	//     if((i != 0 && i % 64 == 0 )|| (i == __MAX_COUNTER_NUM - 1))
	//         std::cout << std::endl;
	//     std::cout << local_region->mesg_arr[i / 7].counters[i % 7] << " ";
	// }
	//         std::cout << std::endl;
}

bool Agent::muliti_node_sync()
{
	SDS_INFO("Start multi node sync...");
	uint32_t rkey_;
	uintptr_t remote_va_;
	sds::GlobalAddress remote_addr(0, 0);
	conn->manager_.get_remote_memory_key(remote_addr.node,
					     remote_addr.mr_id,
					     remote_addr.offset, 8, rkey_,
					     remote_va_);
	remote_addr.offset = local_offset;
	char *buf = (char *)conn->alloc_cache(8);
	conn->fetch_and_add(buf, remote_addr, 1,
			    sds::Initiator::Option::PostRequest);
	while (*(uint64_t *)buf != nr_machine) {
		conn->read(buf, remote_addr, sizeof(uint64_t),
			   sds::Initiator::Option::PostRequest);
	}
	int sync_read_cnt = 0;
	// drain CQEs from CQ
	while (sync_read_cnt < 10000) {
		struct ibv_wc sync_wc[10];
		ibv_poll_cq(conn->manager_.get_cq_ptr(0), 10, &sync_wc[0]);
		sync_read_cnt++;
	}
	sleep(3);
	SDS_INFO("Multi node sync completed...");
}

bool Agent::check_server_meta()
{
	rpc_meta *shm_meta_ = (rpc_meta *)&((sh_local_region *)ptr)->rmeta_;
	// rpc_meta * local_meta_ = (rpc_meta *)&((sh_local_region *)local_addr)->rmeta_
	rpc_meta *local_meta_ =
		(rpc_meta *)(local_addr + sizeof(resp) +
			     sizeof(mesg_unit) * __MESG_UNIT_NUM * 2);
	if (local_meta_->length != 0 &&
	    local_meta_->rpc_addr != shm_meta_->rpc_addr) {
		memcpy(shm_meta_, local_meta_, sizeof(rpc_meta));
		msync(shm_meta_, sizeof(rpc_meta), MS_SYNC);
		SDS_INFO(
			"Receive server rpc meta successfully! shm ptr: %llu, rpc addr: %llu",
			ptr, shm_meta_->rpc_addr);
		std::cout << "rmeta_ offset: "
			  << ((uint64_t) &
			      ((sh_local_region *)ptr)->rmeta_ - (uint64_t)ptr)
			  << std::endl;
		return true;
	}
	return false;
}

void Agent::start_client(uint64_t machine_num, uint64_t port)
{
	tt_count = 0;
	// nr_machine = machine_num;
	// assert(machine_num > 0);
	make_shm();
	muliti_node_sync();
	client_layout *local_region = (client_layout *)local_addr;
	// only one agent server, so rkey[0] is sufficient
	conn->manager_.get_remote_memory_key(0, sds::MAIN_MEMORY_MR_ID, 0, 8,
					     rkey[0], remote_va[0]);
	// first 64B of target's registered memory is for metadata
	remote_va[0] = remote_va[0] + local_offset;
	uint64_t run_cnt = 0;
	uint64_t *phase_buf =
		(uint64_t *)conn->alloc_cache(sizeof(uint64_t) * 2);
	*(phase_buf) = 1;
	// char * buf = (char *)conn->alloc_cache(sizeof(mesg));
	bool check_flag = true;
	// ensure metadata is synced before starting
	while (check_server_meta() != true) {
		;
	}
	while (1) {
		// 1. scan driver-side info
		usleep(1000000);

		// if(check_flag) check_flag =
		check_server_meta();

		collect();
		// client_mesg.crc = __CRC_COMPLETE;
		struct ibv_send_wr wr[2], *bad_wr[2];
		struct ibv_sge sge[2];
		sds::GlobalAddress remote(0, 0);

		// 2.1 send recorded info from this phase to the server
		// write to agent server's per-machine slot (remote_va[0] + sizeof(machine_unit) * machine_id)
		// only one server, so rkey/remote_va indices are all 0; set this request to unsignaled
		// *2 because offloaded-to-CPU records are also included
		remote.offset =
			sizeof(resp) + sizeof(machine_unit) * machine_id;
		add_request(wr[0], sge[0], run_cnt, IBV_WR_RDMA_WRITE,
			    &local_region->mesg_arr[0], remote, rkey[0],
			    remote_va[0],
			    sizeof(mesg_unit) * __MESG_UNIT_NUM * 2, 0, 0, 0,
			    conn->manager_);
		int ret = ibv_post_send(conn->manager_.get_qp_ptr(0), &wr[0],
					&bad_wr[0]);
		if (ret < 0)
			SDS_PERROR("ibv_post_send failed...");

		// 2.2 increment phase field value
		(*phase_buf)++;
		// phase_buf + 1 is unused, was for testing
		(*(phase_buf + 1)) = 0;
		// increment remote phase value, defined in server_layout
		remote.offset = sizeof(resp) +
				sizeof(machine_unit) * (machine_id + 1) -
				sizeof(uint64_t);
		add_request(wr[1], sge[1], run_cnt * 2 + 1,
			    IBV_WR_ATOMIC_FETCH_AND_ADD, phase_buf + 1, remote,
			    rkey[0], remote_va[0], sizeof(uint64_t), 1, 0,
			    IBV_SEND_SIGNALED, conn->manager_);
		ret = ibv_post_send(conn->manager_.get_qp_ptr(0), &wr[1],
				    &bad_wr[1]);
		if (ret < 0)
			SDS_PERROR("ibv_post_send failed...");
		ibv_wc wc;
		int comps = poll_n(1, conn->manager_.get_cq_ptr(0), &wc);

		SDS_INFO("wc.opcode: %d", wc.opcode);

		// 3. wait for server reply, compare next phase number; if equal, proceed
		while (local_region->strategies.phase != ((*phase_buf)))
			;

		// 4. copy server strategy to shared memory
		// use CAS first, just in case
		// uint64_t *shm_strategies = (uint64_t *)&((sh_local_region *)ptr)->strag.flags[0];
		uint64_t *shm_strategies =
			(uint64_t *)&((sh_local_region *)ptr)->strag.credit[0];

		// local region is agent client's registered memory; server strategy is sent here first
		// uint64_t *new_strategies = (uint64_t *)(&local_region->strategies.flags[0]);

		uint64_t *new_strategies =
			(uint64_t *)(&local_region->strategies.credit[0]);
		std::cout << "phase: " << local_region->strategies.phase
			  << std::endl;

		for (int i = 0; i < sizeof(strategies) / sizeof(uint64_t);
		     i++) {
			if (*(new_strategies + i) != 0) {
				// std::cout << " counter_idx: " << i << " ";
			}
			// 8B read/write, no CAS needed
			//    CAS(shm_strategies + i, (shm_strategies + i), *(new_strategies + i));
			*(shm_strategies + i) = *(new_strategies + i);
		}

		// wait for consensus
		wait_sync();

		// all QPs reached consensus, FAA to server
		(*(phase_buf + 1)) = 0;
		remote.offset = sizeof(server_layout) - sizeof(uint64_t);
		add_request(wr[1], sge[1], 66666, IBV_WR_ATOMIC_FETCH_AND_ADD,
			    phase_buf + 1, remote, rkey[0], remote_va[0],
			    sizeof(uint64_t), 1, 0, IBV_SEND_SIGNALED,
			    conn->manager_);
		ret = ibv_post_send(conn->manager_.get_qp_ptr(0), &wr[1],
				    &bad_wr[1]);
		if (ret < 0)
			SDS_PERROR("ibv_post_send failed...");
		comps = poll_n(1, conn->manager_.get_cq_ptr(0), &wc);

		std::cout << std::endl;
		// for(int i = 0; i < __MAX_COUNTER_NUM; i ++) {
		//     if(local_region->strategies.flags[i] == true) {
		//         std::cout << "phase: " << local_region->strategies.phase << " counter_idx: " << i << std::endl;
		//     }
		// }
		run_cnt++;
	}
}

void Agent::wait_sync()
{
#ifndef __FULL_ONLOAD
	sh_local_region *sh_mem_ptr = (sh_local_region *)ptr;
	std::queue<std::tuple<int, uint64_t, uint64_t> > sync_queue;
	for (int i = 0; i < __MAX_RECORDER_NUM; i++) {
		if (sh_mem_ptr->records[i].qpn != 0) {
			sync_queue.push(
				std::make_tuple(i, sh_mem_ptr->records[i].qpn,
						sh_mem_ptr->epoch[i]));
		}
	}

	while (sync_queue.empty() == false) {
		auto cur = sync_queue.front();
		int idx = std::get<0>(cur);
		uint64_t qpn = std::get<1>(cur);
		uint64_t ep = std::get<2>(cur);

		if (ep != sh_mem_ptr->epoch[idx] ||
		    sh_mem_ptr->running[idx] == 0) {
			sync_queue.pop();
		}
	}
#endif
}

void Agent::send_server_meta(rpc_meta *cur_meta)
{
	// wait for app server to write to shared memory
	volatile uint64_t *tmp_crc = &(((rpc_meta *)server_shm_ptr)->crc);
	while ((*tmp_crc) != __CRC_COMPLETE) {
		;
	}

	// if app server memory address hasn't changed, no need to sync metadata
	if (cur_meta->rpc_addr == ((rpc_meta *)server_shm_ptr)->rpc_addr)
		return;

	// 5.2 if synced, send memory metadata to agent client, which notifies its driver
	struct ibv_send_wr wr, *bad_wr;
	struct ibv_sge sge;
	struct ibv_wc wc;
	sds::GlobalAddress remote(0, 0);
	// reuse the skipped region (only 64B)
	memcpy(local_addr - sizeof(rpc_meta), server_shm_ptr, sizeof(rpc_meta));
	for (int i = 0; i < nr_machine; i++) {
		// set offset per client_layout
		rpc_meta *meta_ = (rpc_meta *)(local_addr - sizeof(rpc_meta));
		meta_->machine_id = i;
		// write rpc_meta into agent client's slot per client_layout offset
		remote.offset =
			sizeof(resp) + sizeof(mesg_unit) * __MESG_UNIT_NUM * 2;
		SDS_INFO("Start write rpc_meta to agent client, rpc addr: %llu",
			 meta_->rpc_addr);
		add_request(wr, sge, 999, IBV_WR_RDMA_WRITE, meta_, remote,
			    rkey[i], remote_va[i], sizeof(rpc_meta), 0, 0,
			    IBV_SEND_SIGNALED, mem_handler->manager_);
		int ret = ibv_post_send(mem_handler->manager_.get_qp_ptr(i),
					&wr, &bad_wr);
		if (ret < 0)
			SDS_PERROR("ibv_post_send failed...");
		ibv_wc wc;
		int comps = poll_n(1, mem_handler->manager_.get_cq_ptr(i), &wc);
	}
	// update cur_meta
	cur_meta->rkey = ((rpc_meta *)server_shm_ptr)->rkey;
	cur_meta->rpc_addr = ((rpc_meta *)server_shm_ptr)->rpc_addr;
	cur_meta->length = ((rpc_meta *)server_shm_ptr)->length;
	return;
}

void Agent::make_shm4server()
{
	// 1. register shared memory for agent server and memory server communication
	SDS_INFO("Register shared memory for server...");
	// create shared memory
	int shm_fd =
		shm_open(SERVER_SHM_NAME, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
	if (shm_fd == -1) {
		throw std::runtime_error("Error opening shared memory: " +
					 std::string(strerror(errno)));
	}

	// set shared memory size
	if (ftruncate(shm_fd, shm_size) == -1) {
		close(shm_fd);
		throw std::runtime_error(
			"Error setting size of shared memory: " +
			std::string(strerror(errno)));
	}

	// map shared memory
	server_shm_ptr = mmap(0, SERVER_SHM_SIZE, PROT_READ | PROT_WRITE,
			      MAP_SHARED, shm_fd, 0);
	if (server_shm_ptr == MAP_FAILED) {
		close(shm_fd);
		throw std::runtime_error("Error mapping shared memory: " +
					 std::string(strerror(errno)));
	}

	memset(server_shm_ptr, 0, SERVER_SHM_SIZE);
	SDS_INFO("Allocate shared meta memory for server completed...");

	int strag_shm_fd = shm_open(SERVER_STRATEGY_SHM, O_CREAT | O_RDWR,
				    S_IRUSR | S_IWUSR);
	if (strag_shm_fd == -1) {
		throw std::runtime_error("Error opening shared memory: " +
					 std::string(strerror(errno)));
	}

	if (ftruncate(strag_shm_fd, strategy_shm_size) == -1) {
		close(strag_shm_fd);
		throw std::runtime_error(
			"Error setting size of shared memory: " +
			std::string(strerror(errno)));
	}

	server_strategy_shm_ptr =
		mmap(0, strategy_shm_size, PROT_READ | PROT_WRITE, MAP_SHARED,
		     strag_shm_fd, 0);
	if (server_strategy_shm_ptr == MAP_FAILED) {
		close(strag_shm_fd);
		throw std::runtime_error("Error mapping shared memory: " +
					 std::string(strerror(errno)));
	}

	memset(server_strategy_shm_ptr, 0, strategy_shm_size);
	SDS_INFO("Allocate shared strategy memory for server completed...");
}

void Agent::start_server(uint64_t machine_num, uint64_t port, bool enable)
{
	agent_enabled = enable;
	overThres = threshold;
	onload_nums = (uint64_t *)malloc(sizeof(uint64_t) * __MAX_COUNTER_NUM);
	memset(onload_nums, 0, sizeof(uint64_t) * __MAX_COUNTER_NUM);
	memset(counters, 0,
	       sizeof(counter_) * __MAX_COUNTER_NUM * __SLOT_GRANULARITY);
	memset(c_counters, 0,
	       sizeof(counter_) * __MAX_COUNTER_NUM * __SLOT_GRANULARITY);
	memset(real_counters, 0, sizeof(counter_) * __MAX_COUNTER_NUM);
	memset(real_c_counters, 0, sizeof(counter_) * __MAX_COUNTER_NUM);
	nr_machine = machine_num;
	server_running = true;
	assert(machine_num > 0);
	// 1. start listener thread to accept incoming RDMA connections
	int rc = mem_handler->start(port);
	assert(!rc);
	SDS_INFO("Agent server starts...");
	std::cout << "sync val: " << *(uint64_t *)local_addr << std::endl;
	// uint64_t sync_cnt = 0;
	volatile uint64_t *sync_cnt = (uint64_t *)local_addr;
	while (*sync_cnt != nr_machine) {
		// sync_cnt ++;
		// if(sync_cnt == 100000)
		//     std::cout << *(uint64_t *)local_addr << std::endl;
		// 2. wait for agent client connections to finish
		;
	}
	// sleep(5);
	SDS_INFO("All connections established...");
	// 3. connections ready, init remote info for agent server
	sds::GlobalAddress remote;
	for (int i = 0; i < nr_machine; i++) {
		remote.node = i;
		remote.mr_id = sds::MAIN_MEMORY_MR_ID;
		remote.offset = 0;
		if (mem_handler->manager_.get_remote_memory_key(
			    remote.node, remote.mr_id, remote.offset, 8,
			    rkey[i], remote_va[i])) {
			SDS_WARN("Agent get remote node info failed...");
		}
		// client has no super_chunk, no skip needed
		// remote_va[i] = remote_va[i] + local_offset;
	}
	// 4. start additional thread to run server_func
	std::thread server_thread(&Agent::server_func, this);

	// get pthread handle
	pthread_t pthread_id = server_thread.native_handle();

	// set CPU affinity
	cpu_set_t cpuset;
	CPU_ZERO(&cpuset); // clear CPU set
	uint64_t core_id = 31;
	CPU_SET(core_id, &cpuset); // bind to core_id

	int ret =
		pthread_setaffinity_np(pthread_id, sizeof(cpu_set_t), &cpuset);
	if (ret != 0) {
		std::cerr << "Failed to set thread affinity, error: " << ret
			  << std::endl;
	} else {
		std::cout << "Agent Server thread binded to CPU " << core_id
			  << std::endl;
	}

	server_thread.detach(); // let thread run independently

	// server_thread.join();  // block until thread exits
}

void Agent::server_func()
{
	// default by 4
	// TODO add a parameter for multicore experiments, otherwise it is constrained
	uint64_t core_num = core_num_;
	uint64_t TOTAL_CPU_CAP = __CPU_CAPACITY * core_num;
	server_layout *local_region = (server_layout *)local_addr;
	memset(&local_region->uniform_strag, 0, sizeof(resp));
	bool scanned[nr_machine];
	for (int i = 0; i < nr_machine; i++)
		scanned[i] = false;
	local_region->strategies.phase = 1;
	rpc_meta meta_;
	while (server_running) {
		// 0. check if application metadata changed
		send_server_meta(&meta_);
		// 1. scan communication region, fetch client info
		int scanned_cnt = 0;
		// strategy-related variables
		uint64_t tt_rnic_req = 0;
		uint64_t tt_cpu_req = 0;
		bool machine_is_uniform[nr_machine];
		uint64_t machine_req_per_slot[nr_machine];
		uint64_t machine_onload_req[nr_machine];
		uint64_t machine_max_req[nr_machine];
		bool is_uniform = true;
		std::chrono::high_resolution_clock::time_point start, end;
		while (1) {
			for (int i = 0; i < nr_machine; i++) {
				// 1.1 check phase to avoid duplicate scans
				if (local_region->machine_arr[i].phase ==
					    local_region->strategies.phase &&
				    scanned[i] == false) {
					if (scanned_cnt == 0)
						start = std::chrono::
							high_resolution_clock::
								now();
					scanned[i] = true;
					machine_is_uniform[i] = true;
					machine_req_per_slot[i] = 0;
					machine_onload_req[i] = 0;
					machine_max_req[i] = 0;
					uint64_t record_cnt = 0;
					// clear records
					if (scanned_cnt == 0) {
						for (int k = 0;
						     k < __MAX_COUNTER_NUM;
						     k++) {
							real_counters[k] = 0;
							real_c_counters[k] = 0;
						}
					}
					// ensure all messages are scanned
					while (record_cnt <
					       __MAX_COUNTER_NUM *
						       __SLOT_GRANULARITY) {
						uint64_t tmp_slot_req = 0;
						for (int j = 0;
						     j <
						     __MAX_COUNTER_NUM *
							     __SLOT_GRANULARITY;
						     j++) {
							uint64_t tmp_idx =
								j /
								__COUNTER_PER_MESG;
							uint64_t tmp_counter_idx =
								j %
								__COUNTER_PER_MESG;
							// record at machine granularity, for detecting uniform vs zipfian
							machine_req_per_slot[i] +=
								local_region
									->machine_arr
										[i]
									.mesg_arr[tmp_idx]
									.counters[tmp_counter_idx];
							machine_onload_req[i] +=
								local_region
									->machine_arr
										[i]
									.c_mesg_arr
										[tmp_idx]
									.counters[tmp_counter_idx];
							tmp_slot_req +=
								local_region
									->machine_arr
										[i]
									.mesg_arr[tmp_idx]
									.counters[tmp_counter_idx];

							// because we split a slot into __SLOT_GRANULARITY counters,
							// when j is a multiple of __SLOT_GRANULARITY, one slot's counters are fully scanned
							if (j != 0 &&
							    j % __SLOT_GRANULARITY ==
								    0) {
								// record max request count per slot per machine
								if (tmp_slot_req >
								    machine_max_req
									    [i]) {
									machine_max_req
										[i] = tmp_slot_req;
								}
								tmp_slot_req =
									0;
							}

							// 1.2 accumulate counter values across nr_machine machines
							local_region
								->machine_arr[i]
								.mesg_arr[tmp_idx]
								.crc = 0;

							// 1.3 on first valid scan in this phase, clear previous phase records
							if (scanned_cnt == 0) {
								counters[j] =
									local_region
										->machine_arr
											[i]
										.mesg_arr[tmp_idx]
										.counters[tmp_counter_idx];
								c_counters[j] =
									local_region
										->machine_arr
											[i]
										.c_mesg_arr
											[tmp_idx]
										.counters[tmp_counter_idx];
							} else {
								counters[j] +=
									local_region
										->machine_arr
											[i]
										.mesg_arr[tmp_idx]
										.counters[tmp_counter_idx];
								c_counters[j] +=
									local_region
										->machine_arr
											[i]
										.c_mesg_arr
											[tmp_idx]
										.counters[tmp_counter_idx];
							}

							// 1.4 peer machine data is organized as 7 counters per 64B
							// so we use real_counters and real_c_counters for actual total request count across all slots
							real_counters
								[j %
								 __MAX_COUNTER_NUM] +=
								local_region
									->machine_arr
										[i]
									.mesg_arr[tmp_idx]
									.counters[tmp_counter_idx];
							real_c_counters
								[j %
								 __MAX_COUNTER_NUM] +=
								local_region
									->machine_arr
										[i]
									.c_mesg_arr
										[tmp_idx]
									.counters[tmp_counter_idx];

							// 1.5 aggregate request count across all machines
							tt_rnic_req +=
								local_region
									->machine_arr
										[i]
									.mesg_arr[tmp_idx]
									.counters[tmp_counter_idx];
							tt_cpu_req +=
								local_region
									->machine_arr
										[i]
									.c_mesg_arr
										[tmp_idx]
									.counters[tmp_counter_idx];
							record_cnt++;
						}
					}
					scanned_cnt++;
				}
			}
			if (scanned_cnt == nr_machine) {
				// 1.6 scan done, start making strategy
				std::priority_queue<cpu_task> cpu_tasks;
				uint64_t cpu_taks_cnt = 0;
				uint64_t tt_thres_count = 0;
				uint64_t tt_over_count = 0;
				uint64_t tt_req = tt_rnic_req + tt_cpu_req;
				uint64_t hot_req = 0;

				// 1.7 compute average requests per slot in previous phase
				uint64_t req_per_slot =
					tt_req / __MAX_COUNTER_NUM;
				overThres = req_per_slot > threshold ?
						    req_per_slot :
						    threshold;
				print_server_counters();
#ifndef __SEQUENCER
				// disable agent, all handled by NIC
				if (agent_enabled == false ||
				    threshold == RNIC_THRESHOLD) {
					for (int i = 0;
					     i < __MAX_COUNTER_NUM *
							 __SLOT_GRANULARITY;
					     i++) {
						local_region->strategies
							.credit[i] = 0;
					}
					local_region->strategies.phase += 1;
					std::cout << std::endl;
					std::cout << "tt rnic req cnt "
						  << tt_rnic_req << std::endl;
					std::cout << "tt req cnt " << tt_cpu_req
						  << std::endl;
					std::cout << "cpu task cnt "
						  << cpu_taks_cnt << std::endl;
					std::cout << std::endl;
					std::cout << std::endl;
				}
				// agent enabled with threshold==0: all offloaded to CPU, so all set to 1
				else if (threshold == 0) {
					for (int i = 0;
					     i < __MAX_COUNTER_NUM *
							 __SLOT_GRANULARITY;
					     i++) {
						local_region->strategies
							.credit[i] = 1;
					}
					local_region->strategies.phase += 1;
					std::cout << std::endl;
					std::cout << "tt rnic req cnt "
						  << tt_rnic_req << std::endl;
					std::cout << "tt req cnt " << tt_cpu_req
						  << std::endl;
					std::cout << "cpu task cnt "
						  << cpu_taks_cnt << std::endl;
					std::cout << std::endl;
					std::cout << std::endl;
				}
				// agent normally enabled
				else {
					// 2.0 determine load type per machine
					for (int i = 0; i < nr_machine; i++) {
						machine_req_per_slot[i] =
							machine_req_per_slot[i] /
							(__MAX_COUNTER_NUM *
							 __SLOT_GRANULARITY);
						// if max > 2x average, it's not uniform
						if (machine_max_req[i] >
						    machine_req_per_slot[i] *
							    2) {
							machine_is_uniform[i] =
								false;
						}
						// only with PCIe Atomic enabled, uniform is offloaded to CPU
						else if (machine_onload_req[i] >
								 0 &&
							 pa_enabled == false) {
							machine_is_uniform[i] =
								false;
						}
					}

					// 2.1 then determine overall load type
					for (uint64_t i = 0;
					     i < __MAX_COUNTER_NUM; i++) {
						if (real_counters[i] +
							    real_c_counters[i] >=
						    req_per_slot * 2) {
							is_uniform = false;
							break;
						}
					}
					std::cout
						<< "currently server total workload type is uniform: "
						<< is_uniform << std::endl;
					// PCIe Atomic enabled, NIC capacity differs, handle separately
					if (pa_enabled == true) {
						// raw CAS with PCIe Atomic is ~14M; app adds an RDMA READ, so divide by 2
						uint64_t RNIC_CAP_W_PA =
							14000000 / 2;
						if (is_uniform == true) {
							// __SLOT_GRANULARITY partitions
							uint64_t onload_sub_slot =
								0;
							onload_sub_slot = static_cast<
								uint64_t>(std::round(
								(TOTAL_CPU_CAP /
								 static_cast<
									 double>(
									 TOTAL_CPU_CAP +
									 RNIC_CAP_W_PA)) *
								__SLOT_GRANULARITY));
							if (onload_sub_slot >
							    __SLOT_GRANULARITY) {
								SDS_INFO(
									"Error happened while processing strategies under [Uniform workload, PCIe Atomic enable]");
								onload_sub_slot =
									0;
							}
							// only do when onload_sub_slot is greater than 0
							if (onload_sub_slot > 0)
								for (uint64_t i =
									     0;
								     i <
								     __MAX_COUNTER_NUM *
									     onload_sub_slot;
								     i++) {
									local_region
										->strategies
										.credit[i] =
										1;
								}
						} else if (is_uniform ==
							   false) {
							// PCIe Atomic is slow, so we can offload more
							overThres =
								req_per_slot > (threshold /
										2) ?
									req_per_slot :
									threshold;
							for (uint64_t i = 0;
							     i <
							     __MAX_COUNTER_NUM;
							     i++) {
								// tasks that might need offloading
								if (real_counters
									    [i] >=
								    overThres) {
									hot_req += real_counters
										[i];
									for (int j = 0;
									     j <
									     __SLOT_GRANULARITY;
									     j++) {
										if (counters[i +
											     j * __MAX_COUNTER_NUM] >
										    0) {
											cpu_task task{
												counters[i +
													 j * __MAX_COUNTER_NUM],
												i,
												j
											};
											cpu_tasks
												.push(task);
										}
										onload_nums[i] =
											real_counters
												[i] -
											req_per_slot;
										tt_over_count +=
											(real_counters
												 [i] -
											 req_per_slot);
										tt_thres_count += real_counters
											[i];
									}
								} // NIC has spare capacity, decide accordingly
								else {
									uint64_t slot_reset_cnt =
										0;
									for (int j = 0;
									     j <
									     __SLOT_GRANULARITY;
									     j++) {
										if (c_counters[i +
											       j * __MAX_COUNTER_NUM] +
											    slot_reset_cnt +
											    real_counters
												    [i] <
										    overThres) {
											local_region
												->strategies
												.credit[i +
													j * __MAX_COUNTER_NUM] =
												0;
											slot_reset_cnt += c_counters
												[i +
												 j * __MAX_COUNTER_NUM];
										}
									}
								}
							}
							cpu_task attempt_task;
							uint64_t attempts = 0;
#ifdef __SERVER_PRINT_SLOTS
							std::cout
								<< std::endl
								<< "req_per_slot: "
								<< req_per_slot
								<< " overThres: "
								<< overThres
								<< " tt_need_onload_num: "
								<< tt_thres_count
								<< std::endl;
#endif
							for (uint64_t i = 0;
							     i <
							     cpu_tasks.size();
							     i++) {
								if (attempts >
								    TOTAL_CPU_CAP)
									break;
								attempt_task =
									cpu_tasks
										.top();
								cpu_tasks.pop();
#ifndef __FULL_ONLOAD
								// offload
								if (attempt_task.req_cnt <
									    onload_nums
										    [attempt_task
											     .idx_] ||
								    attempt_task.req_cnt >
									    counters[attempt_task
											     .idx_] *
										    0.9) {
									onload_nums[attempt_task
											    .idx_] =
										onload_nums
											[attempt_task
												 .idx_] -
										attempt_task
											.req_cnt;
									// offload slot i, address j
									local_region
										->strategies
										.credit[attempt_task
												.idx_ +
											attempt_task.inner_idx *
												__MAX_COUNTER_NUM] =
										1;
									// try to assign task to CPU
									attempts =
										attempts +
										counters[attempt_task
												 .idx_ +
											 attempt_task.inner_idx *
												 __MAX_COUNTER_NUM];
									// std::cout << "onload location: [i] " << attempt_task.idx_  << " [j] " << attempt_task.inner_idx << " [num] " << attempt_task.req_cnt << std::endl;
								} else {
									local_region
										->strategies
										.credit[attempt_task
												.idx_ +
											attempt_task.inner_idx *
												__MAX_COUNTER_NUM] =
										0;
								}
#else
								local_region
									->strategies
									.credit[attempt_task
											.idx_ +
										attempt_task.inner_idx *
											__MAX_COUNTER_NUM] =
									1;
#endif // __FULL_ONLOAD
							}
							if (attempts <
								    TOTAL_CPU_CAP &&
							    0) {
								// __SLOT_GRANULARITY partitions
								uint64_t onload_sub_slot =
									0;
								onload_sub_slot = static_cast<
									uint64_t>(std::round(
									((TOTAL_CPU_CAP -
									  attempts) /
									 static_cast<
										 double>(
										 (TOTAL_CPU_CAP -
										  attempts) +
										 (RNIC_CAP_W_PA -
										  attempts))) *
									__SLOT_GRANULARITY));
								if (onload_sub_slot >
								    __SLOT_GRANULARITY) {
									SDS_INFO(
										"Error happened while processing strategies under [Uniform workload, PCIe Atomic enable]");
									onload_sub_slot =
										0;
								}
								// only do when onload_sub_slot is greater than 0
								if (onload_sub_slot >
								    0)
									for (uint64_t i =
										     0;
									     i <
									     __MAX_COUNTER_NUM *
										     onload_sub_slot;
									     i++) {
										local_region
											->strategies
											.credit[i] =
											1;
									}
							}
						}
					} else {
						// 2.2 different load types use different offload strategies; without PCIe atomic, uniform has no offload
						// fuse to 0 since PCIe Atomic is off, no offload needed
						if (is_uniform == true && 0) {
							for (uint64_t i = 0;
							     i <
							     __MAX_COUNTER_NUM;
							     i++) {
								for (int j = 0;
								     j < 2;
								     j++) {
									local_region
										->strategies
										.credit[i +
											j * __MAX_COUNTER_NUM] =
										1;
								}
							}
#ifdef __SERVER_PRINT_SLOTS
							std::cout
								<< std::endl
								<< "req_per_slot: "
								<< req_per_slot
								<< " overThres: "
								<< overThres
								<< " tt_need_onload_num: "
								<< tt_thres_count
								<< std::endl;
#endif
						}
						// zipfian: reaching this if means it's definitely zipfian
						else if (1) {
							FAA(&local_region
								     ->old_strategies
								     .phase,
							    1);
							for (uint64_t i = 0;
							     i <
							     __MAX_COUNTER_NUM;
							     i++) {
								if (real_counters
									    [i] >=
								    overThres) {
									for (int j = 0;
									     j <
									     __SLOT_GRANULARITY;
									     j++) {
										if (counters[i +
											     j * __MAX_COUNTER_NUM] >
										    0) {
											cpu_task task{
												counters[i +
													 j * __MAX_COUNTER_NUM],
												i,
												j
											};
											cpu_tasks
												.push(task);
										}
									}
									onload_nums[i] =
										real_counters
											[i] -
										req_per_slot;
									tt_over_count +=
										(real_counters
											 [i] -
										 req_per_slot);
									tt_thres_count += real_counters
										[i];
								}
								// NIC has spare capacity, decide accordingly
								else {
									uint64_t slot_reset_cnt =
										0;
									for (int j = 0;
									     j <
									     __SLOT_GRANULARITY;
									     j++) {
										if (c_counters[i +
											       j * __MAX_COUNTER_NUM] +
											    slot_reset_cnt +
											    real_counters
												    [i] <
										    overThres) {
											local_region
												->old_strategies
												.credit[i +
													j * __MAX_COUNTER_NUM] =
												local_region
													->strategies
													.credit[i +
														j * __MAX_COUNTER_NUM];
											local_region
												->strategies
												.credit[i +
													j * __MAX_COUNTER_NUM] =
												0;
											slot_reset_cnt += c_counters
												[i +
												 j * __MAX_COUNTER_NUM];
										}
									}
								}
							}
							cpu_task attempt_task;
							uint64_t attempts = 0;

							// 2.3 dynamically control the amount offloaded to CPU
							// uint64_t factor = (uint64_t)round( (__CPU_CAPACITY / (double)(__CPU_CAPACITY + tt_thres_count)) * 100 );
							double factor =
								(TOTAL_CPU_CAP /
								 (double)(TOTAL_CPU_CAP +
									  tt_over_count));
#ifdef __SERVER_PRINT_SLOTS
							std::cout
								<< std::endl
								<< "total cpu cap: "
								<< TOTAL_CPU_CAP
								<< std::endl;
							std::cout
								<< std::endl
								<< "req_per_slot: "
								<< req_per_slot
								<< " overThres: "
								<< overThres
								<< " tt_need_onload_num: "
								<< tt_thres_count
								<< "\tfactor: "
								<< factor
								<< std::endl;
#endif
							for (uint64_t i = 0;
							     i <
							     cpu_tasks.size();
							     i++) {
								attempt_task =
									cpu_tasks
										.top();
								cpu_tasks.pop();
#ifndef __FULL_ONLOAD
								// offload
								if (attempt_task.req_cnt <
									    onload_nums
										    [attempt_task
											     .idx_] ||
								    attempt_task.req_cnt >
									    counters[attempt_task
											     .idx_] *
										    0.9) {
									onload_nums[attempt_task
											    .idx_] =
										onload_nums
											[attempt_task
												 .idx_] -
										attempt_task
											.req_cnt;
									// offload slot i, address j
									local_region
										->old_strategies
										.credit[attempt_task
												.idx_ +
											attempt_task.inner_idx *
												__MAX_COUNTER_NUM] =
										local_region
											->strategies
											.credit[attempt_task
													.idx_ +
												attempt_task.inner_idx *
													__MAX_COUNTER_NUM];
									local_region
										->strategies
										.credit[attempt_task
												.idx_ +
											attempt_task.inner_idx *
												__MAX_COUNTER_NUM] =
										1;
									// try to assign task to CPU
									attempts =
										attempts +
										counters[attempt_task
												 .idx_ +
											 attempt_task.inner_idx *
												 __MAX_COUNTER_NUM];
									if (attempts >
									    TOTAL_CPU_CAP) {
										// attempts = attempts - counters[attempt_task.idx_ + attempt_task.inner_idx * __MAX_COUNTER_NUM];
										// local_region->strategies.credit[attempt_task.idx_ + attempt_task.inner_idx * __MAX_COUNTER_NUM] = 0;
										break;
									}
									// std::cout << "onload location: [i] " << attempt_task.idx_  << " [j] " << attempt_task.inner_idx << " [num] " << attempt_task.req_cnt << std::endl;
								} else {
									local_region
										->old_strategies
										.credit[attempt_task
												.idx_ +
											attempt_task.inner_idx *
												__MAX_COUNTER_NUM] =
										local_region
											->strategies
											.credit[attempt_task
													.idx_ +
												attempt_task.inner_idx *
													__MAX_COUNTER_NUM];
									local_region
										->strategies
										.credit[attempt_task
												.idx_ +
											attempt_task.inner_idx *
												__MAX_COUNTER_NUM] =
										0;
								}
#else // __FULL_ONLOAD
								local_region
									->strategies
									.credit[attempt_task
											.idx_ +
										attempt_task.inner_idx *
											__MAX_COUNTER_NUM] =
									1;
#endif // __FULL_ONLOAD
							}
						}
					}

					// start next phase
#ifdef __SERVER_PRINT_SLOTS
					std::cout << "tt rnic req cnt "
						  << tt_rnic_req << std::endl;
					std::cout << "tt req cnt " << tt_cpu_req
						  << std::endl;
					std::cout << "cpu task cnt "
						  << cpu_taks_cnt << std::endl;
					std::cout << std::endl;
					std::cout << std::endl;
#endif
					FAA(&local_region->strategies.phase, 1);
					local_region->uniform_strag.phase =
						local_region->strategies.phase;
				}
#else // __SEQUENCER
				FAA(&local_region->strategies.phase, 1);
				local_region->uniform_strag.phase =
					local_region->strategies.phase;
				// 2.0 determine load type on machine
				for (int i = 0; i < nr_machine; i++) {
					machine_is_uniform[i] = false;
				}

				// 2.1 then determine overall load type
				for (uint64_t i = 0; i < __MAX_COUNTER_NUM;
				     i++) {
					if (real_counters[i] +
						    real_c_counters[i] >=
					    req_per_slot * 2) {
						is_uniform = false;
						break;
					}
				}
				std::cout
					<< "currently server total workload type is uniform: "
					<< is_uniform << std::endl;
				for (uint64_t i = 0; i < __MAX_COUNTER_NUM;
				     i++) {
					// tasks that might need offloading
					if (real_counters[i] >= overThres) {
						hot_req += real_counters[i];
						for (int j = 0;
						     j < __SLOT_GRANULARITY;
						     j++) {
							if (counters[i +
								     j * __MAX_COUNTER_NUM] >
							    0) {
								// local_region->strategies.credit[i + j * __MAX_COUNTER_NUM] = (uint64_t)round(double(real_counters[i]) / (double(real_counters[i]) + double(overThres)));
								// offload 40%, don't remember why 2025-11-12
								local_region
									->strategies
									.credit[i +
										j * __MAX_COUNTER_NUM] =
									40;
							}
							// per-slot excess, needs onload
							onload_nums[i] =
								real_counters[i] -
								req_per_slot;
							// total excess
							tt_over_count +=
								(real_counters[i] -
								 req_per_slot);
							// sum of excess slots
							tt_thres_count +=
								real_counters[i];
						}
					} // NIC has spare capacity, decide accordingly
					else {
						uint64_t slot_reset_cnt = 0;
						for (int j = 0;
						     j < __SLOT_GRANULARITY;
						     j++) {
							if (c_counters[i +
								       j * __MAX_COUNTER_NUM] +
								    slot_reset_cnt +
								    real_counters
									    [i] <
							    overThres) {
								local_region
									->strategies
									.credit[i +
										j * __MAX_COUNTER_NUM] =
									0;
								slot_reset_cnt += c_counters
									[i +
									 j * __MAX_COUNTER_NUM];
							}
						}
					}
				}
#ifdef __SERVER_PRINT_SLOTS
				std::cout << std::endl
					  << "req_per_slot: " << req_per_slot
					  << " overThres: " << overThres
					  << " tt_need_onload_num: "
					  << tt_thres_count << std::endl;
#endif
#endif // __SEQUENCER
				for (int i = 0; i < nr_machine; i++)
					scanned[i] = false;
				break;
			}
		}
		// end = std::chrono::high_resolution_clock::now();
		// SDS_INFO("Make strategies time: %d us", std::chrono::duration_cast<std::chrono::microseconds>(end - start).count());
		start = std::chrono::high_resolution_clock::now();
		uint64_t tmp_consensus = local_region->consensus;
		// 2. make strategy based on info
		sds::GlobalAddress remote(0, 0);
		for (int i = 0; i < nr_machine; i++) {
			// 3. send strategy to each client
			struct ibv_send_wr wr, *bad_wr;
			struct ibv_sge sge;
			remote.node = i;
			// when some clients are zipfian and some uniform with PA disabled, handle strategy sending for uniform clients
			if (is_uniform == false &&
			    machine_is_uniform[i] == true &&
			    pa_enabled == false) {
				add_request(wr, sge, 0, IBV_WR_RDMA_WRITE,
					    &local_region->uniform_strag,
					    remote, rkey[i], remote_va[i],
					    sizeof(resp), 0, 0,
					    IBV_SEND_SIGNALED,
					    mem_handler->manager_);
			} else {
				// client_layout strategies at the start of the address
				add_request(wr, sge, 0, IBV_WR_RDMA_WRITE,
					    &local_region->strategies, remote,
					    rkey[i], remote_va[i], sizeof(resp),
					    0, 0, IBV_SEND_SIGNALED,
					    mem_handler->manager_);
			}
			int ret = ibv_post_send(
				mem_handler->manager_.get_qp_ptr(i), &wr,
				&bad_wr);
			if (ret < 0)
				SDS_PERROR("ibv_post_send failed...");
		}
		for (int i = 0; i < nr_machine; ++i) {
			ibv_wc wc;
			int comps = poll_n(
				1, mem_handler->manager_.get_cq_ptr(i), &wc);
		}
		end = std::chrono::high_resolution_clock::now();
		SDS_INFO("server send time: %d us",
			 std::chrono::duration_cast<std::chrono::microseconds>(
				 end - start)
				 .count());
#ifndef __FULL_ONLOAD
		start = std::chrono::high_resolution_clock::now();
		while (tmp_consensus == local_region->consensus ||
		       local_region->consensus % nr_machine != 0) {
			;
		}
		end = std::chrono::high_resolution_clock::now();
		double consensus_latency =
			std::chrono::duration<double, std::micro>(end - start)
				.count();
		std::cout << "server consensus time: " << consensus_latency
			  << std::endl;
#endif
	}
}

void Agent::stop_server()
{
	server_running = false;
	return;
}

int Agent::add_request(struct ibv_send_wr &wr, struct ibv_sge &sge,
		       uint64_t wr_id, ibv_wr_opcode opcode, const void *local,
		       const sds::GlobalAddress &remote, uint32_t rkey,
		       uintptr_t remote_va, size_t length, uint64_t compare_add,
		       uint64_t swap, int flags, sds::ResourceManager &manager_)
{
	sge.addr = (uintptr_t)local;
	sge.length = length;
	sge.lkey = manager_.get_local_memory_lkey(0);

	wr.wr_id = wr_id;
	wr.opcode = opcode;
	wr.num_sge = 1;
	wr.sg_list = &sge;
	wr.send_flags = flags;
	wr.next = NULL;

	if (opcode == IBV_WR_ATOMIC_FETCH_AND_ADD ||
	    opcode == IBV_WR_ATOMIC_CMP_AND_SWP) {
		wr.wr.atomic.rkey = rkey;
		wr.wr.atomic.remote_addr = remote_va + remote.offset;
		wr.wr.atomic.compare_add = compare_add;
		wr.wr.atomic.swap = swap;
	} else if (opcode == IBV_WR_SEND) {
		;
	} else {
		wr.wr.rdma.rkey = rkey;
		wr.wr.rdma.remote_addr = remote_va + remote.offset;
	}
	if (opcode == IBV_WR_RDMA_WRITE &&
	    length <= manager_.config().max_inline_data) {
		wr.send_flags |= IBV_SEND_INLINE;
	}
	return 0;
}

int Agent::poll_n(size_t n, ibv_cq *cq, ibv_wc *wc)
{
	int tt_comps = 0;
	while (tt_comps < n) {
		int new_comps = ibv_poll_cq(cq, n - tt_comps, &wc[tt_comps]);
		if (new_comps > 0) {
			if (wc[tt_comps].status != 0) {
				fprintf(stderr, "Bad poll %d wc status %d\n",
					wc[tt_comps].opcode,
					wc[tt_comps].status);
				while (1)
					;
				exit(0);
			}
			tt_comps += new_comps;
		}
	}
	return tt_comps;
}

void Agent::print()
{
	std::cout << "Agent Type: " << agent_type << std::endl;
	std::cout << "Number of machines: " << nr_machine << std::endl;
	std::cout << "Machine ID: " << machine_id << std::endl;

	// print shared memory info
	std::cout << "Shared Memory Name: " << (shm_name ? shm_name : "NULL")
		  << std::endl;
	std::cout << "Shared Memory Size: " << shm_size << std::endl;
	std::cout << "Shared Memory FD: " << shm_fd << std::endl;
	std::cout << "Shared Memory Pointer: " << ptr << std::endl;

	// print counters array info
	// std::cout << "Counters: ";
	// for (size_t i = 0; i < __MAX_COUNTER_NUM; ++i) {
	//     std::cout << counters[i].value << " ";  // print each counter's value
	// }
	// std::cout << std::endl;

	// print connection object info
	// if (conn) {
	//     conn->display();  // call Initiator::display()
	// } else {
	//     std::cout << "Connection Initiator: NULL" << std::endl;
	// }

	// print memory handler info
	// if (mem_handler) {
	//     mem_handler->display();  // call Target::display()
	// } else {
	//     std::cout << "Memory Handler: NULL" << std::endl;
	// }

	// print local address
	std::cout << "Local Address: " << local_addr << std::endl;

	// print rkey and remote_va arrays
	// std::cout << "Rkey values: ";
	// for (size_t i = 0; i < __MAX_MACHINE_NUM; ++i) {
	//     std::cout << rkey[i] << " ";
	// }
	// std::cout << std::endl;

	// std::cout << "Remote VA values: ";
	// for (size_t i = 0; i < __MAX_MACHINE_NUM; ++i) {
	//     std::cout << remote_va[i] << " ";
	// }
	std::cout << std::endl;

	// print server state
	std::cout << "Server Running: " << (server_running ? "Yes" : "No")
		  << std::endl;
}