#include "basic_client.h"
#include <execinfo.h>
// int BasicClient::set_conns(std::shared_ptr<sds::Initiator> conn, size_t mem_node_id)
// {
//     // validate mem_node_id range
//     if (mem_node_id < 0 || mem_node_id >= nr_memory_nodes) {
//         SDS_WARN("error memory node id while setting conns");
//         return -1;  // error: node id out of range
//     }
//     conns[mem_node_id] = conn;
//     return 0;  // connection set successfully
// }

int BasicClient::set_conns(sds::Initiator *conn, size_t mem_node_id, int nouse)
{
	// validate mem_node_id range
	if (mem_node_id < 0 || mem_node_id >= nr_memory_nodes) {
		SDS_WARN("error memory node id while setting conns");
		return -1; // error: node id out of range
	}
	conns[mem_node_id] = conn;
	SDS_INFO("Set conns[%llu] : %llu successfully! ", mem_node_id,
		 conns[mem_node_id]);
	return 0; // connection set successfully
}

bool BasicClient::muliti_node_sync(uint64_t server_id)
{
	std::cout << "Start multi node sync..." << std::endl;
	uint32_t rkey_;
	uintptr_t remote_va_;
	sds::GlobalAddress remote_addr(0, 0);
	SDS_INFO("this: %llu", this);
	SDS_INFO("conns[%llu]: %llu", server_id, conns[server_id]);
	conns[server_id]->manager_.get_remote_memory_key(remote_addr.node,
							 remote_addr.mr_id,
							 remote_addr.offset, 8,
							 rkey_, remote_va_);
	// remote_addr.offset = SERVER_OFFSET;
	// first 128KB of server is for rpc
	uint64_t rpc_region_size = 128 * 1024;
	remote_addr.offset = rpc_region_size;
	char *buf = (char *)conns[server_id]->alloc_cache(8);
	// conns[server_id]->fetch_and_add(buf, remote_addr, 1, sds::Initiator::Option::PostRequest);
	struct ibv_send_wr wr, *bad_send_wr;
	struct ibv_sge sge;
	struct ibv_wc wc;
	// wr_id = 898989, no specific meaning
	add_request(wr, sge, 898989, IBV_WR_ATOMIC_FETCH_AND_ADD, buf,
		    remote_addr, rkey_, remote_va_, sizeof(uint64_t), 1, 0,
		    IBV_SEND_SIGNALED, conns[server_id]->manager_);
	int ret = ibv_post_send(conns[server_id]->manager_.get_qp_ptr(0), &wr,
				&bad_send_wr);
	if (ret < 0) {
		std::cout << "ibv_post_send_failed" << std::endl;
		SDS_PERROR("ibv_post_send failed...");
		exit(1);
	}
	// poll RDMA FAA
	int comp = poll_n(1, conns[server_id]->manager_.get_cq_ptr(0), &wc);
	volatile uint64_t *check = (uint64_t *)buf;
	while (*(uint64_t *)check != machine_num) {
		// conns[server_id]->read(buf, remote_addr, sizeof(uint64_t), sds::Initiator::Option::PostRequest);
		add_request(wr, sge, 989898, IBV_WR_RDMA_READ, buf, remote_addr,
			    rkey_, remote_va_, sizeof(uint64_t), 0, 0,
			    IBV_SEND_SIGNALED, conns[server_id]->manager_);
		ret = ibv_post_send(conns[server_id]->manager_.get_qp_ptr(0),
				    &wr, &bad_send_wr);
		if (ret < 0) {
			std::cout << "ibv_post_send_failed" << std::endl;
			SDS_PERROR("ibv_post_send failed...");
			exit(1);
		}
		comp = poll_n(1, conns[server_id]->manager_.get_cq_ptr(0), &wc);
	}
	// int sync_read_cnt = 0;
	// struct ibv_wc sync_wc[10];
	// while(sync_read_cnt < 1000) {
	//     ibv_poll_cq(conns[server_id]->manager_.get_cq_ptr(0), 10, &sync_wc[0]);
	//     sync_read_cnt ++;
	// }
	SDS_INFO("sync num is: %llu", *check);
	sleep(1);
	std::cout << "Multi node sync completed..." << std::endl;
}

int BasicClient::add_request(struct ibv_send_wr &wr, struct ibv_sge &sge,
			     uint64_t wr_id, ibv_wr_opcode opcode, void *local,
			     const sds::GlobalAddress &remote, uint32_t rkey,
			     uintptr_t remote_va, size_t length,
			     uint64_t compare_add, uint64_t swap, int flags,
			     sds::ResourceManager &manager_)
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

int BasicClient::poll_n(size_t n, ibv_cq *cq, ibv_wc *wc)
{
	int tt_comps = 0;
	while (tt_comps < n) {
		int new_comps = ibv_poll_cq(cq, n - tt_comps, &wc[tt_comps]);
		if (new_comps > 0) {
			if (wc[tt_comps].status != 0) {
				fprintf(stderr, "Bad poll %d wc status %d\n",
					wc[tt_comps].opcode,
					wc[tt_comps].status);
				// while(1);
				// exit(0);
			}
			tt_comps += new_comps;
		}
	}
	return tt_comps;
}

// fix 2025-04 use map function to allocate qpn
uint64_t BasicClient::map_qpn_to_idx(uint64_t qpn, uint64_t *qpn_table)
{
	uint64_t first_idx = qpn % __MAX_RECORDER_NUM;
	if (qpn_table[first_idx] == 0) {
		qpn_table[first_idx] = qpn;
		return first_idx;
	} else {
		for (int i = first_idx + 1; i < first_idx + __MAX_RECORDER_NUM;
		     i++) {
			if (qpn_table[i % __MAX_RECORDER_NUM] == 0) {
				uint64_t second_idx = i % __MAX_RECORDER_NUM;
				qpn_table[second_idx] = qpn;
				return second_idx;
			}
		}
	}
}

void BasicClient::start(size_t nr_threads)
{
	this->nr_threads = nr_threads;
	// SDS_INFO("asdjaskdjaisdakd, %llu", nr_threads);
	// SDS_INFO("Start with %llu threads", nr_threads);
	// muliti_node_sync(0);
	// tentative: 32
	pthread_t tid[32];
	thread_arg args[32];
	pthread_attr_t attr;
	pthread_barrier_t barrier;
	struct timeval start_tv, end_tv;
	// Initialize the pthread attribute object
	pthread_attr_init(&attr);
	pthread_barrier_init(&barrier, NULL, nr_threads + 1);
	// fix 2025-03-15-17:08 machine3 qpn allocation sometimes skips by 2, causing herd to hang
	// default qp allocation stride should be +1
	uint64_t qp_stride = 1;
	// if(conns[0]->manager_.get_qp_size(0) > 1) {
	//     qp_stride = conns[0]->manager_.get_qp_ptr(1)->qp_num > conns[0]->manager_.get_qp_ptr(0)->qp_num
	// ? conns[0]->manager_.get_qp_ptr(1)->qp_num - conns[0]->manager_.get_qp_ptr(0)->qp_num
	// : conns[0]->manager_.get_qp_ptr(0)->qp_num - conns[0]->manager_.get_qp_ptr(1)->qp_num;
	// }
	uint64_t qpn_table[__MAX_RECORDER_NUM];
	memset(qpn_table, 0, sizeof(uint64_t) * __MAX_RECORDER_NUM);
	for (long i = 0; i < nr_threads; ++i) {
		cpu_set_t cpuset;
		CPU_ZERO(&cpuset);
		CPU_SET(i * 2 + 1,
			&cpuset); // Bind thread to a specific NUMA node

		// init thread_arg struct, pass BasicClient instance
		uint64_t qpn = conns[0]->manager_.get_qp_ptr(i)->qp_num;
		// 2025-03-15-17:08 workaround for machine3 qpn stride-2 bug, prevent duplicate thread_id
		args[i].rpc_idx = map_qpn_to_idx(qpn, qpn_table);
		args[i].thread_id = qpn % nr_threads;
		SDS_INFO("qpn: %llu, thread id: %llu", qpn, args[i].thread_id);
		args[i].server_id = 0; // assign server_id as needed
		args[i].client = this; // pass current object to thread
		args[i].depth = depth_;
		args[i].barrier = &barrier; // pass barrier
		// Create thread with the specified affinity and pass the argument
		pthread_create(&tid[i], &attr, thread_test_fun,
			       (void *)&args[i]);
		pthread_setaffinity_np(tid[i], sizeof(cpu_set_t), &cpuset);
		std::cout << "create thread: " << args[i].thread_id
			  << std::endl;
	}
	stop_signal = false;
	// first barrier waits for child threads to start
	pthread_barrier_wait(&barrier);
	gettimeofday(&start_tv, NULL);
	// second barrier waits for child threads to finish
	sleep(60);
	// sleep(10);
	// sleep(1);
	stop_signal = true;
	pthread_barrier_wait(&barrier);
	gettimeofday(&end_tv, NULL);
	for (int i = 0; i < nr_threads; ++i) {
		pthread_join(tid[i], NULL);
	}

	double elapsed_time = (end_tv.tv_sec - start_tv.tv_sec) * 1.0 +
			      (end_tv.tv_usec - start_tv.tv_usec) / 1000000.0;
	double usec_time = (end_tv.tv_sec - start_tv.tv_sec) * 1000000.0 +
			   (end_tv.tv_usec - start_tv.tv_usec) * 1.0;
	std::cout << "elapsed_usec_time: " << usec_time << std::endl;
	report(elapsed_time, usec_time / 1000000.0);
	// pthread_barrier_destroy(&barrier);
}

void *BasicClient::thread_test_fun(void *arg)
{
	thread_arg *t_arg = (thread_arg *)arg;
	uint64_t thread_id = t_arg->thread_id;
	uint64_t rpc_idx = t_arg->rpc_idx;
	uint64_t server_id = t_arg->server_id;
	size_t depth = t_arg->depth;
	BasicClient *client = t_arg->client;
	pthread_barrier_t *barrier = t_arg->barrier;

	pthread_barrier_wait(barrier);
	// virtual call, actual impl depends on object type (BasicClient or derived)
	client->thread_test_fun_impl(rpc_idx, thread_id, server_id, depth);
	pthread_barrier_wait(barrier);
	return NULL;
}

// this function is introduced by FUSIONCAS driver modifications; apps must call it
// carves out a portion of client's registered memory as driver rpc buf
void BasicClient::set_shm(void *addr, uint64_t length)
{
	// 1. open shared memory between driver and agent client (hardcoded name/size)
	int shm_fd = shm_open("SHM_RECORD", O_RDWR, S_IRUSR | S_IWUSR);
	if (shm_fd == -1) {
		printf("error open shared memory\n");
	}
	void *shm_ptr = mmap(0, SHM_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED,
			     shm_fd, 0);
	// 2. write to local meta region in shared memory
	local_rpc_meta *local_meta =
		(local_rpc_meta *)&(((sh_local_region *)shm_ptr)->lmeta_);
	// only one server, so it's 0
	local_meta->machine_id = machine_id;
	local_meta->lkey = conns[0]->manager_.get_local_memory_lkey(
		sds::MAIN_MEMORY_MR_ID);
	// if not specified by upper layer, use smart registered buffer
	if (addr == NULL && length == 0) {
		// set local rpc buffer region to 128KB
		local_meta->r_buf_length = 128 * 1024ul / 2;
		local_meta->s_buf_length = 128 * 1024ul / 2;
		local_meta->rpc_addr = conns[0]->get_cache();
		conns[0]->set_cache(local_meta->r_buf_length +
				    local_meta->s_buf_length);
	} else {
		local_meta->s_buf_length = length / 2;
		local_meta->r_buf_length = length / 2;
		local_meta->rpc_addr = addr;
	}
	msync(local_meta, sizeof(local_rpc_meta), MS_SYNC);
	SDS_INFO(
		"sync shm on client, local rpc addr: %llu, remote rpc addr: %llu",
		local_meta->rpc_addr,
		((sh_local_region *)shm_ptr)->rmeta_.rpc_addr);
	std::cout << "rmeta_ offset: "
		  << ((uint64_t) & (((sh_local_region *)shm_ptr)->rmeta_) -
					   (uint64_t)shm_ptr)
		  << std::endl;
	close(shm_fd);
	munmap(shm_ptr, SHM_SIZE);
	return;
}

void BasicClient::report(uint64_t elapsed_time, double usec_time)
{
	std::cout << "total_attempts: " << total_attempts << std::endl;
	std::cout << "elapsed_time: " << elapsed_time << std::endl;
	// auto bandwidth = total_attempts * block_size / usec_time / 1024.0 / 1024.0;
	auto throughput = total_attempts / usec_time / 1000000.0;
	// auto throughput = 111;
	// SDS_INFO("%s: #threads=%d, #depth=%ld, #block_size=%ld, BW=%.3lf MB/s, IOPS=%.3lf M/s, conn establish time=%.3lf ms",
	//          dump_prefix.c_str(), nr_threads, depth, block_size, bandwidth, throughput, connect_time);
	SDS_INFO("IOPS=%.3lf M/s", throughput);
	// SDS_INFO("report finished!");
}