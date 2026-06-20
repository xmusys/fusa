#include "basic_client.h"

class CASClient : public BasicClient {
    public:
	CASClient(size_t nr_memory_nodes, size_t machine_num, size_t machine_id,
		  size_t depth, uint64_t attempts_num)
		: BasicClient(nr_memory_nodes, machine_num, machine_id, depth,
			      attempts_num)
	{
	}

	void thread_test_fun_impl(uint64_t local_rpc_idx, uint64_t thread_id,
				  uint64_t server_id, size_t depth) override
	{
		// derived class virtual function implementation
		SDS_INFO(
			"BasicClient thread_test_fun_impl() executed for thread %d and server %d",
			thread_id, server_id);
		// actual thread logic
		uint64_t attempts = 0;
		struct ibv_send_wr wr[depth], *bad_wr[depth];
		struct ibv_sge sge[depth];
		struct ibv_wc wc[depth];
		uint64_t cmp = 999;
		uint32_t rkey;
		uint32_t lkey =
			conns[server_id]->manager_.get_local_memory_lkey(0);
		uintptr_t remote_va;
		sds::GlobalAddress remote_addr(0, 0);

		conns[server_id]->manager_.get_remote_memory_key(
			remote_addr.node, remote_addr.mr_id, remote_addr.offset,
			0, rkey, remote_va);
		uint64_t *buf = (uint64_t *)conns[server_id]->alloc_cache(
			sizeof(uint64_t) * depth);
		// first 128KB is the rpc region
		remote_va = remote_va + 128 * 1024ul;
		uint64_t thread_attepmts_num =
			attempts_num / (machine_num * nr_threads);
		uint64_t thread_global_id =
			machine_id * nr_threads + thread_id % nr_threads;
		*(buf) = 999;
		while (attempts < thread_attepmts_num) {
			for (int i = 0; i < depth; i++) {
				// remote_addr.offset = attempts * 4096;
				remote_addr.offset = thread_global_id * 8;
				add_request(wr[i], sge[i], i,
					    IBV_WR_ATOMIC_CMP_AND_SWP, buf + i,
					    remote_addr, rkey, remote_va,
					    sizeof(uint64_t), 0, attempts + i,
					    IBV_SEND_SIGNALED,
					    conns[server_id]->manager_);
				int ret = ibv_post_send(
					conns[server_id]->manager_.get_qp_ptr(
						thread_id),
					&wr[i], &bad_wr[i]);
				// std::cout << "buf " << i << " " << *(buf+i) << std::endl;
				if (ret < 0)
					SDS_PERROR("send request error...");
			}
			int comps =
				poll_n(depth,
				       conns[server_id]->manager_.get_cq_ptr(
					       thread_id),
				       wc);
			attempts += depth;
		}
		total_attempts.fetch_add(attempts);
		// std::cout << "attempts: " << attempts << std::endl;
	}
};