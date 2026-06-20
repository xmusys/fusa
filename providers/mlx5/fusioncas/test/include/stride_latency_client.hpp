#include "basic_client.h"
#include <algorithm>

class StrideLatencyClient : public BasicClient {
    public:
	uint64_t stride;
	double *latencies;
	std::vector<double> thread_tt_latencies;

    public:
	StrideLatencyClient(size_t nr_memory_nodes, size_t machine_num,
			    size_t machine_id, size_t depth,
			    uint64_t attempts_num)
		: BasicClient(nr_memory_nodes, machine_num, machine_id, depth,
			      attempts_num)
	{
		latencies = NULL;
	}

	void set_stride(uint64_t stride_)
	{
		stride = stride_;
		SDS_INFO("Stride is set to %llu", stride);
	}

	int poll_n_latency(
		size_t n, ibv_cq *cq, ibv_wc *wc,
		std::vector<std::chrono::high_resolution_clock::time_point>
			&ends)
	{
		int tt_comps = 0;
		while (tt_comps < n) {
			int new_comps =
				ibv_poll_cq(cq, n - tt_comps, &wc[tt_comps]);
			if (new_comps > 0) {
				if (wc[tt_comps].status != 0) {
					fprintf(stderr,
						"Bad poll %d wc status %d\n",
						wc[tt_comps].opcode,
						wc[tt_comps].status);
					while (1)
						;
					exit(0);
				}
				for (int i = tt_comps; i < tt_comps + new_comps;
				     i++) {
					ends[wc[i].wr_id] = std::chrono::
						high_resolution_clock::now();
				}
				tt_comps += new_comps;
			}
		}
		return tt_comps;
	}

	~StrideLatencyClient()
	{
		if (latencies != NULL)
			free(latencies);
	}

	void alloc_latencies()
	{
		thread_tt_latencies.resize(__MAX_RECORDER_NUM);
		latencies = (double *)malloc(sizeof(double) * attempts_num);
		memset(latencies, 0, sizeof(double) * attempts_num);
	}

	void report_latencies()
	{
		std::cout << "---CAS Latencies---" << std::endl;
		std::cout << "Avg\tMIN\tP50\tP99\tP99.9\tP99.99\tMAX"
			  << std::endl;
		double tt_latencies = 0;
		for (int i = 0; i < nr_threads; i++) {
			tt_latencies += thread_tt_latencies[i];
		}

		std::sort(latencies, latencies + attempts_num);
		uint64_t p50 = 0.5 * attempts_num;
		uint64_t p99 = 0.99 * attempts_num;
		uint64_t p999 = 0.999 * attempts_num;
		uint64_t p9999 = 0.9999 * attempts_num;
		std::cout << tt_latencies / attempts_num << "\t" << latencies[0]
			  << "\t" << latencies[p50] << "\t";
		std::cout << latencies[p99] << "\t" << latencies[p999] << "\t"
			  << latencies[p9999] << "\t"
			  << latencies[attempts_num - 1] << std::endl;
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
		// used when total is 100M
		// uint64_t thread_attepmts_num = attempts_num / (machine_num * nr_threads);
		uint64_t thread_attempts_num = attempts_num / nr_threads;
		uint64_t thread_global_id =
			machine_id * nr_threads + thread_id % nr_threads;
		*(buf) = 999;
		cmp = 0;
		uint64_t swap = 0;

		std::vector<std::chrono::high_resolution_clock::time_point>
			starts;
		std::vector<std::chrono::high_resolution_clock::time_point> ends;

		starts.resize(depth);
		ends.resize(depth);
		while (attempts < thread_attempts_num) {
			// while(!stop_signal) {
			for (int i = 0; i < depth; i++) {
				// remote_addr.offset = attempts * 4096;
				// remote_addr.offset = (thread_attepmts_num * thread_global_id + attempts + i) * 8;
				remote_addr.offset =
					(thread_global_id * depth + i) * stride;
				cmp = (attempts == 0 ?
					       0 :
					       (attempts / depth + i - 1));
				swap = attempts / depth + i;
				add_request(wr[i], sge[i], i,
					    IBV_WR_ATOMIC_CMP_AND_SWP, buf + i,
					    remote_addr, rkey, remote_va,
					    sizeof(uint64_t), cmp, swap,
					    IBV_SEND_SIGNALED,
					    conns[server_id]->manager_);
				starts[i] =
					std::chrono::high_resolution_clock::now();
				int ret = ibv_post_send(
					conns[server_id]->manager_.get_qp_ptr(
						thread_id),
					&wr[i], &bad_wr[i]);
				// std::cout << "buf " << i << " " << *(buf+i) << std::endl;
				if (ret < 0)
					SDS_PERROR("send request error...");
			}
			int comps = poll_n_latency(
				depth,
				conns[server_id]->manager_.get_cq_ptr(
					thread_id),
				wc, ends);
			for (int i = 0; i < depth; i++) {
				latencies[(thread_id % nr_threads) *
						  thread_attempts_num +
					  attempts + i] =
					std::chrono::duration<double,
							      std::micro>(
						ends[i] - starts[i])
						.count();
				thread_tt_latencies[thread_id % nr_threads] +=
					latencies[(thread_id % nr_threads) *
							  thread_attempts_num +
						  attempts + i];
			}
			attempts += depth;
		}
		total_attempts.fetch_add(attempts);
		// std::cout << "attempts: " << attempts << std::endl;
	}
};