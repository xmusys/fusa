#include "basic_client.h"
#include "ycsb_loader.h"
#include <random>
#include <mutex>
#include <algorithm>

#define YCSB_LATENCY

class YCSBEXPClient : public BasicClient {
    public:
	uint64_t *keys;
	char *methods;
	double *latencies;
	uint64_t lat_idx;
	std::vector<double> thread_tt_latencies;
	double tt_avg_latencies;
	std::mutex mtx;
	std::atomic<int> my_barrier;

    public:
	YCSBEXPClient(size_t nr_memory_nodes, size_t machine_num,
		      size_t machine_id, size_t depth, uint64_t attempts_num)
		: BasicClient(nr_memory_nodes, machine_num, machine_id, depth,
			      attempts_num)
	{
#ifdef YCSB_LATENCY
		latencies = NULL;
		lat_idx = 0;
		for (int i = 0; i < __MAX_RECORDER_NUM; i++) {
			thread_tt_latencies.push_back(0);
		}
		my_barrier = 0;
		tt_avg_latencies = 0;
#endif
	}

	~YCSBEXPClient()
	{
		if (keys != NULL)
			free(keys);
		if (methods != NULL)
			free(methods);
		if (latencies != NULL)
			free(latencies);
	}

	// only load keys and methods for now
	void ycsb_load(std::string &ycsb_path)
	{
		total_retry_cnt = 0;
		keys = NULL;
		methods = NULL;
		YCSBLoader ycsb;
		keys = (uint64_t *)malloc(sizeof(uint64_t) * attempts_num);
		methods = (char *)malloc(sizeof(char) * attempts_num);
		ycsb.load(ycsb_path, methods, keys, attempts_num);
	}

	void client_report(uint64_t report_thread, uint64_t thread_id,
			   std::vector<double> &tl_latencies,
			   double tl_avg_latency)
	{
#ifdef YCSB_LATENCY
		// SDS_INFO("%d, avg: %.3f", thread_id, tl_avg_latency);
		if (thread_id == report_thread) {
			latencies = (double *)malloc(sizeof(double) *
						     total_attempts.load());
			memset(latencies, 0,
			       sizeof(double) * total_attempts.load());
		}
		my_barrier.fetch_add(1, std::memory_order_relaxed);
		while (my_barrier % nr_threads != 0) {
			;
		}
		mtx.lock();
		tt_avg_latencies += tl_avg_latency;
		for (int i = 0; i < tl_latencies.size(); i++) {
			latencies[lat_idx] = tl_latencies[i];
			lat_idx++;
		}
		mtx.unlock();
		my_barrier.fetch_add(1, std::memory_order_relaxed);
		while (my_barrier % nr_threads != 0) {
			;
		}
		if (thread_id == report_thread) {
			SDS_INFO("Herd Clinet report retry");
			SDS_INFO("total retry cnt: %llu",
				 total_retry_cnt.load());
			tt_avg_latencies = tt_avg_latencies / nr_threads;
			SDS_INFO("lat_idx: %llu, total attempts: %llu", lat_idx,
				 total_attempts.load());

			std::sort(latencies, latencies + lat_idx);
			uint64_t p50 = 0.5 * lat_idx;
			uint64_t p99 = 0.99 * lat_idx;
			uint64_t p999 = 0.999 * lat_idx;
			uint64_t p9999 = 0.9999 * lat_idx;
			SDS_INFO("Herd Clinet report Latencies");
			SDS_INFO("Avg\tmin\tP50\tP99\tP999\tmax");
			SDS_INFO("%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t",
				 tt_avg_latencies, latencies[0], latencies[p50],
				 latencies[p99], latencies[p999],
				 latencies[total_attempts.load() - 1]);
		}
		// my_barrier.fetch_add(1, std::memory_order_relaxed);
		// while(my_barrier % nr_threads != 0) {
		//     ;
		// }
#else
		SDS_INFO("YCSB Clinet report retry");
		SDS_INFO("total retry cnt: %llu", total_retry_cnt.load());
#endif
	}

	int poll_n_latency(
		size_t n, ibv_cq *cq, ibv_wc *rwc,
		std::vector<std::chrono::high_resolution_clock::time_point>
			&ends)
	{
		int tt_comps = 0;
		int tt_try = 0;
		while (tt_comps < n) {
			int new_comps =
				ibv_poll_cq(cq, n - tt_comps, &rwc[tt_comps]);
			if (new_comps > 0) {
				if (rwc[tt_comps].status != 0) {
					fprintf(stderr,
						"Bad poll %d wc status %d\n",
						rwc[tt_comps].opcode,
						rwc[tt_comps].status);
					exit(0);
				}
				for (int i = tt_comps; i < tt_comps + new_comps;
				     i++) {
					// 200000 is recv wr_id base, 20000 is nops wr_id base
					if (rwc[i].wr_id < 20000)
						ends[rwc[i].wr_id] = std::chrono::
							high_resolution_clock::
								now();
				}
				tt_comps += new_comps;
			}
		}
		return tt_comps;
	}

	void thread_test_fun_impl(uint64_t local_rpc_idx, uint64_t thread_id,
				  uint64_t server_id, size_t depth) override
	{
		uint64_t mod_thread_id = thread_id % nr_threads;
		// derived class virtual function implementation
		SDS_INFO(
			"YCSBEXPClient thread_test_fun_impl() executed for thread %d and server %d",
			thread_id, server_id);
		// actual thread logic
		uint64_t attempts = 0;
		struct ibv_send_wr wr[depth], *bad_wr[depth];
		struct ibv_sge sge[depth];
		struct ibv_wc wc[depth];
		// uint64_t cmp = 999;
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
		uint64_t thread_attepmts_num =
			attempts_num / (machine_num * nr_threads);
		uint64_t thread_global_id =
			machine_id * nr_threads + thread_id % nr_threads;
		*(buf) = 999;
		// while(attempts < thread_attepmts_num) {
		uint64_t cmp[depth];
		uint64_t swap[depth];
		for (int i = 0; i < depth; i++) {
			cmp[i] = 0;
			swap[i] = 0;
		}
		uint64_t tl_retry_cnt = 0;
		uint64_t header = 0;
		header = thread_global_id << 32;

		std::vector<std::chrono::high_resolution_clock::time_point>
			starts;
		std::vector<std::chrono::high_resolution_clock::time_point> ends;
#ifdef YCSB_LATENCY
		starts.resize(depth);
		ends.resize(depth);
		uint64_t nops_wr_base;

		std::vector<double> tl_latencies;
		tl_latencies.reserve(100000000);
#endif
		while (!stop_signal) {
			// while(attempts < thread_attepmts_num) {
			for (int i = 0; i < depth; i++) {
				// remote_addr.offset = keys[thread_attepmts_num * thread_global_id + attempts + i] * 8;
				// remote_addr.offset = keys[(attempts + i) % attempts_num] * 8;
				remote_addr.offset =
					keys[thread_attepmts_num *
						     thread_global_id +
					     (attempts + i) %
						     thread_attepmts_num] *
					8;
				if (methods[thread_attepmts_num *
						    thread_global_id +
					    (attempts +
					     i) % thread_attepmts_num] == 'u') {
					add_request(wr[i], sge[i], i,
						    IBV_WR_RDMA_READ, buf + i,
						    remote_addr, rkey,
						    remote_va, sizeof(uint64_t),
						    0, 0, IBV_SEND_SIGNALED,
						    conns[server_id]->manager_);
					// add_request(wr[i], sge[i], i, IBV_WR_ATOMIC_CMP_AND_SWP, buf + i, remote_addr, rkey, remote_va, sizeof(uint64_t), cmp, swap,
					// IBV_SEND_SIGNALED, conns[server_id]->manager_);
				} else if (methods[thread_attepmts_num *
							   thread_global_id +
						   (attempts +
						    i) % thread_attepmts_num] ==
					   'r') {
					add_request(wr[i], sge[i], i,
						    IBV_WR_RDMA_READ, buf + i,
						    remote_addr, rkey,
						    remote_va, sizeof(uint64_t),
						    0, 0, IBV_SEND_SIGNALED,
						    conns[server_id]->manager_);
				}
#ifdef YCSB_LATENCY
				starts[i] =
					std::chrono::high_resolution_clock::now();
#endif
				int ret = ibv_post_send(
					conns[server_id]->manager_.get_qp_ptr(
						mod_thread_id),
					&wr[i], &bad_wr[i]);
				// std::cout << "buf " << i << " " << *(buf+i) << std::endl;
				if (ret < 0)
					SDS_PERROR("send request error...");
			}
			// poll read cqe
#ifdef YCSB_LATENCY
			int comps = poll_n_latency(
				depth,
				conns[server_id]->manager_.get_cq_ptr(
					mod_thread_id),
				wc, ends);
#else
			int comps =
				poll_n(depth,
				       conns[server_id]->manager_.get_cq_ptr(
					       mod_thread_id),
				       wc);
#endif
			// send update request
			size_t update_cnt = 0;
			uint64_t tmp_cmp[depth];
			for (int i = 0; i < depth; i++) {
				if (methods[thread_attepmts_num *
						    thread_global_id +
					    (attempts +
					     i) % thread_attepmts_num] == 'u') {
					update_cnt++;
					// cmp = thread_global_id;
					// cmp = (attempts == 0 ? 0 : (attempts / depth + i - 1));
					// RDMA READ value as compare
					cmp[i] = *(buf + i);
					tmp_cmp[i] = cmp[i];
					swap[i] =
						header | (attempts / depth + i);
					// std::cout << "thread_global_id: " << thread_global_id << " attempts: " << attempts << " cmp: " << cmp << std::endl;
					add_request(wr[i], sge[i], i,
						    IBV_WR_ATOMIC_CMP_AND_SWP,
						    buf + i, remote_addr, rkey,
						    remote_va, sizeof(uint64_t),
						    cmp[i], swap[i],
						    IBV_SEND_SIGNALED,
						    conns[server_id]->manager_);
					int ret = ibv_post_send(
						conns[server_id]
							->manager_.get_qp_ptr(
								mod_thread_id),
						&wr[i], &bad_wr[i]);
					// std::cout << "buf " << i << " " << *(buf+i) << std::endl;
					if (ret < 0)
						SDS_PERROR(
							"send request error...");
				}
			}
#ifdef YCSB_LATENCY
			comps = poll_n_latency(
				update_cnt,
				conns[server_id]->manager_.get_cq_ptr(
					mod_thread_id),
				wc, ends);
#else
			comps = poll_n(update_cnt,
				       conns[server_id]->manager_.get_cq_ptr(
					       mod_thread_id),
				       wc);
#endif

			// ensure update request succeeds
			while (1) {
				uint64_t need_retry = 0;
				for (int i = 0; i < depth; i++) {
					// retry
					if (methods[thread_attepmts_num *
							    thread_global_id +
						    (attempts +
						     i) % thread_attepmts_num] ==
					    'u') {
						remote_addr.offset =
							keys[(attempts + i) %
							     attempts_num] *
							8;
						uint64_t resp = *(buf + i);
						// if CAS result value is wrong
						if (resp != tmp_cmp[i] ||
						    wc[i].imm_data ==
							    ntohl(__RETRY_IMM_CODE)) {
							// std::cout << "attmepts: " << attempts << " resp header: " << (resp >> 32) << " resp body: " << (resp & 0xffffffff) <<  " expected: " << (attempts / depth + i) << std::endl;
							need_retry++;
							cmp[i] = resp;
							tmp_cmp[i] = cmp[i];
							swap[i] =
								header |
								(attempts /
									 depth +
								 i);
							add_request(
								wr[i], sge[i],
								i,
								IBV_WR_ATOMIC_CMP_AND_SWP,
								buf + i,
								remote_addr,
								rkey, remote_va,
								sizeof(uint64_t),
								cmp[i], swap[i],
								IBV_SEND_SIGNALED,
								conns[server_id]
									->manager_);
							int ret = ibv_post_send(
								conns[server_id]
									->manager_
									.get_qp_ptr(
										mod_thread_id),
								&wr[i],
								&bad_wr[i]);
							if (ret < 0)
								SDS_PERROR(
									"send request error...");
						}
					}
				}
				tl_retry_cnt += need_retry;
				if (need_retry > 0) {
#ifdef YCSB_LATENCY
					int cc = poll_n_latency(
						need_retry,
						conns[server_id]
							->manager_.get_cq_ptr(
								mod_thread_id),
						wc, ends);
#else
					int cc = poll_n(
						need_retry,
						conns[server_id]
							->manager_.get_cq_ptr(
								mod_thread_id),
						wc);
#endif
				} else
					break;
				// depth set to 1 for now, would need changes for >1
			}
#ifdef YCSB_LATENCY
			for (int i = 0; i < depth; i++) {
				// thread_tt_latencies[thread_id % nr_threads] += latencies[(thread_id % nr_threads) * thread_attempts_num + attempts + i];
				double lat = std::chrono::duration<double,
								   std::micro>(
						     ends[i] - starts[i])
						     .count();
				tl_latencies.push_back(lat);
				thread_tt_latencies[mod_thread_id] += lat;
			}
#endif
			attempts += depth;
		}
		total_retry_cnt.fetch_add(tl_retry_cnt);
		total_attempts.fetch_add(attempts);
		// std::cout << "attempts: " << attempts << std::endl;
		uint64_t report_thread = 0;
#ifdef YCSB_LATENCY
		double tl_avg_latency =
			thread_tt_latencies[mod_thread_id] / (double)attempts;
		my_barrier.fetch_add(1, std::memory_order_relaxed);
		while (my_barrier != nr_threads) {
			;
		}
		SDS_INFO("start report, report_thread: %llu", report_thread);
		// std::cout << "attempts: " << attempts << std::endl;
		client_report(report_thread, thread_id, tl_latencies,
			      tl_avg_latency);
#else
		std::vector<double> nouse;
		client_report(report_thread, thread_id, nouse, 0);
#endif
	}
};