#include "basic_client.h"
#include "ycsb_loader.h"
#include <random>
#include <mutex>
#include <algorithm>

#define YCSB_LATENCY

using namespace std::placeholders;

int coro_add_request(struct ibv_send_wr &wr, struct ibv_sge &sge,
		     uint64_t wr_id, ibv_wr_opcode opcode, void *local,
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

// Coroutine 0 in each thread does polling
void PollCompletion(coro_yield_t &yield, CoroutineScheduler *coro_sched,
		    bool *stop_run)
{
	while (true) {
		coro_sched->PollCompletion(); // get ACK
		Coroutine *next = coro_sched->coro_head->next_coro;
		if (next->coro_id != POLL_ROUTINE_ID) // if not self, execute
		{
			// RDMA_LOG(DBG) << "Coro 0 yields to coro " << next->coro_id;
			coro_sched->RunCoroutine(yield, next); // yield to next
		}
		if (coro_sched->stop_run == true)
			break;
	}
}

void run_task(coro_yield_t &yield, coro_id_t coro_id, coro_args *arg,
	      bool *stop_signal)
{
	uint64_t *keys = arg->keys;
	char *methods = arg->methods;
	sds::Initiator *conn = arg->conns;

	uint32_t rkey;
	uint32_t lkey = conn->manager_.get_local_memory_lkey(0);
	uintptr_t remote_va;
	sds::GlobalAddress remote_addr(0, 0);

	// first 128KB is the rpc region

	conn->manager_.get_remote_memory_key(remote_addr.node,
					     remote_addr.mr_id,
					     remote_addr.offset, 0, rkey,
					     remote_va);

	remote_va = remote_va + 128 * 1024ul;

	uint64_t buf_len = arg->buf_len;
	uint64_t *cmp_buf = arg->rdma_buf;
	uint64_t *read_buf = arg->rdma_buf + buf_len / 2;

	uint64_t thread_id = arg->thread_id;
	uint64_t attempts = 0;
	uint64_t retry = 0;
	uint64_t coro_start = arg->coro_start;
	uint64_t coro_attempts_num = arg->coro_attempts_num;
	CoroutineScheduler *coro_sched = arg->coro_sched;
#ifdef YCSB_LATENCY
	std::chrono::high_resolution_clock::time_point start, end;
	std::vector<double> *tl_latencies = arg->tl_latencies;
	double tl_tt_latency = 0;
#endif
	struct ibv_qp *qp = conn->manager_.get_qp_ptr(thread_id);
	while ((*stop_signal) == false) {
		struct ibv_send_wr wr, *bad_wr;
		struct ibv_sge sge;
		remote_addr.offset =
			keys[coro_start + attempts % coro_attempts_num] * 8;
#ifdef YCSB_LATENCY
		start = std::chrono::high_resolution_clock::now();
#endif
		// rdma read
		if (methods[coro_start + attempts % coro_attempts_num] == 'u') {
			coro_add_request(wr, sge, coro_id, IBV_WR_RDMA_READ,
					 read_buf, remote_addr, rkey, remote_va,
					 sizeof(uint64_t), 0, 0,
					 IBV_SEND_SIGNALED, conn->manager_);
		} else if (methods[coro_start + attempts % coro_attempts_num] ==
			   'r') {
			coro_add_request(wr, sge, coro_id, IBV_WR_RDMA_READ,
					 read_buf, remote_addr, rkey, remote_va,
					 sizeof(uint64_t), 0, 0,
					 IBV_SEND_SIGNALED, conn->manager_);
		}

		// int ret = ibv_post_send(qp, &wr, &bad_wr);
		coro_sched->RDMASend(coro_id, qp, &wr, &bad_wr);
		coro_sched->Yield(yield, coro_id);

		uint64_t header = (arg->thread_gid << 32);
		uint64_t cmp = *read_buf;
		uint64_t swap = header | attempts;
		if (methods[coro_start + attempts % coro_attempts_num] == 'u') {
			coro_add_request(wr, sge, coro_id,
					 IBV_WR_ATOMIC_CMP_AND_SWP, cmp_buf,
					 remote_addr, rkey, remote_va,
					 sizeof(uint64_t), cmp, swap,
					 IBV_SEND_SIGNALED, conn->manager_);
			// ret = ibv_post_send(qp, &wr, &bad_wr);
			coro_sched->RDMASend(coro_id, qp, &wr, &bad_wr);

			coro_sched->Yield(yield, coro_id);

			while (cmp != *cmp_buf) {
				retry++;
				// re-read
				coro_add_request(wr, sge, coro_id,
						 IBV_WR_RDMA_READ, read_buf,
						 remote_addr, rkey, remote_va,
						 sizeof(uint64_t), 0, 0,
						 IBV_SEND_SIGNALED,
						 conn->manager_);
				coro_sched->RDMASend(coro_id, qp, &wr, &bad_wr);
				coro_sched->Yield(yield, coro_id);
				// re-cas
				cmp = *read_buf;
				coro_add_request(wr, sge, coro_id,
						 IBV_WR_ATOMIC_CMP_AND_SWP,
						 cmp_buf, remote_addr, rkey,
						 remote_va, sizeof(uint64_t),
						 cmp, swap, IBV_SEND_SIGNALED,
						 conn->manager_);
				// ret = ibv_post_send(conn->manager_.get_qp_ptr(thread_id), &wr, &bad_wr);
				coro_sched->RDMASend(coro_id, qp, &wr, &bad_wr);
				coro_sched->Yield(yield, coro_id);
			}
		}
#ifdef YCSB_LATENCY
		end = std::chrono::high_resolution_clock::now();
		double lat =
			std::chrono::duration<double, std::micro>(end - start)
				.count();
		tl_latencies->push_back(lat);
		tl_tt_latency += lat;
#endif
		attempts++;
	}
	FAA(&coro_sched->finish, 1);
	if (coro_sched->finish == arg->coro_num - 1) {
		coro_sched->stop_run = true;
	}
#ifdef YCSB_LATENCY
	*(arg->tl_tt_latency) += tl_tt_latency;
#endif
	// std::cout << coro_id << " " << attempts << std::endl;
	uint64_t *finished_attempts = arg->finished_attempts;
	uint64_t *retry_cnt = arg->retry_cnt;
	// *finished_attempts = 0;
	*finished_attempts = attempts;
	*retry_cnt = retry;
	coro_sched->FinishYield(yield, coro_id);
}

class CASRetryClient : public BasicClient {
    public:
	uint64_t *keys;
	char *methods;
	double *latencies;
	uint64_t lat_idx;
	std::vector<double> thread_tt_latencies;
	double tt_avg_latencies;
	std::mutex mtx;
	std::atomic<int> my_barrier;

	uint64_t tt_hotness[__MAX_COUNTER_NUM];
	double tt_slot_latencies[__MAX_COUNTER_NUM];
	double sum_vec_latency;

    public:
	CASRetryClient(size_t nr_memory_nodes, size_t machine_num,
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
		memset(tt_hotness, 0, sizeof(uint64_t) * __MAX_COUNTER_NUM);
		memset(tt_slot_latencies, 0,
		       sizeof(double) * __MAX_COUNTER_NUM);
	}

	~CASRetryClient()
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

	void print_hotness_latency()
	{
#ifdef YCSB_LATENCY
		// set to 10s for now, parameterize later
		uint64_t run_time = 10;
		std::cout << "start print latency" << std::endl;
		for (int i = 1; i <= __MAX_COUNTER_NUM; i++) {
			std::cout
				<< tt_slot_latencies[i - 1] / tt_hotness[i - 1]
				<< " ";
			if (i % 32 == 0) {
				std::cout << std::endl;
			}
		}
		std::cout << "start print hotness" << std::endl;
		for (int i = 1; i <= __MAX_COUNTER_NUM; i++) {
			std::cout << tt_hotness[i - 1] / 10 << " ";
			if (i % 32 == 0) {
				std::cout << std::endl;
			}
		}
#else
		SDS_INFO("no hotness and latency info");
#endif
	}

	void client_report(uint64_t report_thread, uint64_t thread_id,
			   std::vector<double> &tl_latencies,
			   double tl_avg_latency, uint64_t *tl_hotness,
			   double *tl_slot_latency)
	{
#ifdef YCSB_LATENCY
		// SDS_INFO("%d, avg: %.3f", thread_id, tl_avg_latency);
		sum_vec_latency = 0;
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
			sum_vec_latency += tl_latencies[i];
			latencies[lat_idx] = tl_latencies[i];
			lat_idx++;
		}
		// hotness and slot latency
		// for (int i = 0; i < __MAX_COUNTER_NUM; i++) {
		// 	tt_hotness[i] += tl_hotness[i];
		// 	tt_slot_latencies[i] += tl_slot_latency[i];
		// }
		mtx.unlock();
		my_barrier.fetch_add(1, std::memory_order_relaxed);
		while (my_barrier % nr_threads != 0) {
			;
		}
		if (thread_id == report_thread) {
			SDS_INFO("CASRetry Clinet report retry");
			SDS_INFO("total retry cnt: %llu",
				 total_retry_cnt.load());
			tt_avg_latencies = tt_avg_latencies / nr_threads;
			SDS_INFO("lat_idx: %llu, total attempts: %llu", lat_idx,
				 total_attempts.load());
			// print_hotness_latency();
			std::sort(latencies, latencies + lat_idx);
			uint64_t p50 = 0.5 * lat_idx;
			uint64_t p99 = 0.99 * lat_idx;
			uint64_t p999 = 0.999 * lat_idx;
			uint64_t p9999 = 0.9999 * lat_idx;
			SDS_INFO("Herd Clinet report Latencies");
			SDS_INFO("Avg\tmin\tP50\tP99\tP999\tmax");
			SDS_INFO("%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t",
				 sum_vec_latency / lat_idx, latencies[0],
				 latencies[p50], latencies[p99],
				 latencies[p999],
				 latencies[total_attempts.load() - 1]);
			SDS_INFO("sum of latencies: %.3f", sum_vec_latency);
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
		double tl_tt_latency = 0;
		tl_latencies.reserve(100000000);
#endif
		uint64_t tl_hotness[__MAX_COUNTER_NUM] = { 0 };
		double tl_slot_latency[__MAX_COUNTER_NUM] = { 0 };

		// update-coroutine
		bool stop_run = false;
		bool *stop_ptr = &stop_signal;
		uint64_t coro_num = depth + 1;
		FastRandom *random_generator = NULL;
		CoroutineScheduler *coro_sched =
			new CoroutineScheduler(thread_global_id, coro_num);
		// init coroutine random gens for TPCC benchmark, plus one extra for poll coroutine
		random_generator = new FastRandom[coro_num];
		coro_args *args = new coro_args[coro_num];
		uint64_t coro_attempts_num =
			thread_attepmts_num / (coro_num - 1);
		uint64_t finished_attempts[coro_num];
		uint64_t retry_cnt[coro_num];
		for (coro_id_t coro_i = 0; coro_i < coro_num; coro_i++) {
			uint64_t coro_seed = static_cast<uint64_t>(
				(static_cast<uint64_t>(thread_global_id)
				 << 32) |
				static_cast<uint64_t>(coro_i));
			random_generator[coro_i].SetSeed(coro_seed);
			coro_sched->coro_array[coro_i].coro_id = coro_i;
			//Bind func
			if (coro_i == POLL_ROUTINE_ID) {
				// coro_sched->coro_array[coro_i].func = coro_call_t([this, coro_sched, stop_ptr](coro_yield_t &yield) {
				//     this->PollCompletion(yield, coro_sched, stop_ptr);
				// });
				coro_sched->coro_array[coro_i].func =
					coro_call_t(bind(PollCompletion, _1,
							 coro_sched, stop_ptr));
			} else {
				// each coroutine gets 8KB buffer
				uint64_t *rdma_buf =
					(uint64_t *)conns[server_id]
						->alloc_cache(sizeof(uint64_t) *
							      1024);
				args[coro_i].server_id = server_id;
				args[coro_i].thread_gid = thread_global_id;
				args[coro_i].thread_id = thread_id;
				args[coro_i].coro_start =
					thread_attepmts_num * thread_global_id +
					coro_attempts_num * (coro_i - 1);
				args[coro_i].coro_attempts_num =
					coro_attempts_num;
				args[coro_i].rdma_buf = rdma_buf;
				args[coro_i].buf_len = sizeof(uint64_t) * 1024;
				args[coro_i].coro_sched = coro_sched;
				args[coro_i].keys = keys;
				args[coro_i].methods = methods;
				args[coro_i].conns = conns[server_id];
				args[coro_i].coro_num = coro_num;
				args[coro_i].finished_attempts =
					&finished_attempts[coro_i];
				args[coro_i].retry_cnt = &retry_cnt[coro_i];
#ifdef YCSB_LATENCY
				args[coro_i].tl_latencies = &tl_latencies;
				args[coro_i].tl_tt_latency = &tl_tt_latency;
#else
				args[coro_i].tl_latencies = NULL;
				args[coro_i].tl_tt_latency = NULL;
#endif
				coro_args *arg_ptr = &args[coro_i];
				// coro_sched->coro_array[coro_i].func = coro_call_t([this, coro_i, arg_ptr, stop_ptr](coro_yield_t &yield) {
				//     this->run_task(yield, coro_i, arg_ptr, stop_ptr);
				// });
				coro_sched->coro_array[coro_i].func =
					coro_call_t(bind(run_task, _1, coro_i,
							 arg_ptr, stop_ptr));
				// std::cout << "global id" << std::endl;
			}
		}

		coro_sched->LoopLinkCoroutine(coro_num);

		coro_sched->coro_array[0].func();

		for (coro_id_t coro_i = POLL_ROUTINE_ID + 1; coro_i < coro_num;
		     coro_i++) {
			attempts += finished_attempts[coro_i];
			tl_retry_cnt += retry_cnt[coro_i];
		}

		//         while(!stop_signal) {
		//         // while(attempts < thread_attepmts_num) {
		//             for(int i = 0; i < depth; i++) {
		//                 // remote_addr.offset = keys[thread_attepmts_num * thread_global_id + attempts + i] * 8;
		//                 // remote_addr.offset = keys[(attempts + i) % attempts_num] * 8;
		//                 remote_addr.offset = keys[thread_attepmts_num * thread_global_id + (attempts + i) % thread_attepmts_num] * 8;
		//                 if(methods[thread_attepmts_num * thread_global_id + (attempts + i) % thread_attepmts_num] == 'u') {
		//                     add_request(wr[i], sge[i], i, IBV_WR_RDMA_READ, buf + i, remote_addr, rkey, remote_va, sizeof(uint64_t), 0, 0,
		//                     IBV_SEND_SIGNALED, conns[server_id]->manager_);
		//                     // add_request(wr[i], sge[i], i, IBV_WR_ATOMIC_CMP_AND_SWP, buf + i, remote_addr, rkey, remote_va, sizeof(uint64_t), cmp, swap,
		//                     // IBV_SEND_SIGNALED, conns[server_id]->manager_);
		//                 }
		//                 else if(methods[thread_attepmts_num * thread_global_id + (attempts + i) % thread_attepmts_num] == 'r'){
		//                     add_request(wr[i], sge[i], i, IBV_WR_RDMA_READ, buf + i, remote_addr, rkey, remote_va, sizeof(uint64_t), 0, 0,
		//                     IBV_SEND_SIGNALED, conns[server_id]->manager_);
		//                 }
		// #ifdef YCSB_LATENCY
		//                 starts[i] = std::chrono::high_resolution_clock::now();
		// #endif
		//                 int ret = ibv_post_send(conns[server_id]->manager_.get_qp_ptr(mod_thread_id), &wr[i], &bad_wr[i]);
		//                 // std::cout << "buf " << i << " " << *(buf+i) << std::endl;
		//                 if(ret < 0)
		//                     SDS_PERROR("send request error...");
		//             }
		//             // poll read cqe
		// #ifdef YCSB_LATENCY
		//             int comps = poll_n_latency(depth,  conns[server_id]->manager_.get_cq_ptr(mod_thread_id), wc, ends);
		// #else
		//             int comps = poll_n(depth, conns[server_id]->manager_.get_cq_ptr(mod_thread_id), wc);
		// #endif
		//             // send update request
		//             size_t update_cnt = 0;
		//             uint64_t tmp_cmp[depth];
		//             for(int i = 0;i < depth; i ++) {
		//                 if(methods[thread_attepmts_num * thread_global_id + (attempts + i) % thread_attepmts_num] == 'u') {
		//                     update_cnt ++;
		//                     // cmp = thread_global_id;
		//                     // cmp = (attempts == 0 ? 0 : (attempts / depth + i - 1));
		//                     // RDMA READ value as compare
		//                     cmp[i] = *(buf + i);
		//                     tmp_cmp[i] = cmp[i];
		//                     swap[i] = header | (attempts / depth + i);
		//                     // std::cout << "thread_global_id: " << thread_global_id << " attempts: " << attempts << " cmp: " << cmp << std::endl;
		//                     add_request(wr[i], sge[i], i, IBV_WR_ATOMIC_CMP_AND_SWP, buf + i, remote_addr, rkey, remote_va, sizeof(uint64_t), cmp[i], swap[i],
		//                     IBV_SEND_SIGNALED, conns[server_id]->manager_);
		//                     int ret = ibv_post_send(conns[server_id]->manager_.get_qp_ptr(mod_thread_id), &wr[i], &bad_wr[i]);
		//                     // std::cout << "buf " << i << " " << *(buf+i) << std::endl;
		//                     if(ret < 0)
		//                         SDS_PERROR("send request error...");
		//                 }
		//             }
		// #ifdef YCSB_LATENCY
		//             comps = poll_n_latency(update_cnt,  conns[server_id]->manager_.get_cq_ptr(mod_thread_id), wc, ends);
		// #else
		//             comps = poll_n(update_cnt, conns[server_id]->manager_.get_cq_ptr(mod_thread_id), wc);
		// #endif

		//             // ensure update request succeeds
		//             while(1) {
		//                 uint64_t need_retry = 0;
		//                 for(int i = 0; i < depth; i ++) {
		//                     // retry
		//                     if(methods[thread_attepmts_num * thread_global_id + (attempts + i) % thread_attepmts_num] == 'u') {
		//                         remote_addr.offset = keys[(attempts + i) % attempts_num] * 8;
		//                         uint64_t resp = *(buf + i);
		//                         // if CAS result's upper 32 bits != own thread_global_id or result is wrong
		//                         if(resp != tmp_cmp[i]) {
		//                             // std::cout << "attmepts: " << attempts << " resp header: " << (resp >> 32) << " resp body: " << (resp & 0xffffffff) <<  " expected: " << (attempts / depth + i) << std::endl;
		//                             need_retry ++;
		//                             cmp[i] = resp;
		//                             tmp_cmp[i] = cmp[i];
		//                             swap[i] = header | (attempts / depth + i);
		//                             add_request(wr[i], sge[i], i, IBV_WR_ATOMIC_CMP_AND_SWP, buf + i, remote_addr, rkey, remote_va, sizeof(uint64_t), cmp[i], swap[i],
		//                             IBV_SEND_SIGNALED, conns[server_id]->manager_);
		//                             int ret = ibv_post_send(conns[server_id]->manager_.get_qp_ptr(mod_thread_id), &wr[i], &bad_wr[i]);
		//                             if(ret < 0)
		//                                 SDS_PERROR("send request error...");
		//                         }
		//                     }
		//                 }
		//                 tl_retry_cnt += need_retry;
		//                 if(need_retry > 0) {
		// #ifdef YCSB_LATENCY
		//                      int cc = poll_n_latency(need_retry,  conns[server_id]->manager_.get_cq_ptr(mod_thread_id), wc, ends);
		// #else
		//                      int cc = poll_n(need_retry, conns[server_id]->manager_.get_cq_ptr(mod_thread_id), wc);
		// #endif
		//                 }
		//                 else
		//                     break;
		//                 // depth set to 1 for now, would need changes for >1

		//             }
		// #ifdef YCSB_LATENCY
		//             for(int i = 0; i < depth; i ++) {
		//                 // thread_tt_latencies[thread_id % nr_threads] += latencies[(thread_id % nr_threads) * thread_attempts_num + attempts + i];
		//                 double lat = std::chrono::duration<double, std::micro>(ends[i] - starts[i]).count();
		//                 tl_latencies.push_back(lat);
		//                 thread_tt_latencies[mod_thread_id] += lat;
		//                 if(methods[thread_attepmts_num * thread_global_id + (attempts + i) % thread_attepmts_num] == 'u') {
		//                     uint64_t idx_;
		//                     idx_ = ((remote_va + keys[thread_attepmts_num * thread_global_id + (attempts + i) % thread_attepmts_num] * 8) % 4096 )/ 8;
		//                     tl_hotness[idx_] ++;
		//                     tl_slot_latency[idx_] += lat;
		//                 }
		//             }
		// #endif
		//             attempts += depth;
		//         }
		total_retry_cnt.fetch_add(tl_retry_cnt);
		total_attempts.fetch_add(attempts);
		// std::cout << "attempts: " << attempts << std::endl;
		uint64_t report_thread = 0;
#ifdef YCSB_LATENCY
		thread_tt_latencies[thread_id] = tl_tt_latency;
		double tl_avg_latency =
			thread_tt_latencies[thread_id] / (double)attempts;
		my_barrier.fetch_add(1, std::memory_order_relaxed);
		while (my_barrier != nr_threads) {
			;
		}
		SDS_INFO("start report, report_thread: %llu", report_thread);
		// std::cout << "attempts: " << attempts << std::endl;
		client_report(report_thread, thread_id, tl_latencies,
			      tl_avg_latency, tl_hotness, tl_slot_latency);
#else
		std::vector<double> nouse;
		client_report(report_thread, thread_id, nouse, 0, NULL, NULL);
#endif
	}
};