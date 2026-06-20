#pragma once
#include "basic_client.h"
#include <algorithm>
#include <chrono>
#include <random>
#include <atomic>
#include <mutex>
#include <thread>

#define YCSB_LATENCY

// DrTM client implemented on fusioncas coroutine framework
// Semantics follow shiftlock/src/baselines/drtm.rs

namespace drtm_fusioncas
{

namespace time {
    static constexpr uint64_t DELTA = 50'000;            // 50us
    static constexpr uint64_t RLEASE_TIME = 1'000'000;   // 1ms
    static constexpr uint64_t TX_TIME = 500'000;         // 0.5ms
    inline uint64_t now() {
        using namespace std::chrono;
        return duration_cast<nanoseconds>(high_resolution_clock::now().time_since_epoch()).count();
    }
    inline bool valid(uint64_t ts) { return now() < ts - DELTA - TX_TIME; }
    inline bool expired(uint64_t ts) { return now() > ts + DELTA; }
}


// 64-bit lock entry layout (LSB->MSB)
// [0] write_lock (1 bit)
// [1..9) owner_id (8 bits)
// [9..64) read_lease (55 bits)
struct DrtmEntry {
	uint64_t bits{ 0 };
	static inline uint8_t write_lock(uint64_t v)
	{
		return v & 0x1;
	}
	static inline uint8_t owner_id(uint64_t v)
	{
		return (v >> 1) & 0xFFu;
	}
	static inline uint64_t read_lease(uint64_t v)
	{
		return (v >> 9);
	}
	static inline uint64_t state_locked(uint8_t id)
	{
		return (1ull) | (static_cast<uint64_t>(id) << 1);
	}
	static inline uint64_t state_read_leased(uint64_t end_time)
	{
		return (end_time << 9);
	}
};

// helpers mirrored from cas_retry_client.hpp but scoped here
static inline int
coro_add_request(ibv_send_wr &wr, ibv_sge &sge, uint64_t wr_id,
		 ibv_wr_opcode opcode, void *local,
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
	wr.next = nullptr;
	if (opcode == IBV_WR_ATOMIC_FETCH_AND_ADD ||
	    opcode == IBV_WR_ATOMIC_CMP_AND_SWP) {
		wr.wr.atomic.rkey = rkey;
		wr.wr.atomic.remote_addr = remote_va + remote.offset;
		wr.wr.atomic.compare_add = compare_add;
		wr.wr.atomic.swap = swap;
	} else if (opcode == IBV_WR_RDMA_READ || opcode == IBV_WR_RDMA_WRITE) {
		wr.wr.rdma.rkey = rkey;
		wr.wr.rdma.remote_addr = remote_va + remote.offset;
	}
	if (opcode == IBV_WR_RDMA_WRITE &&
	    length <= manager_.config().max_inline_data) {
		wr.send_flags |= IBV_SEND_INLINE;
	}
	return 0;
}

// Coroutine 0 polls completions (same pattern as cas_retry_client.hpp)
static inline void PollCompletion(coro_yield_t &yield,
				  CoroutineScheduler *coro_sched,
				  bool *stop_run)
{
	while (true) {
		coro_sched->PollCompletion();
		Coroutine *next = coro_sched->coro_head->next_coro;
		if (next->coro_id != POLL_ROUTINE_ID) {
			coro_sched->RunCoroutine(yield, next);
		}
		if (coro_sched->stop_run == true)
			break;
	}
}

class DrtmClient : public BasicClient {
    public:
	using BasicClient::BasicClient;

    public:
#ifdef YCSB_LATENCY
	double *latencies{ nullptr };
	uint64_t lat_idx{ 0 };
	std::vector<double> thread_tt_latencies{ std::vector<double>(
		__MAX_RECORDER_NUM, 0) };
	double tt_avg_latencies{ 0 };
	std::mutex mtx;
	std::atomic<int> my_barrier{ 0 };
	std::atomic<int> barrier_count{ 0 };
	std::atomic<int> barrier_phase{ 0 };
	double sum_vec_latency{ 0 };
#endif

	// Load a simple workload: methods 'w' (writer) and 'r' (reader), keys are lock indices
	void load_workload(std::vector<char> ms, std::vector<uint64_t> ks)
	{
		attempts_num = ms.size();
		methods.resize(attempts_num);
		keys.resize(attempts_num);
		std::copy(ms.begin(), ms.end(), methods.begin());
		std::copy(ks.begin(), ks.end(), keys.begin());
	}

	// MicroZipf workload parameters (ShiftLock style)
	void set_microzipf(uint64_t count, double theta, double read_ratio,
			   uint64_t seed = 123456789)
	{
		mz_enabled = true;
		mz_count = count;
		mz_theta = theta;
		mz_read_ratio = read_ratio;
		mz_rng.seed(seed);
	}

	// Trace workload (ShiftLock format): lines of "TxnID,0,TxnType,LockID,LockType"; LockType: 1=SH, 2=EX
	bool set_trace(const std::string &path)
	{
		trace_enabled = false;
		trace_txns.clear();
		FILE *f = fopen(path.c_str(), "r");
		if (!f)
			return false;
		char *line = nullptr;
		size_t n = 0;
		uint64_t cur_txn_id = 0;
		uint8_t cur_ty = 0;
		std::vector<LockInstr> cur_locks;
		auto push_txn = [&]() {
			if (cur_txn_id != 0 && !cur_locks.empty()) {
				std::sort(cur_locks.begin(), cur_locks.end(),
					  [](const LockInstr &a,
					     const LockInstr &b) {
						  return a.id < b.id;
					  });
				trace_txns.push_back({ (uint32_t)cur_txn_id,
						       cur_ty,
						       std::move(cur_locks) });
				cur_locks.clear();
			}
		};
		while (getline(&line, &n, f) != -1) {
			// parse CSV
			// TxnID,0,TxnType,LockID,LockType\n
			char *p = line;
			char *end = line + strlen(line);
			// TxnID
			uint64_t txn_id = strtoull(p, &p, 10);
			if (p >= end)
				continue;
			if (*p == ',')
				++p; // skip comma
			// skip 0
			(void)strtoull(p, &p, 10);
			if (p >= end)
				continue;
			if (*p == ',')
				++p;
			// TxnType (1-based in Rust, convert to 0-based)
			uint64_t ty = strtoull(p, &p, 10);
			if (p >= end)
				continue;
			if (*p == ',')
				++p;
			// LockID
			uint64_t lock_id = strtoull(p, &p, 10);
			if (p >= end)
				continue;
			if (*p == ',')
				++p;
			// LockType
			uint64_t lty = strtoull(p, &p, 10);

			if (txn_id != cur_txn_id) {
				push_txn();
				cur_txn_id = txn_id;
				cur_ty = (uint8_t)(ty - 1);
			}
			cur_locks.push_back({ (uint32_t)lock_id, lty == 1 });
		}
		if (line)
			free(line);
		fclose(f);
		push_txn();
		trace_enabled = !trace_txns.empty();
		attempts_num = trace_txns.size();
		return trace_enabled;
	}

	// Thread entry implementation using coroutines
	void thread_test_fun_impl(uint64_t local_rpc_idx, uint64_t thread_id,
				  uint64_t server_id, size_t depth) override
	{
		uint64_t mod_thread_id = thread_id % nr_threads;
		uint64_t attempts = 0;
		uint32_t rkey;
		uint32_t lkey =
			conns[server_id]->manager_.get_local_memory_lkey(0);
		uintptr_t remote_va;
		sds::GlobalAddress remote_addr(0, 0);
		conns[server_id]->manager_.get_remote_memory_key(
			remote_addr.node, remote_addr.mr_id, remote_addr.offset,
			0, rkey, remote_va);
		// Reserve first 128KB for RPC (align with existing tests)
		remote_va += 128 * 1024ull;

		// Per-thread coroutine scheduler
		uint64_t thread_attempts_num =
			attempts_num / (machine_num * nr_threads);
		uint64_t thread_global_id =
			machine_id * nr_threads + thread_id % nr_threads;

		uint64_t coro_num = depth + 1; // +1 poller
		CoroutineScheduler *coro_sched =
			new CoroutineScheduler(thread_global_id, coro_num);
		std::vector<FastRandom> random_generator(coro_num);
		std::vector<coro_args> args(coro_num);
		uint64_t coro_attempts_num =
			thread_attempts_num / (coro_num - 1);
		std::vector<uint64_t> finished_attempts(coro_num, 0);
		std::vector<uint64_t> retry_cnt(coro_num, 0);
		std::vector<double> tl_latencies;
		double tl_tt_latency = 0;
#ifdef YCSB_LATENCY
		tl_latencies.reserve(1000000);
#endif

		auto t_attempts = std::make_shared<std::atomic<uint64_t> >(0);
		for (coro_id_t cid = 0; cid < coro_num; ++cid) {
			uint64_t coro_seed =
				((uint64_t)thread_global_id << 32) |
				(uint64_t)cid;
			random_generator[cid].SetSeed(coro_seed);
			coro_sched->coro_array[cid].coro_id = cid;
			if (cid == POLL_ROUTINE_ID) {
				bool *stop_ptr = &stop_signal;
				coro_sched->coro_array[cid].func = coro_call_t(
					std::bind(PollCompletion,
						  std::placeholders::_1,
						  coro_sched, stop_ptr));
			} else {
				// Allocate 16KB buffer per coroutine
				uint64_t *rdma_buf =
					(uint64_t *)conns[server_id]
						->alloc_cache(sizeof(uint64_t) *
							      2048);
				args[cid].server_id = server_id;
				args[cid].thread_gid = thread_global_id;
				args[cid].thread_id = thread_id;
				args[cid].coro_start =
					thread_attempts_num * thread_global_id +
					coro_attempts_num * (cid - 1);
				args[cid].coro_attempts_num = coro_attempts_num;
				args[cid].rdma_buf = rdma_buf;
				args[cid].buf_len = sizeof(uint64_t) * 2048;
				args[cid].coro_sched = coro_sched;
				args[cid].conns = conns[server_id];
				args[cid].coro_num = coro_num;
				args[cid].finished_attempts =
					&finished_attempts[cid];
				args[cid].retry_cnt = &retry_cnt[cid];
#ifdef YCSB_LATENCY
				args[cid].tl_latencies = &tl_latencies;
				args[cid].tl_tt_latency = &tl_tt_latency;
#else
				args[cid].tl_latencies = nullptr;
				args[cid].tl_tt_latency = nullptr;
#endif

				// Bind lambda that executes DrTM acquire based on methods[]
				coro_args *ap = &args[cid];
				auto attempts_ptr = t_attempts;
				coro_sched->coro_array[cid].func =
					coro_call_t([=](coro_yield_t &yield) {
						run_task(yield, cid, *ap, rkey,
							 remote_va,
							 attempts_ptr);
					});
			}
		}

		coro_sched->LoopLinkCoroutine(coro_num);
		coro_sched->coro_array[0].func();

		for (coro_id_t cid = POLL_ROUTINE_ID + 1; cid < coro_num;
		     ++cid) {
			attempts += finished_attempts[cid];
		}
		total_attempts.fetch_add(attempts);

		// Aggregate and report retries for this thread
		uint64_t retries_sum = 0;
		for (coro_id_t cid = POLL_ROUTINE_ID + 1; cid < coro_num;
		     ++cid) {
			retries_sum += retry_cnt[cid];
		}
		total_retries.fetch_add(retries_sum);
		fprintf(stdout, "[DrTM] thread %lu retries: %lu\n", thread_id,
			retries_sum);

#ifdef YCSB_LATENCY
		thread_tt_latencies[thread_id] = tl_tt_latency;
		double tl_avg_latency =
			attempts > 0 ? (thread_tt_latencies[thread_id] /
					(double)attempts) :
				       0.0;
		barrier_wait();
		client_report(0, thread_id, tl_latencies, tl_avg_latency);
#endif

		delete coro_sched;
	}

    private:
	void run_task(coro_yield_t &yield, coro_id_t cid, coro_args ap,
		      uint32_t rkey, uintptr_t remote_va,
		      std::shared_ptr<std::atomic<uint64_t> > t_attempts)
	{
		sds::Initiator *conn = ap.conns;
		CoroutineScheduler *coro_sched = ap.coro_sched;
		struct ibv_qp *qp = conn->manager_.get_qp_ptr(ap.thread_id);
		uint64_t *cmp_buf = ap.rdma_buf; // for CAS response
		uint64_t *read_buf =
			ap.rdma_buf + ap.buf_len / sizeof(uint64_t) /
					      2; // second half for reads
		sds::GlobalAddress remote_addr(0, 0);
		struct ibv_send_wr wr;
		struct ibv_send_wr *bad_wr;
		struct ibv_sge sge;
		uint64_t attempts = 0;
		std::uniform_real_distribution<double> uni01(0.0, 1.0);
		while (!stop_signal) {
			std::chrono::high_resolution_clock::time_point start_ts,
				end_ts;
			char m;
			uint64_t key;
			if (trace_enabled) {
				if (ap.coro_attempts_num == 0)
					break; // no fragment, end this coroutine
				size_t idx = ap.coro_start +
					     (attempts % ap.coro_attempts_num);
				const Txn &txn = trace_txns[idx];
#ifdef YCSB_LATENCY
				start_ts =
					std::chrono::high_resolution_clock::now();
#endif

				// Collect writer releases to perform after acquiring all locks
				struct PendingRel {
					uint64_t key;
					uint64_t desired;
				};
				std::vector<PendingRel> pending_rels;

				for (const auto &li : txn.locks) {
					key = li.id;
					m = li.shared ? 'r' : 'w';
					remote_addr.offset =
						key * sizeof(uint64_t);
					if (m == 'w') {
						while (true) {
							// RDMA READ current state
							coro_add_request(
								wr, sge, cid,
								IBV_WR_RDMA_READ,
								read_buf,
								remote_addr,
								rkey, remote_va,
								sizeof(uint64_t),
								0, 0,
								IBV_SEND_SIGNALED,
								conn->manager_);
							coro_sched->RDMASend(
								cid, qp, &wr,
								&bad_wr);
							coro_sched->Yield(yield,
									  cid);
							uint64_t cur_state =
								*read_buf;
							uint64_t desired = DrtmEntry::state_locked(
								(uint8_t)(ap.thread_gid &
									  0xFF));
							// CAS expected=cur_state -> desired
							coro_add_request(
								wr, sge, cid,
								IBV_WR_ATOMIC_CMP_AND_SWP,
								cmp_buf,
								remote_addr,
								rkey, remote_va,
								sizeof(uint64_t),
								cur_state,
								desired,
								IBV_SEND_SIGNALED,
								conn->manager_);
							coro_sched->RDMASend(
								cid, qp, &wr,
								&bad_wr);
							coro_sched->Yield(yield,
									  cid);
							uint64_t prev =
								*cmp_buf;
							if (prev == cur_state) {
								pending_rels.push_back(
									{ key,
									  desired });
								break;
							} else if (
								DrtmEntry::write_lock(
									prev) ==
									0 &&
								time::expired(DrtmEntry::read_lease(
									prev))) {
								cur_state =
									prev;
								if (ap.retry_cnt) {
									++(*ap.retry_cnt);
								}
							} else {
								uint8_t owner = DrtmEntry::
									owner_id(
										prev);
								if (DrtmEntry::write_lock(
									    prev) !=
									    0 &&
								    owner ==
									    (uint8_t)(ap.thread_gid &
										      0xFF)) {
									pending_rels
										.push_back(
											{ key,
											  prev });
									break;
								}
								cur_state =
									prev;
								// keep spinning per drtm.rs
								if (ap.retry_cnt) {
									++(*ap.retry_cnt);
								}
							}
						}
					} else { // reader
						while (true) {
							// RDMA READ current state
							coro_add_request(
								wr, sge, cid,
								IBV_WR_RDMA_READ,
								read_buf,
								remote_addr,
								rkey, remote_va,
								sizeof(uint64_t),
								0, 0,
								IBV_SEND_SIGNALED,
								conn->manager_);
							coro_sched->RDMASend(
								cid, qp, &wr,
								&bad_wr);
							coro_sched->Yield(yield,
									  cid);
							uint64_t cur_state =
								*read_buf;
							uint64_t end_time =
								time::now() +
								time::RLEASE_TIME;
							uint64_t desired = DrtmEntry::
								state_read_leased(
									end_time);
							coro_add_request(
								wr, sge, cid,
								IBV_WR_ATOMIC_CMP_AND_SWP,
								cmp_buf,
								remote_addr,
								rkey, remote_va,
								sizeof(uint64_t),
								cur_state,
								desired,
								IBV_SEND_SIGNALED,
								conn->manager_);
							coro_sched->RDMASend(
								cid, qp, &wr,
								&bad_wr);
							coro_sched->Yield(yield,
									  cid);
							uint64_t prev =
								*cmp_buf;
							if (prev == cur_state) {
								break; // acquired lease we installed
							} else if (DrtmEntry::write_lock(
									   prev) !=
								   0) {
								cur_state =
									0; // writer present
								if (ap.retry_cnt) {
									++(*ap.retry_cnt);
								}
							} else if (
								time::expired(DrtmEntry::read_lease(
									prev))) {
								cur_state =
									prev; // try to replace expired lease
								if (ap.retry_cnt) {
									++(*ap.retry_cnt);
								}
							} else if (
								time::valid(DrtmEntry::read_lease(
									prev))) {
								break; // fast path: existing valid lease
							} else {
								// continue spinning
								if (ap.retry_cnt) {
									++(*ap.retry_cnt);
								}
							}
						}
					}
				}

				// Application and think_time omitted (as in baseline), then release writers
				for (const auto &rel : pending_rels) {
					remote_addr.offset =
						rel.key * sizeof(uint64_t);
					coro_add_request(
						wr, sge, cid,
						IBV_WR_ATOMIC_CMP_AND_SWP,
						cmp_buf, remote_addr, rkey,
						remote_va, sizeof(uint64_t),
						rel.desired, 0,
						IBV_SEND_SIGNALED,
						conn->manager_);
					coro_sched->RDMASend(cid, qp, &wr,
							     &bad_wr);
					coro_sched->Yield(yield, cid);
				}
#ifdef YCSB_LATENCY
				end_ts =
					std::chrono::high_resolution_clock::now();
				double lat = std::chrono::duration<double,
								   std::micro>(
						     end_ts - start_ts)
						     .count();
				if (ap.tl_latencies)
					ap.tl_latencies->push_back(lat);
				if (ap.tl_tt_latency)
					(*ap.tl_tt_latency) += lat;
#endif
			} else {
				uint64_t idx = ap.coro_start + attempts;
				if (idx >= keys.size())
					break;
				key = keys[idx];
				m = methods[idx];
#ifdef YCSB_LATENCY
				// No RDMA path implemented for non-trace; still record a zero-lat marker for counting
				if (ap.tl_latencies)
					ap.tl_latencies->push_back(0.0);
#endif
			}

			attempts++;
		}

		*(ap.finished_attempts) = attempts;
		// Notify scheduler this coroutine has finished; stop poller when all workers are done
		FAA(&coro_sched->finish, 1);
		if (coro_sched->finish == ap.coro_num - 1) {
			coro_sched->stop_run = true;
		}
		coro_sched->FinishYield(yield, cid);
	}

    private:
	std::vector<uint64_t> keys;
	std::vector<char> methods;
	// MicroZipf params
	bool mz_enabled{ false };
	uint64_t mz_count{ 0 };
	double mz_theta{ 1.1 };
	double mz_read_ratio{ 0.8 };
	std::mt19937_64 mz_rng{ 123456789 };
	// Trace params
	struct LockInstr {
		uint32_t id;
		bool shared;
	};
	struct Txn {
		uint32_t id;
		uint8_t ty;
		std::vector<LockInstr> locks;
	};
	bool trace_enabled{ false };
	std::vector<Txn> trace_txns;
	std::atomic<uint64_t> total_retries{ 0 };

#ifdef YCSB_LATENCY
    private:
	void client_report(uint64_t report_thread, uint64_t thread_id,
			   std::vector<double> &tl_latencies,
			   double tl_avg_latency)
	{
		sum_vec_latency = 0;
		if (thread_id == report_thread) {
			latencies = (double *)malloc(sizeof(double) *
						     total_attempts.load());
			memset(latencies, 0,
			       sizeof(double) * total_attempts.load());
		}
		barrier_wait();

		mtx.lock();
		tt_avg_latencies += tl_avg_latency;
		for (size_t i = 0; i < tl_latencies.size(); i++) {
			sum_vec_latency += tl_latencies[i];
			latencies[lat_idx] = tl_latencies[i];
			lat_idx++;
		}
		mtx.unlock();

		barrier_wait();

		if (thread_id == report_thread) {
			tt_avg_latencies = tt_avg_latencies / nr_threads;
			std::sort(latencies, latencies + lat_idx);
			uint64_t p50 = 0.5 * lat_idx;
			uint64_t p99 = 0.99 * lat_idx;
			uint64_t p999 = 0.999 * lat_idx;
			SDS_INFO("DrtmClient Latencies");
			SDS_INFO("Avg\tmin\tP50\tP99\tP999\tmax");
			if (lat_idx > 0) {
				SDS_INFO("%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t",
					 sum_vec_latency / lat_idx,
					 latencies[0], latencies[p50],
					 latencies[p99], latencies[p999],
					 latencies[lat_idx - 1]);
			} else {
				SDS_INFO("No latency samples");
			}
			SDS_INFO("sum of latencies: %.3f", sum_vec_latency);
		}
	}

	inline void barrier_wait()
	{
		int local_phase = barrier_phase.load(std::memory_order_acquire);
		int prev =
			barrier_count.fetch_add(1, std::memory_order_acq_rel);
		if (prev + 1 == (int)nr_threads) {
			barrier_count.store(0, std::memory_order_release);
			barrier_phase.fetch_add(1, std::memory_order_acq_rel);
		} else {
			while (barrier_phase.load(std::memory_order_acquire) ==
			       local_phase) {
				std::this_thread::yield();
			}
		}
	}
#endif
};

} // namespace drtm_fusioncas
