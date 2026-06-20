#include "basic_client.h"
#include "ycsb_loader.h"
#include <random>
#include <mutex>
#include <algorithm>

#define HERD_LATENCY

class HerdClient : public BasicClient {
    public:
	uint64_t *keys;
	char *methods;
	uint64_t unsignal_batch;
	double *latencies;
	uint64_t lat_idx;
	std::vector<double> thread_tt_latencies;
	double tt_avg_latencies;
	std::mutex mtx;
	std::atomic<int> my_barrier;

    public:
	HerdClient(size_t nr_memory_nodes, size_t machine_num,
		   size_t machine_id, size_t depth, uint64_t attempts_num)
		: BasicClient(nr_memory_nodes, machine_num, machine_id, depth,
			      attempts_num)
	{
		assert(nr_memory_nodes >= 1 &&
		       nr_memory_nodes <= sds::kMaxMemoryNodes);
		assert(machine_num >= 1);
		assert(machine_id < machine_num);
		assert(depth > 0);
		for (int i = 0; i < sds::kMaxMemoryNodes; i++) {
			conns.push_back(NULL);
		}
#ifdef HERD_LATENCY
		latencies = NULL;
		lat_idx = 0;
		for (int i = 0; i < __MAX_RECORDER_NUM; i++) {
			thread_tt_latencies.push_back(0);
		}
		my_barrier = 0;
		tt_avg_latencies = 0;
#endif
		// SDS_INFO("HERDClient Init with %d conns", conns.size());
		// std::cerr << "HerdClient constructor, this = " << this << std::endl;
	}

	~HerdClient()
	{
		// std::cout << "total retry cnt: " << total_retry_cnt << std::endl;
		if (keys != NULL)
			free(keys);
		if (methods != NULL)
			free(methods);
		if (latencies != NULL)
			free(latencies);
	}

	void set_unsignal_batch(uint64_t unsignal_batch_)
	{
		unsignal_batch = unsignal_batch_;
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

	int herd_poll_n(size_t n, ibv_cq *cq, ibv_wc *rwc, uint64_t thread_id)
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
				// for(int i = tt_comps; i < tt_comps + new_comps; i ++) {
				//     if(rwc[i].opcode == IBV_WC_RECV) {
				//         if((rwc[i].wr_id >> 32) != thread_id) {
				//             SDS_INFO("thread %llu wrong qp!!!!!!!!!!", thread_id);
				//             while(1);
				//         }
				//     }
				// }

				tt_comps += new_comps;
			}
		}
		return tt_comps;
	}

	int herd_poll_n_latency(
		size_t n, ibv_cq *cq, ibv_wc *rwc,
		std::vector<std::chrono::high_resolution_clock::time_point>
			&ends,
		uint64_t thread_id)
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

	void client_report(uint64_t report_thread, uint64_t thread_id,
			   std::vector<double> &tl_latencies,
			   double tl_avg_latency)
	{
#ifdef HERD_LATENCY
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
		SDS_INFO("Herd Clinet report retry");
		SDS_INFO("total retry cnt: %llu", total_retry_cnt.load());
#endif
	}
	// void set_shm_1(void *addr, uint64_t length) {}

	int herd_add_request(struct ibv_send_wr &wr, struct ibv_sge &sge,
			     uint64_t wr_id, ibv_wr_opcode opcode, void *local,
			     const sds::GlobalAddress &remote, uint32_t rkey,
			     uintptr_t remote_va, size_t length,
			     uint64_t compare_add, uint64_t swap, int flags,
			     sds::ResourceManager &manager_, ibv_send_wr *next)
	{
		sge.addr = (uintptr_t)local;
		sge.length = length;
		sge.lkey = manager_.get_local_memory_lkey(0);

		wr.wr_id = wr_id;
		wr.opcode = opcode;
		wr.num_sge = 1;
		wr.sg_list = &sge;
		wr.send_flags = flags;
		wr.next = next;

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

	void thread_test_fun_impl(uint64_t local_rpc_idx, uint64_t thread_id,
				  uint64_t server_id, size_t depth) override
	{
		// derived class virtual function implementation
		SDS_INFO(
			"HerdClinent thread_test_fun_impl() executed for thread %d and server %d",
			thread_id, server_id);
		uint64_t qp_num =
			conns[server_id]->manager_.get_qp_ptr(thread_id)->qp_num;
		// actual thread logic
		uint64_t attempts = 0;
		uint64_t tl_send_num = 0;
		rpc_op *read_buf = (rpc_op *)conns[server_id]->alloc_cache(
			sizeof(rpc_op) * depth);
		rpc_op *recv_buf = (rpc_op *)conns[server_id]->alloc_cache(
			sizeof(rpc_op) * depth);
		rpc_op *read_reqs_buf = (rpc_op *)conns[server_id]->alloc_cache(
			sizeof(rpc_op) * depth);
		rpc_op *reqs_buf = (rpc_op *)conns[server_id]->alloc_cache(
			sizeof(rpc_op) * depth);
		rpc_op *nops_buf = (rpc_op *)conns[server_id]->alloc_cache(
			sizeof(rpc_op) * depth);

		struct ibv_send_wr wr[depth], *bad_wr[depth];
		struct ibv_sge sge[depth];
		struct ibv_wc wc[depth * 2];

		struct ibv_recv_wr rr[depth], *bad_recv_wr[depth];
		struct ibv_sge rsge[depth];
		struct ibv_wc rwc[depth * 2];

		// only one memory node for now
		uint32_t rkey;
		uint32_t lkey =
			conns[server_id]->manager_.get_local_memory_lkey(0);
		uintptr_t remote_va;
		uintptr_t remote_rpc_va;
		sds::GlobalAddress remote_addr(0, 0);

		conns[server_id]->manager_.get_remote_memory_key(
			remote_addr.node, remote_addr.mr_id, remote_addr.offset,
			0, rkey, remote_va);

		// message queue start position
		uint64_t thread_global_id =
			(__MAX_RECORDER_NUM * machine_id) + thread_id;
		uint64_t mod_t_global_id =
			(nr_threads * machine_id) + thread_id;
		uint64_t window_size =
			128 * 1024 /
			(2 * machine_num * __MAX_RECORDER_NUM * sizeof(rpc_op));
		uint64_t global_wr_base = thread_global_id * window_size;
		for (int i = 0; i < depth; i++) {
			rsge[i].length = sizeof(rpc_op);
			rsge[i].lkey = lkey;
			rsge[i].addr = (uint64_t)&recv_buf[i];

			rr[i].sg_list = &rsge[i];
			rr[i].num_sge = 1;
			rr[i].wr_id = 200000 + global_wr_base + i;
			// rr[i].wr_id = (thread_id << 32);
			// rr[i].next = (i == (depth - 1)) ? NULL : &rr[i + 1];
			rr[i].next = NULL;
			// int ret = ibv_post_recv(conns[server_id]->manager_.get_qp_ptr(thread_id), &rr[i], &bad_recv_wr);
		}

		// uint64_t * buf = (uint64_t *)conns[server_id]->alloc_cache(sizeof(uint64_t) * depth);
		// copied from fusioncas server, first 128KB is rpc region
		remote_rpc_va = remote_va;
		remote_va = remote_va + 128 * 1024ul;
		// used when total is 100M
		uint64_t thread_attepmts_num =
			attempts_num / (machine_num * nr_threads);

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
		uint64_t nops_wr_base = 20000;
#ifdef HERD_LATENCY
		starts.resize(depth);
		ends.resize(depth);

		std::vector<double> tl_latencies;
		tl_latencies.reserve(50000000);
#endif
		while (!stop_signal) {
			// while(attempts < thread_attepmts_num) {
			// both update and read need a read first, use RDMA READ
			uint64_t update_ops = 0;

			for (int i = 0; i < depth; i++) {
				// rr[i].wr_id = (thread_id << 32);
				if (methods[thread_attepmts_num *
						    mod_t_global_id +
					    (attempts + i) %
						    thread_attepmts_num] == 'u')
					update_ops++;
				remote_addr.offset =
					keys[thread_attepmts_num *
						     mod_t_global_id +
					     (attempts + i) %
						     thread_attepmts_num] *
					8;
				// remote_addr.offset = (thread_attepmts_num * thread_global_id + (attempts + i) % thread_attepmts_num) * 8;
#ifdef HERD_LATENCY
				starts[i] =
					std::chrono::high_resolution_clock::now();
#endif
				// use i as wr_id for actual ops
				herd_add_request(
					wr[i], sge[i], i, IBV_WR_RDMA_READ,
					&read_buf[i].comp, remote_addr, rkey,
					remote_va, sizeof(uint64_t), 0, 0,
					IBV_SEND_SIGNALED,
					conns[server_id]->manager_,
					i == (depth - 1) ? NULL : &wr[i + 1]);
				tl_send_num++;
			}

			int ret = ibv_post_send(
				conns[server_id]->manager_.get_qp_ptr(
					thread_id),
				wr, bad_wr);
			// std::cout << "buf " << i << " " << *(buf+i) << std::endl;
			if (ret < 0)
				SDS_PERROR("send request error...");

				// poll RDMA READ cqe
				// int comps = herd_poll_n(depth, conns[server_id]->manager_.get_cq_ptr(thread_id), wc);
#ifdef HERD_LATENCY
			int comps = herd_poll_n_latency(
				depth,
				conns[server_id]->manager_.get_cq_ptr(
					thread_id),
				wc, ends, thread_id);
#else
			int comps = herd_poll_n(
				depth,
				conns[server_id]->manager_.get_cq_ptr(
					thread_id),
				wc, thread_id);
#endif
			// send update request via rpc
			size_t update_cnt = 0;
			uint64_t tmp_cmp[depth];

			// written with depth = batch for now
			for (int i = 0; i < depth; i++) {
				if (methods[thread_attepmts_num *
						    mod_t_global_id +
					    (attempts +
					     i) % thread_attepmts_num] == 'u') {
					rsge[i].addr = (uint64_t)&recv_buf[i];
					// rr[i].wr_id |= (200000 + attempts + i);
					// rr[i].wr_id |= (200000 + tl_send_num);
					int ret = ibv_post_recv(
						conns[server_id]
							->manager_.get_qp_ptr(
								thread_id),
						&rr[i], &bad_recv_wr[i]);
					if (ret < 0) {
						fprintf(stderr,
							"Error in ibv_post_recv: %d\n",
							ret);
						exit(1);
					}
					update_cnt++;

					remote_addr.offset =
						(global_wr_base + i) *
						sizeof(rpc_op);
					// RDMA READ value as compare
					cmp[i] = read_buf[i].comp;
					tmp_cmp[i] = cmp[i];
					swap[i] =
						header | (attempts / depth + i);
					uint64_t offset =
						keys[thread_attepmts_num *
							     mod_t_global_id +
						     (attempts +
						      i) % thread_attepmts_num] *
						8;
					// uint64_t offset = (thread_attepmts_num * thread_global_id + (attempts + i) % thread_attepmts_num) * 8;
					reqs_buf[i].opcode = 1;
					reqs_buf[i].wr.atomic.cmp = cmp[i];
					reqs_buf[i].wr.atomic.swap = swap[i];
					reqs_buf[i].wr.atomic.remote_addr =
						remote_va + offset;
					reqs_buf[i].qpn = qp_num;
					reqs_buf[i].crc = 888;
					// std::cout << "thread_global_id: " << thread_global_id << " attempts: " << attempts << " cmp: " << cmp << std::endl;
					herd_add_request(
						wr[i], sge[i], i,
						IBV_WR_RDMA_WRITE, reqs_buf + i,
						remote_addr, rkey,
						remote_rpc_va, sizeof(rpc_op),
						0, 0, IBV_SEND_SIGNALED,
						conns[server_id]->manager_,
						i == (depth - 1) ? NULL :
								   &wr[i + 1]);

					// ((tl_send_num & unsignal_batch) == 0 ? IBV_SEND_SIGNALED : 0), conns[server_id]->manager_);

					tl_send_num++;
				}
				// written with depth=2, so judge accordingly
				else if (update_ops != 0 &&
					 methods[thread_attepmts_num *
							 mod_t_global_id +
						 (attempts + i) %
							 thread_attepmts_num] ==
						 'r') {
					rsge[i].addr = (uint64_t)&recv_buf[i];
					// rr[i].wr_id = (200000 + attempts + i);
					// rr[i].wr_id |= (200000 + tl_send_num);
					int ret = ibv_post_recv(
						conns[server_id]
							->manager_.get_qp_ptr(
								thread_id),
						&rr[i], &bad_recv_wr[i]);
					if (ret < 0) {
						fprintf(stderr,
							"Error in ibv_post_recv: %d\n",
							ret);
						exit(1);
					}
					// 100 represents for nop
					remote_addr.offset =
						(global_wr_base + i) *
						sizeof(rpc_op);
					reqs_buf[i].opcode = 100;
					reqs_buf[i].qpn = qp_num;
					reqs_buf[i].crc = 888;

					herd_add_request(
						wr[i], sge[i], nops_wr_base + i,
						IBV_WR_RDMA_WRITE, reqs_buf + i,
						remote_addr, rkey,
						remote_rpc_va, sizeof(rpc_op),
						0, 0, IBV_SEND_SIGNALED,
						conns[server_id]->manager_,
						i == (depth - 1) ? NULL :
								   &wr[i + 1]);
					tl_send_num++;
				}
			}

			// ensure update request succeeds
			if (update_cnt > 0) {
				ret = ibv_post_send(
					conns[server_id]->manager_.get_qp_ptr(
						thread_id),
					wr, bad_wr);
				// std::cout << "buf " << i << " " << *(buf+i) << std::endl;
				if (ret < 0)
					SDS_PERROR("send request error...");
					// poll rpc reply again; since recv also generates cqe, multiply by 2
					// comps = herd_poll_n(depth * 2, conns[server_id]->manager_.get_cq_ptr(thread_id), rwc);
#ifdef HERD_LATENCY
				comps = herd_poll_n_latency(
					depth * 2,
					conns[server_id]->manager_.get_cq_ptr(
						thread_id),
					rwc, ends, thread_id);
#else
				comps = herd_poll_n(
					depth * 2,
					conns[server_id]->manager_.get_cq_ptr(
						thread_id),
					rwc, thread_id);
#endif
				uint64_t cur_retry = 0;
				while (1) {
					uint64_t need_retry = 0;
					uint64_t retry[depth];
					struct ibv_recv_wr retry_rr[depth],
						*bad_retry_rr[depth];
					struct ibv_sge retry_rsge[depth];
					struct ibv_wc retry_wc[depth * 2];
					for (int i = 0; i < depth; i++) {
						if (methods[thread_attepmts_num *
								    mod_t_global_id +
							    (attempts +
							     i) % thread_attepmts_num] ==
						    'u') {
							uint64_t resp =
								recv_buf[i].comp;
							// if CAS result upper 32 bits != own thread_global_id or value is wrong (333 is no-op return)
							if (resp != tmp_cmp[i] &&
							    resp != 333) {
								need_retry++;
							}
						}
					}
					uint64_t post_recv_num = 0;
					if (need_retry > 0) {
						for (int i = 0; i < depth;
						     i++) {
							retry_rsge[i].length =
								sizeof(rpc_op);
							retry_rsge[i].lkey =
								lkey;
							retry_rsge[i].addr =
								(uint64_t)&recv_buf
									[i];

							retry_rr[i].sg_list =
								&retry_rsge[i];
							retry_rr[i].num_sge = 1;
							retry_rr[i].wr_id =
								200000 +
								global_wr_base +
								i;
							// retry_rr[i].wr_id = (thread_id << 32);
							// retry_rr[i].wr_id |= (200000 + tl_send_num);

							retry_rr[i].next = NULL;
							int ret = ibv_post_recv(
								conns[server_id]
									->manager_
									.get_qp_ptr(
										thread_id),
								&retry_rr[i],
								&bad_retry_rr[i]);
							if (ret < 0) {
								fprintf(stderr,
									"Error in ibv_post_recv: %d\n",
									ret);
								exit(1);
							}
							post_recv_num++;
						}
						for (int i = 0; i < depth;
						     i++) {
							// retry
							if (methods[thread_attepmts_num *
									    mod_t_global_id +
								    (attempts +
								     i) % thread_attepmts_num] ==
							    'u') {
								// remote_addr.offset = keys[(attempts + i) % attempts_num] * 8;
								uint64_t resp =
									recv_buf[i]
										.comp;
								if (recv_buf[i]
									    .qpn !=
								    qp_num) {
									SDS_INFO(
										"recv qpn: %llu, local qpn: %llu",
										recv_buf[i]
											.qpn,
										qp_num);
									SDS_INFO(
										"thread %llu wrong qp!!!!",
										thread_id);
								}
								recv_buf[i].crc =
									100;
								remote_addr
									.offset =
									(global_wr_base +
									 i) *
									sizeof(rpc_op);
								// if CAS result upper 32 bits != own thread_global_id or value is wrong
								reqs_buf[i]
									.resp_addr =
									tl_send_num;
								if (resp != tmp_cmp[i] &&
								    resp != 333) {
									retry[i] =
										1;
									// std::cout << "attmepts: " << attempts << " resp header: " << (resp >> 32) << " resp body: " << (resp & 0xffffffff) << " cmp: " << tmp_cmp[i] << " expected: " << (attempts / depth + i) << std::endl;
									uint64_t offset =
										keys[thread_attepmts_num *
											     mod_t_global_id +
										     (attempts +
										      i) % thread_attepmts_num] *
										8;
									// uint64_t offset = (thread_attepmts_num * thread_global_id + (attempts + i) % thread_attepmts_num) * 8;
									cmp[i] =
										resp;
									tmp_cmp[i] = cmp
										[i];
									// swap[i] = header | (attempts / depth + i);
									reqs_buf[i]
										.opcode =
										1;
									reqs_buf[i]
										.wr
										.atomic
										.cmp = cmp
										[i];
									reqs_buf[i]
										.wr
										.atomic
										.swap = swap
										[i];
									reqs_buf[i]
										.wr
										.atomic
										.remote_addr =
										remote_va +
										offset;
									reqs_buf[i]
										.qpn =
										qp_num;
									reqs_buf[i]
										.crc =
										888;
									herd_add_request(
										wr[i],
										sge[i],
										i,
										IBV_WR_RDMA_WRITE,
										reqs_buf +
											i,
										remote_addr,
										rkey,
										remote_rpc_va,
										sizeof(rpc_op),
										0,
										0,
										IBV_SEND_SIGNALED,
										conns[server_id]
											->manager_,
										i == (depth -
										      1) ?
											NULL :
											&wr[i +
											    1]);
									tl_send_num++;
								}
								// if two update ops and one succeeded, skip retry for the successful one
								else {
									retry[i] =
										0;
									// 100 represents for nop
									reqs_buf[i]
										.opcode =
										100;
									reqs_buf[i]
										.qpn =
										qp_num;
									reqs_buf[i]
										.crc =
										888;

									herd_add_request(
										wr[i],
										sge[i],
										nops_wr_base +
											i,
										IBV_WR_RDMA_WRITE,
										reqs_buf +
											i,
										remote_addr,
										rkey,
										remote_rpc_va,
										sizeof(rpc_op),
										0,
										0,
										IBV_SEND_SIGNALED,
										conns[server_id]
											->manager_,
										i == (depth -
										      1) ?
											NULL :
											&wr[i +
											    1]);
									tl_send_num++;
								}
							} else if (
								methods[thread_attepmts_num *
										mod_t_global_id +
									(attempts +
									 i) % thread_attepmts_num] ==
								'r') {
								remote_addr
									.offset =
									(global_wr_base +
									 i) *
									sizeof(rpc_op);
								// 100 represents for nop
								reqs_buf[i]
									.opcode =
									100;
								reqs_buf[i].qpn =
									qp_num;
								// reqs_buf[i].resp_addr =
								reqs_buf[i].crc =
									888;

								herd_add_request(
									wr[i],
									sge[i],
									nops_wr_base +
										i,
									IBV_WR_RDMA_WRITE,
									reqs_buf +
										i,
									remote_addr,
									rkey,
									remote_rpc_va,
									sizeof(rpc_op),
									0, 0,
									IBV_SEND_SIGNALED,
									conns[server_id]
										->manager_,
									i == (depth -
									      1) ?
										NULL :
										&wr[i +
										    1]);
								tl_send_num++;
							}
						}
						tl_retry_cnt += need_retry;
						cur_retry += need_retry;
						ret = ibv_post_send(
							conns[server_id]
								->manager_
								.get_qp_ptr(
									thread_id),
							wr, bad_wr);
						if (ret < 0)
							SDS_PERROR(
								"send request error...");
							// int cc = herd_poll_n(depth * 2, conns[server_id]->manager_.get_cq_ptr(thread_id), rwc);
#ifdef HERD_LATENCY
						int cc = herd_poll_n_latency(
							depth * 2,
							conns[server_id]
								->manager_
								.get_cq_ptr(
									thread_id),
							retry_wc, ends,
							thread_id);
#else
						int cc = herd_poll_n(
							depth + post_recv_num,
							conns[server_id]
								->manager_
								.get_cq_ptr(
									thread_id),
							rwc, thread_id);
#endif
					} else
						break;
				}
			}
#ifdef HERD_LATENCY
			for (int i = 0; i < depth; i++) {
				// thread_tt_latencies[thread_id % nr_threads] += latencies[(thread_id % nr_threads) * thread_attempts_num + attempts + i];
				double lat = std::chrono::duration<double,
								   std::micro>(
						     ends[i] - starts[i])
						     .count();
				tl_latencies.push_back(lat);
				thread_tt_latencies[thread_id] += lat;
			}
#endif
			attempts += depth;
		}
		total_retry_cnt.fetch_add(tl_retry_cnt);
		total_attempts.fetch_add(attempts);
		uint64_t report_thread = 0;
#ifdef HERD_LATENCY
		double tl_avg_latency =
			thread_tt_latencies[thread_id] / (double)attempts;
		my_barrier.fetch_add(1, std::memory_order_relaxed);
		while (my_barrier != nr_threads) {
			;
		}
		SDS_INFO("start report %d", my_barrier.load());
		// std::cout << "attempts: " << attempts << std::endl;
		client_report(report_thread, thread_id, tl_latencies,
			      tl_avg_latency);
#else
		std::vector<double> nouse;
		client_report(report_thread, thread_id, nouse, 0);
#endif
	}
};