#include "herd_server.h"

void HerdServer::init_rpc(JsonConfig &rpc_config)
{
	rpc_config_.compute_thread_num =
		(uint64_t)rpc_config.get("compute_thread_num").get_uint64();
	rpc_config_.rpc_thread_num =
		(uint64_t)rpc_config.get("rpc_thread_num").get_uint64();
	rpc_config_.send_batch =
		(uint64_t)rpc_config.get("send_batch").get_uint64();
	rpc_config_.window_size =
		(uint64_t)rpc_config.get("window_size").get_uint64();
	rpc_config_.unsignal_batch =
		(uint64_t)rpc_config.get("unsignal_batch").get_uint64() - 1;
	rpc_config_.thread_qp_batch =
		(uint64_t)rpc_config.get("thread_qp_batch").get_uint64();
}

int HerdServer::start()
{
	// register memory for rnic

	rpc_addr = sds::mmap_huge_page(mem_pool_size + rpc_region_size);
	int rc = memory_node.register_main_memory(
		rpc_addr, mem_pool_size + rpc_region_size);
	assert(!rc);

	local_addr = rpc_addr + rpc_region_size;

	rc = memory_node.start(tcp_port);
	assert(!rc);

	volatile uint64_t *check = (uint64_t *)local_addr;
	while (*check != machine_num) {
		;
	}
	SDS_INFO("Herd Server sync remote node...");
	SDS_INFO("Herd Server starts...");
	SDS_INFO("Herd RPC address: %llu", rpc_addr);
	if (rpc_enabled)
		run_rpc();
}

// code introduced by FUSIONCAS
void HerdServer::run_rpc()
{
	// 1. register shared memory for agent server communication
	// SDS_INFO("Register server shared memory...");
	// // create shared memory
	// int shm_fd = shm_open(shm_name, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
	// if (shm_fd == -1) {
	//     throw std::runtime_error("Error opening shared memory: " + std::string(strerror(errno)));
	// }

	// // map shared memory
	// server_shm_ptr = mmap(0, shm_size, PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);
	// if (server_shm_ptr == MAP_FAILED) {
	//     close(shm_fd);
	//     throw std::runtime_error("Error mapping shared memory: " + std::string(strerror(errno)));
	// }

	// SDS_INFO("Register shared memory completed...");
	// // 2. write local rpc_addr and rkey to shared memory
	// rpc_meta * meta_ = (rpc_meta *)server_shm_ptr;
	// meta_->machine_num = machine_num;
	// // uint64_t rkey = memory_node.manager_.get_local_memory_rkey(sds::MAIN_MEMORY_MR_ID);
	// meta_->rkey = memory_node.manager_.get_local_memory_rkey(sds::MAIN_MEMORY_MR_ID);
	// meta_->rpc_addr = rpc_addr;
	// meta_->length = rpc_region_size / 2;
	// meta_->crc = __CRC_COMPLETE;

	// std::cout << "aaaaaaaaaa"<<std::endl;
	// CAS(&meta_->machine_num, &meta_->machine_num, machine_num);
	// CAS(&meta_->rpc_addr, &meta_->rpc_addr, rkey);
	// CAS()
	pthread_t tid[32];
	thread_arg args[32];
	pthread_attr_t attr;
	pthread_attr_init(&attr);
	// 3. start rpc
	for (uint64_t i = 0; i < rpc_config_.rpc_thread_num; ++i) {
		cpu_set_t cpuset;
		CPU_ZERO(&cpuset);
		CPU_SET(32 + i * 2 + 1,
			&cpuset); // Bind thread to a specific NUMA node

		args[i].thread_id = i;
		args[i].server = this;
		// Create thread with the specified affinity and pass the argument
		pthread_create(&tid[i], &attr, rpc_func, (void *)&args[i]);
		pthread_setaffinity_np(tid[i], sizeof(cpu_set_t), &cpuset);
	}
	for (int i = 0; i < rpc_config_.rpc_thread_num; ++i) {
		pthread_join(tid[i], NULL);
	}
	// for(uint64_t i = 0; i )
}

void *HerdServer::rpc_func(void *rarg)
{
	uint64_t rpc_thread_id = ((thread_arg *)rarg)->thread_id;
	HerdServer *server = ((thread_arg *)rarg)->server;
	// std::cout << "bbbbbbbbb" << std::endl;
	server->rpc_func_impl(rpc_thread_id);
}

void *HerdServer::rpc_func_impl(uint64_t rpc_thread_id)
{
	// init qp info
	std::unordered_map<uint64_t, uint64_t> qpn_table;
	uint64_t tmp_tt_qp = 0;
	for (int i = 0; i < machine_num; i++) {
		uint64_t nr_qp = memory_node.manager_.get_qp_size(i);
		for (int j = 0; j < nr_qp; j++) {
			qpn_table.emplace(
				memory_node.manager_.get_qp_dest_qp_num(
					tmp_tt_qp + j) +
					i * QP_OFF,
				tmp_tt_qp + j);
		}
		tmp_tt_qp += nr_qp;
	}
	uint64_t logic_id = rpc_thread_id;
	uint64_t thread_num = __MAX_RECORDER_NUM;
	// uint64_t thread_num = rpc_config_.compute_thread_num;
	// number of reply batches; larger values increase latency
	uint64_t thread_qp_batch = rpc_config_.thread_qp_batch;
	uint64_t unsignal_batch = rpc_config_.unsignal_batch;
	uint64_t rpc_thread_num = rpc_config_.rpc_thread_num;
	// uint64_t tt_thread_num = __MAX_RECORDER_NUM * machine_num;
	uint64_t tt_thread_num = __MAX_RECORDER_NUM * machine_num;
	// rpc_op capacity per thread buf per machine
	uint64_t window_size =
		rpc_region_size / (2 * tt_thread_num * sizeof(rpc_op));

	rpc_op *req_buf = (rpc_op *)rpc_addr;
	uint64_t resp_buf = (uint64_t)rpc_addr + rpc_region_size / 2;

	uint64_t lkey = memory_node.manager_.get_local_memory_lkey(
		sds::MAIN_MEMORY_MR_ID);
	uint64_t send_tx[tt_thread_num];
	memset(send_tx, 0, sizeof(uint64_t) * tt_thread_num);

	struct ibv_send_wr *wr[tt_thread_num], *bad_wr;
	struct ibv_sge *sge[tt_thread_num];
	uint64_t thread_batch_cnt[tt_thread_num];
	memset(thread_batch_cnt, 0, sizeof(uint64_t) * tt_thread_num);

	for (int i = 0; i < tt_thread_num; i++) {
		wr[i] = (struct ibv_send_wr *)malloc(
			sizeof(struct ibv_send_wr) * thread_qp_batch);
		memset(wr[i], 0, sizeof(struct ibv_send_wr) * thread_qp_batch);
		sge[i] = (struct ibv_sge *)malloc(sizeof(struct ibv_sge) *
						  thread_qp_batch);
		memset(sge[i], 0, sizeof(struct ibv_sge) * thread_qp_batch);
		for (int j = 0; j < thread_qp_batch; j++) {
			// sge[i][j].addr = (uint64_t)&resp[i * thread_qp_batch + j];
			// sge[i][j].addr = resp_buf + sizeof(rpc_op) * (i * thread_qp_batch + j);
			sge[i][j].length = sizeof(rpc_op);
			sge[i][j].lkey = lkey;
			// wr[i][j].opcode = IBV_WR_RDMA_WRITE;
			wr[i][j].opcode = IBV_WR_SEND;
			wr[i][j].num_sge = 1;
			wr[i][j].next = (j == thread_qp_batch - 1) ?
						NULL :
						&wr[i][j + 1];
			wr[i][j].sg_list = &sge[i][j];
		}
	}

	while (1) {
		uint64_t tt_qp = 0;
		// derive server qp info from machine_id and client qpn
		for (int k = 0; k < machine_num; k++) {
			uint32_t rkey;
			uint64_t remote_va;
			memory_node.manager_.get_remote_memory_key(
				k, sds::MAIN_MEMORY_MR_ID, 0, 8, rkey,
				remote_va);
			//             uint64_t nr_qp = memory_node.manager_.get_qp_size(k);
			uint64_t nr_qp = memory_node.manager_.get_qp_size(k);
			// req buf for all threads on each machine
			for (int i = thread_num / rpc_thread_num * logic_id;
			     i < thread_num / rpc_thread_num * (logic_id + 1);
			     i++) {
				// each thread has window_size rpc requests and replies
				uint64_t t_local_id;
				uint64_t t_global_id;
				for (int j = 0; j < window_size; j++) {
					// id of thread cur_idx
					uint64_t cur_idx = k * thread_num + i;
					// compute current req position
					uint64_t req_off =
						cur_idx * window_size;
					rpc_op *cur_req =
						(req_buf + req_off + j);
					rpc_op *cur_resp = ((rpc_op *)resp_buf +
							    req_off + j);
					// use 888 as check value for now
					if (cur_req->crc == 888) {
						//                             t_local_id = cur_req->qpn % nr_qp;
						//                             t_global_id = tt_qp + t_local_id;
						// CAS
						// set return value to final result
						t_global_id =
							qpn_table
								.find(cur_req->qpn)
								->second;
						// cur_resp->comp = cur_req->wr.atomic.swap;
						if (cur_req->opcode == 1) {
							// t_local_id = cur_req->qpn % nr_qp;
							// t_global_id = tt_qp + t_local_id;
							if (*(uint64_t
								      *)(cur_req->wr
										 .atomic
										 .remote_addr) ==
							    cur_req->wr.atomic
								    .cmp) {
								CAS((uint64_t
									     *)(cur_req->wr
											.atomic
											.remote_addr),
								    (uint64_t *)&(
									    cur_req->wr
										    .atomic
										    .cmp),
								    cur_req->wr
									    .atomic
									    .swap);
								cur_resp->comp =
									cur_req->wr
										.atomic
										.cmp;
							} else {
								cur_resp->comp = *(
									uint64_t
										*)(cur_req->wr
											   .atomic
											   .remote_addr);
							}
						} else if (cur_req->opcode ==
							   2) {
							cur_resp->comp = *(
								uint64_t
									*)(cur_req->wr
										   .rdma
										   .remote_addr);
						} else if (cur_req->opcode ==
							   10) {
							FAA((uint64_t
								     *)(cur_req->wr
										.atomic
										.remote_addr),
							    1);
							cur_resp->comp = *(
								uint64_t
									*)(cur_req->wr
										   .atomic
										   .remote_addr);
						} else if (cur_req->opcode ==
							   100) {
							cur_resp->comp = 333;
						}
						// for debug
						cur_resp->qpn = cur_req->qpn;
						cur_resp->opcode =
							send_tx[cur_idx];

						cur_resp->crc = 888;
						cur_req->crc = 100;
						// ibv_qp * cur_qp = memory_node.manager_.get_qp_ptr(t_global_id);
						wr[cur_idx]
						  [thread_batch_cnt[cur_idx]]
							  .wr_id =
							send_tx[cur_idx];
						// temporarily set to unsignaled
						wr[cur_idx]
						  [thread_batch_cnt[cur_idx]]
							  .send_flags =
							(send_tx[cur_idx] &
							 unsignal_batch) == 0 ?
								IBV_SEND_SIGNALED :
								0;
						;
						// wr[cur_idx][thread_batch_cnt[cur_idx]].wr.rdma.remote_addr = cur_req->resp_addr;
						wr[cur_idx]
						  [thread_batch_cnt[cur_idx]]
							  .wr.rdma.rkey = rkey;
						sge[cur_idx]
						   [thread_batch_cnt[cur_idx]]
							   .addr =
							(uint64_t)cur_resp;

						if ((send_tx[cur_idx] &
						     unsignal_batch) ==
						    unsignal_batch) {
							struct ibv_wc wc;
							uint64_t comp = 0;
							while (comp == 0) {
								comp = ibv_poll_cq(
									memory_node
										.manager_
										.get_cq_ptr(
											t_global_id),
									1, &wc);
								if (comp > 0) {
									if (wc.status !=
									    0) {
										fprintf(stderr,
											"Bad wc status %d\n",
											wc.status);
									}
								}
							}
						}
						// wr[cur_idx][thread_batch_cnt[cur_idx]].send_flags |= IBV_SEND_INLINE;
						// batch reply
						if ((thread_batch_cnt[cur_idx] &
						     (thread_qp_batch - 1)) ==
						    (thread_qp_batch - 1)) {
							int ret = ibv_post_send(
								memory_node
									.manager_
									.get_qp_ptr(
										t_global_id),
								wr[cur_idx],
								&bad_wr);
							if (ret < 0)
								SDS_PERROR(
									"rpc write resp error!");
						}
						thread_batch_cnt[cur_idx] =
							((++thread_batch_cnt
								  [cur_idx]) %
							 thread_qp_batch);
						send_tx[cur_idx]++;
					}
				}
			}
			tt_qp += nr_qp;
		}
	}
}

// commented-out fusioncas rpc code

// void * HerdServer::rpc_func_impl(uint64_t rpc_thread_id)
// {
//     uint64_t logic_id = rpc_thread_id;
//     uint64_t thread_num = __MAX_RECORDER_NUM;
//     // number of reply batches; larger values increase latency
//     uint64_t thread_qp_batch = rpc_config_.thread_qp_batch;
//     uint64_t unsignal_batch = rpc_config_.unsignal_batch;
//     uint64_t rpc_thread_num = rpc_config_.rpc_thread_num;
//     uint64_t tt_thread_num = __MAX_RECORDER_NUM * machine_num;
//     // rpc_op capacity per thread buf per machine
//     uint64_t window_size = rpc_region_size / (2 * tt_thread_num * sizeof(rpc_op));

//     rpc_op * req_buf = (rpc_op *)rpc_addr;
//     uint64_t resp_buf = (uint64_t)rpc_addr + rpc_region_size / 2;

//     uint64_t lkey = memory_node.manager_.get_local_memory_lkey(sds::MAIN_MEMORY_MR_ID);
//     uint64_t send_tx[tt_thread_num];
//     memset(send_tx, 0, sizeof(uint64_t) * tt_thread_num);

//     struct ibv_send_wr * wr[tt_thread_num], *bad_wr;
//     struct ibv_sge * sge[tt_thread_num];
//     uint64_t thread_batch_cnt[tt_thread_num];
//     memset(thread_batch_cnt, 0, sizeof(uint64_t) * tt_thread_num);

//     for(int i = 0; i < tt_thread_num; i ++) {
//         wr[i] = (struct ibv_send_wr *)malloc(sizeof(struct ibv_send_wr) * thread_qp_batch);
//         memset(wr[i], 0, sizeof(struct ibv_send_wr) * thread_qp_batch);
//         sge[i] = (struct ibv_sge *)malloc(sizeof(struct ibv_sge) * thread_qp_batch);
//         memset(sge[i], 0, sizeof(struct ibv_sge) * thread_qp_batch);
//         for(int j = 0; j < thread_qp_batch; j ++) {
//             // sge[i][j].addr = (uint64_t)&resp[i * thread_qp_batch + j];
//             // sge[i][j].addr = resp_buf + sizeof(rpc_op) * (i * thread_qp_batch + j);
//             sge[i][j].length = sizeof(rpc_op);
//             sge[i][j].lkey = lkey;
//             wr[i][j].opcode = IBV_WR_RDMA_WRITE;
//             wr[i][j].num_sge = 1;
//             wr[i][j].next = (j == thread_qp_batch - 1) ? NULL : &wr[i][j + 1];
//             wr[i][j].sg_list = &sge[i][j];
//         }
//     }

//     while(1) {
//         uint64_t tt_qp = 0;
//         // derive server qp info from machine_id and client qpn
//         for(int k = 0; k < machine_num; k ++) {
//             uint32_t rkey;
//             uint64_t remote_va;
//             memory_node.manager_.get_remote_memory_key(k, sds::MAIN_MEMORY_MR_ID, 0, 8, rkey, remote_va);
//             uint64_t nr_qp = memory_node.manager_.get_qp_size(k);
//             // req buf for all threads on each machine
//             for(int i = thread_num / rpc_thread_num * logic_id; i < thread_num / rpc_thread_num * (logic_id + 1); i ++) {
//                 // each thread has window_size rpc requests and replies
//                 uint64_t t_local_id;
//                 uint64_t t_global_id;
//                 for(int j = 0; j < window_size; j ++) {
//                     uint64_t cur_idx = k * thread_num + i;
//                     // compute current req position
//                     uint64_t req_off = cur_idx * window_size;
//                     rpc_op * cur_req = (req_buf + req_off + j);
//                     rpc_op * cur_resp = ((rpc_op *)resp_buf + req_off + j);
//                     // use 888 as check value for now
//                     if(cur_req->crc == 888) {
//                         // CAS
//                         if(cur_req->opcode == 1) {
//                             // if(send_tx[cur_idx] % 100 == 0) {
//                                 // std::cout << "thread " << cur_idx << " send cas" << std::endl;
//                             // }
//                             // t_local_id is the qp id for this qpn on machine k
//                             // but server stores qp_ids for all machines, so compute global qp id
//                             t_local_id = cur_req->qpn % nr_qp;
//                             t_global_id = tt_qp + t_local_id;
//                             CAS((uint64_t *)(cur_req->wr.atomic.remote_addr), (uint64_t *)&(cur_req->wr.atomic.cmp), cur_req->wr.atomic.swap);
//                             cur_resp->comp = *(uint64_t *)(cur_req->wr.atomic.remote_addr);
//                             cur_resp->crc = 888;
//                         }
//                         cur_req->crc = 100;
//                         // ibv_qp * cur_qp = memory_node.manager_.get_qp_ptr(t_global_id);
//                         wr[cur_idx][thread_batch_cnt[cur_idx]].wr_id = send_tx[cur_idx];
//                         // temporarily set to unsignaled
//                         wr[cur_idx][thread_batch_cnt[cur_idx]].send_flags = (send_tx[cur_idx] & unsignal_batch) == 0 ? IBV_SEND_SIGNALED : 0;;
//                         wr[cur_idx][thread_batch_cnt[cur_idx]].wr.rdma.remote_addr = cur_req->resp_addr;
//                         wr[cur_idx][thread_batch_cnt[cur_idx]].wr.rdma.rkey = rkey;
//                         sge[cur_idx][thread_batch_cnt[cur_idx]].addr = (uint64_t)cur_resp;

//                         if((send_tx[cur_idx] & unsignal_batch) == unsignal_batch) {
//                             struct ibv_wc wc;
//                             uint64_t comp = 0;
//                             while(comp == 0) {
//                                 comp = ibv_poll_cq(memory_node.manager_.get_cq_ptr(t_global_id), 1, &wc);
//                                 if(comp > 0) {
//                                     if(wc.status != 0) {
//                                         fprintf(stderr, "Bad wc status %d\n", wc.status);
//                                     }
//                                 }
//                             }
//                         }

//                         // batch reply
//                         if((thread_batch_cnt[cur_idx] & (thread_qp_batch - 1)) == (thread_qp_batch - 1)) {
//                             int ret = ibv_post_send(memory_node.manager_.get_qp_ptr(t_global_id), wr[cur_idx], &bad_wr);
//                             if(ret < 0)
//                                 SDS_PERROR("rpc write resp error!");
//                         }
//                         thread_batch_cnt[cur_idx] = ((++thread_batch_cnt[cur_idx]) % thread_qp_batch);
//                         send_tx[cur_idx] ++;
//                     }
//                 }

//             }
//             tt_qp += nr_qp;
//         }
//     }
// }