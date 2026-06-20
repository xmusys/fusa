#include "server.h"

#define HERD_RPC

#ifndef HERD_RPC
#define FUSA_RPC
#endif

static uint64_t reject_cnt = 0;

void Server::init_rpc(JsonConfig &rpc_config)
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
		(uint64_t)rpc_config.get("unsignal_batch").get_uint64();
	rpc_config_.thread_qp_batch =
		(uint64_t)rpc_config.get("thread_qp_batch").get_uint64();
}

int Server::start()
{
	// register memory for rnic

	rpc_addr = sds::mmap_huge_page(mem_pool_size + rpc_region_size);
	int rc = memory_node.register_main_memory(
		rpc_addr, mem_pool_size + rpc_region_size);
	assert(!rc);

	local_addr = rpc_addr + rpc_region_size;

	rc = memory_node.start(tcp_port);

	volatile uint64_t *check = (uint64_t *)local_addr;
	while (*check != machine_num) {
		;
	}
	assert(!rc);
	std::cout << "Alignment of rpc_op: " << alignof(rpc_op) << " bytes"
		  << std::endl;
	SDS_INFO("Herd Server sync remote %llu node...", *check);
	SDS_INFO("Server starts...");
	SDS_INFO("RPC address: %llu", rpc_addr);
	if (rpc_enabled)
		run_rpc();
}

// this is the code for FUSIONCAS server
void Server::run_rpc()
{
	// 1. register shared memory, pass information to the agent server
	SDS_INFO("Allocate server shared memory...");
	// create shared memory
	int shm_fd = shm_open(shm_name, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR);
	if (shm_fd == -1) {
		throw std::runtime_error("Error opening shared memory: " +
					 std::string(strerror(errno)));
	}

	// map shared memory
	server_shm_ptr = mmap(0, shm_size, PROT_READ | PROT_WRITE, MAP_SHARED,
			      shm_fd, 0);
	if (server_shm_ptr == MAP_FAILED) {
		close(shm_fd);
		throw std::runtime_error("Error mapping shared memory: " +
					 std::string(strerror(errno)));
	}

	SDS_INFO("Allocate shared memory completed...");

	SDS_INFO("Allocate strategy shared memory...");
	// create shared memory
	int strategy_shm_fd = shm_open(SERVER_STRATEGY_SHM, O_CREAT | O_RDWR,
				       S_IRUSR | S_IWUSR);
	if (strategy_shm_fd == -1) {
		throw std::runtime_error("Error opening shared memory: " +
					 std::string(strerror(errno)));
	}

	// map shared memory
	server_strategy_shm_ptr =
		mmap(0, strategy_shm_size, PROT_READ | PROT_WRITE, MAP_SHARED,
		     strategy_shm_fd, 0);
	if (server_strategy_shm_ptr == MAP_FAILED) {
		close(strategy_shm_fd);
		throw std::runtime_error("Error mapping shared memory: " +
					 std::string(strerror(errno)));
	}

	SDS_INFO("Allocate strategy shared memory completed...");

	// 2. write local rpc_addr and rkey to shared memory
	rpc_meta *meta_ = (rpc_meta *)server_shm_ptr;
	meta_->machine_num = machine_num;
	// uint64_t rkey = memory_node.manager_.get_local_memory_rkey(sds::MAIN_MEMORY_MR_ID);
	meta_->rkey = memory_node.manager_.get_local_memory_rkey(
		sds::MAIN_MEMORY_MR_ID);
	meta_->rpc_addr = rpc_addr;
	meta_->length = rpc_region_size / 2;
	meta_->crc = __CRC_COMPLETE;

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
}

void *Server::rpc_func(void *rarg)
{
	uint64_t rpc_thread_id = ((thread_arg *)rarg)->thread_id;
	Server *server = ((thread_arg *)rarg)->server;
	server->rpc_func_impl(rpc_thread_id);
}

void *Server::rpc_func_impl(uint64_t rpc_thread_id)
{
	std::unordered_map<uint64_t, uint64_t> qpn_table;
	uint64_t tmp_tt_qp = 0;
	// note that here we add a simple offset to the qpn mapping to prevent the collision of qpn mapping when all four clients restart at the same time
	// if the client gets stuck in mlx5_post_send, it may be because of the offset problem
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
	// the number of reply batches; larger values mean higher latency
	uint64_t thread_qp_batch = rpc_config_.thread_qp_batch;
	uint64_t unsignal_batch = rpc_config_.unsignal_batch;
	uint64_t signal_mask = unsignal_batch - 1;
	uint64_t rpc_thread_num = rpc_config_.rpc_thread_num;
	uint64_t tt_thread_num = __MAX_RECORDER_NUM * machine_num;
	// the number of rpc_op that each thread can hold
	uint64_t window_size =
		rpc_region_size / (2 * tt_thread_num * sizeof(rpc_op));

	rpc_op *req_buf = (rpc_op *)rpc_addr;
	uint64_t resp_buf = (uint64_t)rpc_addr + rpc_region_size / 2;

	uint64_t lkey = memory_node.manager_.get_local_memory_lkey(
		sds::MAIN_MEMORY_MR_ID);
	uint64_t send_tx[tt_thread_num];
	uint64_t last_signaled[tt_thread_num] = { 0 };
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
			// fusa-rpc
#ifdef FUSA_RPC
			sge[i][j].length = sizeof(rpc_op);
			wr[i][j].opcode = IBV_WR_RDMA_WRITE;
#endif

#ifdef HERD_RPC
			sge[i][j].length = sizeof(uint64_t);
			wr[i][j].opcode = IBV_WR_SEND;
#endif
			sge[i][j].lkey = lkey;

			wr[i][j].num_sge = 1;
			wr[i][j].next = (j == thread_qp_batch - 1) ?
						NULL :
						&wr[i][j + 1];
			wr[i][j].sg_list = &sge[i][j];
		}
	}

	server_layout *layout_ =
		(server_layout *)(server_strategy_shm_ptr + 64);

	while (1) {
		uint64_t tt_qp = 0;
		// get the qp information on server corresponding to the qpn of the client
		for (int k = 0; k < machine_num; k++) {
			uint32_t rkey;
			uint64_t remote_va;
			memory_node.manager_.get_remote_memory_key(
				k, sds::MAIN_MEMORY_MR_ID, 0, 8, rkey,
				remote_va);
			uint64_t nr_qp = memory_node.manager_.get_qp_size(k);
			// the req buf of all threads on the machine
			for (int i = thread_num / rpc_thread_num * logic_id;
			     i < thread_num / rpc_thread_num * (logic_id + 1);
			     i++) {
				// each thread can hold window_size rpc requests and replies
				uint64_t t_local_id;
				uint64_t t_global_id;
				for (int j = 0; j < window_size; j++) {
					uint64_t cur_idx = k * thread_num + i;
					// calculate the position of the current req
					uint64_t req_off =
						cur_idx * window_size;
					rpc_op *cur_req =
						(req_buf + req_off + j);
					rpc_op *cur_resp = ((rpc_op *)resp_buf +
							    req_off + j);
					if (cur_req->crc == CRC_CHECK_MAGIC) {
						bool reject_resp = false;
#ifndef __FULL_ONLOAD
						// consensus code
						// read the credit of the counter of the new and old strategies
						uint64_t counter_idx =
							((cur_req->wr.atomic
								  .remote_addr) %
							 (__MAX_COUNTER_NUM *
							  __SLOT_GRANULARITY *
							  8)) /
							8;
						uint16_t new_credit =
							layout_->strategies.credit
								[counter_idx];
						uint16_t old_credit =
							layout_->old_strategies.credit
								[counter_idx];
						bool credit_changed =
							(new_credit !=
							 old_credit);
						bool consensus = false;

						// reject if the credit is 0
						if (new_credit == 0) {
							reject_resp = true;
						}

						// check if the strategy has switched
						// compute consistent bit in the paper
						if ((new_credit ^ old_credit) ==
						    1) {
							// we only focus on the case that the credit changes from 0 to 1
							if (new_credit == 1) {
								// reject first, until there is consensus
								// we don't set the consensus = 0 after the consensus is reached
								// so we need % machine_num to check if the consensus is reached
								if (layout_->consensus %
									    machine_num ==
								    0) {
									reject_resp =
										false;
								} else {
									reject_resp =
										true;
								}
							}
						}

						if (reject_resp) {
							reject_cnt++;
						}
#endif
						auto it = qpn_table.find(
							cur_req->qpn);
						t_global_id = it->second;
						if (cur_req->opcode == 1 &&
						    reject_resp == false) {
							t_local_id =
								cur_req->qpn %
								nr_qp;
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
							cur_resp->crc =
								CRC_CHECK_MAGIC;
						} else if (cur_req->opcode ==
							   FAA_OP_CODE) {
							SDS_INFO(
								"Server receive FAA request");
						} else if (reject_resp ==
							   true) {
							// use imm to pass the reject information to the client
							// wr[cur_idx][thread_batch_cnt[cur_idx]].opcode = IBV_WR_SEND_WITH_IMM;
							// wr[cur_idx][thread_batch_cnt[cur_idx]].imm_data = htonl(__RETRY_IMM_CODE);
							cur_resp->comp =
								cur_req->wr
									.atomic
									.swap;
							cur_resp->crc =
								CRC_CHECK_MAGIC;
						}
						cur_req->crc = 0;
						wr[cur_idx]
						  [thread_batch_cnt[cur_idx]]
							  .wr_id =
							send_tx[cur_idx];
						// set to unsignaled for now
						wr[cur_idx]
						  [thread_batch_cnt[cur_idx]]
							  .send_flags =
							(((send_tx[cur_idx] &
							   signal_mask) == 0) ?
								 IBV_SEND_SIGNALED :
								 0);
						wr[cur_idx]
						  [thread_batch_cnt[cur_idx]]
							  .wr.rdma.rkey = rkey;
// FUSA_RPC is the RNIC-Friendly RPC
#ifdef FUSA_RPC
						wr[cur_idx]
						  [thread_batch_cnt[cur_idx]]
							  .wr.rdma.remote_addr =
							cur_req->resp_addr;
						sge[cur_idx]
						   [thread_batch_cnt[cur_idx]]
							   .addr =
							(uint64_t)cur_resp;
#endif
// HERD_RPC is the Coroutine-Friendly RPC
#ifdef HERD_RPC
						sge[cur_idx]
						   [thread_batch_cnt[cur_idx]]
							   .addr =
							(uint64_t) &
							(cur_resp->comp);
#endif
						if ((send_tx[cur_idx] &
						     signal_mask) == 0 &&
						    last_signaled[cur_idx] !=
							    send_tx[cur_idx]) {
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

						if (wr[cur_idx]
						      [thread_batch_cnt[cur_idx]]
							      .send_flags ==
						    IBV_SEND_SIGNALED) {
							last_signaled[cur_idx] =
								send_tx[cur_idx];
						}

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