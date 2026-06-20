#include <unistd.h>
#include <atomic>
#include <sys/time.h>
#include <iostream>
#include "smart/initiator.h"
#include "smart/rpc_common.h"
#include "coroutine.h"
#include "coroutine_scheduler.h"
#include "fast_random.h"
#include <functional>
#include <arpa/inet.h>

#define SHM_NAME "SHM_RECORD"
#define SHM_SIZE 209715200

#ifndef __MAX_COUNTER_NUM
#define __MAX_COUNTER_NUM 512
#endif

#ifndef __MAX_RECORDER_NUM
#define __MAX_RECORDER_NUM 32
#endif

#ifndef __FUSIONCAS_ATOMIC
#define CAS(_p, _u, _v)                                                        \
	(__atomic_compare_exchange_n(_p, _u, _v, false, __ATOMIC_ACQUIRE,      \
				     __ATOMIC_ACQUIRE))
#define FAA(_p, _v) (__atomic_fetch_add(_p, _v, __ATOMIC_ACQ_REL))
#endif

const coro_id_t POLL_ROUTINE_ID = 0;

typedef struct {
	uint32_t server_id;
	uint64_t thread_gid;
	uint32_t thread_id;
	uint64_t coro_num;
	uint64_t coro_start;
	uint64_t coro_attempts_num;
	uint64_t *rdma_buf;
	uint32_t buf_len;
	uint64_t *keys;
	sds::Initiator *conns;
	char *methods;
	uint64_t *finished_attempts;
	uint64_t *retry_cnt;
	CoroutineScheduler *coro_sched;
	std::vector<double> *tl_latencies;
	double *tl_tt_latency;
	int *running_tasks;
} coro_args;

class BasicClient {
    public:
	typedef struct {
		uint64_t thread_id;
		uint64_t rpc_idx;
		uint64_t server_id;
		size_t depth;
		BasicClient *client;
		pthread_barrier_t *barrier;
	} thread_arg;

	BasicClient(size_t nr_memory_nodes, size_t machine_num,
		    size_t machine_id, size_t depth, uint64_t attempts_num)
		: nr_memory_nodes(nr_memory_nodes), machine_num(machine_num),
		  machine_id(machine_id), depth_(depth), total_attempts(0),
		  attempts_num(attempts_num), nr_threads(1)
	{
		assert(nr_memory_nodes >= 1 &&
		       nr_memory_nodes <= sds::kMaxMemoryNodes);
		assert(machine_num >= 1);
		assert(machine_id < machine_num);
		assert(depth > 0);
		for (int i = 0; i < sds::kMaxMemoryNodes; i++) {
			conns.push_back(NULL);
		}
		// std::cerr << "BasicClient constructor, this = " << this << std::endl;
	}

	int set_conns(std::shared_ptr<sds::Initiator> conn, size_t mem_node_id);

	int set_conns(sds::Initiator *conn, size_t mem_node_id, int nouse);

	bool muliti_node_sync(uint64_t server_id);

	int add_request(struct ibv_send_wr &wr, struct ibv_sge &sge,
			uint64_t wr_id, ibv_wr_opcode opcode, void *local,
			const sds::GlobalAddress &remote, uint32_t rkey,
			uintptr_t remote_va, size_t length,
			uint64_t compare_add, uint64_t swap, int flags,
			sds::ResourceManager &manager_);

	int poll_n(size_t n, ibv_cq *cq, ibv_wc *wc);

	void start(size_t nr_threads);

	void set_shm(void *addr = NULL, uint64_t length = 0);

	void report(uint64_t elapsed_time, double usec_time);

	// current machine supports at most 32 threads, keep it for now
	uint64_t map_qpn_to_idx(uint64_t qpn, uint64_t *qpn_table);

	// declare virtual function for subclass impl
	virtual void thread_test_fun_impl(uint64_t local_rpc_idx,
					  uint64_t thread_id,
					  uint64_t server_id, size_t depth) = 0;

	// thread entry, static function
	static void *thread_test_fun(void *arg);

	// one Initiator object contains all qps
    public:
	size_t machine_num;
	size_t machine_id;
	size_t nr_threads;
	// std::vector<std::shared_ptr<sds::Initiator>> conns;
	std::vector<sds::Initiator *> conns;
	uint64_t attempts_num;
	std::atomic<uint64_t> total_attempts;
	std::atomic<uint64_t> total_retry_cnt;
	bool stop_signal;
	const static size_t SERVER_OFFSET = 64ul;
	size_t nr_memory_nodes;
	size_t depth_;
};