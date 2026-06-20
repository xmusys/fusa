#include <iostream>
#include <sys/mman.h> // mmap, munmap
#include <sys/stat.h> // S_IRUSR, S_IWUSR
#include <fcntl.h> // O_CREAT, O_RDWR
#include <unistd.h> // close
#include <cstring> // strerror
#include <string>
#include <bitset>
#include <queue>
#include <math.h>

#include "smart/initiator.h"
#include "smart/target.h"
#include "smart/rpc_common.h"

#define SHM_NAME "SHM_RECORD"
#define SHM_SIZE 209715200

#define SERVER_SHM_NAME "SERVER_SHM"
#define SERVER_SHM_SIZE 1024

#define SERVER_STRATEGY_SHM "SERVER_STRATEGY_SHM"

#ifndef __CRC_COMPLETE
#define __CRC_COMPLETE 88888
#endif

// temporarily set to 10000
#ifndef __CONTENTION_THRESHOLD
#define __CONTENTION_THRESHOLD 10000
#endif

#ifndef __CPU_CAPACITY
#define __CPU_CAPACITY 2500000 // 2.5M for CPU
#endif

#ifndef __RNIC_CAPACITY
#define __RNIC_CAPACITY 40000000 // 40M for RNIC
#endif

#ifndef __SEQUENCER
// #define __SEQUENCER
#endif

// use this threshold to distinguish between pure NIC and other cases
#define RNIC_THRESHOLD 10000000

#define __SERVER_PRINT_SLOTS

#define CAS(_p, _u, _v)                                                        \
	(__atomic_compare_exchange_n(_p, _u, _v, false, __ATOMIC_ACQUIRE,      \
				     __ATOMIC_ACQUIRE))
#define FAA(_p, _v) (__atomic_fetch_add(_p, _v, __ATOMIC_ACQ_REL))

static uint64_t local_offset = 64;

// const uint64_t counter_per_mesg = (64 - sizeof(uint64_t)) / sizeof(counter_);

class Agent {
    public:
	typedef struct cpu_task {
		counter_ req_cnt;
		uint64_t idx_;
		uint64_t inner_idx;
		// overload < operator (max heap)
		bool operator<(const cpu_task &other) const
		{
			return req_cnt < other.req_cnt;
		}
	} cpu_task;

	Agent(const char *shm_name, size_t shm_size)
		: agent_type("unknown"), shm_name(shm_name), shm_size(shm_size),
		  shm_fd(-1), ptr(nullptr)
	{
		memset(counters, 0, sizeof(counter_) * __MAX_COUNTER_NUM);
		memset(rkey, 0, sizeof(uint32_t) * __MAX_MACHINE_NUM);
		memset(remote_va, 0, sizeof(uintptr_t) * __MAX_MACHINE_NUM);
	}
	Agent()
		: agent_type("unknown"), shm_name(SHM_NAME), shm_size(SHM_SIZE),
		  shm_fd(-1), ptr(nullptr)
	{
		memset(counters, 0, sizeof(counter_) * __MAX_COUNTER_NUM);
		memset(rkey, 0, sizeof(uint32_t) * __MAX_MACHINE_NUM);
		memset(remote_va, 0, sizeof(uintptr_t) * __MAX_MACHINE_NUM);
	}
	~Agent();

	void make_shm();

	void make_shm4server();

	void collect();

	void post_send();

	void set_type(std::string type);

	void set_strategy_shm_size(size_t strategy_shm_sz)
	{
		strategy_shm_size = strategy_shm_sz;
	}

	void set_conn(uint64_t machine_num, uint64_t machine_idx,
		      std::shared_ptr<sds::Initiator> agent_conn,
		      uint64_t mem_size);

	void *get_local_addr()
	{
		return local_addr;
	}

	void register_memory(void *addr, size_t length);

	void register_shm_memory();

	bool muliti_node_sync();

	bool check_server_meta();

	// send the rpc address of the application to the agent client
	void send_server_meta(rpc_meta *cur_meta);

	void start_client(uint64_t machine_num, uint64_t port);

	void start_server(uint64_t machine_num, uint64_t port,
			  bool enable = true);

	void stop_server();

	void server_func();

	void set_threshold(size_t threshold_)
	{
		threshold = threshold_;
	}

	void set_pa_enable(bool pa_enable)
	{
		pa_enabled = pa_enable;
	}

	int add_request(struct ibv_send_wr &wr, struct ibv_sge &sge,
			uint64_t wr_id, ibv_wr_opcode opcode, const void *local,
			const sds::GlobalAddress &remote, uint32_t rkey,
			uintptr_t remote_va, size_t length,
			uint64_t compare_add, uint64_t swap, int flags,
			sds::ResourceManager &manager_);

	int poll_n(size_t n, ibv_cq *cq, ibv_wc *wc);

	void wait_sync();

	void print();

	void print_server_counters()
	{
#ifdef __SERVER_PRINT_SLOTS
		for (uint64_t i = 0; i < __MAX_COUNTER_NUM; i++) {
			if (i != 0 && i % 32 == 0)
				std::cout << std::endl;
			std::cout << "rnic: " << real_counters[i] << "\t"
				  << "cpu: " << real_c_counters[i] << "\t";
		}
#else
		;
#endif
	}

    public:
	uint64_t threshold;
	uint64_t cpu_threshold;
	uint64_t filterThres;
	uint64_t overThres;
	uint64_t *onload_nums;
	uint64_t core_num_;
	bool sequencer;

    private:
	std::string agent_type;
	size_t nr_machine;
	uint64_t machine_id;

	// used for client
	const char *shm_name;
	size_t shm_size;
	int shm_fd;

	size_t strategy_shm_size;

	// pointer to the shared memory area
	void *ptr;
	void *server_shm_ptr;
	void *server_strategy_shm_ptr;

	// used for RNIC
	counter_ counters[__MAX_COUNTER_NUM * __SLOT_GRANULARITY];
	// used for cpu
	counter_ c_counters[__MAX_COUNTER_NUM * __SLOT_GRANULARITY];

	counter_ real_counters[__MAX_COUNTER_NUM];
	counter_ real_c_counters[__MAX_COUNTER_NUM];

	std::shared_ptr<sds::Initiator> conn;
	std::shared_ptr<sds::Target> mem_handler;
	void *local_addr;

	// used for server
	uint32_t rkey[__MAX_MACHINE_NUM];
	uintptr_t remote_va[__MAX_MACHINE_NUM];
	bool server_running;
	uint64_t tt_count;
	bool agent_enabled;
	// PCIe Atomic enable flag for PCIe Atomic
	bool pa_enabled;

	// deprecated
	std::bitset<8> windows[__MAX_COUNTER_NUM];
};