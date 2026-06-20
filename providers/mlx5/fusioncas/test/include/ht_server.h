#include <unistd.h>
#include <iostream>

#include "smart/initiator.h"
#include "smart/target.h"
#include "smart/common.h"
#include "smart/rpc_common.h"
#include "../../smart_ht/hashtable.h"

#define SERVER_SHM_NAME "SERVER_SHM"
#define SERVER_SHM_SIZE 1024

#ifndef FUSIONCAS_SERVER_CPP
#define FUSIONCAS_SERVER_CPP

#define CAS(_p, _u, _v)                                                        \
	(__atomic_compare_exchange_n(_p, _u, _v, false, __ATOMIC_ACQUIRE,      \
				     __ATOMIC_ACQUIRE))
#define FAA(_p, _v) (__atomic_fetch_add(_p, _v, __ATOMIC_ACQ_REL))

class HTServer {
    public:
	typedef struct {
		uint64_t thread_id;
		HTServer *server;
		pthread_barrier_t *barrier;
	} thread_arg;
	typedef struct {
		uint64_t compute_thread_num;
		uint64_t rpc_thread_num;
		uint64_t send_batch;
		uint64_t window_size;
		uint64_t unsignal_batch;
		uint64_t thread_qp_batch;
	} rpc_param;
	// Constructor
	HTServer(size_t tcp_port, uint64_t numa, size_t mem_pool_size,
		 size_t rpc_region_size, uint64_t machine_num,
		 bool rpc_enabled = false)
		: tcp_port(tcp_port), // default by 12345
		  memory_node(), // default construct memory_node
		  numa(numa), machine_num(machine_num),
		  rpc_enabled(rpc_enabled), mem_pool_size(mem_pool_size),
		  rpc_region_size(rpc_region_size), shm_name(SERVER_SHM_NAME),
		  shm_size(SERVER_SHM_SIZE)
	{
		// Initialize the memory regions to nullptr by default
		local_addr = nullptr;
		rpc_addr = nullptr;

		if (rpc_enabled == false)
			rpc_region_size = 0;

		assert(rpc_region_size < mem_pool_size);

		// Initialize rpc_enabled flag
		std::cout << "Server initialized with memory pool size: "
			  << mem_pool_size << " and RPC enabled: "
			  << (rpc_enabled ? "Yes" : "No") << std::endl;
	}
	void init_rpc(JsonConfig &rpc_config);

	int start();

	void run_rpc();

	static void *rpc_func(void *rarg);

	void *rpc_func_impl(uint64_t rpc_thread_id);

    private:
	size_t tcp_port;
	sds::Target memory_node; // Target object
	uint64_t numa; // needed for rpc
	uint64_t machine_num;
	void *local_addr;
	void *rpc_addr;
	// GB
	size_t mem_pool_size;
	// MB
	size_t rpc_region_size;
	bool rpc_enabled;

	rpc_param rpc_config_;

	const char *shm_name;
	// B
	size_t shm_size;
	void *server_shm_ptr;
};

#endif