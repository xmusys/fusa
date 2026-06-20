#include <unistd.h>
#include <iostream>

#ifndef counter_
#define counter_ uint64_t
#endif

#ifndef __MAX_COUNTER_NUM
#define __MAX_COUNTER_NUM 512
#endif

#ifndef __MAX_RECORDER_NUM
#define __MAX_RECORDER_NUM 32
#endif

#ifndef __CRC_COMPLETE
#define __CRC_COMPLETE 88888
#endif

#ifndef __SLOT_GRANULARITY
#define __SLOT_GRANULARITY 16
#endif

#ifndef __MAX_MACHINE_NUM
#define __MAX_MACHINE_NUM 16
#endif

// we must ensure that a message unit is 64B, because RDMA Write is only guaranteed to be in one Cacheline
#ifndef __COUNTER_PER_MESG
#define __COUNTER_PER_MESG ((64 - sizeof(uint64_t)) / sizeof(counter_))
#endif

// there are __MAX_COUNTER_NUM * __SLOT_GRANULARITY records
#ifndef __MESG_UNIT_NUM
#define __MESG_UNIT_NUM                                                        \
	((__MAX_COUNTER_NUM * __SLOT_GRANULARITY + __COUNTER_PER_MESG - 1) /   \
	 __COUNTER_PER_MESG)
#endif

#ifndef __RETRY_IMM_CODE
#define __RETRY_IMM_CODE 567
#endif

#ifndef __FULL_ONLOAD
// #define __FULL_ONLOAD
#endif

#ifndef __RPC_HEADER
#define __RPC_HEADER

#define CAS_OP_CODE (1)
#define FAA_OP_CODE (977)
#define CRC_CHECK_MAGIC (888)

#define QP_OFF 100000

// typedef struct {
//     bool flags[__MAX_COUNTER_NUM];
// }__attribute__((aligned(8))) strategies;

typedef struct {
	uint16_t credit[__MAX_COUNTER_NUM * __SLOT_GRANULARITY];
} strategies;

// used for driver
typedef struct {
	uint64_t qpn;
	counter_ counters[__MAX_COUNTER_NUM * __SLOT_GRANULARITY];
	counter_ c_counters[__MAX_COUNTER_NUM * __SLOT_GRANULARITY];
} recorder;

typedef struct {
	uint64_t machine_num;
	uint64_t machine_id;
	uint64_t rkey;
	void *rpc_addr;
	uint64_t length; // rpc_buf length
	uint64_t crc;
} rpc_meta;

typedef struct {
	uint64_t machine_num;
	uint64_t machine_id;
	uint64_t lkey;
	void *rpc_addr;
	uint64_t s_buf_length; // for send request
	uint64_t r_buf_length; // for receive request
} local_rpc_meta;

typedef struct {
	strategies strag;
	recorder records[__MAX_RECORDER_NUM];
	rpc_meta rmeta_; // remote meta
	local_rpc_meta lmeta_; // local meta
	uint32_t remote_off[__MAX_RECORDER_NUM];
	uint32_t local_off[__MAX_RECORDER_NUM];
	uint64_t running[__MAX_RECORDER_NUM];
	uint64_t epoch[__MAX_RECORDER_NUM];
} sh_local_region;

struct alignas(64) rpc_op
{
	union {
		struct {
			uint64_t remote_addr;
			uint64_t swap;
			uint64_t cmp;
		} atomic;
		struct {
			uint64_t remote_addr;
		} rdma;
	} wr;
	uint64_t resp_addr;
	uint64_t qpn;
	uint64_t opcode;
	uint64_t comp;
	uint64_t crc;
};

// unit for communication between agents, 64B
typedef struct {
	counter_ counters[__COUNTER_PER_MESG];
	uint64_t crc;
} mesg_unit;

typedef struct {
	uint64_t phase;
	uint16_t credit[__MAX_COUNTER_NUM * __SLOT_GRANULARITY];
} resp;

typedef struct {
	resp strategies;
	mesg_unit mesg_arr[__MESG_UNIT_NUM];
	mesg_unit c_mesg_arr[__MESG_UNIT_NUM];
	rpc_meta s_meta_;
} client_layout;

typedef struct {
	mesg_unit mesg_arr[__MESG_UNIT_NUM];
	mesg_unit c_mesg_arr[__MESG_UNIT_NUM];
	uint64_t phase;
} machine_unit;

typedef struct {
	resp strategies;
	machine_unit machine_arr[__MAX_MACHINE_NUM];
	resp uniform_strag;
	resp old_strategies;
	uint64_t consensus;
} server_layout;

#endif