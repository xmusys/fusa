#include <unistd.h>
#include <stdbool.h>
#include <stdint.h>
#include <time.h>
#include <x86intrin.h>
// #include <city.h>
#include <pthread.h>
#include <stddef.h>

#define __FUSIONCAS

#ifdef __FUSIONCAS

#ifndef counter_
#define counter_ uint64_t
#endif

#define SHM_NAME "SHM_RECORD"
#define SHM_SIZE 209715200

// RNIC Locking Table slot number
#ifndef __MAX_COUNTER_NUM
#define __MAX_COUNTER_NUM 512
#endif

// QP number
#ifndef __MAX_RECORDER_NUM
#define __MAX_RECORDER_NUM 32
#endif

// Group Size = __MAX_COUNTER_NUM * __SLOT_GRANULARITY
#ifndef __SLOT_GRANULARITY
#define __SLOT_GRANULARITY 16
#endif

#ifndef __STATIC_STRAG
// #define __STATIC_STRAG
#endif

#ifndef __SEQUENCER
// #define __SEQUENCER
#endif

#define BLOOM_ROWS 256 // number of rows
#define BLOOM_COLS 256 // number of columns

#define CAS_OP_CODE (1)
#define FAA_OP_CODE (977)
#define CRC_CHECK_MAGIC (888)

// #define __LOCAL_LOCK_TABLE
// #define __RETRY_AVOID

#define HERD_RPC

#ifndef HERD_RPC
#define FUSA_RPC
#endif

// _p, _u, _v represent for
#define CAS(_p, _u, _v)                                                        \
	(__atomic_compare_exchange_n(_p, _u, _v, false, __ATOMIC_ACQUIRE,      \
				     __ATOMIC_ACQUIRE))
#define FAA(_p, _v) (__atomic_fetch_add(_p, _v, __ATOMIC_ACQ_REL))

// typedef struct {
//     bool flags[__MAX_COUNTER_NUM];
// }__attribute__((aligned(8))) strategies;

#define QP_OFF 100000

#define f_seed 0xc70697UL
#define s_seed 0xc63548UL

extern uint64_t MaxBackoff;
extern uint64_t BackoffUnit;

// **2D Counting Bloom Filter**
typedef struct {
	uint32_t filter[BLOOM_ROWS][BLOOM_COLS]; // counter array
} CountingBloom2D;

typedef struct {
	uint64_t local_lock[BLOOM_ROWS][BLOOM_COLS];
} LocalLockTable;

typedef struct {
	uint16_t credit[__MAX_COUNTER_NUM * __SLOT_GRANULARITY];
} strategies;

typedef struct {
	uint64_t qpn;
	counter_ counters[__MAX_COUNTER_NUM * __SLOT_GRANULARITY];
	counter_ c_counters[__MAX_COUNTER_NUM * __SLOT_GRANULARITY];
} recorder;

// parse server's RPC metadata
typedef struct {
	uint64_t machine_num; // number of machines
	uint64_t machine_id; // machine id
	uint64_t rkey; // remote key
	void *rpc_addr; // RPC buffer address
	uint64_t length; // RPC buffer length
	uint64_t crc; // CRC
} rpc_meta;

// parse client's RPC metadata
typedef struct {
	uint64_t machine_num; // number of machines
	uint64_t machine_id; // machine id
	uint64_t lkey; // local key
	void *rpc_addr; // RPC buffer address
	uint64_t s_buf_length; // send buffer length
	uint64_t r_buf_length; // receive buffer length
} local_rpc_meta;

// local shared memory region metadata, i.e., Fusa-SHM
typedef struct {
	strategies strag;
	recorder records[__MAX_RECORDER_NUM]; // per QP metadata
	rpc_meta rmeta_; // remote meta
	local_rpc_meta lmeta_; // local meta
	uint32_t remote_off[__MAX_RECORDER_NUM]; // remote RPC region pointer
	uint32_t local_off[__MAX_RECORDER_NUM]; // local RPC region pointer
	uint64_t running[__MAX_RECORDER_NUM]; // running flag
	uint64_t epoch[__MAX_RECORDER_NUM]; // epoch
	uint64_t inflight[__MAX_RECORDER_NUM]; // inflight flag
	CountingBloom2D hotness_bloom; // hotness bloom filter, staled
	LocalLockTable local_lock_table; // local locking table, staled
} sh_local_region;

// 64B
// RPC request message
typedef struct {
	union { // atomic or rdma
		struct {
			uint64_t remote_addr;
			uint64_t swap;
			uint64_t cmp;
		} atomic;
		struct {
			uint64_t remote_addr;
		} rdma;
	} wr;
	uint64_t resp_addr; // not used
	uint64_t qpn;
	uint64_t opcode; // FAA_OP_CODE or CAS_OP_CODE
	uint64_t comp;
	uint64_t crc;
} rpc_op;

extern void *shm_ptr;

static inline uint32_t fast_rand(uint64_t *seed)
{
	*seed = __rdtsc(); // read timestamp counter
	return (*seed >> 32) ^ *seed; // mix high and low bits
}

// mapping qp_id -> idx in array
static inline uint64_t map_qpn_to_idx(uint64_t qpn, uint64_t insert)
{
	uint64_t first_idx = qpn % __MAX_RECORDER_NUM;
	uint64_t cur_pos = ((sh_local_region *)shm_ptr)->records[first_idx].qpn;
	if (insert == 1) {
		if (cur_pos == 0) {
			return first_idx;
		} else {
			for (int i = first_idx + 1;
			     i < first_idx + __MAX_RECORDER_NUM; i++) {
				cur_pos =
					((sh_local_region *)shm_ptr)
						->records[i % __MAX_RECORDER_NUM]
						.qpn;
				if (cur_pos == 0) {
					uint64_t second_idx =
						i % __MAX_RECORDER_NUM;
					return second_idx;
				}
			}
		}
	} else {
		for (int i = first_idx; i < first_idx + __MAX_RECORDER_NUM;
		     i++) {
			if (((sh_local_region *)shm_ptr)->records[i].qpn == qpn)
				return i;
		}
	}
}

void bloom2D_init(CountingBloom2D *bloom);

void local_lock_table_init(LocalLockTable *table);

static inline uint64_t MyCityHash64WithSeed(uint64_t *data, size_t len,
					    uint64_t seed)
{
	uint64_t hash = seed ^ len;
	size_t num_uint64 =
		len / sizeof(uint64_t); // number of complete uint64_t blocks

	for (size_t i = 0; i < num_uint64; i++) {
		hash ^= data[i]; // XOR with 64-bit data
		hash *= 0x9e3779b97f4a7c15ULL; // multiply by golden ratio
	}

	// handle remaining bytes (if len is not a multiple of 8)
	uint8_t *byte_data = (uint8_t *)(data + num_uint64);
	size_t remaining_bytes = len % sizeof(uint64_t);
	for (size_t i = 0; i < remaining_bytes; i++) {
		hash ^= (uint64_t)byte_data[i]
			<< (i * 8); // handle partial trailing bytes
		hash *= 0x9e3779b97f4a7c15ULL;
	}

	return hash;
}

//  key -> row index
static inline uint32_t hash_row(uint64_t key)
{
	uint64_t row = MyCityHash64WithSeed(&key, sizeof(uint64_t), f_seed);
	return (row % BLOOM_ROWS);
}

// key -> col index
static inline uint32_t hash_col(uint64_t key)
{
	uint64_t col = MyCityHash64WithSeed(&key, sizeof(uint64_t), s_seed);
	return (col % BLOOM_COLS);
}

// insert key into bloom filter
static inline uint64_t bloom2D_insert(CountingBloom2D *bloom, uint64_t key)
{
	uint64_t row = hash_row(key);
	uint64_t col = hash_col(key);
	// bloom->filter[row][col]++;  // **increment count**
	FAA(&(bloom->filter[row][col]), 1);
	return bloom->filter[row][col];
}

static inline void bloom2D_decre(CountingBloom2D *bloom, uint64_t key)
{
	uint64_t row = hash_row(key);
	uint64_t col = hash_col(key);
	// bloom->filter[row][col]++;  // **increment count**
	FAA(&(bloom->filter[row][col]), -1);
	// return bloom->filter[row][col];
}

// **query key frequency**
static inline uint32_t bloom2D_query(CountingBloom2D *bloom, uint64_t key)
{
	uint64_t row = hash_row(key);
	uint64_t col = hash_col(key);
	return bloom->filter[row][col]; // **return count**
}

static inline bool locking_local_table(LocalLockTable *table, uint64_t key)
{
	uint64_t row = hash_row(key);
	uint64_t col = hash_col(key);

	uint64_t cmp = 0;
	do {
		cmp = 0;
		CAS(&(table->local_lock[row][col]), &cmp, 1);
	} while (cmp != 0);
	return true;
}

static inline bool unlock_local_table(LocalLockTable *table, uint64_t key)
{
	uint64_t row = hash_row(key);
	uint64_t col = hash_col(key);

	uint64_t cmp = 1;
	do {
		cmp = 1;
		CAS(&(table->local_lock[row][col]), &cmp, 0);
	} while (cmp != 1);
	return true;
}

#endif