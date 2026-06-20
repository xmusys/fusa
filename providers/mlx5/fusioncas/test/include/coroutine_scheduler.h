// Author: Ming Zhang
// Copyright (c) 2022

#pragma once

#include <list>
#include <infiniband/verbs.h>

#if defined(__GNUC__) || defined(__clang__)
#define ALWAYS_INLINE inline __attribute__((always_inline))
#elif defined(_MSC_VER)
#define ALWAYS_INLINE __forceinline
#else
#define ALWAYS_INLINE inline
#endif

#if defined(__GNUC__) || defined(__clang__)
#define likely(x) __builtin_expect(!!(x), 1)
#define unlikely(x) __builtin_expect(!!(x), 0)
#else
#define likely(x) (x)
#define unlikely(x) (x)
#endif

#include "coroutine.h"

using t_id_t = uint32_t; // Thread id type

// Scheduling coroutines. Each txn thread only has ONE scheduler
class CoroutineScheduler {
    public:
	// The coro_num includes all the coroutines
	CoroutineScheduler(t_id_t thread_id, coro_id_t coro_num)
	{
		t_id = thread_id;
		pending_counts = new int[coro_num];
		pending_log_counts = new int[coro_num];
		for (coro_id_t c = 0; c < coro_num; c++) {
			pending_counts[c] = 0;
			pending_log_counts[c] = 0;
		}
		coro_array = new Coroutine[coro_num];
		stop_run = false;
		finish = 0;
	}
	~CoroutineScheduler()
	{
		if (pending_counts)
			delete[] pending_counts;
		if (pending_log_counts)
			delete[] pending_log_counts;
		if (coro_array)
			delete[] coro_array;
	}

	// For RDMA requests
	void AddPendingQP(coro_id_t coro_id, struct ibv_qp *qp);

	// For polling
	void PollCompletion(); // There is a coroutine polling ACKs

	void PollRegularCompletion();

	bool CheckLogAck(coro_id_t c_id);

	// Link coroutines in a loop manner
	void LoopLinkCoroutine(coro_id_t coro_num);

	// For coroutine yield, used by transactions
	void Yield(coro_yield_t &yield, coro_id_t cid);

	// For Finish coroutine yield, used by transactions
	void FinishYield(coro_yield_t &yield, coro_id_t cid);

	// Append this coroutine to the tail of the yield-able coroutine list
	// Used by coroutine 0
	void AppendCoroutine(Coroutine *coro);

	// Start this coroutine. Used by coroutine 0
	void RunCoroutine(coro_yield_t &yield, Coroutine *coro);

	void RDMASend(coro_id_t coro_id, struct ibv_qp *qp,
		      struct ibv_send_wr *wr, struct ibv_send_wr **bad_wr);

    public:
	Coroutine *coro_array;

	Coroutine *coro_head;

	Coroutine *coro_tail;

	uint64_t finish;

	bool stop_run;

    private:
	t_id_t t_id;

	std::list<struct ibv_qp *> pending_qps;

	// number of pending qps (i.e., the ack has not received) per coroutine
	int *pending_counts;

	// number of pending log qps (i.e., the ack has not received) per coroutine
	int *pending_log_counts;
};

ALWAYS_INLINE
void CoroutineScheduler::AddPendingQP(coro_id_t coro_id, struct ibv_qp *qp)
{
	pending_qps.push_back(qp);
	pending_counts[coro_id] += 1;
}

// Link coroutines in a loop manner
ALWAYS_INLINE
void CoroutineScheduler::LoopLinkCoroutine(coro_id_t coro_num)
{
	// The coroutines are maintained in an array,
	// but linked via pointers for efficient yield scheduling, init finish
	for (uint i = 0; i < coro_num; ++i) {
		coro_array[i].prev_coro = coro_array + i - 1;
		coro_array[i].next_coro = coro_array + i + 1;
	}
	coro_head = &(coro_array[0]); // coro 0 is always poll
	coro_tail = &(coro_array[coro_num - 1]);
	coro_array[0].prev_coro = coro_tail;
	coro_array[coro_num - 1].next_coro = coro_head;
	finish = 0;
}

// For coroutine yield, used by transactions
ALWAYS_INLINE
void CoroutineScheduler::Yield(coro_yield_t &yield, coro_id_t cid)
{
	int i;
	// if no pending to process
	switch (cid) {
	case 0:
		i = 1;
		break;
	case 1:
		i = 1;
		break;
	case 2:
		i = 1;
		break;
	case 3:
		i = 1;
		break;
	case 4:
		i = 1;
		break;
	case 5:
		i = 1;
		break;
	case 6:
		i = 1;
		break;
	case 7:
		i = 1;
		break;
	case 8:
		i = 1;
		break;
	case 9:
		i = 1;
		break;
	case 10:
		i = 1;
		break;
	case 11:
		i = 1;
		break;
	case 12:
		i = 1;
		break;
	case 13:
		i = 1;
		break;
	case 14:
		i = 1;
		break;
	case 15:
		i = 1;
		break;
	case 16:
		i = 1;
		break;
	default:
		break;
	}
	if (unlikely(pending_counts[cid] == 0)) {
		return;
	}
	// 1. Remove this coroutine from the yield-able coroutine list
	Coroutine *coro = &coro_array[cid]; // current coroutine
	assert(coro->is_wait_poll == false);
	Coroutine *next = coro->next_coro;
	coro->prev_coro->next_coro = next;
	next->prev_coro = coro->prev_coro;
	if (coro_tail == coro)
		coro_tail = coro->prev_coro; // remove from wait queue
	coro->is_wait_poll = true;
	// 2. Yield to the next coroutine
	// RDMA_LOG(DBG) << "coro: " << cid << " yields to coro " << next->coro_id;
	RunCoroutine(yield, next);
}

// For coroutine yield, used by transactions
ALWAYS_INLINE
void CoroutineScheduler::FinishYield(coro_yield_t &yield, coro_id_t cid)
{
	Coroutine *coro = &coro_array[cid]; // current coroutine
	// assert(coro->is_wait_poll == false);
	Coroutine *next = coro->next_coro;
	coro->prev_coro->next_coro = next;
	next->prev_coro = coro->prev_coro;
	if (coro_tail == coro)
		coro_tail = coro->prev_coro; // remove from wait queue

	// 2. Yield to the next coroutine
	// RDMA_LOG(DBG) << "coro: " << cid << " yields to coro " << next->coro_id;
	RunCoroutine(yield, next);
}

// Start this coroutine. Used by coroutine 0 and Yield()
ALWAYS_INLINE
void CoroutineScheduler::RunCoroutine(coro_yield_t &yield, Coroutine *coro)
{
	// RDMA_LOG(DBG) << "yield to coro: " << coro->coro_id;
	coro->is_wait_poll = false; // running this means not waiting
	yield(coro->func);
}

// Append this coroutine to the tail of the yield-able coroutine list. Used by coroutine 0
ALWAYS_INLINE
void CoroutineScheduler::AppendCoroutine(Coroutine *coro)
{
	if (!coro->is_wait_poll)
		return;
	Coroutine *prev = coro_tail; // append after tail
	prev->next_coro = coro;
	coro_tail = coro;
	coro_tail->next_coro = coro_head;
	coro_tail->prev_coro = prev;
}
