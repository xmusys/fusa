#include <stdio.h>

#include "recorder.h"

void *shm_ptr = NULL;

uint64_t MaxBackoff = 2400; // 1us
uint64_t BackoffUnit = 2400 * 16; // 16us

// **initialize Bloom filter**
void bloom2D_init(CountingBloom2D *bloom)
{
	memset(bloom->filter, 0, sizeof(bloom->filter));
}

void local_lock_table_init(LocalLockTable *table)
{
	memset(table->local_lock, 0, sizeof(table->local_lock));
}