/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <stddef.h>

typedef struct address_range address_range;
struct address_range {
    size_t start;
    size_t size;
    size_t present;
    size_t swap;
};

#define PAGESIZE		4096UL

#define NUM_SB              129
#define SLAB_GROUP_SIZE     8
#define SLAB_GROUP_COUNT    NUM_SB
#define SLABSIZE            ((uintptr_t) (1 << 16))

/* Slab sizes 0 to 512 bytes in steps of 16 */
#define SB_MAX			((NUM_SB - 1) * 16)

#define BLOCK_GROUP_SIZE 4
// 0 - 8k, 8k - 4M, step 64k
// 4M-32M, step 1M
// 32M - 128M, step 4M
#define BLOCK_GROUP_STEP (64 * 1024)
#define BLOCK_GROUP_STEP_1 (2lu * 1024 * 1024)
#define BLOCK_GROUP_STEP_2 (16lu * 1024 * 1024)
#define BLOCK_GROUP_SMALL_COUNT 17
#define BLOCK_GROUP_MIDDLE_COUNT (BLOCK_GROUP_SMALL_COUNT + 16)
#define BLOCK_GROUP_COUNT (BLOCK_GROUP_MIDDLE_COUNT + 6)
#define BLOCK_GROUP_0_MAX_SIZE 8 * 1024
#define BLOCK_GROUP_MIDDLE_START_SIZE (1lu * 1024 * 1024)
#define BLOCK_GROUP_BIG_START_SIZE (33lu * 1024 * 1024)

typedef struct thread_slab_slot_info thread_slab_slot_info;
struct thread_slab_slot_info {
    size_t size;
    size_t chunk_max_count;
    size_t active_chunk_count;
    size_t full_chunk_count;
};

typedef struct thread_slab_stat thread_slab_stat;
struct thread_slab_stat {
    thread_slab_slot_info info[NUM_SB];
};

typedef struct thread_pool_stat thread_pool_stat;
struct thread_pool_stat {
    size_t low_use_chunk_count;
    size_t closed_chunk_count;
    size_t total_slab_chunk_count;
};

typedef struct thread_stat thread_stat;
struct thread_stat {
    void *tl;
    size_t pid;
    char name[32];
    size_t free_chunk_count;
    thread_slab_stat slab;
    thread_pool_stat pool;
};

typedef struct central_slab_atls_info central_slab_atls_info;
struct central_slab_atls_info {
    void *tl;
    thread_slab_slot_info summary;
    size_t active_total_count;
    size_t active_used_count;
    size_t full_total_count;
    size_t full_used_count;
    size_t slab_16_count;
};

typedef struct central_slab_group_info central_slab_group_info;
struct central_slab_group_info {
    size_t atls_count;
    central_slab_atls_info slab_info[SLAB_GROUP_SIZE];
};

typedef struct central_slab_stat central_slab_stat;
struct central_slab_stat {
    central_slab_group_info info[SLAB_GROUP_COUNT];
};

typedef struct central_pool_atls_info central_pool_atls_info;
struct central_pool_atls_info {
    void *tl;
    size_t low_use_chunk_count;
    size_t closed_chunk_count;
    size_t total_slab_chunk_count;
    size_t closed_total_use_count;
    size_t closed_total_free_count;
    size_t closed_total_free_size;
    size_t low_total_use_count;
    size_t low_total_free_count;
    size_t low_total_free_size;
    size_t free_chunk_count;
};

typedef struct central_pool_stat central_pool_stat;
struct central_pool_stat {
    size_t atls_count;
    central_pool_atls_info pool_info[SLAB_GROUP_SIZE];
};

typedef struct mem_size mem_size;
struct mem_size {
    size_t size;
    size_t phys_size;
    size_t swap_size;
};

typedef struct central_btree_info central_btree_info;
struct central_btree_info {
    void *tl;
    size_t mmap_count;
    mem_size mmap_size;
    mem_size cache_size;
    mem_size fl_size;
    mem_size queue_size;
    mem_size qs_size;
    size_t qs_count;
    size_t queue_count;
    size_t a_alloced;
    size_t total_leaf_count;
    size_t free_slab_chunk_count;
    thread_slab_stat slab;
    size_t btree_mmap_advise;
    size_t malloc_count;
    size_t free_count;
    size_t last_scan_size;
};

typedef struct central_btree_group_info central_btree_group_info;
struct central_btree_group_info {
    size_t atls_count;
    size_t munmap_count;
    size_t munmap_size;
    central_btree_info info[BLOCK_GROUP_SIZE];
};

typedef struct central_btree_stat central_btree_stat;
struct central_btree_stat {
    central_btree_group_info info[BLOCK_GROUP_COUNT];
};

typedef struct slab_free_queue_stat slab_free_queue_stat;
struct slab_free_queue_stat {
    size_t chunk_count;
    size_t queue_size_limit;
    size_t recent_pop_failed_count;
    size_t limit_dec_count;
};

typedef struct recycle_queue_stat recycle_queue_stat;
struct recycle_queue_stat {
    size_t chunk_count;
};

typedef struct misc_stat misc_stat;
struct misc_stat {
    size_t big_alloc_size;
    size_t sbrk_start;
    size_t sbrk_size;
    size_t sbrk_phys_size;
    size_t sbrk_swap_size;
    size_t sbrk_waste_size;
    size_t sbrk_chunk_count;
};

typedef struct lockless_stat lockless_stat;
struct lockless_stat {
    size_t thread_count;
    thread_stat *thread;
    central_slab_stat slab;
    central_pool_stat pool;
    slab_free_queue_stat free_queue;
    recycle_queue_stat recycle;
    central_btree_stat btree;
    misc_stat misc;
};

typedef struct metric_stat metric_stat;
struct metric_stat {
    mem_size slab_size;
    mem_size btree_size;
    mem_size btree_in_use_size;
    size_t big_size;
    size_t central_slab_size;
    size_t central_slab_in_use_size;
    size_t free_queue_slab_size;
};
