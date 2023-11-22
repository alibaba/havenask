//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once

#include <assert.h>
#include <stddef.h>
#include <stdint.h>
#include <string>

#include "autil/Autovector.h"
#include "autil/ConstString.h"
#include "autil/Lock.h"
#include "autil/cache/cache.h"
#include "autil/cache/cache_allocator.h"
#include "sharded_cache.h"

namespace autil {

// LRU cache implementation

// An entry is a variable length heap-allocated structure.
// Entries are referenced by cache and/or by any external entity.
// The cache keeps all its entries in table. Some elements
// are also stored on LRU list.
//
// LRUHandle can be in these states:
// 1. Referenced externally AND in hash table.
//  In that case the entry is *not* in the LRU. (refs > 1 && in_cache == true)
// 2. Not referenced externally and in hash table. In that case the entry is
// in the LRU and can be freed. (refs == 1 && in_cache == true)
// 3. Referenced externally and not in hash table. In that case the entry is
// in not on LRU and not in table. (refs >= 1 && in_cache == false)
//
// All newly created LRUHandles are in state 1. If you call
// LRUCacheShard::Release
// on entry in state 1, it will go into state 2. To move from state 1 to
// state 3, either call LRUCacheShard::Erase or LRUCacheShard::Insert with the
// same key.
// To move from state 2 to state 1, use LRUCacheShard::Lookup.
// Before destruction, make sure that no handles are in state 1. This means
// that any successful LRUCacheShard::Lookup/LRUCacheShard::Insert have a
// matching
// RUCacheBase::Release (to move into state 2) or LRUCacheShard::Erase (for state 3)

struct LRUHandle {
    void *value;
    void (*deleter)(const autil::StringView &, void *value, const CacheAllocatorPtr &allocator);
    LRUHandle *next_hash;
    LRUHandle *next;
    LRUHandle *prev;
    size_t charge; // TODO(opt): Only allow uint32_t?
    size_t key_length;
    // The hash of key(). Used for fast sharding and comparisons.
    uint32_t hash;
    // The number of external refs to this entry. The cache itself is not counted.
    uint32_t refs;

    // Mutable flags - access controlled by mutex
    // The m_ and M_ prefixes (and im_ and IM_ later) are to hopefully avoid
    // checking an M_ flag on im_flags or an IM_ flag on m_flags.
    uint8_t m_flags;
    enum MFlags : uint8_t {
        // Whether this entry is referenced by the hash table.
        M_IN_CACHE = (1 << 0),
        // Whether this entry has had any lookups (hits).
        // TODO(chenggua): M_HAS_HIT has not been applied in lru_cache.cpp
        M_HAS_HIT = (1 << 1),
        // Whether this entry is in high-pri pool.
        M_IN_HIGH_PRI_POOL = (1 << 2),
        // Whether this entry is in low-pri pool.
        M_IN_LOW_PRI_POOL = (1 << 3),
    };

    // "Immutable" flags - only set in single-threaded context and then
    // can be accessed without mutex
    uint8_t im_flags;
    enum ImFlags : uint8_t {
        // Whether this entry is high priority entry.
        IM_IS_HIGH_PRI = (1 << 0),
        // Whether this entry is low priority entry.
        IM_IS_LOW_PRI = (1 << 1),
        // Marks result handles that should not be inserted into cache
        // TODO(chenggua): IM_IS_STANDALONE has not been applied in lru_cache.cpp
        IM_IS_STANDALONE = (1 << 2),
    };

    // Beginning of the key (MUST BE THE LAST FIELD IN THIS STRUCT!)
    char key_data[1];

    autil::StringView key() const {
        // For cheaper lookups, we allow a temporary Handle object
        // to store a pointer to a key in "value".
        if (next == this) {
            return *(reinterpret_cast<autil::StringView *>(value));
        } else {
            return autil::StringView(key_data, key_length);
        }
    }

    // For HandleImpl concept
    uint32_t GetHash() const { return hash; }

    bool InCache() const { return m_flags & M_IN_CACHE; }
    bool IsHighPri() const { return im_flags & IM_IS_HIGH_PRI; }
    bool InHighPriPool() const { return m_flags & M_IN_HIGH_PRI_POOL; }
    bool IsLowPri() const { return im_flags & IM_IS_LOW_PRI; }
    bool InLowPriPool() const { return m_flags & M_IN_LOW_PRI_POOL; }
    bool HasHit() const { return m_flags & M_HAS_HIT; }
    bool IsStandalone() const { return im_flags & IM_IS_STANDALONE; }

    void SetInCache(bool in_cache) {
        if (in_cache) {
            m_flags |= M_IN_CACHE;
        } else {
            m_flags &= ~M_IN_CACHE;
        }
    }

    void SetPriority(CacheBase::Priority priority) {
        if (priority == CacheBase::Priority::HIGH) {
            im_flags |= IM_IS_HIGH_PRI;
            im_flags &= ~IM_IS_LOW_PRI;
        } else if (priority == CacheBase::Priority::LOW) {
            im_flags &= ~IM_IS_HIGH_PRI;
            im_flags |= IM_IS_LOW_PRI;
        } else {
            im_flags &= ~IM_IS_HIGH_PRI;
            im_flags &= ~IM_IS_LOW_PRI;
        }
    }

    void SetInHighPriPool(bool in_high_pri_pool) {
        if (in_high_pri_pool) {
            m_flags |= M_IN_HIGH_PRI_POOL;
        } else {
            m_flags &= ~M_IN_HIGH_PRI_POOL;
        }
    }

    void SetInLowPriPool(bool in_low_pri_pool) {
        if (in_low_pri_pool) {
            m_flags |= M_IN_LOW_PRI_POOL;
        } else {
            m_flags &= ~M_IN_LOW_PRI_POOL;
        }
    }

    void SetHit() { m_flags |= M_HAS_HIT; }

    void SetIsStandalone(bool is_standalone) {
        if (is_standalone) {
            im_flags |= IM_IS_STANDALONE;
        } else {
            im_flags &= ~IM_IS_STANDALONE;
        }
    }

    void Free(const CacheAllocatorPtr &allocator) {
        assert((refs == 1 && InCache()) || (refs == 0 && !InCache()));
        if (deleter) {
            (*deleter)(key(), value, allocator);
        }
        delete[] reinterpret_cast<char *>(this);
    }
};

// We provide our own simple hash table since it removes a whole bunch
// of porting hacks and is also faster than some of the built-in hash
// table implementations in some of the compiler/runtime combinations
// we have tested.  E.g., readrandom speeds up by ~5% over the g++
// 4.4.3's builtin hashtable.
class LRUHandleTable {
public:
    LRUHandleTable();
    ~LRUHandleTable();

    void SetAllocator(const CacheAllocatorPtr &allocator) { allocator_ = allocator; }

    LRUHandle *Lookup(const autil::StringView &key, uint32_t hash);
    LRUHandle *Insert(LRUHandle *h);
    LRUHandle *Remove(const autil::StringView &key, uint32_t hash);

    template <typename T>
    void ApplyToAllCacheEntries(T func) {
        for (uint32_t i = 0; i < length_; i++) {
            LRUHandle *h = list_[i];
            while (h != nullptr) {
                auto n = h->next_hash;
                assert(h->InCache());
                func(h);
                h = n;
            }
        }
    }

private:
    // Return a pointer to slot that points to a cache entry that
    // matches key/hash.  If there is no such cache entry, return a
    // pointer to the trailing slot in the corresponding linked list.
    LRUHandle **FindPointer(const autil::StringView &key, uint32_t hash);

    void Resize();

    // The table consists of an array of buckets where each bucket is
    // a linked list of cache entries that hash into the bucket.
    uint32_t length_;
    uint32_t elems_;
    LRUHandle **list_;
    CacheAllocatorPtr allocator_;
};

// A single shard of sharded cache.
class LRUCacheShard : public CacheShard {
public:
    LRUCacheShard();
    virtual ~LRUCacheShard();

    // Separate from constructor so caller can easily make an array of LRUCache
    // if current usage is more than new capacity, the function will attempt to
    // free the needed space
    virtual void SetCapacity(size_t capacity) override;

    // Set the flag to reject insertion if cache if full.
    virtual void SetStrictCapacityLimit(bool strict_capacity_limit) override;

    // Initialize percentage of capacity reveserved for different capacity.
    void InitializePriorityPoolRatio(double high_pri_pool_ratio, double low_pri_pool_ratio);

    // Set percentage of capacity reserved for high-pri cache entries.
    void SetHighPriorityPoolRatio(double high_pri_pool_ratio);

    // Set percentage of capacity reserved for low-pri cache entries.
    void SetLowPriorityPoolRatio(double low_pri_pool_ratio);

    // Set allocatory
    void SetAllocator(const CacheAllocatorPtr &allocator);

    // Like Cache methods, but with an extra "hash" parameter.
    virtual bool Insert(const autil::StringView &key,
                        uint32_t hash,
                        void *value,
                        size_t charge,
                        void (*deleter)(const autil::StringView &key, void *value, const CacheAllocatorPtr &allocator),
                        CacheBase::Handle **handle,
                        CacheBase::Priority priority) override;
    virtual CacheBase::Handle *Lookup(const autil::StringView &key, uint32_t hash) override;
    virtual bool Ref(CacheBase::Handle *handle) override;
    virtual void Release(CacheBase::Handle *handle) override;
    virtual void Erase(const autil::StringView &key, uint32_t hash) override;

    // Although in some platforms the update of size_t is atomic, to make sure
    // GetUsage() and GetPinnedUsage() work correctly under any platform, we'll
    // protect them with mutex_.

    virtual size_t GetUsage() const override;
    virtual size_t GetPinnedUsage() const override;

    // Retrieves high pri pool ratio
    double GetHighPriPoolRatio();

    // Retrieves low pri pool ratio
    double GetLowPriPoolRatio();

    virtual void ApplyToAllCacheEntries(void (*callback)(void *, size_t), bool thread_safe) override;

    virtual void EraseUnRefEntries() override;

    virtual std::string GetPrintableOptions() const override;

    void TEST_GetLRUList(LRUHandle **lru, LRUHandle **lru_low_pri, LRUHandle **lru_bottom_pri);

private:
    void LRU_Remove(LRUHandle *e);
    void LRU_Insert(LRUHandle *e);

    // Overflow the last entry in high-pri pool to low-pri pool until size of
    // high-pri pool is no larger than the size specify by high_pri_pool_pct.
    void MaintainPoolSize();

    // Just reduce the reference count by 1.
    // Return true if last reference
    bool Unref(LRUHandle *e);

    // Free some space following strict LRU policy until enough space
    // to hold (usage_ + charge) is freed or the lru list is empty
    // This function is not thread safe - it needs to be executed while
    // holding the mutex_
    void EvictFromLRU(size_t charge, autovector<LRUHandle *> *deleted);

    // Initialized before use.
    size_t capacity_;

    // Memory size for entries residing in the cache
    size_t usage_;

    // Memory size for entries residing only in the LRU list
    size_t lru_usage_;

    // Memory size for entries in high-pri pool.
    size_t high_pri_pool_usage_;

    // Memory size for entries in low-pri pool.
    size_t low_pri_pool_usage_;

    // Whether to reject insertion if cache reaches its full capacity.
    bool strict_capacity_limit_;

    // Ratio of capacity reserved for high priority cache entries.
    double high_pri_pool_ratio_;

    // Ratio of capacity reserved for high priority cache entries.
    double low_pri_pool_ratio_;

    // High-pri pool size, equals to capacity * high_pri_pool_ratio.
    // Remember the value to avoid recomputing each time.
    double high_pri_pool_capacity_;

    // Low-pri pool size, equals to capacity * low_pri_pool_ratio.
    // Remember the value to avoid recomputing each time.
    double low_pri_pool_capacity_;

    // mutex_ protects the following state.
    // We don't count mutex_ as the cache's internal state so semantically we
    // don't mind mutex_ invoking the non-const actions.
    mutable autil::ThreadMutex mutex_;

    // Dummy head of LRU list.
    // lru.prev is newest entry, lru.next is oldest entry.
    // LRU contains items which can be evicted, ie reference only by cache
    LRUHandle lru_;

    // Pointer to head of low-pri pool in LRU list.
    LRUHandle *lru_low_pri_;

    // Pointer to head of bottom-pri pool in LRU list.
    LRUHandle *lru_bottom_pri_;

    LRUHandleTable table_;

    CacheAllocatorPtr allocator_;
};

class LRUCache : public ShardedCache {
public:
    LRUCache(size_t capacity,
             int num_shard_bits,
             bool strict_capacity_limit,
             double high_pri_pool_ratio,
             double low_pri_pool_ratio,
             const CacheAllocatorPtr &allocator);
    virtual ~LRUCache();
    virtual const char *Name() const override { return "LRUCache"; }
    virtual CacheShard *GetShard(int shard) override;
    virtual const CacheShard *GetShard(int shard) const override;
    virtual void *Value(Handle *handle) override;
    virtual size_t GetCharge(Handle *handle) const override;
    virtual uint32_t GetHash(Handle *handle) const override;
    virtual void DisownData() override;

private:
    LRUCacheShard *shards_;
};

} // namespace autil
