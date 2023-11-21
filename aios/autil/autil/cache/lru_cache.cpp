//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include "autil/cache/lru_cache.h"

#include <algorithm>
#include <assert.h>
#include <memory>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <string>

#include "autil/Autovector.h"
#include "autil/Lock.h"
#include "autil/Span.h"
#include "autil/cache/cache.h"
#include "autil/cache/cache_allocator.h"
#include "autil/cache/sharded_cache.h"
#include "lockless_allocator/MallocPoolScope.h"

namespace autil {

LRUHandleTable::LRUHandleTable() : length_(0), elems_(0), list_(nullptr) { Resize(); }

LRUHandleTable::~LRUHandleTable() {
    ApplyToAllCacheEntries([this](LRUHandle *h) {
        if (h->refs == 1) {
            h->Free(allocator_);
        }
    });
    delete[] list_;
}

LRUHandle *LRUHandleTable::Lookup(const StringView &key, uint32_t hash) { return *FindPointer(key, hash); }

LRUHandle *LRUHandleTable::Insert(LRUHandle *h) {
    LRUHandle **ptr = FindPointer(h->key(), h->hash);
    LRUHandle *old = *ptr;
    h->next_hash = (old == nullptr ? nullptr : old->next_hash);
    *ptr = h;
    if (old == nullptr) {
        ++elems_;
        if (elems_ > length_) {
            // Since each cache entry is fairly large, we aim for a small
            // average linked list length (<= 1).
            Resize();
        }
    }
    return old;
}

LRUHandle *LRUHandleTable::Remove(const StringView &key, uint32_t hash) {
    LRUHandle **ptr = FindPointer(key, hash);
    LRUHandle *result = *ptr;
    if (result != nullptr) {
        *ptr = result->next_hash;
        --elems_;
    }
    return result;
}

LRUHandle **LRUHandleTable::FindPointer(const StringView &key, uint32_t hash) {
    LRUHandle **ptr = &list_[hash & (length_ - 1)];
    while (*ptr != nullptr && ((*ptr)->hash != hash || key != (*ptr)->key())) {
        ptr = &(*ptr)->next_hash;
    }
    return ptr;
}

void LRUHandleTable::Resize() {
    DisablePoolScope disableScope;
    uint32_t new_length = 16;
    while (new_length < elems_ * 1.5) {
        new_length *= 2;
    }
    LRUHandle **new_list = new LRUHandle *[new_length];
    memset(new_list, 0, sizeof(new_list[0]) * new_length);
    uint32_t count = 0;
    for (uint32_t i = 0; i < length_; i++) {
        LRUHandle *h = list_[i];
        while (h != nullptr) {
            LRUHandle *next = h->next_hash;
            uint32_t hash = h->hash;
            LRUHandle **ptr = &new_list[hash & (new_length - 1)];
            h->next_hash = *ptr;
            *ptr = h;
            h = next;
            count++;
        }
    }
    assert(elems_ == count);
    delete[] list_;
    list_ = new_list;
    length_ = new_length;
}

LRUCacheShard::LRUCacheShard() : usage_(0), lru_usage_(0), high_pri_pool_usage_(0), low_pri_pool_usage_(0) {
    // Make empty circular linked list
    lru_.next = &lru_;
    lru_.prev = &lru_;
    lru_low_pri_ = &lru_;
    lru_bottom_pri_ = &lru_;
}

LRUCacheShard::~LRUCacheShard() {}

bool LRUCacheShard::Unref(LRUHandle *e) {
    assert(e->refs > 0);
    e->refs--;
    return e->refs == 0;
}

// Call deleter and free

void LRUCacheShard::EraseUnRefEntries() {
    autovector<LRUHandle *> last_reference_list;
    {
        ScopedLock l(mutex_);
        while (lru_.next != &lru_) {
            LRUHandle *old = lru_.next;
            assert(old->InCache());
            assert(old->refs == 1); // LRU list contains elements which may be evicted
            LRU_Remove(old);
            table_.Remove(old->key(), old->hash);
            old->SetInCache(false);
            Unref(old);
            usage_ -= old->charge;
            last_reference_list.push_back(old);
        }
    }

    for (auto entry : last_reference_list) {
        entry->Free(allocator_);
    }
}

void LRUCacheShard::ApplyToAllCacheEntries(void (*callback)(void *, size_t), bool thread_safe) {
    if (thread_safe) {
        int ret = mutex_.lock();
        assert(ret == 0);
        (void)ret;
    }
    table_.ApplyToAllCacheEntries([callback](LRUHandle *h) { callback(h->value, h->charge); });
    if (thread_safe) {
        int ret = mutex_.unlock();
        assert(ret == 0);
        (void)ret;
    }
}

void LRUCacheShard::TEST_GetLRUList(LRUHandle **lru, LRUHandle **lru_low_pri, LRUHandle **lru_bottom_pri) {
    *lru = &lru_;
    *lru_low_pri = lru_low_pri_;
    *lru_bottom_pri = lru_bottom_pri_;
}

double LRUCacheShard::GetHighPriPoolRatio() {
    ScopedLock l(mutex_);
    return high_pri_pool_ratio_;
}

double LRUCacheShard::GetLowPriPoolRatio() {
    ScopedLock l(mutex_);
    return low_pri_pool_ratio_;
}

void LRUCacheShard::LRU_Remove(LRUHandle *e) {
    assert(e->next != nullptr);
    assert(e->prev != nullptr);
    if (lru_low_pri_ == e) {
        lru_low_pri_ = e->prev;
    }
    if (lru_bottom_pri_ == e) {
        lru_bottom_pri_ = e->prev;
    }
    e->next->prev = e->prev;
    e->prev->next = e->next;
    e->prev = e->next = nullptr;
    assert(lru_usage_ >= e->charge);
    lru_usage_ -= e->charge;
    assert(!e->InHighPriPool() || !e->InLowPriPool());
    if (e->InHighPriPool()) {
        assert(high_pri_pool_usage_ >= e->charge);
        high_pri_pool_usage_ -= e->charge;
    } else if (e->InLowPriPool()) {
        assert(low_pri_pool_usage_ >= e->charge);
        low_pri_pool_usage_ -= e->charge;
    }
}

void LRUCacheShard::LRU_Insert(LRUHandle *e) {
    assert(e->next == nullptr);
    assert(e->prev == nullptr);
    if (high_pri_pool_ratio_ > 0 && (e->IsHighPri() /* || e->HasHit() */)) {
        // Inset "e" to head of LRU list.
        e->next = &lru_;
        e->prev = lru_.prev;
        e->prev->next = e;
        e->next->prev = e;
        e->SetInHighPriPool(true);
        e->SetInLowPriPool(false);
        high_pri_pool_usage_ += e->charge;
        MaintainPoolSize();
    } else if (low_pri_pool_ratio_ > 0 && (e->IsHighPri() || e->IsLowPri() /* || e->HasHit() */)) {
        // Insert "e" to the head of low-pri pool.
        e->next = lru_low_pri_->next;
        e->prev = lru_low_pri_;
        e->prev->next = e;
        e->next->prev = e;
        e->SetInHighPriPool(false);
        e->SetInLowPriPool(true);
        low_pri_pool_usage_ += e->charge;
        MaintainPoolSize();
        lru_low_pri_ = e;
    } else {
        // Insert "e" to the head of bottom-pri pool.
        e->next = lru_bottom_pri_->next;
        e->prev = lru_bottom_pri_;
        e->prev->next = e;
        e->next->prev = e;
        e->SetInHighPriPool(false);
        e->SetInLowPriPool(false);
        // if the low-pri pool is empty, lru_low_pri_ also needs to be updated.
        if (lru_bottom_pri_ == lru_low_pri_) {
            lru_low_pri_ = e;
        }
        lru_bottom_pri_ = e;
    }
    lru_usage_ += e->charge;
}

void LRUCacheShard::MaintainPoolSize() {
    while (high_pri_pool_usage_ > high_pri_pool_capacity_) {
        // Overflow last entry in high-pri pool to low-pri pool.
        lru_low_pri_ = lru_low_pri_->next;
        assert(lru_low_pri_ != &lru_);
        lru_low_pri_->SetInHighPriPool(false);
        lru_low_pri_->SetInLowPriPool(true);
        assert(high_pri_pool_usage_ >= lru_low_pri_->charge);
        high_pri_pool_usage_ -= lru_low_pri_->charge;
        low_pri_pool_usage_ += lru_low_pri_->charge;
    }

    while (low_pri_pool_usage_ > low_pri_pool_capacity_) {
        // Overflow last entry in low-pri pool to bottom-pri pool.
        lru_bottom_pri_ = lru_bottom_pri_->next;
        assert(lru_bottom_pri_ != &lru_);
        lru_bottom_pri_->SetInHighPriPool(false);
        lru_bottom_pri_->SetInLowPriPool(false);
        assert(low_pri_pool_usage_ >= lru_bottom_pri_->charge);
        low_pri_pool_usage_ -= lru_bottom_pri_->charge;
    }
}

void LRUCacheShard::EvictFromLRU(size_t charge, autovector<LRUHandle *> *deleted) {
    while (usage_ + charge > capacity_ && lru_.next != &lru_) {
        LRUHandle *old = lru_.next;
        assert(old->InCache());
        assert(old->refs == 1); // LRU list contains elements which may be evicted
        LRU_Remove(old);
        table_.Remove(old->key(), old->hash);
        old->SetInCache(false);
        Unref(old);
        usage_ -= old->charge;
        deleted->push_back(old);
    }
}

void LRUCacheShard::SetCapacity(size_t capacity) {
    autovector<LRUHandle *> last_reference_list;
    {
        ScopedLock l(mutex_);
        capacity_ = capacity;
        high_pri_pool_capacity_ = capacity_ * high_pri_pool_ratio_;
        low_pri_pool_capacity_ = capacity_ * low_pri_pool_ratio_;
        EvictFromLRU(0, &last_reference_list);
    }
    // we free the entries here outside of mutex for
    // performance reasons
    for (auto entry : last_reference_list) {
        entry->Free(allocator_);
    }
}

void LRUCacheShard::SetStrictCapacityLimit(bool strict_capacity_limit) {
    ScopedLock l(mutex_);
    strict_capacity_limit_ = strict_capacity_limit;
}

CacheBase::Handle *LRUCacheShard::Lookup(const StringView &key, uint32_t hash) {
    ScopedLock l(mutex_);
    LRUHandle *e = table_.Lookup(key, hash);
    if (e != nullptr) {
        assert(e->InCache());
        if (e->refs == 1) {
            LRU_Remove(e);
        }
        e->refs++;
    }
    return reinterpret_cast<CacheBase::Handle *>(e);
}

bool LRUCacheShard::Ref(CacheBase::Handle *h) {
    LRUHandle *handle = reinterpret_cast<LRUHandle *>(h);
    ScopedLock l(mutex_);
    if (handle->InCache()) {
        if (handle->refs == 1) {
            LRU_Remove(handle);
        }
        handle->refs++;
        return true;
    }
    return false;
}

void LRUCacheShard::InitializePriorityPoolRatio(double high_pri_pool_ratio, double low_pri_pool_ratio) {
    ScopedLock l(mutex_);
    high_pri_pool_ratio_ = high_pri_pool_ratio;
    high_pri_pool_capacity_ = capacity_ * high_pri_pool_ratio_;
    low_pri_pool_ratio_ = low_pri_pool_ratio;
    low_pri_pool_capacity_ = capacity_ * low_pri_pool_ratio_;
    MaintainPoolSize();
}

void LRUCacheShard::SetHighPriorityPoolRatio(double high_pri_pool_ratio) {
    ScopedLock l(mutex_);
    high_pri_pool_ratio_ = high_pri_pool_ratio;
    high_pri_pool_capacity_ = capacity_ * high_pri_pool_ratio_;
    MaintainPoolSize();
}

void LRUCacheShard::SetLowPriorityPoolRatio(double low_pri_pool_ratio) {
    ScopedLock l(mutex_);
    low_pri_pool_ratio_ = low_pri_pool_ratio;
    low_pri_pool_capacity_ = capacity_ * low_pri_pool_ratio_;
    MaintainPoolSize();
}

void LRUCacheShard::SetAllocator(const CacheAllocatorPtr &allocator) {
    ScopedLock l(mutex_);
    allocator_ = allocator;
    table_.SetAllocator(allocator);
}

void LRUCacheShard::Release(CacheBase::Handle *handle) {
    if (handle == nullptr) {
        return;
    }
    LRUHandle *e = reinterpret_cast<LRUHandle *>(handle);
    bool last_reference = false;
    {
        ScopedLock l(mutex_);
        last_reference = Unref(e);
        if (last_reference) {
            usage_ -= e->charge;
        }
        if (e->refs == 1 && e->InCache()) {
            // The item is still in cache, and nobody else holds a reference to it
            if (usage_ > capacity_) {
                // the cache is full
                // The LRU list must be empty since the cache is full
                assert(lru_.next == &lru_);
                // take this opportunity and remove the item
                table_.Remove(e->key(), e->hash);
                e->SetInCache(false);
                Unref(e);
                usage_ -= e->charge;
                last_reference = true;
            } else {
                // put the item on the list to be potentially freed
                LRU_Insert(e);
            }
        }
    }

    // free outside of mutex
    if (last_reference) {
        e->Free(allocator_);
    }
}

bool LRUCacheShard::Insert(const StringView &key,
                           uint32_t hash,
                           void *value,
                           size_t charge,
                           void (*deleter)(const StringView &key, void *value, const CacheAllocatorPtr &allocator),
                           CacheBase::Handle **handle,
                           CacheBase::Priority priority) {
    DisablePoolScope disableScope;
    // Allocate the memory here outside of the mutex
    // If the cache is full, we'll have to release it
    // It shouldn't happen very often though.
    int handle_size = sizeof(LRUHandle) - 1 + key.size();
    LRUHandle *e = reinterpret_cast<LRUHandle *>(new char[handle_size]);
    autovector<LRUHandle *> last_reference_list;

    bool s = true;

    e->value = value;
    e->deleter = deleter;
    e->charge = charge + handle_size;
    e->key_length = key.size();
    e->hash = hash;
    e->refs = (handle == nullptr ? 1 : 2); // One from LRUCache, one for the returned handle
    e->next = e->prev = nullptr;
    e->SetInCache(true);
    e->SetPriority(priority);
    memcpy(e->key_data, key.data(), key.size());

    {
        ScopedLock l(mutex_);

        // Free the space following strict LRU policy until enough space
        // is freed or the lru list is empty
        EvictFromLRU(e->charge, &last_reference_list);

        if (usage_ - lru_usage_ + e->charge > capacity_ && (strict_capacity_limit_ || handle == nullptr)) {
            if (handle == nullptr) {
                // Don't insert the entry but still return ok, as if the entry inserted
                // into cache and get evicted immediately.
                last_reference_list.push_back(e);
            } else {
                delete[] reinterpret_cast<char *>(e);
                *handle = nullptr;
                s = false;
                // s = Status::Incomplete(StringView("Insert failed due to LRU cache being full."));
            }
        } else {
            // insert into the cache
            // note that the cache might get larger than its capacity if not enough
            // space was freed
            LRUHandle *old = table_.Insert(e);
            usage_ += e->charge;
            if (old != nullptr) {
                old->SetInCache(false);
                if (Unref(old)) {
                    usage_ -= old->charge;
                    // old is on LRU because it's in cache and its reference count
                    // was just 1 (Unref returned 0)
                    LRU_Remove(old);
                    last_reference_list.push_back(old);
                }
            }
            if (handle == nullptr) {
                LRU_Insert(e);
            } else {
                *handle = reinterpret_cast<CacheBase::Handle *>(e);
            }
            s = true;
        }
    }

    // we free the entries here outside of mutex for
    // performance reasons
    for (auto entry : last_reference_list) {
        entry->Free(allocator_);
    }

    return s;
}

void LRUCacheShard::Erase(const StringView &key, uint32_t hash) {
    LRUHandle *e;
    bool last_reference = false;
    {
        ScopedLock l(mutex_);
        e = table_.Remove(key, hash);
        if (e != nullptr) {
            last_reference = Unref(e);
            if (last_reference) {
                usage_ -= e->charge;
            }
            if (last_reference && e->InCache()) {
                LRU_Remove(e);
            }
            e->SetInCache(false);
        }
    }

    // mutex not held here
    // last_reference will only be true if e != nullptr
    if (last_reference) {
        e->Free(allocator_);
    }
}

size_t LRUCacheShard::GetUsage() const {
    ScopedLock l(mutex_);
    return usage_;
}

size_t LRUCacheShard::GetPinnedUsage() const {
    ScopedLock l(mutex_);
    assert(usage_ >= lru_usage_);
    return usage_ - lru_usage_;
}

std::string LRUCacheShard::GetPrintableOptions() const {
    const int kBufferSize = 200;
    char buffer[kBufferSize];
    {
        ScopedLock l(mutex_);
        snprintf(buffer, kBufferSize, "    high_pri_pool_ratio: %.3lf\n", high_pri_pool_ratio_);
        snprintf(buffer, kBufferSize, "    low_pri_pool_ratio: %.3lf\n", low_pri_pool_ratio_);
    }
    return std::string(buffer);
}

LRUCache::LRUCache(size_t capacity,
                   int num_shard_bits,
                   bool strict_capacity_limit,
                   double high_pri_pool_ratio,
                   double low_pri_pool_ratio,
                   const CacheAllocatorPtr &allocator)
    : ShardedCache(capacity, num_shard_bits, strict_capacity_limit) {
    int num_shards = 1 << num_shard_bits;
    shards_ = new LRUCacheShard[num_shards];
    SetCapacity(capacity);
    SetStrictCapacityLimit(strict_capacity_limit);
    for (int i = 0; i < num_shards; i++) {
        shards_[i].InitializePriorityPoolRatio(high_pri_pool_ratio, low_pri_pool_ratio);
        shards_[i].SetAllocator(allocator);
    }
}

LRUCache::~LRUCache() { delete[] shards_; }

CacheShard *LRUCache::GetShard(int shard) { return reinterpret_cast<CacheShard *>(&shards_[shard]); }

const CacheShard *LRUCache::GetShard(int shard) const { return reinterpret_cast<CacheShard *>(&shards_[shard]); }

void *LRUCache::Value(Handle *handle) { return reinterpret_cast<const LRUHandle *>(handle)->value; }

size_t LRUCache::GetCharge(Handle *handle) const { return reinterpret_cast<const LRUHandle *>(handle)->charge; }

uint32_t LRUCache::GetHash(Handle *handle) const { return reinterpret_cast<const LRUHandle *>(handle)->hash; }

void LRUCache::DisownData() { shards_ = nullptr; }

// create LRU cache with no bottom priority
std::shared_ptr<CacheBase> NewLRUCache(size_t capacity,
                                       int num_shard_bits,
                                       bool strict_capacity_limit,
                                       double high_pri_pool_ratio,
                                       const CacheAllocatorPtr &allocator) {
    double low_pri_pool_ratio = 1.0 - high_pri_pool_ratio;
    return NewLRUCache(
        capacity, num_shard_bits, strict_capacity_limit, high_pri_pool_ratio, low_pri_pool_ratio, allocator);
}

// create LRU cache with bottom priority
std::shared_ptr<CacheBase> NewLRUCache(size_t capacity,
                                       int num_shard_bits,
                                       bool strict_capacity_limit,
                                       double high_pri_pool_ratio,
                                       double low_pri_pool_ratio,
                                       const CacheAllocatorPtr &allocator) {
    if (num_shard_bits >= 20) {
        return nullptr; // the cache cannot be sharded into too many fine pieces
    }
    if (high_pri_pool_ratio < 0.0 || high_pri_pool_ratio > 1.0) {
        // invalid high_pri_pool_ratio
        return nullptr;
    }
    if (low_pri_pool_ratio < 0.0 || low_pri_pool_ratio > 1.0) {
        // Invalid low_pri_pool_ratio
        return nullptr;
    }
    if (low_pri_pool_ratio + high_pri_pool_ratio > 1.0 + 0.000001) {
        // Invalid high_pri_pool_ratio and low_pri_pool_ratio combination
        return nullptr;
    }
    return std::make_shared<LRUCache>(
        capacity, num_shard_bits, strict_capacity_limit, high_pri_pool_ratio, low_pri_pool_ratio, allocator);
}

} // namespace autil
