//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <atomic>
#include <stddef.h>
#include <stdint.h>
#include <string>

#include "autil/Lock.h"
#include "autil/cache/cache.h"
#include "autil/cache/cache_allocator.h"

namespace autil {

// Single cache shard interface.
class CacheShard {
public:
    CacheShard() = default;
    virtual ~CacheShard() = default;

    virtual bool Insert(const autil::StringView &key,
                        uint32_t hash,
                        void *value,
                        size_t charge,
                        void (*deleter)(const autil::StringView &key, void *value, const CacheAllocatorPtr &allocator),
                        CacheBase::Handle **handle,
                        CacheBase::Priority priority) = 0;
    virtual CacheBase::Handle *Lookup(const autil::StringView &key, uint32_t hash) = 0;
    virtual bool Ref(CacheBase::Handle *handle) = 0;
    virtual void Release(CacheBase::Handle *handle) = 0;
    virtual void Erase(const autil::StringView &key, uint32_t hash) = 0;
    virtual void SetCapacity(size_t capacity) = 0;
    virtual void SetStrictCapacityLimit(bool strict_capacity_limit) = 0;
    virtual size_t GetUsage() const = 0;
    virtual size_t GetPinnedUsage() const = 0;
    virtual void ApplyToAllCacheEntries(void (*callback)(void *, size_t), bool thread_safe) = 0;
    virtual void EraseUnRefEntries() = 0;
    virtual std::string GetPrintableOptions() const { return ""; }
};

// Generic cache interface which shards cache by hash of keys. 2^num_shard_bits
// shards will be created, with capacity split evenly to each of the shards.
// Keys are sharded by the highest num_shard_bits bits of hash value.
class ShardedCache : public CacheBase {
public:
    ShardedCache(size_t capacity, int num_shard_bits, bool strict_capacity_limit);
    virtual ~ShardedCache() = default;
    virtual const char *Name() const override = 0;
    virtual CacheShard *GetShard(int shard) = 0;
    virtual const CacheShard *GetShard(int shard) const = 0;
    virtual void *Value(Handle *handle) override = 0;
    virtual size_t GetCharge(Handle *handle) const = 0;
    virtual uint32_t GetHash(Handle *handle) const = 0;
    virtual void DisownData() override = 0;

    virtual void SetCapacity(size_t capacity) override;
    virtual void SetStrictCapacityLimit(bool strict_capacity_limit) override;

    virtual bool Insert(const autil::StringView &key,
                        void *value,
                        size_t charge,
                        void (*deleter)(const autil::StringView &key, void *value, const CacheAllocatorPtr &allocator),
                        Handle **handle,
                        Priority priority) override;
    virtual Handle *Lookup(const autil::StringView &key) override;
    virtual bool Ref(Handle *handle) override;
    virtual void Release(Handle *handle) override;
    virtual void Erase(const autil::StringView &key) override;
    virtual uint64_t NewId() override;
    virtual size_t GetCapacity() const override;
    virtual bool HasStrictCapacityLimit() const override;
    virtual size_t GetUsage() const override;
    virtual size_t GetUsage(Handle *handle) const override;
    virtual size_t GetPinnedUsage() const override;
    virtual void ApplyToAllCacheEntries(void (*callback)(void *, size_t), bool thread_safe) override;
    virtual void EraseUnRefEntries() override;
    virtual std::string GetPrintableOptions() const override;

private:
    uint32_t Shard(uint32_t hash) {
        // Note, hash >> 32 yields hash in gcc, not the zero we expect!
        return (num_shard_bits_ > 0) ? (hash >> (32 - num_shard_bits_)) : 0;
    }

    int num_shard_bits_;
    mutable autil::ThreadMutex capacity_mutex_;
    size_t capacity_;
    bool strict_capacity_limit_;
    std::atomic<uint64_t> last_id_;
};

} // namespace autil
