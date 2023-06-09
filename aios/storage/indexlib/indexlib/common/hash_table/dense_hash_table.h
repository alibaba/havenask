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
#ifndef __INDEXLIB_DENSE_HASH_TABLE_H
#define __INDEXLIB_DENSE_HASH_TABLE_H

#include <vector> // for shrink, use as bitmap
//#include <immintrin.h>

#include "indexlib/common/hash_table/bucket_compressor.h"
#include "indexlib/common/hash_table/closed_hash_table_iterator.h"
#include "indexlib/common/hash_table/closed_hash_table_traits.h"
#include "indexlib/common/hash_table/hash_table_base.h"
#include "indexlib/common/hash_table/hash_table_options.h"
#include "indexlib/common_define.h"
#include "indexlib/util/Status.h"
#include "indexlib/util/counter/AccumulativeCounter.h"

namespace indexlib { namespace common {

// #define DENSE_HASH_TABLE_JUMP(num_probes) (num_probes) // Quadratic probing[(n^2+n)/2]
#define DENSE_HASH_TABLE_JUMP(num_probes) (1) // Linear probing

template <typename _KT, typename _VT, bool HasSpecialKey = ClosedHashTableTraits<_KT, _VT, false>::HasSpecialKey,
          bool useCompactBucket = false>
class DenseHashTableBase : public common::HashTableBase
{
public:
    DenseHashTableBase()
        : mBucket(NULL)
        , mBucketCount(0)
        , mOccupancyPct(0)
        , mDeleteCount(0)
        , mTotalProbeCount(std::make_shared<util::AccumulativeCounter>())
        , mTotalCollisionTimes(std::make_shared<util::AccumulativeCounter>())
    {
    }

    ~DenseHashTableBase() {}

public:
    typedef _KT KeyType;
    typedef _VT ValueType;

public:
    // higher OCCUPANCY_PCT causes to probe too much, though it saves MemoryUse
    static const int32_t OCCUPANCY_PCT = 50;

    // public for DenseHashTableFileIterator & DenseHashTableFileReader
    struct HashTableHeader {
        uint64_t bucketCount;
        uint64_t keyCount;
    };

public:
    typedef ClosedHashTableTraits<_KT, _VT, useCompactBucket> Traits;
    typedef typename Traits::Bucket Bucket;

public:
    virtual util::Status Find(uint64_t key, autil::StringView& value) const override = 0;
    virtual util::Status FindForReadWrite(uint64_t key, autil::StringView& value,
                                          autil::mem_pool::Pool* pool) const override = 0;

public:
    int32_t GetRecommendedOccupancy(int32_t occupancy) const override final
    {
        return DoGetRecommendedOccupancy(occupancy);
    }
    size_t BuildMemoryToTableMemory(size_t buildMemory, int32_t occupancyPct) const override final
    {
        return DoBuildMemoryToTableMemory(buildMemory, occupancyPct);
    }

public:
    uint64_t CapacityToTableMemory(uint64_t maxKeyCount, const HashTableOptions& options) const override;
    uint64_t CapacityToBuildMemory(uint64_t maxKeyCount, const HashTableOptions& options) const override;
    size_t TableMemroyToCapacity(size_t tableMemory, int32_t occupancyPct) const override;
    size_t BuildMemoryToCapacity(size_t buildMemory, int32_t occupancyPct) const override;
    uint64_t Capacity() const override;

public:
    static int32_t DoGetRecommendedOccupancy(int32_t occupancy) { return (occupancy > 80) ? 80 : occupancy; }
    static size_t DoBuildMemoryToTableMemory(size_t buildMemory, int32_t occupancyPct) { return buildMemory; }
    static uint64_t DoCapacityToTableMemory(uint64_t maxKeyCount, const HashTableOptions& options);
    static uint64_t DoCapacityToBuildMemory(uint64_t maxKeyCount, const HashTableOptions& options);
    static size_t DoTableMemroyToCapacity(size_t tableMemory, int32_t occupancyPct);
    static size_t DoBuildMemoryToCapacity(size_t buildMemory, int32_t occupancyPct);

public:
    bool MountForWrite(void* data, size_t size, const HashTableOptions& options = OCCUPANCY_PCT) override;
    bool MountForRead(const void* data, size_t size) override;
    uint64_t Size() const override { return Header()->keyCount; }
    bool IsFull() const override { return Size() >= Capacity(); }
    uint64_t MemoryUse() const override;
    uint64_t BuildAssistantMemoryUse() const override { return 0; }
    bool Shrink(int32_t occupancyPct = 0) override;
    int32_t GetOccupancyPct() const override { return mOccupancyPct; }
    uint64_t GetDeleteCount() const override { return mDeleteCount; }

    bool Insert(uint64_t key, const autil::StringView& value) override;
    bool Delete(uint64_t key, const autil::StringView& value = autil::StringView()) override;

public:
    static bool Probe(const _KT& key, uint64_t& probeCount, uint64_t& bucketId, uint64_t bucketCount);
    static uint64_t BucketCountToCapacity(uint64_t bucketCount, int32_t occupancyPct);

public:
    util::Status Find(const _KT& key, const _VT*& value) const;
    util::Status FindForReadWrite(const _KT& key, _VT& value) const;

    bool Insert(const _KT& key, const _VT& value);
    void* Address() const override { return Header(); }
    uint64_t BucketCount() const { return mBucketCount; }
    bool Stretch() override
    {
        assert(false);
        return true;
    }
    // ATTENTION: ClosedHashTableTraits<_KT, _VT>::HasSpecialKey may not equal HasSpecialKey
    typedef ClosedHashTableIterator<_KT, _VT, Bucket, ClosedHashTableTraits<_KT, _VT, useCompactBucket>::HasSpecialKey>
        Iterator;
    Iterator* CreateIterator() { return new Iterator(mBucket, mBucketCount, Size()); }
    size_t Compress(BucketCompressor* bucketCompressor) override;

protected:
    HashTableHeader* Header() const { return (HashTableHeader*)((uint8_t*)mBucket - sizeof(HashTableHeader)); }
    template <typename functor>
    bool InternalInsert(const _KT& key, const _VT& value);
    Bucket* InternalFindBucket(const _KT& key) const;

private:
    static uint64_t CapacityToBucketCount(uint64_t maxKeyCount, int32_t occupancyPct = OCCUPANCY_PCT);
    bool EvictionInsert(const Bucket& firstSeekingBucket, std::vector<bool>& bitmap);

public:
    std::string name;

protected:
    Bucket* mBucket;
    uint64_t mBucketCount;
    int32_t mOccupancyPct;
    uint64_t mDeleteCount;
    std::shared_ptr<util::AccumulativeCounter> mTotalProbeCount;
    std::shared_ptr<util::AccumulativeCounter> mTotalCollisionTimes;

protected:
    // friend for Probe using in DenseHashTableFileReader
    template <typename, typename, bool, bool>
    friend class DenseHashTableFileReaderBase;
    friend class DenseHashTableTest;
    IE_LOG_DECLARE();
};

///////////////////////////////////////////////////////////////////////////////
IE_LOG_SETUP_TEMPLATE4_CUSTOM(typename, typename, bool, bool, util, DenseHashTableBase);

template <typename _KT, typename _VT, bool HasSpecialKey, bool useCompactBucket>
bool DenseHashTableBase<_KT, _VT, HasSpecialKey, useCompactBucket>::Insert(const _KT& key, const _VT& value)
{
    struct InsertFunctor {
        void operator()(Bucket& bucket, const _KT& key, const _VT& value, uint64_t& deleteCount)
        {
            if (bucket.IsDeleted()) {
                deleteCount--;
            }
            bucket.Set(key, value);
        }
    };
    return InternalInsert<InsertFunctor>(key, value);
}

template <typename _KT, typename _VT, bool HasSpecialKey, bool useCompactBucket>
inline bool DenseHashTableBase<_KT, _VT, HasSpecialKey, useCompactBucket>::Insert(uint64_t key,
                                                                                  const autil::StringView& value)
{
    const _VT& v = *reinterpret_cast<const _VT*>(value.data());
    assert(sizeof(_VT) == value.size());
    return Insert(key, v);
}

template <typename _KT, typename _VT, bool HasSpecialKey, bool useCompactBucket>
inline bool DenseHashTableBase<_KT, _VT, HasSpecialKey, useCompactBucket>::Delete(uint64_t key,
                                                                                  const autil::StringView& value)
{
    struct DeleteFunctor {
        void operator()(Bucket& bucket, const _KT& key, const _VT& value, uint64_t& deleteCount)
        {
            if (!bucket.IsDeleted()) {
                deleteCount++;
            }
            bucket.SetDelete(key, value);
        }
    };

    if (value.empty()) {
        return InternalInsert<DeleteFunctor>(key, _VT());
    }
    const _VT& v = *reinterpret_cast<const _VT*>(value.data());
    assert(sizeof(_VT) == value.size());
    // NOTE: we always *insert* a delete due to KV & KKV requrires.
    return InternalInsert<DeleteFunctor>(key, v);
}

template <typename _KT, typename _VT, bool HasSpecialKey, bool useCompactBucket>
inline size_t
DenseHashTableBase<_KT, _VT, HasSpecialKey, useCompactBucket>::Compress(BucketCompressor* bucketCompressor)
{
    char* begin = static_cast<char*>((void*)mBucket);
    char* outBuffer = begin;
    for (uint64_t i = 0u; i < mBucketCount; ++i) {
        size_t compressedBucketSize =
            bucketCompressor->Compress(static_cast<char*>((void*)(mBucket + i)), sizeof(Bucket), outBuffer);
        outBuffer += compressedBucketSize;
    }
    return sizeof(HashTableHeader) + (outBuffer - begin);
}

template <typename _KT, typename _VT, bool HasSpecialKey, bool useCompactBucket>
inline uint64_t DenseHashTableBase<_KT, _VT, HasSpecialKey, useCompactBucket>::CapacityToTableMemory(
    uint64_t maxKeyCount, const HashTableOptions& options) const
{
    return DoCapacityToTableMemory(maxKeyCount, options);
}

template <typename _KT, typename _VT, bool HasSpecialKey, bool useCompactBucket>
inline uint64_t
DenseHashTableBase<_KT, _VT, HasSpecialKey, useCompactBucket>::DoCapacityToTableMemory(uint64_t maxKeyCount,
                                                                                       const HashTableOptions& options)
{
    return sizeof(HashTableHeader) + sizeof(Bucket) * CapacityToBucketCount(maxKeyCount, options.occupancyPct);
}

template <typename _KT, typename _VT, bool HasSpecialKey, bool useCompactBucket>
inline uint64_t DenseHashTableBase<_KT, _VT, HasSpecialKey, useCompactBucket>::CapacityToBuildMemory(
    uint64_t maxKeyCount, const HashTableOptions& options) const
{
    return DoCapacityToBuildMemory(maxKeyCount, options);
}

template <typename _KT, typename _VT, bool HasSpecialKey, bool useCompactBucket>
inline uint64_t
DenseHashTableBase<_KT, _VT, HasSpecialKey, useCompactBucket>::DoCapacityToBuildMemory(uint64_t maxKeyCount,
                                                                                       const HashTableOptions& options)
{
    return DoCapacityToTableMemory(maxKeyCount, options);
}

template <typename _KT, typename _VT, bool HasSpecialKey, bool useCompactBucket>
inline size_t
DenseHashTableBase<_KT, _VT, HasSpecialKey, useCompactBucket>::TableMemroyToCapacity(size_t tableMemory,
                                                                                     int32_t occupancyPct) const
{
    return DoTableMemroyToCapacity(tableMemory, occupancyPct);
}

template <typename _KT, typename _VT, bool HasSpecialKey, bool useCompactBucket>
inline size_t
DenseHashTableBase<_KT, _VT, HasSpecialKey, useCompactBucket>::DoTableMemroyToCapacity(size_t tableMemory,
                                                                                       int32_t occupancyPct)
{
    if (tableMemory < sizeof(HashTableHeader)) {
        return 0;
    }
    uint64_t bucketCount = (tableMemory - sizeof(HashTableHeader)) / sizeof(Bucket);
    return bucketCount * occupancyPct / 100;
}

template <typename _KT, typename _VT, bool HasSpecialKey, bool useCompactBucket>
inline size_t
DenseHashTableBase<_KT, _VT, HasSpecialKey, useCompactBucket>::BuildMemoryToCapacity(size_t buildMemory,
                                                                                     int32_t occupancyPct) const
{
    return DoBuildMemoryToCapacity(buildMemory, occupancyPct);
}

template <typename _KT, typename _VT, bool HasSpecialKey, bool useCompactBucket>
inline size_t
DenseHashTableBase<_KT, _VT, HasSpecialKey, useCompactBucket>::DoBuildMemoryToCapacity(size_t buildMemory,
                                                                                       int32_t occupancyPct)
{
    return DoTableMemroyToCapacity(buildMemory, occupancyPct);
}

template <typename _KT, typename _VT, bool HasSpecialKey, bool useCompactBucket>
inline bool
DenseHashTableBase<_KT, _VT, HasSpecialKey, useCompactBucket>::MountForWrite(void* data, size_t size,
                                                                             const HashTableOptions& inputOptions)
{
    IE_LOG(INFO, "begin mount for write, size[%lu]", size);
    HashTableOptions options(OCCUPANCY_PCT);
    if (inputOptions.Valid()) {
        options = inputOptions;
    }

    if (size < sizeof(HashTableHeader)) {
        IE_LOG(ERROR, "not enough space, header size[%lu], give[%lu]", sizeof(HashTableHeader), size);
        return false;
    }

    mOccupancyPct = options.occupancyPct;
    mBucketCount = (size - sizeof(HashTableHeader)) / sizeof(Bucket);
    HashTableHeader* header = (HashTableHeader*)data;
    header->bucketCount = mBucketCount;
    header->keyCount = 0;
    mBucket = (Bucket*)(header + 1);
    new (mBucket) Bucket[mBucketCount];

    IE_LOG(INFO, "mount for write, size[%lu], occupancyPct[%d], bucketCount[%lu]", size, mOccupancyPct, mBucketCount);

    return true;
}

template <typename _KT, typename _VT, bool HasSpecialKey, bool useCompactBucket>
inline bool DenseHashTableBase<_KT, _VT, HasSpecialKey, useCompactBucket>::MountForRead(const void* data, size_t size)
{
    assert(data);
    if (size < sizeof(HashTableHeader)) {
        IE_LOG(ERROR, "too small size[%lu], header size[%lu]", size, sizeof(HashTableHeader));
        return false;
    }
    HashTableHeader* header = (HashTableHeader*)data;
    mBucketCount = header->bucketCount;
    mOccupancyPct = header->keyCount * 100 / mBucketCount;
    if (size < sizeof(HashTableHeader) + mBucketCount * sizeof(Bucket)) {
        IE_LOG(ERROR, "too small size[%lu], header size[%lu], bucket size[%lu]", size, sizeof(HashTableHeader),
               mBucketCount * sizeof(Bucket));
        return false;
    }

    mBucket = (Bucket*)(header + 1);
    return true;
}

template <typename _KT, typename _VT, bool HasSpecialKey, bool useCompactBucket>
inline util::Status DenseHashTableBase<_KT, _VT, HasSpecialKey, useCompactBucket>::Find(const _KT& key,
                                                                                        const _VT*& value) const
{
    auto bucket = InternalFindBucket(key);
    if (!bucket || bucket->IsEmpty()) {
        return util::NOT_FOUND;
    }
    value = &bucket->Value();
    return bucket->IsDeleted() ? util::DELETED : util::OK;
}

template <typename _KT, typename _VT, bool HasSpecialKey, bool useCompactBucket>
inline util::Status DenseHashTableBase<_KT, _VT, HasSpecialKey, useCompactBucket>::FindForReadWrite(const _KT& key,
                                                                                                    _VT& value) const
{
    auto bucket = InternalFindBucket(key);
    if (!bucket || bucket->IsEmpty()) {
        return util::NOT_FOUND;
    }
    auto [isDeleted, tmpValue] = bucket->DeletedOrValue();
    value = tmpValue;
    if (isDeleted) {
        return util::DELETED;
    }
    return util::OK;
}

template <typename _KT, typename _VT, bool HasSpecialKey, bool useCompactBucket>
inline uint64_t DenseHashTableBase<_KT, _VT, HasSpecialKey, useCompactBucket>::Capacity() const
{
    return BucketCountToCapacity(mBucketCount, mOccupancyPct);
}

template <typename _KT, typename _VT, bool HasSpecialKey, bool useCompactBucket>
inline uint64_t DenseHashTableBase<_KT, _VT, HasSpecialKey, useCompactBucket>::MemoryUse() const
{
    return mBucketCount * sizeof(Bucket) + sizeof(HashTableHeader);
}

template <typename _KT, typename _VT, bool HasSpecialKey, bool useCompactBucket>
inline bool DenseHashTableBase<_KT, _VT, HasSpecialKey, useCompactBucket>::Shrink(int32_t occupancyPct)
{
    assert(mOccupancyPct > 0 && mOccupancyPct < 100);
    assert(occupancyPct >= 0 && occupancyPct < 100);

    uint64_t oldBucketCount = mBucketCount;
    uint64_t newBucketCount = CapacityToBucketCount(Size(), (occupancyPct > 0 ? occupancyPct : mOccupancyPct));
    if (newBucketCount >= oldBucketCount) {
        return true;
    }
    Header()->bucketCount = newBucketCount;
    mBucketCount = newBucketCount;

    std::vector<bool> bitmap(newBucketCount, false);
    // first part, these buckets in new and also in old region
    for (uint64_t i = 0; i < newBucketCount; ++i) {
        if (bitmap[i]) // processed bucket
        {
            continue;
        }
        if (mBucket[i].IsEmpty()) // still empty
        {
            bitmap[i] = true;
            continue;
        }
        Bucket evictedBucket = mBucket[i];
        new (&mBucket[i]) Bucket;
        bitmap[i] = true;
        if (unlikely(!EvictionInsert(evictedBucket, bitmap))) {
            Header()->bucketCount = oldBucketCount;
            mBucketCount = oldBucketCount;
            return false;
        }
    }
    // second part, these buckets in old region only
    for (size_t i = newBucketCount; i < oldBucketCount; ++i) {
        Bucket& bucket = mBucket[i];
        if (!bucket.IsEmpty()) {
            Bucket* newBucket = InternalFindBucket(bucket.Key());
            if (unlikely(!newBucket)) {
                Header()->bucketCount = oldBucketCount;
                mBucketCount = oldBucketCount;
                return false;
            }
            *newBucket = bucket;
        }
    }
    mOccupancyPct = occupancyPct > 0 ? occupancyPct : mOccupancyPct;
    return true;
}

template <typename _KT, typename _VT, bool HasSpecialKey, bool useCompactBucket>
inline uint64_t
DenseHashTableBase<_KT, _VT, HasSpecialKey, useCompactBucket>::BucketCountToCapacity(uint64_t bucketCount,
                                                                                     int32_t occupancyPct)
{
    assert(occupancyPct > 0 && occupancyPct <= 100);
    uint64_t maxKeyCount = bucketCount * occupancyPct / 100.0;
    return maxKeyCount;
}

template <typename _KT, typename _VT, bool HasSpecialKey, bool useCompactBucket>
inline bool DenseHashTableBase<_KT, _VT, HasSpecialKey, useCompactBucket>::Probe(const _KT& key, uint64_t& probeCount,
                                                                                 uint64_t& bucketId,
                                                                                 uint64_t bucketCount)
{
    ++probeCount;
    bucketId += DENSE_HASH_TABLE_JUMP(probeCount);
    if (unlikely(bucketId >= bucketCount)) {
        bucketId %= bucketCount;
    }
    if (unlikely(probeCount >= bucketCount)) {
        IE_LOG(ERROR,
               "too many probings for key[%lu], probeCount[%lu], "
               "bucketCount[%lu]",
               (uint64_t)key, probeCount, bucketCount);
        return false;
    }
    return true;
}

template <typename _KT, typename _VT, bool HasSpecialKey, bool useCompactBucket>
template <typename functor>
inline bool DenseHashTableBase<_KT, _VT, HasSpecialKey, useCompactBucket>::InternalInsert(const _KT& key,
                                                                                          const _VT& value)
{
    Bucket* bucket = InternalFindBucket(key);
    if (unlikely(!bucket)) {
        return false;
    }
    bool isNewKey = bucket->IsEmpty();
    functor()(*bucket, key, value, mDeleteCount); // insert or delete
    if (isNewKey) {
        ++(Header()->keyCount);
    }
    return true;
}

template <typename _KT, typename _VT, bool HasSpecialKey, bool useCompactBucket>
inline typename DenseHashTableBase<_KT, _VT, HasSpecialKey, useCompactBucket>::Bucket*
DenseHashTableBase<_KT, _VT, HasSpecialKey, useCompactBucket>::InternalFindBucket(const _KT& key) const
{
    uint64_t bucketCount = mBucketCount;
    uint64_t bucketId = key % bucketCount;
    uint64_t probeCount = 0;
    while (true) {
        Bucket& bucket = mBucket[bucketId];
        if (bucket.IsEmpty()        // not found
            || bucket.IsEqual(key)) // found it or deleted
        {
            return &bucket;
        }
        if (unlikely(!Probe(key, probeCount, bucketId, bucketCount))) {
            return NULL;
        }
    }

    if (probeCount > 0) {
        if (probeCount > 100) {
            AUTIL_INTERVAL_LOG2(5, INFO, "too many probings for key [%lu], probeCount[%lu], bucketCount[%lu]",
                                (uint64_t)key, probeCount, bucketCount);
        }
        mTotalCollisionTimes->Increase(1);
        mTotalProbeCount->Increase(probeCount);
        if (mTotalCollisionTimes->GetLocal() == 10000) {
            int64_t totalCount = mTotalProbeCount->GetLocal();
            mTotalProbeCount->Reset();
            mTotalCollisionTimes->Reset();
            AUTIL_LOG(INFO, "average probings count [%ld] in latest 10000 times bucket collision, bucketCount[%lu].",
                      (uint64_t)totalCount / 10000, bucketCount);
        }
    }
    return NULL;
}

template <typename _KT, typename _VT, bool HasSpecialKey, bool useCompactBucket>
inline uint64_t
DenseHashTableBase<_KT, _VT, HasSpecialKey, useCompactBucket>::CapacityToBucketCount(uint64_t maxKeyCount,
                                                                                     int32_t occupancyPct)
{
    assert(occupancyPct > 0 && occupancyPct <= 100);
    return ((maxKeyCount > 0 ? maxKeyCount : 1) * 100.0 + occupancyPct - 1) / occupancyPct;
}

template <typename _KT, typename _VT, bool HasSpecialKey, bool useCompactBucket>
inline bool
DenseHashTableBase<_KT, _VT, HasSpecialKey, useCompactBucket>::EvictionInsert(const Bucket& firstSeekingBucket,
                                                                              std::vector<bool>& bitmap)
{
    uint64_t bucketCount = mBucketCount;
    Bucket seekingBucket = firstSeekingBucket;
    uint64_t bucketId = seekingBucket.Key() % bucketCount;
    uint64_t probeCount = 0;
    uint64_t evictionCount = 0;
    while (true) {
        Bucket& bucket = mBucket[bucketId];
        if (bucket.IsEmpty()) // found
        {
            bitmap[bucketId] = true;
            bucket = seekingBucket;
            return true;
        } else if (bitmap[bucketId]) // need probe
        {
            if (unlikely(!Probe(seekingBucket.Key(), probeCount, bucketId, bucketCount))) {
                assert(false);
                IE_LOG(ERROR, "too many probes [%lu], guradCount[%lu]", probeCount, evictionCount);
                return false;
            }
            continue;
        } else // eviction insert
        {
            bitmap[bucketId] = true;
            Bucket evictedBucket = bucket;
            bucket = seekingBucket;
            seekingBucket = evictedBucket;
            bucketId = seekingBucket.Key() % bucketCount;
            probeCount = 0;
            if (unlikely(++evictionCount > bucketCount)) {
                assert(false);
                IE_LOG(ERROR, "too many eviction insert[%lu], lost key[%lu]", evictionCount,
                       (uint64_t)seekingBucket.Key());
                return false;
            }
        }
    }
    assert(false); // never got hear
    return false;
}

///////////////////////////////////////////////////////////////////////////////
template <typename _KT, typename _VT, bool HasSpecialKey = ClosedHashTableTraits<_KT, _VT, false>::HasSpecialKey,
          bool useCompactBucket = false>
class DenseHashTable final : public DenseHashTableBase<_KT, _VT, HasSpecialKey, useCompactBucket>
{
public:
    typedef DenseHashTableBase<_KT, _VT, HasSpecialKey, useCompactBucket> Base;
    using Base::Find;
    using Base::FindForReadWrite;
    using Base::Insert;
    using Base::OCCUPANCY_PCT;

public:
    util::Status Find(uint64_t key, autil::StringView& value) const override final
    {
        const _VT* typedValuePtr = NULL;
        auto status = Base::Find((_KT)key, typedValuePtr);
        value = {(char*)typedValuePtr, sizeof(_VT)};
        return status;
    }

    util::Status FindForReadWrite(uint64_t key, autil::StringView& value,
                                  autil::mem_pool::Pool* pool) const override final
    {
        assert(pool);
        _VT* valueBuffer = (_VT*)IE_POOL_NEW_VECTOR(pool, char, sizeof(_VT));
        auto status = Base::FindForReadWrite((_KT)key, *valueBuffer);
        value = {(char*)valueBuffer, sizeof(_VT)};
        return status;
    }
};

/////////////////////////////////////////////////////////////////////////////

// hide some methods
template <typename _KT, typename _VT, bool useCompactBucket>
class DenseHashTable<_KT, _VT, true, useCompactBucket> final
    : public DenseHashTableBase<_KT, _VT, false, useCompactBucket>
{
public:
    typedef ClosedHashTableTraits<_KT, _VT, useCompactBucket> Traits;
    typedef typename Traits::Bucket Bucket;
    typedef typename Traits::SpecialBucket SpecialBucket;

private:
    typedef DenseHashTableBase<_KT, _VT, false, useCompactBucket> Base;
    using Base::_logger;
    using Base::mBucket;
    using Base::mBucketCount;
    using Base::OCCUPANCY_PCT;

public:
    // public for DenseHashTableFileIterator & DenseHashTableFileReader
    typedef typename Base::HashTableHeader HashTableHeader;

public:
    util::Status Find(uint64_t key, autil::StringView& value) const override final
    {
        const _VT* typedValuePtr = NULL;
        auto status = Find((_KT)key, typedValuePtr);
        value = {(char*)typedValuePtr, sizeof(_VT)};
        return status;
    }

    util::Status FindForReadWrite(uint64_t key, autil::StringView& value,
                                  autil::mem_pool::Pool* pool) const override final
    {
        assert(pool);
        _VT* valueBuffer = (_VT*)IE_POOL_NEW_VECTOR(pool, char, sizeof(_VT));
        auto status = Base::FindForReadWrite((_KT)key, *valueBuffer);
        value = {(char*)valueBuffer, sizeof(_VT)};
        return status;
    }

    bool Insert(uint64_t key, const autil::StringView& value) override final
    {
        const _VT& v = *reinterpret_cast<const _VT*>(value.data());
        assert(sizeof(_VT) == value.size());
        return Insert(key, v);
    }

    bool Insert(const _KT& key, const _VT& value)
    {
        struct InsertFunctor {
            void operator()(Bucket& bucket, const _KT& key, const _VT& value, uint64_t& deleteCount)
            {
                if (bucket.IsDeleted()) {
                    deleteCount--;
                }
                bucket.Set(key, value);
            }
            void operator()(SpecialBucket& bucket, const _KT& key, const _VT& value, uint64_t& deleteCount)
            {
                if (bucket.IsDeleted()) {
                    deleteCount--;
                }
                bucket.Set(key, value);
            }
        };
        return InternalInsert<InsertFunctor>(key, value);
    }

    bool Delete(uint64_t key, const autil::StringView& value = autil::StringView()) override final
    {
        struct DeleteFunctor {
            void operator()(Bucket& bucket, const _KT& key, const _VT& value, uint64_t& deleteCount)
            {
                if (!bucket.IsDeleted()) {
                    deleteCount++;
                }
                bucket.SetDelete(key, value);
            }
            void operator()(SpecialBucket& bucket, const _KT& key, const _VT& value, uint64_t& deleteCount)
            {
                if (!bucket.IsDeleted()) {
                    deleteCount++;
                }
                bucket.SetDelete(key, value);
            }
        };

        if (value.empty()) {
            return InternalInsert<DeleteFunctor>(key, _VT());
        }
        const _VT& v = *reinterpret_cast<const _VT*>(value.data());
        assert(sizeof(_VT) == value.size());
        return InternalInsert<DeleteFunctor>(key, v);
    }

    uint64_t MemoryUse() const override final { return Base::MemoryUse() + sizeof(SpecialBucket) * 2; }

    bool Shrink(int32_t occupancyPct = 0) override final
    {
        SpecialBucket emptyBucket = *EmptyBucket();
        SpecialBucket deleteBucket = *DeleteBucket();
        if (!Base::Shrink(occupancyPct)) {
            return false;
        }
        *EmptyBucket() = emptyBucket;
        *DeleteBucket() = deleteBucket;
        return true;
    }

    bool MountForWrite(void* data, size_t size, const HashTableOptions& inputOptions = OCCUPANCY_PCT) override final
    {
        HashTableOptions options(OCCUPANCY_PCT);
        if (inputOptions.Valid()) {
            options = inputOptions;
        }

        size_t minSize = sizeof(HashTableHeader) + sizeof(SpecialBucket) * 2;
        if (size < minSize) {
            IE_LOG(ERROR, "not enough space, min size[%lu], give[%lu]", minSize, size);
            return false;
        }
        size_t leftSize = size - sizeof(SpecialBucket) * 2;
        if (!Base::MountForWrite(data, leftSize, options)) {
            return false;
        }
        new (EmptyBucket()) SpecialBucket();
        new (DeleteBucket()) SpecialBucket();

        return true;
    }

    bool MountForRead(const void* data, size_t size) override
    {
        if (size < sizeof(SpecialBucket) * 2) {
            IE_LOG(ERROR, "too small size[%lu], BucketSize[%lu]", size, sizeof(SpecialBucket));
            return false;
        }
        return Base::MountForRead(data, size - sizeof(SpecialBucket) * 2);
    }

public:
    uint64_t CapacityToTableMemory(uint64_t maxKeyCount, const HashTableOptions& options) const override
    {
        return DoCapacityToTableMemory(maxKeyCount, options);
    }

    uint64_t CapacityToBuildMemory(uint64_t maxKeyCount, const HashTableOptions& options) const override
    {
        return DoCapacityToBuildMemory(maxKeyCount, options);
    }

    size_t TableMemroyToCapacity(size_t tableMemory, int32_t occupancyPct) const override
    {
        return DoTableMemroyToCapacity(tableMemory, occupancyPct);
    }

    size_t BuildMemoryToCapacity(size_t buildMemory, int32_t occupancyPct) const override
    {
        return DoBuildMemoryToCapacity(buildMemory, occupancyPct);
    }

public:
    static uint64_t DoCapacityToTableMemory(uint64_t maxKeyCount, const HashTableOptions& options)
    {
        return Base::DoCapacityToTableMemory(maxKeyCount, options) + sizeof(SpecialBucket) * 2;
    }

    static uint64_t DoCapacityToBuildMemory(uint64_t maxKeyCount, const HashTableOptions& options)
    {
        return DoCapacityToTableMemory(maxKeyCount, options);
    }
    static size_t DoBuildMemoryToCapacity(size_t buildMemory, int32_t occupancyPct)
    {
        return DoTableMemroyToCapacity(buildMemory, occupancyPct);
    }
    static size_t DoTableMemroyToCapacity(size_t tableMemory, int32_t occupancyPct)
    {
        return Base::DoTableMemroyToCapacity(tableMemory - sizeof(SpecialBucket) * 2, occupancyPct);
    }

public:
    util::Status Find(const _KT& key, const _VT*& value) const
    {
        if (likely(!Bucket::IsEmptyKey(key) && !Bucket::IsDeleteKey(key))) {
            auto bucket = Base::InternalFindBucket(key);
            if (!bucket || bucket->IsEmpty()) {
                return util::NOT_FOUND;
            }
            value = &bucket->Value();
            return bucket->IsDeleted() ? util::DELETED : util::OK;
        }

        SpecialBucket* bucket = Bucket::IsEmptyKey(key) ? EmptyBucket() : DeleteBucket();
        if (bucket->IsEmpty()) {
            return util::NOT_FOUND;
        }
        value = &(bucket->Value());
        return bucket->IsDeleted() ? util::DELETED : util::OK;
    }

private:
    SpecialBucket* EmptyBucket() const { return reinterpret_cast<SpecialBucket*>(&mBucket[mBucketCount]); }

    SpecialBucket* DeleteBucket() const { return reinterpret_cast<SpecialBucket*>(&mBucket[mBucketCount]) + 1; }

    template <typename functor>
    bool InternalInsert(const _KT& key, const _VT& value)
    {
        bool isNewKey = false;
        if (unlikely(Bucket::IsEmptyKey(key))) {
            SpecialBucket* bucket = EmptyBucket();
            isNewKey = bucket->IsEmpty();
            functor()(*bucket, key, value, Base::mDeleteCount);
        } else if (unlikely(Bucket::IsDeleteKey(key))) {
            SpecialBucket* bucket = DeleteBucket();
            isNewKey = bucket->IsEmpty();
            functor()(*bucket, key, value, Base::mDeleteCount);
        } else {
            Bucket* bucket = Base::InternalFindBucket(key);
            if (unlikely(!bucket)) {
                return false;
            }
            isNewKey = bucket->IsEmpty();
            functor()(*bucket, key, value, Base::mDeleteCount);
        }
        if (isNewKey) {
            ++(Base::Header()->keyCount);
        }
        return true;
    }

private:
    friend class DenseHashTableTest;
};
}} // namespace indexlib::common

#endif //__INDEXLIB_DENSE_HASH_TABLE_H
