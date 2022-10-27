#ifndef __INDEXLIB_DENSE_HASH_TABLE_H
#define __INDEXLIB_DENSE_HASH_TABLE_H

#include <vector> // for shrink, use as bitmap
//#include <immintrin.h>

#include "indexlib/common_define.h"
#include "indexlib/misc/status.h"
#include "indexlib/common/hash_table/closed_hash_table_traits.h"
#include "indexlib/common/hash_table/closed_hash_table_iterator.h"
#include "indexlib/common/hash_table/hash_table_options.h"
#include "indexlib/common/hash_table/bucket_compressor.h"

IE_NAMESPACE_BEGIN(common);

// #define DENSE_HASH_TABLE_JUMP(num_probes) (num_probes) // Quadratic probing[(n^2+n)/2]
#define DENSE_HASH_TABLE_JUMP(num_probes) (1) // Linear probing

// NOTE: A Open addressing hash table implementation
// FORMAT: Header [sizeof(HashTableHeader)]
//         Buckets [sizeof(Bucket) * BucketCount]
//         <SpecialBuckets> [sizeof(Bucket) * 2]
template<typename _KT, typename _VT,
         bool HasSpecialKey = ClosedHashTableTraits<_KT, _VT, false>::HasSpecialKey, 
         bool useCompactBucket = false>
class DenseHashTable
{
public:
    DenseHashTable() : mBucket(NULL), mBucketCount(0), mOccupancyPct(0), mDeleteCount(0) {}
    ~DenseHashTable() {}

public:
    typedef _KT KeyType;
    typedef _VT ValueType;

public:
    // higher OCCUPANCY_PCT causes to probe too much, though it saves memory
    static const int32_t OCCUPANCY_PCT = 50;

    // public for DenseHashTableFileIterator & DenseHashTableFileReader
    struct HashTableHeader
    {
        uint64_t bucketCount;
        uint64_t keyCount;
    };

public:
    typedef ClosedHashTableTraits<_KT, _VT, useCompactBucket> Traits;
    typedef typename Traits::Bucket Bucket;
public:
    static int32_t GetRecommendedOccupancy(int32_t occupancy)
    {
        return (occupancy > 80)? 80 : occupancy;
    }
    static uint64_t CapacityToTableMemory(uint64_t maxKeyCount,
            const HashTableOptions& options);
    static uint64_t CapacityToBuildMemory(uint64_t maxKeyCount,
            const HashTableOptions& optionos);
    static size_t TableMemroyToCapacity(size_t tableMemory, int32_t occupancyPct);
    static size_t BuildMemoryToCapacity(size_t buildMemory, int32_t occupancyPct);
    static size_t BuildMemoryToTableMemory(size_t buildMemory, int32_t occupancyPct)
    { return buildMemory; }

public:
    bool MountForWrite(void* data, size_t size,
                       const HashTableOptions& options = OCCUPANCY_PCT);
    bool MountForRead(const void* data, size_t size);
    bool Insert(const _KT& key, const _VT& value);
    bool Delete(const _KT& key, const _VT& value = _VT());
    misc::Status Find(const _KT& key, const _VT*& value) const;
    misc::Status FindForReadWrite(const _KT& key, _VT& value) const;
    uint64_t Size() const { return Header()->keyCount; }
    uint64_t Capacity() const;
    bool IsFull() const { return Size() >= Capacity(); }
    uint64_t BucketCount() const { return mBucketCount; }
    uint64_t MemoryUse() const;
    uint64_t BuildAssistantMemoryUse() const { return 0; }
    bool Shrink(int32_t occupancyPct = 0);
    bool Stretch() { assert(false); return true; }
    int32_t GetOccupancyPct() const { return mOccupancyPct; }
    void* Address() const { return Header(); }
    // ATTENTION: ClosedHashTableTraits<_KT, _VT>::HasSpecialKey may not equal HasSpecialKey
    typedef ClosedHashTableIterator<
        _KT, _VT, Bucket, ClosedHashTableTraits<_KT, _VT, useCompactBucket>::HasSpecialKey> Iterator;
    Iterator* CreateIterator() { return new Iterator(mBucket, mBucketCount, Size()); }
    size_t Compress(BucketCompressor* bucketCompressor);
    uint64_t GetDeleteCount() const { return mDeleteCount; }

    static uint64_t BucketCountToCapacity(uint64_t bucketCount, int32_t occupancyPct);
    static bool Probe(const _KT& key, uint64_t& probeCount,
                      uint64_t& bucketId, uint64_t bucketCount);
protected:
    HashTableHeader* Header() const
    { return (HashTableHeader*)((uint8_t*)mBucket - sizeof(HashTableHeader)); }
    template <typename functor>
    bool InternalInsert(const _KT& key, const _VT& value);
    Bucket* InternalFindBucket(const _KT& key) const;
    misc::Status InternalFind(const _KT& key, const _VT*& value) const;

private:
    static uint64_t CapacityToBucketCount(uint64_t maxKeyCount,
            int32_t occupancyPct = OCCUPANCY_PCT);
    bool EvictionInsert(const Bucket& firstSeekingBucket, std::vector<bool>& bitmap);
    
protected:
    Bucket* mBucket;
    uint64_t mBucketCount;
    int32_t mOccupancyPct;
    uint64_t mDeleteCount;
    
protected:
    // friend for Probe using in DenseHashTableFileReader
    template<typename, typename, bool, bool> friend class DenseHashTableFileReader;
    friend class DenseHashTableTest;
    IE_LOG_DECLARE();
};

///////////////////////////////////////////////////////////////////////////////
IE_LOG_SETUP_TEMPLATE4_CUSTOM(typename, typename, bool, bool, util, DenseHashTable);


template<typename _KT, typename _VT, bool HasSpecialKey, bool useCompactBucket>
inline size_t DenseHashTable<_KT, _VT, HasSpecialKey, useCompactBucket>::Compress(
        BucketCompressor* bucketCompressor)
{
    char* begin = static_cast<char*>((void*)mBucket);
    char* outBuffer = begin;
    for (uint64_t i = 0u; i < mBucketCount; ++i)
    {
        size_t compressedBucketSize = bucketCompressor->Compress(
                static_cast<char*>((void*)(mBucket + i)), sizeof(Bucket), outBuffer);
        outBuffer += compressedBucketSize;
    }
    return sizeof(HashTableHeader) + (outBuffer - begin);
}

template<typename _KT, typename _VT, bool HasSpecialKey, bool useCompactBucket>
inline uint64_t DenseHashTable<_KT, _VT, HasSpecialKey, useCompactBucket>::CapacityToTableMemory(
        uint64_t maxKeyCount, const HashTableOptions& options)
{ 
    return sizeof(HashTableHeader)
        + sizeof(Bucket) * CapacityToBucketCount(
                maxKeyCount, options.occupancyPct);
}

template<typename _KT, typename _VT, bool HasSpecialKey, bool useCompactBucket>
inline uint64_t DenseHashTable<_KT, _VT, HasSpecialKey, useCompactBucket>::CapacityToBuildMemory(
        uint64_t maxKeyCount, const HashTableOptions& options)
{
    return CapacityToTableMemory(maxKeyCount, options);
}

template<typename _KT, typename _VT, bool HasSpecialKey, bool useCompactBucket>
inline size_t DenseHashTable<_KT, _VT, HasSpecialKey, useCompactBucket>::TableMemroyToCapacity(
        size_t tableMemory, int32_t occupancyPct)
{
    if (tableMemory < sizeof(HashTableHeader))
    {
        return 0;
    }
    uint64_t bucketCount = (tableMemory - sizeof(HashTableHeader)) / sizeof(Bucket);
    return bucketCount * occupancyPct / 100;
}

template<typename _KT, typename _VT, bool HasSpecialKey, bool useCompactBucket>
inline size_t DenseHashTable<_KT, _VT, HasSpecialKey, useCompactBucket>::BuildMemoryToCapacity(
        size_t buildMemory, int32_t occupancyPct)
{
    return TableMemroyToCapacity(buildMemory, occupancyPct);
}

template<typename _KT, typename _VT, bool HasSpecialKey, bool useCompactBucket>
inline bool DenseHashTable<_KT, _VT, HasSpecialKey, useCompactBucket>::MountForWrite(
        void* data, size_t size, const HashTableOptions& options)
{
    if (size < sizeof(HashTableHeader))
    {
        IE_LOG(ERROR, "not enough space, header size[%lu], give[%lu]",
               sizeof(HashTableHeader), size);
        return false;
    }

    mOccupancyPct = options.occupancyPct;
    mBucketCount = (size - sizeof(HashTableHeader)) / sizeof(Bucket);
    HashTableHeader* header = (HashTableHeader*)data;
    header->bucketCount = mBucketCount;
    header->keyCount = 0;
    mBucket = (Bucket*)(header + 1);
    new (mBucket) Bucket[mBucketCount];

    IE_LOG(INFO, "mount for write, size[%lu], occupancyPct[%d], bucketCount[%lu]",
           size, mOccupancyPct, mBucketCount);

    return true;
}

template<typename _KT, typename _VT, bool HasSpecialKey, bool useCompactBucket>
inline bool DenseHashTable<_KT, _VT, HasSpecialKey, useCompactBucket>::MountForRead(
        const void* data, size_t size)
{
    assert(data);
    if (size < sizeof(HashTableHeader))
    {
        IE_LOG(ERROR, "too small size[%lu], header size[%lu]",
               size, sizeof(HashTableHeader));
        return false;
    }
    HashTableHeader* header = (HashTableHeader*)data;
    mBucketCount = header->bucketCount;
    mOccupancyPct = header->keyCount * 100 / mBucketCount;
    if (size < sizeof(HashTableHeader) + mBucketCount * sizeof(Bucket))
    {
        IE_LOG(ERROR, "too small size[%lu], header size[%lu], bucket size[%lu]",
               size, sizeof(HashTableHeader), mBucketCount * sizeof(Bucket));
        return false;
    }

    mBucket = (Bucket*)(header + 1);
    return true;
}

template<typename _KT, typename _VT, bool HasSpecialKey, bool useCompactBucket>
inline bool DenseHashTable<_KT, _VT, HasSpecialKey, useCompactBucket>::Insert(
        const _KT& key, const _VT& value)
{
    struct InsertFunctor
    {   
        void operator()(Bucket& bucket,
                        const _KT& key, const _VT& value, uint64_t& deleteCount)
        {
            if (bucket.IsDeleted()) {
                deleteCount--;
            }
            bucket.Set(key, value);
        }
    };
    return InternalInsert<InsertFunctor>(key, value);
}
    
template<typename _KT, typename _VT, bool HasSpecialKey, bool useCompactBucket>
inline bool DenseHashTable<_KT, _VT, HasSpecialKey, useCompactBucket>::Delete(
        const _KT& key, const _VT& value)
{
    // NOTE: we always *insert* a delete due to KV & KKV requrires.
    struct DeleteFunctor
    {
        void operator()(Bucket& bucket,
                        const _KT& key, const _VT& value, uint64_t& deleteCount)
        {
            if (!bucket.IsDeleted())
            {
                deleteCount++;
            }
            bucket.SetDelete(key, value);
        }
    };
    return InternalInsert<DeleteFunctor>(key, value);
}

template<typename _KT, typename _VT, bool HasSpecialKey, bool useCompactBucket>
inline misc::Status DenseHashTable<_KT, _VT, HasSpecialKey, useCompactBucket>::Find(
        const _KT& key, const _VT*& value) const
{
    return InternalFind(key, value);
}

template<typename _KT, typename _VT, bool HasSpecialKey, bool useCompactBucket>
inline misc::Status DenseHashTable<_KT, _VT, HasSpecialKey, useCompactBucket>::FindForReadWrite(
        const _KT& key, _VT& value) const
{
    const _VT* valuePtr = NULL;
    misc::Status status = InternalFind(key, valuePtr);
    if (status != misc::NOT_FOUND)
    {
        assert(valuePtr);
        value = *valuePtr;
    }
    return status;
}

template<typename _KT, typename _VT, bool HasSpecialKey, bool useCompactBucket>
inline uint64_t DenseHashTable<_KT, _VT, HasSpecialKey, useCompactBucket>::Capacity() const
{
    return BucketCountToCapacity(mBucketCount, mOccupancyPct);
}

template<typename _KT, typename _VT, bool HasSpecialKey, bool useCompactBucket>
inline uint64_t DenseHashTable<_KT, _VT, HasSpecialKey, useCompactBucket>::MemoryUse() const
{
    return mBucketCount * sizeof(Bucket) + sizeof(HashTableHeader);
}

template<typename _KT, typename _VT, bool HasSpecialKey, bool useCompactBucket>
inline bool DenseHashTable<_KT, _VT, HasSpecialKey, useCompactBucket>::Shrink(int32_t occupancyPct)
{
    assert(mOccupancyPct > 0 && mOccupancyPct < 100);
    assert(occupancyPct >= 0 && occupancyPct < 100);
    
    uint64_t oldBucketCount = mBucketCount;
    uint64_t newBucketCount = CapacityToBucketCount(
            Size(), (occupancyPct > 0 ? occupancyPct : mOccupancyPct));
    if (newBucketCount >= oldBucketCount)
    {
        return true;
    }
    Header()->bucketCount = newBucketCount;
    mBucketCount = newBucketCount;

    std::vector<bool> bitmap(newBucketCount, false);
    // first part, these buckets in new and also in old region 
    for (uint64_t i = 0; i < newBucketCount; ++i)
    {
        if (bitmap[i])             // processed bucket
        {
            continue;
        }
        if (mBucket[i].IsEmpty())       // still empty
        {
            bitmap[i] = true;
            continue;
        }
        Bucket evictedBucket = mBucket[i];
        new (&mBucket[i]) Bucket;
        bitmap[i] = true;
        if (unlikely(!EvictionInsert(evictedBucket, bitmap)))
        {
            Header()->bucketCount = oldBucketCount;
            mBucketCount = oldBucketCount;
            return false;
        }
    }
    // second part, these buckets in old region only
    for (size_t i = newBucketCount; i < oldBucketCount; ++i)
    {
        Bucket& bucket = mBucket[i];
        if (!bucket.IsEmpty())
        {
            Bucket* newBucket = InternalFindBucket(bucket.Key());
            if (unlikely(!newBucket))
            {
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

template<typename _KT, typename _VT, bool HasSpecialKey, bool useCompactBucket>
inline uint64_t DenseHashTable<_KT, _VT, HasSpecialKey, useCompactBucket>::BucketCountToCapacity(
        uint64_t bucketCount, int32_t occupancyPct)
{
    assert(occupancyPct > 0 && occupancyPct <= 100);
    uint64_t maxKeyCount = bucketCount * occupancyPct / 100.0;
    return maxKeyCount;
}

template<typename _KT, typename _VT, bool HasSpecialKey, bool useCompactBucket>
inline bool DenseHashTable<_KT, _VT, HasSpecialKey, useCompactBucket>::Probe(
        const _KT& key, uint64_t& probeCount, uint64_t& bucketId, uint64_t bucketCount)
{
    ++probeCount;
    bucketId += DENSE_HASH_TABLE_JUMP(probeCount);
    if (unlikely(bucketId >= bucketCount))
    {
        bucketId %= bucketCount;
    }
    if (unlikely(probeCount >= bucketCount))
    {
        IE_LOG(ERROR, "too many probings for key[%lu], probeCount[%lu], "
               "bucketCount[%lu]", (uint64_t)key, probeCount, bucketCount);
        return false;
    }
    return true;
}

template<typename _KT, typename _VT, bool HasSpecialKey, bool useCompactBucket>
template <typename functor>
inline bool DenseHashTable<_KT, _VT, HasSpecialKey, useCompactBucket>::InternalInsert(
        const _KT& key, const _VT& value)
{
    Bucket* bucket = InternalFindBucket(key);
    if (unlikely(!bucket))
    {
        return false;
    }
    bool isNewKey = bucket->IsEmpty();
    functor()(*bucket, key, value, mDeleteCount); // insert or delete
    if (isNewKey)
    {
        ++(Header()->keyCount);
    }
    return true;        
}

template<typename _KT, typename _VT, bool HasSpecialKey, bool useCompactBucket>
inline typename DenseHashTable<_KT, _VT, HasSpecialKey, useCompactBucket>::Bucket*
DenseHashTable<_KT, _VT, HasSpecialKey, useCompactBucket>::InternalFindBucket(
        const _KT& key) const
{
    uint64_t bucketCount = mBucketCount;
    uint64_t bucketId = key % bucketCount;
    uint64_t probeCount = 0;
    while (true)
    {
        Bucket& bucket = mBucket[bucketId];
        if (bucket.IsEmpty()         // not found
            || bucket.IsEqual(key))  // found it or deleted
        {
            return &bucket;
        }
        if (unlikely(!Probe(key, probeCount, bucketId, bucketCount)))
        {
            return NULL;
        }
    }
    return NULL;
}

template<typename _KT, typename _VT, bool HasSpecialKey, bool useCompactBucket>
inline misc::Status DenseHashTable<_KT, _VT, HasSpecialKey, useCompactBucket>::InternalFind(
        const _KT& key, const _VT*& value) const
{
    uint64_t bucketCount = mBucketCount;
    uint64_t bucketId = key % bucketCount;
    // uint64_t bucketId = (uint32_t)key % (uint32_t)bucketCount;

    // uint64_t empty_key =  std::numeric_limits<int64_t>::max();
    // const __m256i vones = _mm256_set_epi64x(0, 0xFFFFFFFFFFFFFFFF, 0, 0xFFFFFFFFFFFFFFFF);
    // const __m256i vones_1 = _mm256_set_epi64x(0xFFFFFFFFFFFFFFFF, 0, 0xFFFFFFFFFFFFFFFF, 0);

    // __m256i vkey = _mm256_set1_epi64x(key);
    // __m256i vempty_key = _mm256_set_epi64x(empty_key, 0, empty_key, 0);

    // int tstzKeyCmp = 1;
    // int tstzEmptyCmp = 1;
    // uint64_t i = bucketId;
    // for(; i < mBucketCount; i+= 2) {
    //     __m256i vload = _mm256_loadu_si256((__m256i*)(mBucket + i));
    //     __m256i vkeyCmp = _mm256_cmpeq_epi64(vload, vkey);
    //     __m256i vemptCmp = _mm256_cmpeq_epi64(vload, vempty_key);
    //     tstzKeyCmp = _mm256_testz_si256(vkeyCmp, vones);
    //     tstzEmptyCmp = _mm256_testz_si256(vemptCmp, vones_1);
    //     if(tstzKeyCmp != 1 || tstzEmptyCmp != 1)
    //         break;
    // }

    // if(tstzKeyCmp != 1)  {
    //     if(mBucket[i].IsEqual(key)) {
    //        value = &(mBucket[i].Value());
    //        return mBucket[i].IsDeleted() ? misc::DELETED : misc::OK;
    //     } else{
    //        value = &(mBucket[i+1].Value());
    //        return  mBucket[i+1].IsDeleted() ? misc::DELETED : misc::OK;
    //     }
    // }

    // //if(tstzEmptyCmp != 1)
    // return misc::NOT_FOUND;

    uint64_t probeCount = 0;
    while (true)
    {
        Bucket& bucket = mBucket[bucketId];
        if (bucket.IsEmpty()) // not found
        {
            return misc::NOT_FOUND;
        }
        else if (bucket.IsEqual(key)) // found it or deleted
        {
            value = &(bucket.Value());
            return bucket.IsDeleted() ? misc::DELETED : misc::OK;
        }
        if (unlikely(!Probe(key, probeCount, bucketId, bucketCount)))
        {
            break;
        }
    }
    return misc::NOT_FOUND;
}

template<typename _KT, typename _VT, bool HasSpecialKey, bool useCompactBucket>
inline uint64_t DenseHashTable<_KT, _VT, HasSpecialKey, useCompactBucket>::CapacityToBucketCount(
        uint64_t maxKeyCount, int32_t occupancyPct)
{
    assert(occupancyPct > 0 && occupancyPct <= 100);
    return ((maxKeyCount > 0 ? maxKeyCount : 1) * 100.0 + occupancyPct - 1) / occupancyPct;
}

template<typename _KT, typename _VT, bool HasSpecialKey, bool useCompactBucket>
inline bool DenseHashTable<_KT, _VT, HasSpecialKey, useCompactBucket>::EvictionInsert(
        const Bucket& firstSeekingBucket, std::vector<bool>& bitmap)
{
    uint64_t bucketCount = mBucketCount;
    Bucket seekingBucket = firstSeekingBucket;
    uint64_t bucketId = seekingBucket.Key() % bucketCount;
    uint64_t probeCount = 0;
    uint64_t evictionCount = 0;
    while (true)
    {
        Bucket& bucket = mBucket[bucketId];
        if (bucket.IsEmpty())      // found
        {
            bitmap[bucketId] = true;
            bucket = seekingBucket;
            return true;
        }
        else if (bitmap[bucketId]) // need probe
        {
            if (unlikely(!Probe(seekingBucket.Key(), 
                                    probeCount, bucketId, bucketCount)))
            {
                assert(false);
                IE_LOG(ERROR, "too many probes [%lu], guradCount[%lu]",
                       probeCount, evictionCount);
                return false;
            }
            continue;
        }
        else // eviction insert
        {
            bitmap[bucketId] = true;
            Bucket evictedBucket = bucket;
            bucket = seekingBucket;
            seekingBucket = evictedBucket;
            bucketId = seekingBucket.Key() % bucketCount;
            probeCount = 0;
            if (unlikely(++evictionCount > bucketCount))
            {
                assert(false);
                IE_LOG(ERROR, "too many eviction insert[%lu], lost key[%lu]",
                       evictionCount, (uint64_t)seekingBucket.Key());
                return false;
            }
        }
    }
    assert(false); // never got hear
    return false;
}

///////////////////////////////////////////////////////////////////////////////
// hide some methods
template<typename _KT, typename _VT, bool useCompactBucket>
class DenseHashTable<_KT, _VT, true, useCompactBucket> : public DenseHashTable<_KT, _VT, false, useCompactBucket>
{
public:
    typedef ClosedHashTableTraits<_KT, _VT, useCompactBucket> Traits;
    typedef typename Traits::Bucket Bucket;
    typedef typename Traits::SpecialBucket SpecialBucket;
private:
    typedef DenseHashTable<_KT, _VT, false, useCompactBucket> Base;
    using Base::mBucketCount;
    using Base::mBucket;
    using Base::_logger;
    using Base::OCCUPANCY_PCT;

public:
    // public for DenseHashTableFileIterator & DenseHashTableFileReader
    typedef typename Base::HashTableHeader HashTableHeader;

public:
    static uint64_t CapacityToTableMemory(uint64_t maxKeyCount,
                                    const HashTableOptions& options)
    {
        return Base::CapacityToTableMemory(maxKeyCount, options) + 
            sizeof(SpecialBucket) * 2;
    }
    static uint64_t CapacityToBuildMemory(uint64_t maxKeyCount,
            const HashTableOptions& options)
    { return CapacityToTableMemory(maxKeyCount, options); }

    static size_t TableMemroyToCapacity(size_t tableMemory, int32_t occupancyPct)
    {
        return Base::TableMemroyToCapacity(
                tableMemory - sizeof(SpecialBucket) * 2, occupancyPct);
    }
    static size_t BuildMemoryToCapacity(size_t buildMemory, int32_t occupancyPct)
    { return TableMemroyToCapacity(buildMemory, occupancyPct); }

public:
    bool MountForWrite(void* data, size_t size,
                       const HashTableOptions& options = OCCUPANCY_PCT)
    {
        size_t minSize = sizeof(HashTableHeader) + sizeof(SpecialBucket) * 2;
        if (size < minSize)
        {
            IE_LOG(ERROR, "not enough space, min size[%lu], give[%lu]",
                   minSize, size);
            return false;
        }
        size_t leftSize = size - sizeof(SpecialBucket) * 2;
        if (!Base::MountForWrite(data, leftSize, options))
        {
            return false;
        }
        new (EmptyBucket()) SpecialBucket();
        new (DeleteBucket()) SpecialBucket();

        return true;
    }

    bool MountForRead(const void* data, size_t size)
    {
        if (size < sizeof(SpecialBucket) * 2)
        {
            IE_LOG(ERROR, "too small size[%lu], BucketSize[%lu]",
                   size, sizeof(SpecialBucket));
            return false;
        }
        return Base::MountForRead(data, size - sizeof(SpecialBucket) * 2);
    }

    bool Insert(const _KT& key, const _VT& value)
    {
        struct InsertFunctor
        {   
            void operator()(Bucket& bucket,
                            const _KT& key, const _VT& value, uint64_t& deleteCount)
            {
                if (bucket.IsDeleted()) {
                    deleteCount--;
                }
                bucket.Set(key, value);
            }
            void operator()(SpecialBucket& bucket,
                            const _KT& key, const _VT& value, uint64_t& deleteCount)
            {
                if (bucket.IsDeleted()) {
                    deleteCount--;
                }
                bucket.Set(key, value);
            }
        };
        return InternalInsert<InsertFunctor>(key, value);
    }
    
    bool Delete(const _KT& key, const _VT& value = _VT())
    {
        // NOTE: we always *insert* a delete du to KV & KKV requrires.
        struct DeleteFunctor
        {
            void operator()(Bucket& bucket,
                            const _KT& key, const _VT& value, uint64_t& deleteCount)
            {
                if (!bucket.IsDeleted()) {
                    deleteCount++;
                }
                bucket.SetDelete(key, value);
            }
            void operator()(SpecialBucket& bucket,
                            const _KT& key, const _VT& value, uint64_t& deleteCount)
            {
                if (!bucket.IsDeleted()) {
                    deleteCount++;
                }
                bucket.SetDelete(key, value);
            }
        };
        return InternalInsert<DeleteFunctor>(key, value);
    }

    misc::Status Find(const _KT& key, const _VT*& value) const
    {
        if (likely(!Bucket::IsEmptyKey(key) && !Bucket::IsDeleteKey(key)))
        {
            return Base::InternalFind(key, value);
        }

        SpecialBucket* bucket = Bucket::IsEmptyKey(key) ? EmptyBucket() : DeleteBucket();        
        if (bucket->IsEmpty())
        {
            return misc::NOT_FOUND;
        }
        value = &(bucket->Value());
        return bucket->IsDeleted() ? misc::DELETED : misc::OK;
    }

    uint64_t MemoryUse() const
    { return Base::MemoryUse() + sizeof(SpecialBucket) * 2; }

    bool Shrink(int32_t occupancyPct = 0)
    {
        SpecialBucket emptyBucket = *EmptyBucket();
        SpecialBucket deleteBucket = *DeleteBucket();
        if (!Base::Shrink(occupancyPct))
        {
            return false;
        }
        *EmptyBucket() = emptyBucket;
        *DeleteBucket() = deleteBucket;
        return true;
    }

private:
    SpecialBucket* EmptyBucket() const
    { return reinterpret_cast<SpecialBucket*> (&mBucket[mBucketCount]); }

    SpecialBucket* DeleteBucket() const
    { return reinterpret_cast<SpecialBucket*> (&mBucket[mBucketCount]) + 1; }

    template <typename functor>
    bool InternalInsert(const _KT& key, const _VT& value)
    {
        bool isNewKey = false;
        if (unlikely(Bucket::IsEmptyKey(key)))
        {
            SpecialBucket* bucket = EmptyBucket();
            isNewKey = bucket->IsEmpty();
            functor()(*bucket, key, value, Base::mDeleteCount);
        }
        else if (unlikely(Bucket::IsDeleteKey(key)))
        {
            SpecialBucket* bucket = DeleteBucket();
            isNewKey = bucket->IsEmpty();
            functor()(*bucket, key, value, Base::mDeleteCount);
        }
        else
        {
            Bucket* bucket = Base::InternalFindBucket(key);
            if (unlikely(!bucket))
            {
                return false;
            }
            isNewKey = bucket->IsEmpty();
            functor()(*bucket, key, value, Base::mDeleteCount);
        }
        if (isNewKey)
        {
            ++(Base::Header()->keyCount);
        }
        return true;
    }
private:
    friend class DenseHashTableTest;
};

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_DENSE_HASH_TABLE_H
