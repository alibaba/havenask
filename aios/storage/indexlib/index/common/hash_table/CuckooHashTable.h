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
#include <algorithm>

#include "autil/MurmurHash.h"
#include "indexlib/index/common/hash_table/BucketCompressor.h"
#include "indexlib/index/common/hash_table/ClosedHashTableIterator.h"
#include "indexlib/index/common/hash_table/ClosedHashTableTraits.h"
#include "indexlib/index/common/hash_table/HashTableBase.h"
#include "indexlib/index/common/hash_table/HashTableOptions.h"
#include "indexlib/index/common/hash_table/SpecialKeyBucket.h"
#include "indexlib/util/Status.h"

namespace indexlibv2::index {

#ifndef CACHE_LINE_SIZE
#define CACHE_LINE_SIZE 64U
#endif

template <typename _KT, typename _VT, bool HasSpecialKey = ClosedHashTableTraits<_KT, _VT, false>::HasSpecialKey,
          bool useCompactBucket = false>
class CuckooHashTableBase : public HashTableBase
{
public:
    CuckooHashTableBase()
        : _bucket(NULL)
        , _bucketCount(0)
        , _blockCount(0)
        , _nu_hashFunc(2)
        , _occupancyPct(0)
        , _bFSDepth(100)
        , _curCallId(0)
        , _deleteCount(0)
    {
    }
    ~CuckooHashTableBase() {}

public:
    typedef _KT KeyType;
    typedef _VT ValueType;
    struct HashTableHeader {
        uint8_t version = 0;
        uint8_t nu_hashFunc = 0;
        uint8_t maxNu_hashFunc = 8;
        uint64_t bucketCount = 0;
        uint64_t keyCount = 0;
        uint64_t stretchSize = 0;
        uint64_t padding[0] __attribute__((aligned(CACHE_LINE_SIZE)));
    };
    static const int32_t OCCUPANCY_PCT = 80;
    static const uint32_t BLOCK_SIZE = 4;
    static const uint64_t MAX_NUM_BFS_TREE_NODE = 1048576UL; // 1 << 20
    static constexpr double STRETCH_MEM_RATIO = 0.01;

public:
    typedef typename ClosedHashTableTraits<_KT, _VT, useCompactBucket>::Bucket Bucket;

private:
    struct TreeNode {
        static const uint64_t BFS_ROOT_IDX = std::numeric_limits<uint64_t>::max() >> 8;
        uint64_t _bucketId;       // first bucket id in block
        uint64_t _parent    : 56; // parent's idx in bfsTree
        uint64_t _inBlockId : 8;  // parent's inBlockId in its block
        TreeNode(uint64_t bucketId, uint64_t parent, uint8_t inBlockId)
            : _bucketId(bucketId)
            , _parent(parent)
            , _inBlockId(inBlockId)
        {
        }
    };
    typedef std::vector<TreeNode> TreeNodeVec;

public:
    virtual indexlib::util::Status Find(uint64_t key, autil::StringView& value) const override = 0;
    indexlib::util::Status FindForReadWrite(uint64_t key, autil::StringView& value,
                                            autil::mem_pool::Pool* pool) const override final
    {
        assert(false);
        // cuckoo hash table do not support mem table
        return indexlib::util::Status::NOT_FOUND;
    }

public:
    int32_t GetRecommendedOccupancy(int32_t occupancy) const override final
    {
        return DoGetRecommendedOccupancy(occupancy);
    }

    uint64_t CapacityToTableMemory(uint64_t maxKeyCount, const HashTableOptions& options) const override;
    uint64_t CapacityToBuildMemory(uint64_t maxKeyCount, const HashTableOptions& options) const override;
    size_t TableMemroyToCapacity(size_t tableMemory, int32_t occupancyPct) const override;
    size_t BuildMemoryToCapacity(size_t buildMemory, int32_t occupancyPct) const override;
    size_t BuildMemoryToTableMemory(size_t buildMemory, int32_t occupancyPct) const override;
    uint64_t Capacity() const override { return BucketCountToCapacity(_bucketCount, _occupancyPct); }

public:
    static int32_t DoGetRecommendedOccupancy(int32_t occupancy) { return (occupancy > 97) ? 97 : occupancy; }

    static uint64_t DoCapacityToTableMemory(uint64_t maxKeyCount, const HashTableOptions& options);
    static uint64_t DoCapacityToBuildMemory(uint64_t maxKeyCount, const HashTableOptions& options);
    static size_t DoTableMemroyToCapacity(size_t tableMemory, int32_t occupancyPct);
    static size_t DoBuildMemoryToCapacity(size_t buildMemory, int32_t occupancyPct);
    static size_t DoBuildMemoryToTableMemory(size_t buildMemory, int32_t occupancyPct);

public:
    bool Insert(uint64_t key, const autil::StringView& value) override;
    bool Delete(uint64_t key, const autil::StringView& value = autil::StringView()) override;

public:
    bool MountForWrite(void* data, size_t size, const HashTableOptions& options = OCCUPANCY_PCT) override;
    bool MountForRead(const void* data, size_t size) override;
    uint64_t Size() const override { return Header()->keyCount; }
    bool IsFull() const override { return Size() >= Capacity(); }
    uint64_t MemoryUse() const override;
    uint64_t BuildAssistantMemoryUse() const override;
    bool Shrink(int32_t occupancyPct = 0) override;
    int32_t GetOccupancyPct() const override { return _occupancyPct; }
    uint64_t GetDeleteCount() const override { return _deleteCount; }

public:
    bool Insert(const _KT& key, const _VT& value);
    indexlib::util::Status Find(const _KT& key, const _VT*& value) const;
    indexlib::util::Status FindForReadWrite(const _KT& key, _VT& value) const;
    uint64_t BucketCount() const { return _bucketCount; }
    bool Stretch() override;
    void* Address() const override { return Header(); }
    // ATTENTION: ClosedHashTableTraits<_KT, _VT>::HasSpecialKey may not equal HasSpecialKey
    typedef ClosedHashTableIterator<_KT, _VT, Bucket, ClosedHashTableTraits<_KT, _VT, useCompactBucket>::HasSpecialKey>
        Iterator;
    Iterator* CreateIterator() { return new Iterator(_bucket, _bucketCount, Size()); }
    void PrintHashTable() const;
    size_t Compress(BucketCompressor* bucketCompressor) override;

public:
    // public for CuckooHashTableFileReader
    static _KT CuckooHash(const _KT& key, uint32_t hashFuncId);
    static uint64_t GetFirstBucketIdInBlock(const _KT& hash, uint64_t blockCount)
    {
        return (blockCount > std::numeric_limits<uint32_t>::max())
                   ? ((hash % blockCount) * BLOCK_SIZE)
                   : ((uint64_t)((uint32_t)hash % (uint32_t)blockCount) * BLOCK_SIZE);
    }
    // { return (hash % blockCount) * BLOCK_SIZE; }
    // { return ((uint32_t)hash % (uint32_t) blockCount) * BLOCK_SIZE; }
    // { return (hash % (uint32_t) blockCount) * BLOCK_SIZE; }

protected:
    HashTableHeader* Header() const { return (HashTableHeader*)((uint8_t*)_bucket - sizeof(HashTableHeader)); }
    Bucket* InternalFindBucket(const _KT& key);
    Bucket* DoInternalFindBucket(const _KT& key);

private:
    static uint64_t CapacityToBucketCount(uint64_t maxKeyCount, int32_t occupancyPct);
    static uint64_t BucketCountToCapacity(uint64_t bucketCount, int32_t occupancyPct);
    bool ReHash(uint64_t newBucketCount);
    const Bucket* FindBucketForRead(const _KT& key, uint8_t nu_hashFunc) const;
    template <typename functor>
    bool InternalInsert(const _KT key, const _VT& value);
    Bucket* BFSFindBucket(TreeNodeVec& bfsTree);
    Bucket* CuckooKick(TreeNodeVec& bfsTree, uint32_t inEmptyBlockId);
    bool EvictionInsert(Bucket seekingBucket, std::vector<bool>& bitmap);
    Bucket* FindBucketForEviction(const _KT& key, std::vector<bool>& bitmap);
    Bucket* BFSFindBucketForEviction(TreeNodeVec& bfsTree, std::vector<bool>& bitmap);
    bool CheckBucket(const _KT& key, uint64_t targetBucketId) const;

protected:
    Bucket* _bucket;
    uint64_t _bucketCount;
    uint64_t _blockCount;
    uint8_t _nu_hashFunc;
    int32_t _occupancyPct;
    uint32_t _bFSDepth;
    uint32_t _curCallId;
    std::vector<uint32_t> _callIdVec;
    uint64_t _deleteCount;

protected:
    AUTIL_LOG_DECLARE();
    friend class CuckooHashTableTest;
};

///////////////////////////////////////////
template <typename _KT, typename _VT, bool HasSpecialKey, bool useCompactBucket>
alog::Logger* CuckooHashTableBase<_KT, _VT, HasSpecialKey, useCompactBucket>::_logger =
    alog::Logger::getLogger("indexlib.index.CuckooHashTableBase");

template <typename _KT, typename _VT, bool HasSpecialKey, bool useCompactBucket>
bool CuckooHashTableBase<_KT, _VT, HasSpecialKey, useCompactBucket>::Insert(const _KT& key, const _VT& value)
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
inline bool CuckooHashTableBase<_KT, _VT, HasSpecialKey, useCompactBucket>::Insert(uint64_t key,
                                                                                   const autil::StringView& value)
{
    const _VT& v = *reinterpret_cast<const _VT*>(value.data());
    assert(sizeof(_VT) == value.size());
    return Insert(key, v);
}

template <typename _KT, typename _VT, bool HasSpecialKey, bool useCompactBucket>
inline bool CuckooHashTableBase<_KT, _VT, HasSpecialKey, useCompactBucket>::Delete(uint64_t key,
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

    return InternalInsert<DeleteFunctor>(key, v);
}

template <typename _KT, typename _VT, bool HasSpecialKey, bool useCompactBucket>
inline size_t
CuckooHashTableBase<_KT, _VT, HasSpecialKey, useCompactBucket>::Compress(BucketCompressor* bucketCompressor)
{
    char* begin = static_cast<char*>((void*)_bucket);
    char* outBuffer = begin;
    for (uint64_t i = 0u; i < _bucketCount; ++i) {
        size_t compressedBucketSize =
            bucketCompressor->Compress(static_cast<char*>((void*)(_bucket + i)), sizeof(Bucket), outBuffer);
        outBuffer += compressedBucketSize;
    }
    return sizeof(HashTableHeader) + (outBuffer - begin);
}

template <typename _KT, typename _VT, bool HasSpecialKey, bool useCompactBucket>
inline uint64_t CuckooHashTableBase<_KT, _VT, HasSpecialKey, useCompactBucket>::CapacityToTableMemory(
    uint64_t maxKeyCount, const HashTableOptions& options) const
{
    return DoCapacityToTableMemory(maxKeyCount, options);
}

template <typename _KT, typename _VT, bool HasSpecialKey, bool useCompactBucket>
inline uint64_t
CuckooHashTableBase<_KT, _VT, HasSpecialKey, useCompactBucket>::DoCapacityToTableMemory(uint64_t maxKeyCount,
                                                                                        const HashTableOptions& options)
{
    int32_t occupancyPct = options.occupancyPct;
    size_t bucketSize = sizeof(Bucket) * CapacityToBucketCount(maxKeyCount, occupancyPct);
    if (options.mayStretch) {
        bucketSize = bucketSize * (1 + STRETCH_MEM_RATIO) + 1; // +1 for decimal
        bucketSize += sizeof(Bucket) * BLOCK_SIZE;             // at least one block
    }
    return bucketSize + sizeof(HashTableHeader);
}

template <typename _KT, typename _VT, bool HasSpecialKey, bool useCompactBucket>
inline uint64_t CuckooHashTableBase<_KT, _VT, HasSpecialKey, useCompactBucket>::CapacityToBuildMemory(
    uint64_t maxKeyCount, const HashTableOptions& options) const
{
    return DoCapacityToBuildMemory(maxKeyCount, options);
}

template <typename _KT, typename _VT, bool HasSpecialKey, bool useCompactBucket>
inline uint64_t
CuckooHashTableBase<_KT, _VT, HasSpecialKey, useCompactBucket>::DoCapacityToBuildMemory(uint64_t maxKeyCount,
                                                                                        const HashTableOptions& options)
{
    size_t hashTableSize = DoCapacityToTableMemory(maxKeyCount, options);
    size_t bucketCount = CapacityToBucketCount(maxKeyCount, options.occupancyPct);
    size_t blockCount = bucketCount / BLOCK_SIZE;
    size_t callIdSize = blockCount * sizeof(uint32_t);
    size_t bfsTreeSize = std::min(blockCount, (size_t)MAX_NUM_BFS_TREE_NODE) * sizeof(TreeNode);
    return hashTableSize + callIdSize + bfsTreeSize;
}

template <typename _KT, typename _VT, bool HasSpecialKey, bool useCompactBucket>
inline size_t
CuckooHashTableBase<_KT, _VT, HasSpecialKey, useCompactBucket>::DoTableMemroyToCapacity(size_t tableMemory,
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
CuckooHashTableBase<_KT, _VT, HasSpecialKey, useCompactBucket>::TableMemroyToCapacity(size_t tableMemory,
                                                                                      int32_t occupancyPct) const
{
    return DoTableMemroyToCapacity(tableMemory, occupancyPct);
}

template <typename _KT, typename _VT, bool HasSpecialKey, bool useCompactBucket>
inline size_t
CuckooHashTableBase<_KT, _VT, HasSpecialKey, useCompactBucket>::DoBuildMemoryToCapacity(size_t buildMemory,
                                                                                        int32_t occupancyPct)
{
    // Header, Bukcet, Callid, BFSTree
    if (buildMemory < sizeof(HashTableHeader)) {
        return 0;
    }

    size_t leftMemory = buildMemory - sizeof(HashTableHeader);
    size_t maxKeyCountThreshhold = MAX_NUM_BFS_TREE_NODE * BLOCK_SIZE * occupancyPct / 100;
    size_t threshhold = DoCapacityToBuildMemory(maxKeyCountThreshhold, occupancyPct);
    size_t bucketCount = 0;
    if (buildMemory >= threshhold) {
        leftMemory -= (size_t)MAX_NUM_BFS_TREE_NODE * sizeof(TreeNode);
        size_t blockCount = leftMemory / (sizeof(uint32_t) + BLOCK_SIZE * sizeof(Bucket));
        bucketCount = blockCount * BLOCK_SIZE;
    } else {
        size_t blockCount = leftMemory / (sizeof(uint32_t) + sizeof(TreeNode) + BLOCK_SIZE * sizeof(Bucket));
        bucketCount = blockCount * BLOCK_SIZE;
    }
    return bucketCount * occupancyPct / 100;
}

template <typename _KT, typename _VT, bool HasSpecialKey, bool useCompactBucket>
inline size_t
CuckooHashTableBase<_KT, _VT, HasSpecialKey, useCompactBucket>::BuildMemoryToCapacity(size_t buildMemory,
                                                                                      int32_t occupancyPct) const
{
    return DoBuildMemoryToCapacity(buildMemory, occupancyPct);
}

template <typename _KT, typename _VT, bool HasSpecialKey, bool useCompactBucket>
inline size_t
CuckooHashTableBase<_KT, _VT, HasSpecialKey, useCompactBucket>::DoBuildMemoryToTableMemory(size_t buildMemory,
                                                                                           int32_t occupancyPct)
{
    size_t maxKeyCount = DoBuildMemoryToCapacity(buildMemory, occupancyPct);
    return DoCapacityToTableMemory(maxKeyCount, occupancyPct);
}

template <typename _KT, typename _VT, bool HasSpecialKey, bool useCompactBucket>
inline size_t
CuckooHashTableBase<_KT, _VT, HasSpecialKey, useCompactBucket>::BuildMemoryToTableMemory(size_t buildMemory,
                                                                                         int32_t occupancyPct) const
{
    return DoBuildMemoryToTableMemory(buildMemory, occupancyPct);
}

template <typename _KT, typename _VT, bool HasSpecialKey, bool useCompactBucket>
inline uint64_t
CuckooHashTableBase<_KT, _VT, HasSpecialKey, useCompactBucket>::CapacityToBucketCount(uint64_t maxKeyCount,
                                                                                      int32_t occupancyPct)
{
    assert(occupancyPct > 0 && occupancyPct <= 100);
    uint64_t bucketCount = ((maxKeyCount > 0 ? maxKeyCount : 1) * 100.0 + occupancyPct - 1) / occupancyPct;
    return (bucketCount + BLOCK_SIZE - 1) / BLOCK_SIZE * BLOCK_SIZE;
}

template <typename _KT, typename _VT, bool HasSpecialKey, bool useCompactBucket>
inline uint64_t
CuckooHashTableBase<_KT, _VT, HasSpecialKey, useCompactBucket>::BucketCountToCapacity(uint64_t bucketCount,
                                                                                      int32_t occupancyPct)
{
    assert(occupancyPct > 0 && occupancyPct <= 100);
    uint64_t maxKeyCount = bucketCount * occupancyPct / 100.0;
    return maxKeyCount;
}

template <typename _KT, typename _VT, bool HasSpecialKey, bool useCompactBucket>
inline bool
CuckooHashTableBase<_KT, _VT, HasSpecialKey, useCompactBucket>::MountForWrite(void* data, size_t size,
                                                                              const HashTableOptions& inputOptions)
{
    HashTableOptions options(OCCUPANCY_PCT);
    if (inputOptions.Valid()) {
        options = inputOptions;
    }
    if (size < sizeof(HashTableHeader)) {
        AUTIL_LOG(ERROR, "not enough space, header size[%lu], give[%lu]", sizeof(HashTableHeader), size);
        return false;
    }
    size_t bucketSize = size - sizeof(HashTableHeader);
    size_t stretchSize = 0;
    if (options.mayStretch) {
        stretchSize = sizeof(Bucket) * BLOCK_SIZE;
        stretchSize += (bucketSize - bucketSize / (1 + STRETCH_MEM_RATIO));
        bucketSize -= stretchSize;
    }
    _occupancyPct = options.occupancyPct;
    _blockCount = bucketSize / sizeof(Bucket) / BLOCK_SIZE;
    if (_blockCount < 1) {
        AUTIL_LOG(ERROR, "size[%lu] too small", size);
        return false;
    }
    _bucketCount = _blockCount * BLOCK_SIZE;
    _callIdVec.resize(_blockCount, 0);
    _curCallId = 0;

    if (!data) {
        AUTIL_LOG(ERROR, "data is null");
        return false;
    }
    HashTableHeader* header = (HashTableHeader*)data;
    header->nu_hashFunc = _nu_hashFunc;
    header->bucketCount = _bucketCount;
    header->keyCount = 0;
    header->maxNu_hashFunc = options.maxNu_hashFunc;
    header->stretchSize = stretchSize;
    _bucket = (Bucket*)(header + 1);
    new (_bucket) Bucket[_bucketCount];

    AUTIL_LOG(INFO,
              "mount for write, size[%lu], occupancyPct[%d], bucketCount[%lu], "
              "_bucket[%lu], headerSize[%lu]",
              size, _occupancyPct, _bucketCount, (uint64_t)_bucket, sizeof(HashTableHeader));
    return true;
}

template <typename _KT, typename _VT, bool HasSpecialKey, bool useCompactBucket>
inline bool CuckooHashTableBase<_KT, _VT, HasSpecialKey, useCompactBucket>::MountForRead(const void* data, size_t size)
{
    assert(data);
    if (size < sizeof(HashTableHeader)) {
        AUTIL_LOG(ERROR, "too small size[%lu], header size[%lu]", size, sizeof(HashTableHeader));
        return false;
    }
    HashTableHeader* header = (HashTableHeader*)data;
    _nu_hashFunc = header->nu_hashFunc;
    _bucketCount = header->bucketCount;
    _blockCount = _bucketCount / BLOCK_SIZE;
    _occupancyPct = header->keyCount * 100 / _bucketCount;
    if (size < sizeof(HashTableHeader) + _bucketCount * sizeof(Bucket)) {
        AUTIL_LOG(ERROR, "too small size[%lu], header size[%lu], bucket size[%lu]", size, sizeof(HashTableHeader),
                  _bucketCount * sizeof(Bucket));
        return false;
    }

    _bucket = (Bucket*)(header + 1);
    if ((uint64_t)_bucket % CACHE_LINE_SIZE != 0) {
        AUTIL_LOG(INFO, "mount for read, headerSize[%lu], _bucket[%lu]", sizeof(HashTableHeader), (uint64_t)_bucket);
    }
    return true;
}

template <typename _KT, typename _VT, bool HasSpecialKey, bool useCompactBucket>
inline indexlib::util::Status
CuckooHashTableBase<_KT, _VT, HasSpecialKey, useCompactBucket>::Find(const _KT& key, const _VT*& value) const
{
    const Bucket* bucket = FindBucketForRead(key, _nu_hashFunc);
    if (bucket == NULL) {
        return indexlib::util::NOT_FOUND;
    }
    value = &(bucket->Value());
    return bucket->IsDeleted() ? indexlib::util::DELETED : indexlib::util::OK;
}

template <typename _KT, typename _VT, bool HasSpecialKey, bool useCompactBucket>
inline indexlib::util::Status
CuckooHashTableBase<_KT, _VT, HasSpecialKey, useCompactBucket>::FindForReadWrite(const _KT& key, _VT& value) const
{
    while (true) {
        const Bucket* bucket = FindBucketForRead(key, Header()->nu_hashFunc);
        indexlib::util::Status status = indexlib::util::NOT_FOUND;

        if (bucket == NULL) {
            // maybe: during a kick process, (false NOT_FOUND) could happen
            return status;
        }

        value = bucket->Value();
        status = (bucket->IsDeleted()) ? indexlib::util::DELETED : indexlib::util::OK;

        // IsEmpty(): old k,v kicked out
        // Key Not Equal: new k,v already replaced old k,v
        // Value not Equal: very rare situation (k,v kicked out and then kicked back with k,v')
        if (likely(!bucket->IsEmpty() && bucket->IsEqual(key) && bucket->Value() == value)) {
            return status;
        }
    }
}

template <typename _KT, typename _VT, bool HasSpecialKey, bool useCompactBucket>
inline void CuckooHashTableBase<_KT, _VT, HasSpecialKey, useCompactBucket>::PrintHashTable() const
{
    std::stringstream ss;
    for (uint32_t i = 0; i < _blockCount; ++i) {
        ss << "[" << i << "] ";
        for (uint32_t inBlockId = 0; inBlockId < BLOCK_SIZE; ++inBlockId) {
            const Bucket& bucket = _bucket[i * BLOCK_SIZE + inBlockId];
            const auto& key = bucket.Key();
            if (bucket.IsEmpty() || bucket.IsDeleted()) {
                ss << (bucket.IsEmpty() ? "E[]  " : "D[]  ");
            } else {
                ss << key << "[";
                for (uint8_t hashFuncId = 0; hashFuncId < _nu_hashFunc - 1; ++hashFuncId) {
                    ss << (uint32_t)CuckooHash(key, hashFuncId) % (uint32_t)_blockCount << ",";
                }
                ss << (uint32_t)CuckooHash(key, _nu_hashFunc - 1) % (uint32_t)_blockCount << "]  ";
            }
        }
        ss << std::endl;
    }
    std::cerr << ss.str() << std::endl;
}

template <typename _KT, typename _VT, bool HasSpecialKey, bool useCompactBucket>
inline bool CuckooHashTableBase<_KT, _VT, HasSpecialKey, useCompactBucket>::CheckBucket(const _KT& key,
                                                                                        uint64_t targetBucketId) const
{
    uint64_t targetBlockId = targetBucketId / BLOCK_SIZE;
    for (uint32_t hashFuncId = 0; hashFuncId < _nu_hashFunc; ++hashFuncId) {
        const _KT& hash = CuckooHash(key, hashFuncId);
        uint64_t blockId = GetFirstBucketIdInBlock(hash, _blockCount) / BLOCK_SIZE;
        if (blockId == targetBlockId) {
            return true;
        }
    }
    std::stringstream ss;
    ss << "MV " << key << "[";
    for (uint8_t hashFuncId = 0; hashFuncId < _nu_hashFunc - 1; ++hashFuncId) {
        const _KT& hash = CuckooHash(key, hashFuncId);
        ss << GetFirstBucketIdInBlock(hash, _blockCount) / BLOCK_SIZE << ",";
    }
    const _KT& hash = CuckooHash(key, _nu_hashFunc);
    ss << GetFirstBucketIdInBlock(hash, _blockCount) / BLOCK_SIZE << "] to wrong block [" << targetBlockId << "]";
    std::cerr << ss.str() << std::endl;
    return false;
}

template <typename _KT, typename _VT, bool HasSpecialKey, bool useCompactBucket>
inline bool CuckooHashTableBase<_KT, _VT, HasSpecialKey, useCompactBucket>::Shrink(int32_t occupancyPct)
{
    assert(_occupancyPct > 0 && _occupancyPct <= 100);
    assert(occupancyPct >= 0 && occupancyPct <= 100);

    uint64_t oldBucketCount = _bucketCount;
    uint64_t newBucketCount = CapacityToBucketCount(Size(), (occupancyPct > 0 ? occupancyPct : _occupancyPct));
    assert(newBucketCount > 0);
    if (newBucketCount >= oldBucketCount) {
        return true;
    }
    if (ReHash(newBucketCount)) {
        _occupancyPct = occupancyPct > 0 ? occupancyPct : _occupancyPct;
        return true;
    }
    AUTIL_LOG(WARN, "Shrink fail, recover it now");
    return ReHash(oldBucketCount);
}

template <typename _KT, typename _VT, bool HasSpecialKey, bool useCompactBucket>
inline bool CuckooHashTableBase<_KT, _VT, HasSpecialKey, useCompactBucket>::Stretch()
{
    uint64_t newBlockCount = Header()->stretchSize / sizeof(Bucket) / BLOCK_SIZE + _blockCount;
    if (newBlockCount <= _blockCount) {
        return true;
    }
    Header()->stretchSize = 0; // only support stretch once now
    uint64_t newBucketCount = newBlockCount * BLOCK_SIZE;
    new (&_bucket[_bucketCount]) Bucket[newBucketCount - _bucketCount];
    return ReHash(newBucketCount);
}

template <typename _KT, typename _VT, bool HasSpecialKey, bool useCompactBucket>
inline bool CuckooHashTableBase<_KT, _VT, HasSpecialKey, useCompactBucket>::ReHash(uint64_t newBucketCount)
{
    uint64_t oldBucketCount = _bucketCount;
    _bucketCount = newBucketCount;
    assert(_bucketCount % BLOCK_SIZE == 0);
    _blockCount = _bucketCount / BLOCK_SIZE;
    _nu_hashFunc = 2;
    Header()->nu_hashFunc = 2;
    Header()->bucketCount = newBucketCount;

    _callIdVec.assign(_blockCount, 0);
    _curCallId = 0;

    std::vector<bool> bitmap(newBucketCount, false);
    // first part, these buckets in new and also in old region
    for (uint64_t i = 0; i < newBucketCount; ++i) {
        if (bitmap[i]) // processed bucket
        {
            continue;
        }
        if (_bucket[i].IsEmpty()) // still empty
        {
            bitmap[i] = true;
            continue;
        }
        Bucket evictedBucket = _bucket[i];
        new (&_bucket[i]) Bucket;
        bitmap[i] = true;
        if (unlikely(!EvictionInsert(evictedBucket, bitmap))) {
            Header()->bucketCount = oldBucketCount;
            _bucketCount = oldBucketCount;
            _blockCount = _bucketCount / BLOCK_SIZE;
            AUTIL_LOG(ERROR, "ReHash failed, newBucketCount[%lu], oldBucketCount[%lu]", newBucketCount, oldBucketCount);
            return false;
        }
    }
    // second part, these buckets in old region only
    for (size_t i = newBucketCount; i < oldBucketCount; ++i) {
        Bucket& bucket = _bucket[i];
        if (!bucket.IsEmpty()) {
            Bucket* newBucket = InternalFindBucket(bucket.Key());
            if (unlikely(!newBucket)) {
                Header()->bucketCount = oldBucketCount;
                _bucketCount = oldBucketCount;
                _blockCount = _bucketCount / BLOCK_SIZE;
                AUTIL_LOG(ERROR, "ReHash failed, newBucketCount[%lu], oldBucketCount[%lu]", newBucketCount,
                          oldBucketCount);
                return false;
            }
            *newBucket = bucket;
        }
    }
    return true;
}

template <typename _KT, typename _VT, bool HasSpecialKey, bool useCompactBucket>
inline bool CuckooHashTableBase<_KT, _VT, HasSpecialKey, useCompactBucket>::EvictionInsert(Bucket seekingBucket,
                                                                                           std::vector<bool>& bitmap)
{
    while (true) {
        Bucket* bucket = FindBucketForEviction(seekingBucket.Key(), bitmap);
        if (!bucket) // can not find a bucket, rehash
        {
            if (_nu_hashFunc >= Header()->maxNu_hashFunc) {
                AUTIL_LOG(ERROR, "can not find bucket to store key[%lu] nu_hashFunc[%u]", (uint64_t)seekingBucket.Key(),
                          _nu_hashFunc);
                return false;
            }
            ++_nu_hashFunc;
            ++Header()->nu_hashFunc;
            AUTIL_LOG(WARN, "Increase nu_hashFunc to [%u], size[%lu], bucketCount[%lu]", _nu_hashFunc, Size(),
                      _bucketCount);
            continue;
        }
        assert(CheckBucket(seekingBucket.Key(), bucket - _bucket));
        if (bucket->IsEmpty()) // got an empty bucket, store and return
        {
            *bucket = seekingBucket;
            return true;
        } else // got a non-empty & unreached bucket, swap and continue
        {
            Bucket evictedBucket = *bucket;
            *bucket = seekingBucket;
            seekingBucket = evictedBucket;
        }
    }
    return true;
}

template <typename _KT, typename _VT, bool HasSpecialKey, bool useCompactBucket>
inline typename CuckooHashTableBase<_KT, _VT, HasSpecialKey, useCompactBucket>::Bucket*
CuckooHashTableBase<_KT, _VT, HasSpecialKey, useCompactBucket>::FindBucketForEviction(const _KT& key,
                                                                                      std::vector<bool>& bitmap)
{
    TreeNodeVec bfsTree;
    for (uint32_t hashFuncId = 0; hashFuncId < _nu_hashFunc; ++hashFuncId) {
        const _KT& hash = CuckooHash(key, hashFuncId);
        uint64_t firstBucketId = GetFirstBucketIdInBlock(hash, _blockCount);
        for (uint32_t curBucketId = firstBucketId; curBucketId < firstBucketId + BLOCK_SIZE; ++curBucketId) {
            Bucket* curBucket = &(_bucket[curBucketId]);
            if (curBucket->IsEmpty() || !bitmap[curBucketId]) {
                bitmap[curBucketId] = true;
                return curBucket;
            }
        }
        bfsTree.push_back(TreeNode(firstBucketId, TreeNode::BFS_ROOT_IDX, 0));
    }
    // find a bucket through cuckoo kick
    // 4 kick 3 kick 2 kick 1, return bucket 4 with 1's old content
    // if an empty bucket was kicked out, we have a new empty bucket
    // if an un-reached bucket was kicked out, store it in the new empty bucket
    return BFSFindBucketForEviction(bfsTree, bitmap);
}

template <typename _KT, typename _VT, bool HasSpecialKey, bool useCompactBucket>
inline typename CuckooHashTableBase<_KT, _VT, HasSpecialKey, useCompactBucket>::Bucket*
CuckooHashTableBase<_KT, _VT, HasSpecialKey, useCompactBucket>::BFSFindBucketForEviction(TreeNodeVec& bfsTree,
                                                                                         std::vector<bool>& bitmap)
{
    ++_curCallId;
    for (uint32_t i = 0; i < bfsTree.size(); ++i) {
        _callIdVec[bfsTree[i]._bucketId / BLOCK_SIZE] = _curCallId;
    }
    uint64_t idx = 0;
    uint64_t nextDepthBeginIdx = 0;
    uint32_t nextDepth = 0;
    while (idx < bfsTree.size()) {
        if (idx == nextDepthBeginIdx) {
            if (++nextDepth > _bFSDepth) // got all nodes with Depth = 0, 1, 2, ..., _dfsDepth
            {
                break;
            }
            nextDepthBeginIdx = bfsTree.size();
        }
        uint64_t bucketId = bfsTree[idx]._bucketId;
        for (uint32_t inBlockId = 0; inBlockId < BLOCK_SIZE; ++inBlockId) {
            const _KT& key = _bucket[bucketId + inBlockId].Key();
            for (uint32_t hashFuncId = 0; hashFuncId < _nu_hashFunc; ++hashFuncId) {
                const _KT& hash = CuckooHash(key, hashFuncId);
                uint64_t firstBucketId = GetFirstBucketIdInBlock(hash, _blockCount);
                if (_callIdVec[firstBucketId / BLOCK_SIZE] == _curCallId) {
                    continue;
                }
                _callIdVec[firstBucketId / BLOCK_SIZE] = _curCallId;
                for (uint32_t curBucketId = firstBucketId; curBucketId < firstBucketId + BLOCK_SIZE; ++curBucketId) {
                    Bucket& curBucket = _bucket[curBucketId];
                    if (curBucket.IsEmpty()) // got an empty bucket
                    {
                        bitmap[curBucketId] = true;
                        bfsTree.push_back(TreeNode(firstBucketId, idx, inBlockId));
                        return CuckooKick(bfsTree, curBucketId - firstBucketId);
                    }
                    if (!bitmap[curBucketId]) // got a non-empty & unreached bucket
                    {
                        Bucket evictedBucket = curBucket;
                        bitmap[curBucketId] = true;
                        bfsTree.push_back(TreeNode(firstBucketId, idx, inBlockId));
                        Bucket* retBucket = CuckooKick(bfsTree, curBucketId - firstBucketId);
                        *retBucket = evictedBucket;
                        return retBucket;
                    }
                }
                if (bfsTree.size() < MAX_NUM_BFS_TREE_NODE) {
                    bfsTree.push_back(TreeNode(firstBucketId, idx, inBlockId));
                }
            }
        }
        ++idx;
    }
    AUTIL_LOG(ERROR, "can not find a bucket");
    return NULL;
}

template <typename _KT, typename _VT, bool HasSpecialKey, bool useCompactBucket>
inline uint64_t CuckooHashTableBase<_KT, _VT, HasSpecialKey, useCompactBucket>::MemoryUse() const
{
    return _bucketCount * sizeof(Bucket) + sizeof(HashTableHeader);
}

template <typename _KT, typename _VT, bool HasSpecialKey, bool useCompactBucket>
inline uint64_t CuckooHashTableBase<_KT, _VT, HasSpecialKey, useCompactBucket>::BuildAssistantMemoryUse() const
{
    size_t callIdSize = _callIdVec.size() * sizeof(uint32_t);
    size_t bfsTreeSize = std::min(_blockCount, (size_t)MAX_NUM_BFS_TREE_NODE) * sizeof(TreeNode);
    return callIdSize + bfsTreeSize;
}

template <typename _KT, typename _VT, bool HasSpecialKey, bool useCompactBucket>
inline typename CuckooHashTableBase<_KT, _VT, HasSpecialKey, useCompactBucket>::Bucket*
CuckooHashTableBase<_KT, _VT, HasSpecialKey, useCompactBucket>::InternalFindBucket(const _KT& key)
{
    while (true) {
        Bucket* retBucket = DoInternalFindBucket(key);
        if (retBucket) {
            return retBucket;
        }
        if (_nu_hashFunc >= Header()->maxNu_hashFunc) {
            break;
        }
        ++_nu_hashFunc;
        ++(Header()->nu_hashFunc);
        AUTIL_LOG(WARN, "Increase nu_hashFunc to [%u], size[%lu], bucketCount[%lu]", _nu_hashFunc, Size(),
                  _bucketCount);
    }
    AUTIL_LOG(ERROR, "can not find bucket to store key[%lu] nu_hashFunc[%u]", (uint64_t)key, _nu_hashFunc);
    return NULL;
}

template <typename _KT, typename _VT, bool HasSpecialKey, bool useCompactBucket>
inline typename CuckooHashTableBase<_KT, _VT, HasSpecialKey, useCompactBucket>::Bucket*
CuckooHashTableBase<_KT, _VT, HasSpecialKey, useCompactBucket>::DoInternalFindBucket(const _KT& key)
{
    Bucket* retBucket = NULL;
    TreeNodeVec bfsTree;
    for (uint32_t hashFuncId = 0; hashFuncId < _nu_hashFunc; ++hashFuncId) {
        const _KT& hash = CuckooHash(key, hashFuncId);
        uint64_t firstBucketId = GetFirstBucketIdInBlock(hash, _blockCount);
        Bucket* curBucket = &(_bucket[firstBucketId]);
        for (uint32_t inBlockId = 0; inBlockId < BLOCK_SIZE; ++inBlockId) {
            if (curBucket->IsEqual(key)) // same key existed
            {
                return curBucket;
            }
            if (curBucket->IsEmpty() && !retBucket) // got an empty bucket
            {
                retBucket = curBucket;
            }
            ++curBucket;
        }
        bfsTree.push_back(TreeNode(firstBucketId, TreeNode::BFS_ROOT_IDX, 0));
    }
    if (retBucket) {
        return retBucket;
    }
    return BFSFindBucket(bfsTree);
}

template <typename _KT, typename _VT, bool HasSpecialKey, bool useCompactBucket>
inline const typename CuckooHashTableBase<_KT, _VT, HasSpecialKey, useCompactBucket>::Bucket*
CuckooHashTableBase<_KT, _VT, HasSpecialKey, useCompactBucket>::FindBucketForRead(const _KT& key,
                                                                                  uint8_t nu_hashFunc) const
{
    uint64_t bucketId = GetFirstBucketIdInBlock(CuckooHash(key, 0), _blockCount);
    __builtin_prefetch(&(_bucket[bucketId]), 0, 1);
    const _KT& hash = CuckooHash(key, 1);
    uint64_t nextId = GetFirstBucketIdInBlock(hash, _blockCount);
    __builtin_prefetch(&(_bucket[nextId]), 0, 1);
    for (uint32_t inBlockId = 0; inBlockId < BLOCK_SIZE; ++inBlockId) { // line 1, hash = key
        const Bucket& curBucket = _bucket[bucketId + inBlockId];
        if (curBucket.IsEmpty()) {
            return NULL; // or continue; for read-write
        } else if (curBucket.IsEqual(key)) {
            return &curBucket;
        }
    }
    for (uint32_t inBlockId = 0; inBlockId < BLOCK_SIZE; ++inBlockId) { // line 2, hash = func1
        const Bucket& curBucket = _bucket[nextId + inBlockId];
        if (curBucket.IsEmpty()) {
            return NULL; // or continue; for read-write
        } else if (curBucket.IsEqual(key)) {
            return &curBucket;
        }
    }
    for (uint32_t hashFuncId = 2; hashFuncId < nu_hashFunc;
         ++hashFuncId) { // line 1,2 put out of this loop because of performance
        const _KT& hash = CuckooHash(key, hashFuncId);
        uint64_t bucketId = GetFirstBucketIdInBlock(hash, _blockCount);
        for (uint32_t inBlockId = 0; inBlockId < BLOCK_SIZE; ++inBlockId) {
            const Bucket& curBucket = _bucket[bucketId + inBlockId];
            if (curBucket.IsEmpty()) {
                return NULL; // or continue; for read-write
            } else if (curBucket.IsEqual(key)) {
                return &curBucket;
            }
        }
    }
    return NULL;
}

template <typename _KT, typename _VT, bool HasSpecialKey, bool useCompactBucket>
template <typename functor>
inline bool CuckooHashTableBase<_KT, _VT, HasSpecialKey, useCompactBucket>::InternalInsert(const _KT key,
                                                                                           const _VT& value)
{
    Bucket* bucket = InternalFindBucket(key);
    if (unlikely(!bucket)) {
        return false;
    }
    bool isNewKey = bucket->IsEmpty();
    functor()(*bucket, key, value, _deleteCount);
    if (isNewKey) {
        ++(Header()->keyCount);
    }
    return true;
}

template <typename _KT, typename _VT, bool HasSpecialKey, bool useCompactBucket>
inline typename CuckooHashTableBase<_KT, _VT, HasSpecialKey, useCompactBucket>::Bucket*
CuckooHashTableBase<_KT, _VT, HasSpecialKey, useCompactBucket>::BFSFindBucket(TreeNodeVec& bfsTree)
{
    ++_curCallId;
    for (uint32_t i = 0; i < bfsTree.size(); ++i) {
        _callIdVec[bfsTree[i]._bucketId / BLOCK_SIZE] = _curCallId;
    }
    uint64_t idx = 0;
    uint64_t nextDepthBeginIdx = 0;
    uint32_t nextDepth = 0;
    while (idx < bfsTree.size()) {
        if (idx == nextDepthBeginIdx) {
            ++nextDepth;
            if (nextDepth > _bFSDepth) // got all nodes with Depth = 0, 1, 2, ..., _dfsDepth
            {
                break;
            }
            nextDepthBeginIdx = bfsTree.size();
        }

        uint64_t bucketId = bfsTree[idx]._bucketId;
        for (uint32_t inBlockId = 0; inBlockId < BLOCK_SIZE; ++inBlockId) {
            const _KT& key = _bucket[bucketId + inBlockId].Key();
            for (uint32_t hashFuncId = 0; hashFuncId < _nu_hashFunc; ++hashFuncId) {
                const _KT& hash = CuckooHash(key, hashFuncId);
                uint64_t firstBucketId = GetFirstBucketIdInBlock(hash, _blockCount);

                if (_callIdVec[firstBucketId / BLOCK_SIZE] == _curCallId) {
                    continue;
                }
                if (bfsTree.size() >= MAX_NUM_BFS_TREE_NODE) {
                    AUTIL_LOG(WARN, "too many[%lu] node in bfs tree more than [%lu], skip it", bfsTree.size(),
                              MAX_NUM_BFS_TREE_NODE);
                    continue;
                }
                _callIdVec[firstBucketId / BLOCK_SIZE] = _curCallId;
                bfsTree.push_back(TreeNode(firstBucketId, idx, inBlockId));

                Bucket* curBucket = &(_bucket[firstBucketId]);
                for (uint32_t inCurBlockId = 0; inCurBlockId < BLOCK_SIZE; ++inCurBlockId, ++curBucket) {
                    if (curBucket->IsEmpty()) {
                        return CuckooKick(bfsTree, inCurBlockId);
                    }
                }
            }
        }
        ++idx;
    }
    AUTIL_LOG(ERROR, "can not find a bucket");
    return NULL;
}

template <typename _KT, typename _VT, bool HasSpecialKey, bool useCompactBucket>
inline typename CuckooHashTableBase<_KT, _VT, HasSpecialKey, useCompactBucket>::Bucket*
CuckooHashTableBase<_KT, _VT, HasSpecialKey, useCompactBucket>::CuckooKick(TreeNodeVec& bfsTree,
                                                                           uint32_t emptyInBlockId)
{
    uint64_t emptyIdx = bfsTree.size() - 1;
    uint64_t prevIdx = bfsTree[emptyIdx]._parent;
    while (prevIdx != TreeNode::BFS_ROOT_IDX) {
        uint8_t prevInBlockId = bfsTree[emptyIdx]._inBlockId;
        Bucket& emptyBucket = _bucket[bfsTree[emptyIdx]._bucketId + emptyInBlockId];
        Bucket& prevBucket = _bucket[bfsTree[prevIdx]._bucketId + prevInBlockId];
        assert(CheckBucket(prevBucket.Key(), bfsTree[emptyIdx]._bucketId + emptyInBlockId));
        emptyBucket.Set(prevBucket);
        prevBucket.SetEmpty();

        emptyInBlockId = prevInBlockId;
        emptyIdx = prevIdx;
        prevIdx = bfsTree[emptyIdx]._parent;
    }
    return &(_bucket[bfsTree[emptyIdx]._bucketId + emptyInBlockId]);
}

template <typename _KT, typename _VT, bool HasSpecialKey, bool useCompactBucket>
inline _KT CuckooHashTableBase<_KT, _VT, HasSpecialKey, useCompactBucket>::CuckooHash(const _KT& key,
                                                                                      uint32_t hashFuncId)
{
    // too slow when use sharding
    // if (hashFuncId == 0) {
    //     return key;
    // }
    static const uint64_t CUCKOO_MURMUR_SEED_MULTIPLIER = 816922183UL;
    return autil::MurmurHash::MurmurHash64A(&key, sizeof(key), CUCKOO_MURMUR_SEED_MULTIPLIER * hashFuncId);
}

///////////////////////////////////////////////////////////////////////////////
template <typename _KT, typename _VT, bool HasSpecialKey = ClosedHashTableTraits<_KT, _VT, false>::HasSpecialKey,
          bool useCompactBucket = false>
class CuckooHashTable final : public CuckooHashTableBase<_KT, _VT, HasSpecialKey, useCompactBucket>
{
public:
    typedef CuckooHashTableBase<_KT, _VT, HasSpecialKey, useCompactBucket> Base;
    using Base::Find;
    using Base::OCCUPANCY_PCT;

public:
    indexlib::util::Status Find(uint64_t key, autil::StringView& value) const override final
    {
        const _VT* typedValuePtr = NULL;
        auto status = Find((_KT)key, typedValuePtr);
        value = {(char*)typedValuePtr, sizeof(_VT)};
        return status;
    }
};

///////////////////////////////////////////////////////////////////////////

// hide some methods
template <typename _KT, typename _VT, bool useCompactBucket>
class CuckooHashTable<_KT, _VT, true, useCompactBucket> final
    : public CuckooHashTableBase<_KT, _VT, false, useCompactBucket>
{
public:
    typedef typename ClosedHashTableTraits<_KT, _VT, useCompactBucket>::Bucket Bucket;
    typedef typename ClosedHashTableTraits<_KT, _VT, useCompactBucket>::SpecialBucket SpecialBucket;

private:
    typedef CuckooHashTableBase<_KT, _VT, false, useCompactBucket> Base;
    using Base::_bucket;
    using Base::_bucketCount;
    using Base::_logger;
    using Base::FindForReadWrite;
    using Base::OCCUPANCY_PCT;

public:
    // public for CuckooHashTableFileIterator & CuckooHashTableFileReader
    typedef typename Base::HashTableHeader HashTableHeader;

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
    size_t BuildMemoryToTableMemory(size_t buildMemory, int32_t occupancyPct) const override
    {
        return DoBuildMemoryToTableMemory(buildMemory, occupancyPct);
    }

public:
    static uint64_t DoCapacityToTableMemory(uint64_t maxKeyCount, const HashTableOptions& options)
    {
        return Base::DoCapacityToTableMemory(maxKeyCount, options) + sizeof(SpecialBucket) * 2;
    }
    static uint64_t DoCapacityToBuildMemory(uint64_t maxKeyCount, const HashTableOptions& options)
    {
        return Base::DoCapacityToBuildMemory(maxKeyCount, options) + sizeof(SpecialBucket) * 2;
    }
    static size_t DoTableMemroyToCapacity(size_t tableMemory, int32_t occupancyPct)
    {
        return Base::DoTableMemroyToCapacity(tableMemory - sizeof(SpecialBucket) * 2, occupancyPct);
    }
    static size_t DoBuildMemoryToCapacity(size_t buildMemory, int32_t occupancyPct)
    {
        return Base::DoBuildMemoryToCapacity(buildMemory - sizeof(SpecialBucket) * 2, occupancyPct);
    }
    static size_t DoBuildMemoryToTableMemory(size_t buildMemory, int32_t occupancyPct)
    {
        size_t maxKeyCount = DoBuildMemoryToCapacity(buildMemory, occupancyPct);
        return DoCapacityToTableMemory(maxKeyCount, occupancyPct);
    }

public:
    indexlib::util::Status Find(uint64_t key, autil::StringView& value) const override final
    {
        const _VT* typedValuePtr = NULL;
        auto status = Find((_KT)key, typedValuePtr);
        value = {(char*)typedValuePtr, sizeof(_VT)};
        return status;
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
    bool Insert(uint64_t key, const autil::StringView& value) override
    {
        const _VT& v = *reinterpret_cast<const _VT*>(value.data());
        assert(sizeof(_VT) == value.size());
        return Insert(key, v);
    }

    bool Delete(uint64_t key, const autil::StringView& value = autil::StringView()) override
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

    bool MountForWrite(void* data, size_t size, const HashTableOptions& inputOptions = OCCUPANCY_PCT) override
    {
        HashTableOptions options(OCCUPANCY_PCT);
        if (inputOptions.Valid()) {
            options = inputOptions;
        }
        size_t minSize = sizeof(HashTableHeader) + sizeof(SpecialBucket) * 2;
        if (size < minSize) {
            AUTIL_LOG(ERROR, "not enough space, min size[%lu], give[%lu]", minSize, size);
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
            AUTIL_LOG(ERROR, "too small size[%lu], BucketSize[%lu]", size, sizeof(SpecialBucket));
            return false;
        }
        return Base::MountForRead(data, size - sizeof(SpecialBucket) * 2);
    }

    indexlib::util::Status Find(const _KT& key, const _VT*& value) const
    {
        if (likely(!Bucket::IsEmptyKey(key) && !Bucket::IsDeleteKey(key))) {
            return Base::Find(key, value);
        }

        SpecialBucket* bucket = Bucket::IsEmptyKey(key) ? EmptyBucket() : DeleteBucket();
        if (bucket->IsEmpty()) {
            return indexlib::util::NOT_FOUND;
        }
        value = &(bucket->Value());
        return bucket->IsDeleted() ? indexlib::util::DELETED : indexlib::util::OK;
    }

    indexlib::util::Status FindForReadWrite(const _KT& key, _VT& value) const
    {
        if (likely(!Bucket::IsEmptyKey(key) && !Bucket::IsDeleteKey(key))) {
            return Base::FindForReadWrite(key, value);
        }

        SpecialBucket* bucket = Bucket::IsEmptyKey(key) ? EmptyBucket() : DeleteBucket();
        if (bucket->IsEmpty()) {
            return indexlib::util::NOT_FOUND;
        }
        value = bucket->Value();
        return bucket->IsDeleted() ? indexlib::util::DELETED : indexlib::util::OK;
    }

    uint64_t MemoryUse() const override { return Base::MemoryUse() + sizeof(SpecialBucket) * 2; }

    bool Shrink(int32_t occupancyPct = 0) override
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
    bool Stretch() override
    {
        SpecialBucket emptyBucket = *EmptyBucket();
        SpecialBucket deleteBucket = *DeleteBucket();
        bool ret = Base::Stretch();
        *EmptyBucket() = emptyBucket;
        *DeleteBucket() = deleteBucket;
        return ret;
    }

protected:
    SpecialBucket* EmptyBucket() const { return reinterpret_cast<SpecialBucket*>(&_bucket[_bucketCount]); }

    SpecialBucket* DeleteBucket() const { return reinterpret_cast<SpecialBucket*>(&_bucket[_bucketCount]) + 1; }

    template <typename functor>
    bool InternalInsert(const _KT& key, const _VT& value)
    {
        bool isNewKey = false;
        if (unlikely(Bucket::IsEmptyKey(key))) {
            SpecialBucket* bucket = EmptyBucket();
            isNewKey = bucket->IsEmpty();
            functor()(*bucket, key, value, Base::_deleteCount);
        } else if (unlikely(Bucket::IsDeleteKey(key))) {
            SpecialBucket* bucket = DeleteBucket();
            isNewKey = bucket->IsEmpty();
            functor()(*bucket, key, value, Base::_deleteCount);
        } else {
            Bucket* bucket = Base::InternalFindBucket(key);
            if (unlikely(!bucket)) {
                return false;
            }
            isNewKey = bucket->IsEmpty();
            functor()(*bucket, key, value, Base::_deleteCount);
        }
        if (isNewKey) {
            ++(Base::Header()->keyCount);
        }
        return true;
    }

private:
    friend class CuckooHashTableTest;
};
} // namespace indexlibv2::index
