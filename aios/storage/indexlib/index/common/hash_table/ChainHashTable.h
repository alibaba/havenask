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

#include "autil/Log.h"
#include "indexlib/index/common/hash_table/ClosedHashTableIterator.h"
#include "indexlib/index/common/hash_table/ClosedHashTableTraits.h"
#include "indexlib/index/common/hash_table/HashTableOptions.h"
#include "indexlib/index/common/hash_table/SpecialKeyBucket.h"
#include "indexlib/util/Status.h"

namespace indexlibv2::index {

template <typename _KT, typename _VT>
class ChainHashTable
{
public:
    ChainHashTable() : _bucket(NULL), _item(NULL), _bucketCount(0), _ite_count(0), _keyCount(0), _ratio(0) {}
    ~ChainHashTable() {}

public:
    typedef _KT KeyType;
    typedef _VT ValueType;
    struct HashTableHeader {
        uint64_t ite_count = 0;
        uint64_t bucketCount = 0;
        uint64_t keyCount = 0;
        uint64_t padding = 0;
    };
    struct Item {
        uint64_t offset : 40;
        uint64_t count  : 24;
    };
    static const int32_t RATIO = 80;

private:
    typedef typename ClosedHashTableTraits<_KT, _VT, false>::Bucket Bucket;

public:
    static uint64_t CapacityToBucketCount(uint64_t maxKeyCount);
    static uint64_t CapacityToTableMemory(uint64_t maxKeyCount, const HashTableOptions& options);
    static uint64_t CapacityToBuildMemory(uint64_t maxKeyCount, const HashTableOptions& options);
    static size_t TableMemroyToCapacity(size_t memoryUse, int32_t occupancyPct);
    bool MountForWrite(void* data, size_t size, const int32_t occupancyPct);
    bool MountForRead(const void* data, size_t size);
    bool Insert(const _KT& key, const _VT& value);
    bool Delete(const _KT& key, const _VT& value = _VT());
    indexlib::util::Status Find(const _KT& key, const _VT*& value) const;
    indexlib::util::Status FindForReadWrite(const _KT& key, _VT& value) const;
    indexlib::util::Status InternalFind(const _KT& key, const _VT*& value) const;
    uint64_t Size() const { return Header()->keyCount; }
    uint64_t Capacity() const { return _bucketCount; }
    bool IsFull() const { return Size() >= Capacity(); }
    uint64_t BucketCount() const { return _bucketCount; }
    uint64_t MemoryUse() const;
    bool Finish();
    bool Shrink(int32_t occupancyPct = 0);
    void* Address() const { return Header(); }

protected:
    HashTableHeader* Header() const { return (HashTableHeader*)((uint8_t*)_item - sizeof(HashTableHeader)); }
    Bucket* InternalFindBucket(const _KT& key);
    Bucket* DoInternalFindBucket(const _KT& key);

protected:
    Bucket* _bucket;
    Item* _item;
    uint64_t _bucketCount;
    uint64_t _ite_count;
    uint64_t _keyCount;
    int32_t _ratio;
    std::vector<std::vector<std::pair<_KT, _VT>>> _vecTable;

protected:
    AUTIL_LOG_DECLARE();
};

///////////////////////////////////////////
AUTIL_LOG_SETUP_TEMPLATE_2(indexlib.index, ChainHashTable, _KT, _VT);

template <typename _KT, typename _VT>
inline uint64_t ChainHashTable<_KT, _VT>::CapacityToTableMemory(uint64_t maxKeyCount, const HashTableOptions& options)
{
    return 0;
}

template <typename _KT, typename _VT>
inline size_t ChainHashTable<_KT, _VT>::TableMemroyToCapacity(size_t memoryUse, int32_t occupancyPct)
{
    return 0;
}

template <typename _KT, typename _VT>
inline uint64_t ChainHashTable<_KT, _VT>::CapacityToBucketCount(uint64_t maxKeyCount)
{
    return 0;
}

template <typename _KT, typename _VT>
inline bool ChainHashTable<_KT, _VT>::MountForWrite(void* data, size_t size, const int32_t occupancyPct)
{
    if (size < sizeof(HashTableHeader)) {
        AUTIL_LOG(ERROR, "not enough space, header size[%lu], give[%lu]", sizeof(HashTableHeader), size);
        return false;
    }
    // bucketcount = itemcount * ntopRatio / 100
    // size = sizeof(header) +
    // (sizeof(item) * itemcount) + (sizeof(bucket) * bucketcount)

    // put these two equation together, then we got:
    // 100 * (size - sizeof(header) =
    // (100 * sizeof(item) + sizeof(bucket) * ntopRatio) * itemcount

    // size_t ite_andBucketSize = size - sizeof(HashTableHeader);
    // size_t factor = 100 * sizeof(Item) + sizeof(Bucket) * ntopRatio;

    // _ite_count = 100 * ite_andBucketSize / factor;
    // _bucketCount = (ite_andBucketSize  - sizeof(Item) * _ite_count)
    //                / sizeof(Bucket);
    // _ratio = ntopRatio;

    size_t bucketSize = (size - sizeof(HashTableHeader)) * occupancyPct / 100;
    _bucketCount = bucketSize / sizeof(Bucket);
    size_t ite_size = size - sizeof(HashTableHeader) - bucketSize;
    _ite_count = ite_size / sizeof(Item);
    _ratio = occupancyPct;

    if (!data) {
        AUTIL_LOG(ERROR, "data is null");
        return false;
    }
    HashTableHeader* header = (HashTableHeader*)data;
    header->ite_count = _ite_count;
    header->bucketCount = _bucketCount;
    header->keyCount = 0;

    _item = (Item*)(header + 1);
    memset(_item, 0, sizeof(Item) * _ite_count);
    _bucket = (Bucket*)(_item + _ite_count);
    new (_bucket) Bucket[_bucketCount];
    _vecTable.resize(_ite_count);

    AUTIL_LOG(INFO, "mount for write, size[%lu], ntopRatio[%d], ite_count[%lu], bucketCount[%lu], _bucket[%lu]", size,
              _ratio, _ite_count, _bucketCount, (uint64_t)_bucket);
    return true;
}

template <typename _KT, typename _VT>
inline bool ChainHashTable<_KT, _VT>::MountForRead(const void* data, size_t size)
{
    assert(data);
    if (size < sizeof(HashTableHeader)) {
        AUTIL_LOG(ERROR, "too small size[%lu], header size[%lu]", size, sizeof(HashTableHeader));
        return false;
    }
    HashTableHeader* header = (HashTableHeader*)data;
    _ite_count = header->ite_count;
    ;
    _bucketCount = header->bucketCount;
    _keyCount = header->keyCount;

    size_t totSize = sizeof(HashTableHeader) + _ite_count * sizeof(Item) + _bucketCount * sizeof(Bucket);
    if (size < totSize) {
        AUTIL_LOG(ERROR, "too small size[%lu], header size[%lu], item size[%lu], bucket size[%lu]", size,
                  sizeof(HashTableHeader), _ite_count * sizeof(Item), _bucketCount * sizeof(Bucket));
        return false;
    }

    _item = (Item*)(header + 1);
    _bucket = (Bucket*)(_item + _ite_count);
    return true;
}

template <typename _KT, typename _VT>
inline bool ChainHashTable<_KT, _VT>::Insert(const _KT& key, const _VT& value)
{
    uint64_t ite_id = key % _ite_count;
    _vecTable[ite_id].push_back(std::make_pair(key, value));
    return true;
}

template <typename _KT, typename _VT>
inline bool ChainHashTable<_KT, _VT>::Delete(const _KT& key, const _VT& value)
{
    return true;
}

template <typename _KT, typename _VT>
inline indexlib::util::Status ChainHashTable<_KT, _VT>::Find(const _KT& key, const _VT*& value) const
{
    return InternalFind(key, value);
}

template <typename _KT, typename _VT>
inline indexlib::util::Status ChainHashTable<_KT, _VT>::FindForReadWrite(const _KT& key, _VT& value) const
{
    const _VT* valuePtr = NULL;
    indexlib::util::Status status = InternalFind(key, valuePtr);
    if (status != indexlib::util::NOT_FOUND) {
        assert(valuePtr);
        value = *valuePtr;
    }
    return status;
}

template <typename _KT, typename _VT>
inline indexlib::util::Status ChainHashTable<_KT, _VT>::InternalFind(const _KT& key, const _VT*& value) const
{
    uint64_t ite_id = key % _ite_count;
    uint64_t bucketTot = _item[ite_id].count;
    Bucket* curBucket = &_bucket[_item[ite_id].offset];
    for (uint64_t i = 0; i < bucketTot; ++i) {
        if (curBucket->IsEqual(key)) // found it or deleted
        {
            value = &(curBucket->Value());
            return curBucket->IsDeleted() ? indexlib::util::DELETED : indexlib::util::OK;
        }
        ++curBucket;
    }
    return indexlib::util::NOT_FOUND;
}

template <typename _KT, typename _VT>
inline bool ChainHashTable<_KT, _VT>::Finish()
{
    uint64_t BucketId = 0;
    for (uint64_t i = 0; i < _ite_count; ++i) {
        _item[i].offset = BucketId;
        _item[i].count = _vecTable[i].size();
        if (BucketId + _item[i].count > _bucketCount) {
            return false;
        }
        for (uint64_t j = 0; j < _item[i].count; ++j) {
            _bucket[BucketId].Set(_vecTable[i][j].first, _vecTable[i][j].second);
            ++BucketId;
            ++_keyCount;
        }
    }
    _vecTable.clear();
    Header()->keyCount = _keyCount;
    return true;
}

template <typename _KT, typename _VT>
inline bool ChainHashTable<_KT, _VT>::Shrink(int32_t occupancyPct)
{
    return true;
}

template <typename _KT, typename _VT>
inline uint64_t ChainHashTable<_KT, _VT>::MemoryUse() const
{
    return _ite_count * sizeof(Item) + _bucketCount * sizeof(Bucket) + sizeof(HashTableHeader);
}
} // namespace indexlibv2::index
