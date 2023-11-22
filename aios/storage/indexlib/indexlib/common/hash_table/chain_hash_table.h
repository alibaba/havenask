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

#include "indexlib/common/hash_table/closed_hash_table_iterator.h"
#include "indexlib/common/hash_table/closed_hash_table_traits.h"
#include "indexlib/common/hash_table/hash_table_options.h"
#include "indexlib/common/hash_table/special_key_bucket.h"
#include "indexlib/common_define.h"
#include "indexlib/util/Status.h"

namespace indexlib { namespace common {

template <typename _KT, typename _VT>
class ChainHashTable
{
public:
    ChainHashTable() : mBucket(NULL), mItem(NULL), mBucketCount(0), mItemCount(0), mKeyCount(0), mRatio(0) {}
    ~ChainHashTable() {}

public:
    typedef _KT KeyType;
    typedef _VT ValueType;
    struct HashTableHeader {
        uint64_t itemCount = 0;
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
    util::Status Find(const _KT& key, const _VT*& value) const;
    util::Status FindForReadWrite(const _KT& key, _VT& value) const;
    util::Status InternalFind(const _KT& key, const _VT*& value) const;
    uint64_t Size() const { return Header()->keyCount; }
    uint64_t Capacity() const { return mBucketCount; }
    bool IsFull() const { return Size() >= Capacity(); }
    uint64_t BucketCount() const { return mBucketCount; }
    uint64_t MemoryUse() const;
    bool Finish();
    bool Shrink(int32_t occupancyPct = 0);
    void* Address() const { return Header(); }

protected:
    HashTableHeader* Header() const { return (HashTableHeader*)((uint8_t*)mItem - sizeof(HashTableHeader)); }
    Bucket* InternalFindBucket(const _KT& key);
    Bucket* DoInternalFindBucket(const _KT& key);

protected:
    Bucket* mBucket;
    Item* mItem;
    uint64_t mBucketCount;
    uint64_t mItemCount;
    uint64_t mKeyCount;
    int32_t mRatio;
    std::vector<std::vector<std::pair<_KT, _VT>>> mVecTable;

protected:
    IE_LOG_DECLARE();
};

///////////////////////////////////////////
IE_LOG_SETUP_TEMPLATE2_CUSTOM(typename, typename, common, ChainHashTable);

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
        IE_LOG(ERROR, "not enough space, header size[%lu], give[%lu]", sizeof(HashTableHeader), size);
        return false;
    }
    // bucketcount = itemcount * ntopRatio / 100
    // size = sizeof(header) +
    // (sizeof(item) * itemcount) + (sizeof(bucket) * bucketcount)

    // put these two equation together, then we got:
    // 100 * (size - sizeof(header) =
    // (100 * sizeof(item) + sizeof(bucket) * ntopRatio) * itemcount

    // size_t itemAndBucketSize = size - sizeof(HashTableHeader);
    // size_t factor = 100 * sizeof(Item) + sizeof(Bucket) * ntopRatio;

    // mItemCount = 100 * itemAndBucketSize / factor;
    // mBucketCount = (itemAndBucketSize  - sizeof(Item) * mItemCount)
    //                / sizeof(Bucket);
    // mRatio = ntopRatio;

    size_t bucketSize = (size - sizeof(HashTableHeader)) * occupancyPct / 100;
    mBucketCount = bucketSize / sizeof(Bucket);
    size_t itemSize = size - sizeof(HashTableHeader) - bucketSize;
    mItemCount = itemSize / sizeof(Item);
    mRatio = occupancyPct;

    if (!data) {
        IE_LOG(ERROR, "data is null");
        return false;
    }
    HashTableHeader* header = (HashTableHeader*)data;
    header->itemCount = mItemCount;
    header->bucketCount = mBucketCount;
    header->keyCount = 0;

    mItem = (Item*)(header + 1);
    memset(mItem, 0, sizeof(Item) * mItemCount);
    mBucket = (Bucket*)(mItem + mItemCount);
    new (mBucket) Bucket[mBucketCount];
    mVecTable.resize(mItemCount);

    IE_LOG(INFO, "mount for write, size[%lu], ntopRatio[%d], itemCount[%lu], bucketCount[%lu], mBucket[%lu]", size,
           mRatio, mItemCount, mBucketCount, (uint64_t)mBucket);
    return true;
}

template <typename _KT, typename _VT>
inline bool ChainHashTable<_KT, _VT>::MountForRead(const void* data, size_t size)
{
    assert(data);
    if (size < sizeof(HashTableHeader)) {
        IE_LOG(ERROR, "too small size[%lu], header size[%lu]", size, sizeof(HashTableHeader));
        return false;
    }
    HashTableHeader* header = (HashTableHeader*)data;
    mItemCount = header->itemCount;
    ;
    mBucketCount = header->bucketCount;
    mKeyCount = header->keyCount;

    size_t totSize = sizeof(HashTableHeader) + mItemCount * sizeof(Item) + mBucketCount * sizeof(Bucket);
    if (size < totSize) {
        IE_LOG(ERROR, "too small size[%lu], header size[%lu], item size[%lu], bucket size[%lu]", size,
               sizeof(HashTableHeader), mItemCount * sizeof(Item), mBucketCount * sizeof(Bucket));
        return false;
    }

    mItem = (Item*)(header + 1);
    mBucket = (Bucket*)(mItem + mItemCount);
    return true;
}

template <typename _KT, typename _VT>
inline bool ChainHashTable<_KT, _VT>::Insert(const _KT& key, const _VT& value)
{
    uint64_t itemId = key % mItemCount;
    mVecTable[itemId].push_back(std::make_pair(key, value));
    return true;
}

template <typename _KT, typename _VT>
inline bool ChainHashTable<_KT, _VT>::Delete(const _KT& key, const _VT& value)
{
    return true;
}

template <typename _KT, typename _VT>
inline util::Status ChainHashTable<_KT, _VT>::Find(const _KT& key, const _VT*& value) const
{
    return InternalFind(key, value);
}

template <typename _KT, typename _VT>
inline util::Status ChainHashTable<_KT, _VT>::FindForReadWrite(const _KT& key, _VT& value) const
{
    const _VT* valuePtr = NULL;
    util::Status status = InternalFind(key, valuePtr);
    if (status != util::NOT_FOUND) {
        assert(valuePtr);
        value = *valuePtr;
    }
    return status;
}

template <typename _KT, typename _VT>
inline util::Status ChainHashTable<_KT, _VT>::InternalFind(const _KT& key, const _VT*& value) const
{
    uint64_t itemId = key % mItemCount;
    uint64_t bucketTot = mItem[itemId].count;
    Bucket* curBucket = &mBucket[mItem[itemId].offset];
    for (uint64_t i = 0; i < bucketTot; ++i) {
        if (curBucket->IsEqual(key)) // found it or deleted
        {
            value = &(curBucket->Value());
            return curBucket->IsDeleted() ? util::DELETED : util::OK;
        }
        ++curBucket;
    }
    return util::NOT_FOUND;
}

template <typename _KT, typename _VT>
inline bool ChainHashTable<_KT, _VT>::Finish()
{
    uint64_t BucketId = 0;
    for (uint64_t i = 0; i < mItemCount; ++i) {
        mItem[i].offset = BucketId;
        mItem[i].count = mVecTable[i].size();
        if (BucketId + mItem[i].count > mBucketCount) {
            return false;
        }
        for (uint64_t j = 0; j < mItem[i].count; ++j) {
            mBucket[BucketId].Set(mVecTable[i][j].first, mVecTable[i][j].second);
            ++BucketId;
            ++mKeyCount;
        }
    }
    mVecTable.clear();
    Header()->keyCount = mKeyCount;
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
    return mItemCount * sizeof(Item) + mBucketCount * sizeof(Bucket) + sizeof(HashTableHeader);
}
}} // namespace indexlib::common
