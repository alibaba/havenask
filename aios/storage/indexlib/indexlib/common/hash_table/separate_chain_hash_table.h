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

#include <memory>

#include "indexlib/common/hash_table/hash_table_writer.h"
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/MMapVector.h"
#include "indexlib/util/MmapPool.h"

namespace indexlib { namespace common {

template <typename KeyType, typename ValueType>
class SeparateChainHashTable
{
public:
    static const uint32_t INVALID_KEY_OFFSET = std::numeric_limits<uint32_t>::max();
    struct KeyNode {
        KeyNode(KeyType _key, ValueType _value, uint32_t _next = INVALID_KEY_OFFSET)
            : key(_key)
            , value(_value)
            , next(_next)
        {
        }
        KeyType key;
        ValueType value;
        uint32_t next;
    };
    typedef std::shared_ptr<util::MMapVector<KeyNode>> KeyNodeVectorPtr;

    class HashTableIterator
    {
    public:
        HashTableIterator(const KeyNodeVectorPtr& keyNodes) : mCursor(0), mKeyNodes(keyNodes) { assert(keyNodes); }

    public:
        bool IsValid() const { return mCursor < mKeyNodes->Size(); }

        void MoveToNext() { mCursor++; }

        void Reset() { mCursor = 0; }

        KeyNode* GetCurrentKeyNode() const
        {
            assert(IsValid());
            return &(*mKeyNodes)[mCursor];
        }

        const KeyType& Key() const { return GetCurrentKeyNode()->key; }

        const ValueType& Value() const { return GetCurrentKeyNode()->value; }

        void SortByKey() // sort by key
        {
            assert(mCursor == 0);
            KeyNode* begin = &(*mKeyNodes)[0];
            KeyNode* end = begin + mKeyNodes->Size();
            std::sort(begin, end, Comp);
        }

        size_t Size() const { return mKeyNodes->Size(); }

    private:
        static bool Comp(const KeyNode& left, KeyNode& right) { return left.key < right.key; }

    private:
        size_t mCursor;
        KeyNodeVectorPtr mKeyNodes;
    };

public:
    SeparateChainHashTable() : mBucket(NULL), mBucketCount(0) {}
    ~SeparateChainHashTable()
    {
        if (mBucket) {
            mPool.deallocate((void*)mBucket, mBucketCount * sizeof(uint32_t));
        }
    }

    void Init(uint32_t bucketCount, size_t reserveSize)
    {
        mBucketCount = bucketCount;
        size_t bucketSize = bucketCount * sizeof(uint32_t);
        mBucket = (uint32_t*)mPool.allocate(bucketSize);
        memset(mBucket, 0xFF, bucketSize);
        mKeyNodes.reset(new util::MMapVector<KeyNode>());
        mKeyNodes->Init(util::MMapAllocatorPtr(new util::MMapAllocator), reserveSize / sizeof(KeyNode) + 1);
    }

    HashTableIterator* CreateKeyNodeIterator() { return new HashTableIterator(mKeyNodes); }

    size_t GetUsedBytes() const { return mBucketCount * sizeof(uint32_t) + mKeyNodes->GetUsedBytes(); }

public:
    KeyNode* Find(const KeyType& key);

    void Insert(const KeyType& key, const ValueType& value);
    size_t Size() const { return mKeyNodes->Size(); }

private:
    util::MmapPool mPool;
    uint32_t* mBucket;
    uint32_t mBucketCount;
    KeyNodeVectorPtr mKeyNodes;

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP_TEMPLATE2(common, SeparateChainHashTable);
///////////////////////////////////////////////////////////////

template <typename KeyType, typename ValueType>
inline typename SeparateChainHashTable<KeyType, ValueType>::KeyNode*
SeparateChainHashTable<KeyType, ValueType>::Find(const KeyType& key)
{
    uint32_t bucketIdx = key % mBucketCount;
    uint32_t offset = mBucket[bucketIdx];
    while (offset != INVALID_KEY_OFFSET) {
        KeyNode* keyNode = &(*mKeyNodes)[offset];
        if (keyNode->key == key) {
            return keyNode;
        }
        offset = keyNode->next;
    }
    return NULL;
}

template <typename KeyType, typename ValueType>
inline void SeparateChainHashTable<KeyType, ValueType>::Insert(const KeyType& key, const ValueType& value)
{
    assert(Find(key) == NULL);
    uint32_t bucketIdx = key % mBucketCount;
    KeyNode keyNode(key, value, mBucket[bucketIdx]);
    uint32_t keyOffset = mKeyNodes->Size();
    mKeyNodes->PushBack(keyNode);
    MEMORY_BARRIER();
    mBucket[bucketIdx] = keyOffset;
}
}} // namespace indexlib::common
