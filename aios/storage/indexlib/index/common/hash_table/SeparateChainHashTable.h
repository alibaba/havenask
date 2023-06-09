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

#include "indexlib/base/Define.h"
#include "indexlib/index/common/hash_table/HashTableWriter.h"
#include "indexlib/util/MMapVector.h"
#include "indexlib/util/MmapPool.h"

namespace indexlibv2::index {

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
    typedef std::shared_ptr<indexlib::util::MMapVector<KeyNode>> KeyNodeVectorPtr;

    class HashTableIterator
    {
    public:
        HashTableIterator(const KeyNodeVectorPtr& keyNodes) : _cursor(0), _keyNodes(keyNodes) { assert(keyNodes); }

    public:
        bool IsValid() const { return _cursor < _keyNodes->Size(); }

        void MoveToNext() { _cursor++; }

        void Reset() { _cursor = 0; }

        KeyNode* GetCurrentKeyNode() const
        {
            assert(IsValid());
            return &(*_keyNodes)[_cursor];
        }

        const KeyType& Key() const { return GetCurrentKeyNode()->key; }

        const ValueType& Value() const { return GetCurrentKeyNode()->value; }

        void SortByKey() // sort by key
        {
            assert(_cursor == 0);
            KeyNode* begin = &(*_keyNodes)[0];
            KeyNode* end = begin + _keyNodes->Size();
            std::sort(begin, end, Comp);
        }

        size_t Size() const { return _keyNodes->Size(); }

    private:
        static bool Comp(const KeyNode& left, KeyNode& right) { return left.key < right.key; }

    private:
        size_t _cursor;
        KeyNodeVectorPtr _keyNodes;
    };

public:
    SeparateChainHashTable() : _bucket(NULL), _bucketCount(0) {}
    ~SeparateChainHashTable()
    {
        if (_bucket) {
            _pool.deallocate((void*)_bucket, _bucketCount * sizeof(uint32_t));
        }
    }

    void Init(uint32_t bucketCount, size_t reserveSize)
    {
        _bucketCount = bucketCount;
        size_t bucketSize = bucketCount * sizeof(uint32_t);
        _bucket = (uint32_t*)_pool.allocate(bucketSize);
        memset(_bucket, 0xFF, bucketSize);
        _keyNodes.reset(new indexlib::util::MMapVector<KeyNode>());
        _keyNodes->Init(indexlib::util::MMapAllocatorPtr(new indexlib::util::MMapAllocator),
                        reserveSize / sizeof(KeyNode) + 1);
    }

    HashTableIterator* CreateKeyNodeIterator() { return new HashTableIterator(_keyNodes); }

    size_t GetUsedBytes() const { return _bucketCount * sizeof(uint32_t) + _keyNodes->GetUsedBytes(); }

public:
    KeyNode* Find(const KeyType& key);

    void Insert(const KeyType& key, const ValueType& value);
    size_t Size() const { return _keyNodes->Size(); }

private:
    indexlib::util::MmapPool _pool;
    uint32_t* _bucket;
    uint32_t _bucketCount;
    KeyNodeVectorPtr _keyNodes;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP_TEMPLATE_2(indexlib.index, SeparateChainHashTable, KeyType, ValueType);
///////////////////////////////////////////////////////////////

template <typename KeyType, typename ValueType>
inline typename SeparateChainHashTable<KeyType, ValueType>::KeyNode*
SeparateChainHashTable<KeyType, ValueType>::Find(const KeyType& key)
{
    uint32_t bucketIdx = key % _bucketCount;
    uint32_t offset = _bucket[bucketIdx];
    while (offset != INVALID_KEY_OFFSET) {
        KeyNode* keyNode = &(*_keyNodes)[offset];
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
    uint32_t bucketIdx = key % _bucketCount;
    KeyNode keyNode(key, value, _bucket[bucketIdx]);
    uint32_t keyOffset = _keyNodes->Size();
    _keyNodes->PushBack(keyNode);
    MEMORY_BARRIER();
    _bucket[bucketIdx] = keyOffset;
}
} // namespace indexlibv2::index
