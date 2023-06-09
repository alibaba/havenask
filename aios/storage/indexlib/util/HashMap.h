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

#include <assert.h>
#include <stdio.h>

#include "autil/Lock.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/base/Constant.h"
#include "indexlib/util/HashBucket.h"
#include "indexlib/util/HashUtil.h"

namespace indexlib { namespace util {

template <typename Key, typename Value, typename Pool = autil::mem_pool::Pool, typename HashFunc = KeyHash<Key>,
          typename Comp = KeyEqual<Key>, int Sparsity = 2>
class HashMap
{
    using Typed = HashMap<Key, Value, Pool, HashFunc, Comp>;
    HashMap(const Typed&) = delete;
    Typed& operator=(const Typed&) = delete;

public:
    typedef Key KeyType;
    typedef Value ValueType;
    typedef std::pair<KeyType, ValueType> KeyValuePair;
    typedef Pool PoolType;
    typedef HashBucket<KeyValuePair, PoolType> BucketType;
    typedef typename BucketType::Block BlockType;
    typedef HashMap<Key, Value, Pool, HashFunc, Comp> HashMapType;

    static const size_t DEFAULT_HASHMAP_POOL_SIZE = indexlibv2::DEFAULT_CHUNK_SIZE * 1024 * 1024; // 10M

public:
    class Iterator
    {
    public:
        Iterator() : _mapSelf(), _buckets(NULL), _currentBlock(NULL), _blockItemPos(0) {}

        Iterator(const HashMap* m) : _mapSelf(m), _buckets(_mapSelf->_bucket), _currentBlock(NULL), _blockItemPos(0)
        {
            Reset();
        }

        void Reset()
        {
            _currentBlock = _buckets->GetFirstBlock();
            while (_currentBlock) {
                Bitmap& bitmap = _currentBlock->bitmap;
                _blockItemPos = 0;
                while (true) {
                    if (_blockItemPos >= _currentBlock->size) {
                        break;
                    }
                    if (bitmap.Test(_blockItemPos)) {
                        return;
                    }
                    ++_blockItemPos;
                }
                _currentBlock = _currentBlock->next;
            }
        }

        inline bool HasNext() const { return _currentBlock && _currentBlock->bitmap.Test(_blockItemPos); }

        inline KeyValuePair& Next()
        {
            KeyValuePair& kv = _currentBlock->begin[_blockItemPos++];
            while (_currentBlock) {
                Bitmap& bitmap = _currentBlock->bitmap;
                while (true) {
                    if (_blockItemPos >= _currentBlock->size) {
                        break;
                    }
                    if (bitmap.Test(_blockItemPos)) {
                        return kv;
                    }
                    ++_blockItemPos;
                }
                _currentBlock = _currentBlock->next;
                _blockItemPos = 0;
            }
            return kv;
        }

    private:
        const HashMap* _mapSelf;
        BucketType* _buckets;
        BlockType* _currentBlock;
        size_t _blockItemPos;
    };

public:
    HashMap(PoolType* pPool, size_t initSize);
    HashMap(size_t initSize);
    HashMap(size_t initSize, size_t poolSize);
    ~HashMap();

public:
    ValueType* Find(const KeyType& x) const;
    const ValueType& Find(const KeyType& x, const ValueType& _none) const;
    ValueType* FindWithLock(const KeyType& x) const;
    void Replace(const KeyType& x, const ValueType& value);

    // the same key may have duplicated entries
    void Insert(const KeyType& key, const ValueType& value);
    void Insert(const KeyValuePair& x);

    void FindAndInsert(const KeyType& key, const ValueType& value);
    bool operator==(const HashMapType& otherMap) const;
    Iterator CreateIterator() const { return Iterator(this); }

    static size_t EstimateFirstBucketSize(size_t initSize);
    //@return : in Byte
    static int64_t EstimateNeededMemory(int64_t keyCount);

public:
    size_t Size() const { return _elemCount; }
    void Clear();

private:
    size_t Threshold();
    KeyValuePair* Lookup(const KeyType& x) const;
    void FindAndInsertInLastBucket(const KeyValuePair& x);
    bool IsValid() const;

private:
    HashFunc _hasher;
    Comp _comparator;
    size_t _elemCount;
    BucketType* _bucket;

    PoolType* _memPool;
    bool _ownPool;

    mutable autil::ReadWriteLock _lock;

private:
    friend class HashMapTest;
    AUTIL_LOG_DECLARE();
};

/////////////////////////////////////////////////////////
// Implementation
template <typename Key, typename Value, typename Pool, typename HashFunc, typename Comp, int Sparsity>
HashMap<Key, Value, Pool, HashFunc, Comp, Sparsity>::HashMap(PoolType* pPool, size_t initSize)
    : _elemCount(0)
    , _bucket(new BucketType(pPool, initSize * Sparsity))
    , _memPool(pPool)
    , _ownPool(false)
{
    assert(_memPool != NULL);
}

template <typename Key, typename Value, typename Pool, typename HashFunc, typename Comp, int Sparsity>
HashMap<Key, Value, Pool, HashFunc, Comp, Sparsity>::HashMap(size_t initSize)
    : _elemCount(0)
    , _memPool(new PoolType(DEFAULT_HASHMAP_POOL_SIZE))
    , _ownPool(true)
{
    _bucket = new BucketType(_memPool, initSize * Sparsity);
}

template <typename Key, typename Value, typename Pool, typename HashFunc, typename Comp, int Sparsity>
HashMap<Key, Value, Pool, HashFunc, Comp, Sparsity>::HashMap(size_t initSize, size_t poolSize)
    : _elemCount(0)
    , _memPool(new PoolType(poolSize))
    , _ownPool(true)
{
    _bucket = new BucketType(_memPool, initSize * Sparsity);
}

template <typename Key, typename Value, typename Pool, typename HashFunc, typename Comp, int Sparsity>
HashMap<Key, Value, Pool, HashFunc, Comp, Sparsity>::~HashMap()
{
    Clear();

    delete _bucket;
    _bucket = NULL;
    if (_ownPool && _memPool) {
        _memPool->release();
        delete _memPool;
        _memPool = NULL;
    }
}

template <typename Key, typename Value, typename Pool, typename HashFunc, typename Comp, int Sparsity>
inline Value* HashMap<Key, Value, Pool, HashFunc, Comp, Sparsity>::Find(const KeyType& x) const
{
    assert(IsValid());
    KeyValuePair* p = Lookup(x);
    if (p != NULL) {
        return &p->second;
    }
    return NULL;
}

template <typename Key, typename Value, typename Pool, typename HashFunc, typename Comp, int Sparsity>
inline const Value& HashMap<Key, Value, Pool, HashFunc, Comp, Sparsity>::Find(const KeyType& x,
                                                                              const ValueType& _none) const
{
    assert(IsValid());
    KeyValuePair* p = Lookup(x);
    if (p != NULL) {
        return p->second;
    }
    return _none;
}

template <typename Key, typename Value, typename Pool, typename HashFunc, typename Comp, int Sparsity>
inline Value* HashMap<Key, Value, Pool, HashFunc, Comp, Sparsity>::FindWithLock(const KeyType& x) const
{
    autil::ScopedReadWriteLock lock(_lock, 'r');
    return Find(x);
}

template <typename Key, typename Value, typename Pool, typename HashFunc, typename Comp, int Sparsity>
inline void HashMap<Key, Value, Pool, HashFunc, Comp, Sparsity>::Replace(const KeyType& x, const ValueType& value)
{
    assert(IsValid());
    KeyValuePair* p = Lookup(x);
    if (p != NULL) {
        p->second = value;
    }
}

template <typename Key, typename Value, typename Pool, typename HashFunc, typename Comp, int Sparsity>
inline void HashMap<Key, Value, Pool, HashFunc, Comp, Sparsity>::Insert(const KeyType& key, const ValueType& value)
{
    Insert(std::make_pair(key, value));
}

template <typename Key, typename Value, typename Pool, typename HashFunc, typename Comp, int Sparsity>
inline void HashMap<Key, Value, Pool, HashFunc, Comp, Sparsity>::FindAndInsert(const KeyType& key,
                                                                               const ValueType& value)
{
    ValueType* retValue = Find(key);
    if (retValue != NULL) {
        *retValue = value;
    } else {
        Insert(key, value);
    }
}

template <typename Key, typename Value, typename Pool, typename HashFunc, typename Comp, int Sparsity>
inline void HashMap<Key, Value, Pool, HashFunc, Comp, Sparsity>::Insert(const KeyValuePair& x)
{
    assert(IsValid());

    if (_elemCount >= Threshold()) {
        _bucket->AddBlock();
    }

    FindAndInsertInLastBucket(x);
}

template <typename Key, typename Value, typename Pool, typename HashFunc, typename Comp, int Sparsity>
void HashMap<Key, Value, Pool, HashFunc, Comp, Sparsity>::Clear()
{
    _bucket->Clear();
    _elemCount = 0;
    if (_ownPool && _memPool) {
        _memPool->reset();
    }
}

template <typename Key, typename Value, typename Pool, typename HashFunc, typename Comp, int Sparsity>
inline size_t HashMap<Key, Value, Pool, HashFunc, Comp, Sparsity>::Threshold()
{
    return _bucket->Size() / Sparsity;
}

template <typename Key, typename Value, typename Pool, typename HashFunc, typename Comp, int Sparsity>
inline typename HashMap<Key, Value, Pool, HashFunc, Comp, Sparsity>::KeyValuePair*
HashMap<Key, Value, Pool, HashFunc, Comp, Sparsity>::Lookup(const KeyType& x) const
{
    BlockType* buff = _bucket->GetLastBlock();
    while (buff) {
        Bitmap& bitmap = buff->bitmap;
        size_t pos = _hasher(x) % buff->size;
        while (bitmap.Test(pos)) {
            KeyValuePair* p = buff->begin + pos;
            if (_comparator(p->first, x)) {
                return p;
            }
            if (++pos >= buff->size) {
                pos = 0;
            }
        }
        buff = buff->prev;
    }
    return NULL;
}

template <typename Key, typename Value, typename Pool, typename HashFunc, typename Comp, int Sparsity>
inline void HashMap<Key, Value, Pool, HashFunc, Comp, Sparsity>::FindAndInsertInLastBucket(const KeyValuePair& x)
{
    BlockType* buff = _bucket->GetLastBlock();
    Bitmap& bitmap = buff->bitmap;
    size_t pos = _hasher(x.first) % buff->size;

    KeyValuePair* p = buff->begin + pos;
    bool isFound = false;
    while (bitmap.Test(pos)) {
        if (_comparator(p->first, x.first)) {
            isFound = true;
            break;
        }
        if (++pos >= buff->size) {
            pos = 0;
        }
        p = buff->begin + pos;
    }
    if (!isFound) {
        _elemCount++;
        _bucket->GetLastBlock()->capacity++;
    }

    p->second = x.second;
    p->first = x.first;

    MEMORY_BARRIER();
    bitmap.Set(pos);
}

template <typename Key, typename Value, typename Pool, typename HashFunc, typename Comp, int Sparsity>
size_t HashMap<Key, Value, Pool, HashFunc, Comp, Sparsity>::EstimateFirstBucketSize(size_t initSize)
{
    size_t allocElemSize = BucketType::GetNextBlockSize(initSize * Sparsity);
    int32_t slotCount = Bitmap::GetSlotCount(allocElemSize);
    return allocElemSize * sizeof(KeyValuePair) + sizeof(BlockType) + slotCount * sizeof(uint32_t);
}

template <typename Key, typename Value, typename Pool, typename HashFunc, typename Comp, int Sparsity>
int64_t HashMap<Key, Value, Pool, HashFunc, Comp, Sparsity>::EstimateNeededMemory(int64_t keyCount)
{
    int64_t memNeed = 0;
    int64_t allocElemSize = 0;
    int64_t capacity = 0;
    while (keyCount > (capacity / Sparsity)) {
        allocElemSize = BucketType::GetNextBlockSize(allocElemSize);
        capacity += allocElemSize;
        int32_t slotCount = Bitmap::GetSlotCount(allocElemSize);
        memNeed += allocElemSize * sizeof(KeyValuePair) + sizeof(BlockType) + slotCount * sizeof(uint32_t);
    }
    return memNeed;
}

template <typename Key, typename Value, typename Pool, typename HashFunc, typename Comp, int Sparsity>
inline bool HashMap<Key, Value, Pool, HashFunc, Comp, Sparsity>::IsValid() const
{
    return (_elemCount >= 0) && (_elemCount <= (size_t)_bucket->Size() / Sparsity);
}

template <typename Key, typename Value, typename Pool, typename HashFunc, typename Comp, int Sparsity>
inline bool HashMap<Key, Value, Pool, HashFunc, Comp, Sparsity>::operator==(const HashMapType& otherMap) const
{
    if (_elemCount != otherMap._elemCount) {
        return false;
    }
    BlockType* buff = _bucket->GetLastBlock();
    BlockType* otherBuff = otherMap._bucket->GetLastBlock();

    while (buff != NULL && otherBuff != NULL) {
        // compare current block
        if (buff->size != otherBuff->size) {
            return false;
        }
        Bitmap& bitmap = buff->bitmap;
        Bitmap& otherBitmap = otherBuff->bitmap;
        if (bitmap != otherBitmap) {
            return false;
        }
        for (size_t i = 0; i < buff->size; ++i) {
            if (!bitmap.Test(i)) {
                continue;
            }
            if (buff->begin[i] != otherBuff->begin[i]) {
                return false;
            }
        }
        buff = buff->prev;
        otherBuff = otherBuff->prev;
    }
    if ((buff == NULL && otherBuff != NULL) || (buff != NULL && otherBuff == NULL)) {
        return false;
    }
    return true;
}
}} // namespace indexlib::util
