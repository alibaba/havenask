#ifndef HASHMAP_H
#define HASHMAP_H

#include <assert.h>
#include <stdio.h>
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include <autil/mem_pool/Pool.h>
#include <autil/Lock.h>
#include "indexlib/util/hash_util.h"
#include "indexlib/util/profiling.h"
#include "indexlib/util/hash_bucket.h"

IE_NAMESPACE_BEGIN(util);

template <typename Key, typename Value, typename Pool = autil::mem_pool::Pool, 
          typename HashFunc = KeyHash<Key>, typename Comp = KeyEqual<Key>, int Sparsity = 2>
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
    
    static const size_t DEFAULT_HASHMAP_POOL_SIZE = DEFAULT_CHUNK_SIZE * 1024 * 1024; // 10M

public:
    class Iterator
    {
    public:
        Iterator()
            : m_pMapSelf()
            , m_pBuckets(NULL)
            , m_pCurrentBlock(NULL)
            , m_blockItemPos(0)
        {
        }

        Iterator(const HashMap* m)
            : m_pMapSelf(m)
            , m_pBuckets(m_pMapSelf->m_pBucket)
            , m_pCurrentBlock(NULL)
            , m_blockItemPos(0)
        {   
            Reset();
        }

        void Reset()
        {
            m_pCurrentBlock = m_pBuckets->GetFirstBlock();
            while(m_pCurrentBlock)
            {
                Bitmap& bitmap = m_pCurrentBlock->bitmap;
                m_blockItemPos = 0;
                while (true)
                {
                    if(m_blockItemPos >= m_pCurrentBlock->size) { break; }
                    if(bitmap.Test(m_blockItemPos)) { return; }
                    ++m_blockItemPos;
                }
                m_pCurrentBlock = m_pCurrentBlock->next;
            }
        }

        inline bool HasNext() const
        {
            return  m_pCurrentBlock && m_pCurrentBlock->bitmap.Test(m_blockItemPos);
        }

        inline KeyValuePair& Next()
        {
            KeyValuePair& kv = m_pCurrentBlock->begin[m_blockItemPos++];
            while(m_pCurrentBlock)
            {
                Bitmap& bitmap = m_pCurrentBlock->bitmap;
                while(true)
                {
                    if(m_blockItemPos >= m_pCurrentBlock->size) { break; }
                    if(bitmap.Test(m_blockItemPos)) { return kv; }
                    ++m_blockItemPos;
                }
                m_pCurrentBlock = m_pCurrentBlock->next;
                m_blockItemPos = 0;
            }
            return kv;
        }

    private:
        const HashMap* m_pMapSelf;
        BucketType* m_pBuckets;
        BlockType* m_pCurrentBlock;
        size_t m_blockItemPos;
    };

public:
    HashMap(PoolType* pPool, size_t initSize);
    HashMap(size_t initSize);
    HashMap(size_t initSize, size_t poolSize);
    ~HashMap();

public:
    ValueType* Find(const KeyType& x) const;
    const ValueType& Find(const KeyType& x, const ValueType &_none) const;
    ValueType* FindWithLock(const KeyType& x) const;
    void Replace(const KeyType& x, const ValueType& value);

    // the same key may have duplicated entries
    void Insert(const KeyType& key, const ValueType& value);
    void Insert(const KeyValuePair& x);

    void FindAndInsert(const KeyType& key, const ValueType& value);
    bool operator ==(const HashMapType &otherMap) const;
    Iterator CreateIterator() const { return Iterator(this);}

    static size_t EstimateFirstBucketSize(size_t initSize);
    //@return : in Byte
    static int64_t EstimateNeededMemory(int64_t keyCount);

public:
    size_t Size() const {return m_nElemCount;}
    void Clear();

private:
    size_t Threshold();
    KeyValuePair* Lookup(const KeyType& x) const;
    void FindAndInsertInLastBucket(const KeyValuePair& x);
    bool IsValid() const;

private:
    HashFunc m_hasher;
    Comp m_comparator;
    size_t m_nElemCount;
    BucketType* m_pBucket;

    PoolType* m_pMemPool;
    bool m_bOwnPool;

    mutable autil::ReadWriteLock mLock;
private:
    friend class HashMapTest;
    IE_LOG_DECLARE();
};

/////////////////////////////////////////////////////////
//Implementation
template <typename Key, typename Value, typename Pool, typename HashFunc, typename Comp, int Sparsity>
HashMap<Key, Value, Pool, HashFunc, Comp, Sparsity>::HashMap(PoolType* pPool, size_t initSize)
    : m_nElemCount(0)
    , m_pBucket(new BucketType(pPool, initSize * Sparsity))
    , m_pMemPool(pPool)
    , m_bOwnPool(false)
{ 
    assert(m_pMemPool != NULL);
}

template <typename Key, typename Value, typename Pool, typename HashFunc, typename Comp, int Sparsity>
HashMap<Key, Value, Pool, HashFunc, Comp, Sparsity>::HashMap(size_t initSize)
    : m_nElemCount(0)
    , m_pMemPool(new PoolType(DEFAULT_HASHMAP_POOL_SIZE))
    , m_bOwnPool(true)
{ 
    m_pBucket = new BucketType(m_pMemPool, initSize * Sparsity);
}

template <typename Key, typename Value, typename Pool, typename HashFunc, typename Comp, int Sparsity>
HashMap<Key, Value, Pool, HashFunc, Comp, Sparsity>::HashMap(size_t initSize, size_t poolSize)
    : m_nElemCount(0)
    , m_pMemPool(new PoolType(poolSize))
    , m_bOwnPool(true)
{ 
    m_pBucket = new BucketType(m_pMemPool, initSize * Sparsity);
}

template <typename Key, typename Value, typename Pool, typename HashFunc, typename Comp, int Sparsity>
HashMap<Key, Value, Pool, HashFunc, Comp, Sparsity>::~HashMap()
{
    Clear();

    delete m_pBucket;
    m_pBucket = NULL;
    if (m_bOwnPool && m_pMemPool)
    {
        m_pMemPool->release();
        delete m_pMemPool;
        m_pMemPool = NULL;
    }
}


template <typename Key, typename Value, typename Pool, typename HashFunc, typename Comp, int Sparsity>
inline Value* HashMap<Key, Value, Pool, HashFunc, Comp, Sparsity>::Find(const KeyType& x) const
{      
    assert(IsValid());
    KeyValuePair* p = Lookup(x);
    if (p != NULL) 
    {
        return &p->second;
    }
    return NULL;
}

template <typename Key, typename Value, typename Pool, typename HashFunc, typename Comp, int Sparsity>
inline const Value& HashMap<Key, Value, Pool, HashFunc, Comp, Sparsity>::Find(const KeyType& x, const ValueType &_none) const
{      
    assert(IsValid());
    KeyValuePair* p = Lookup(x);
    if (p != NULL) 
    {
        return p->second;
    }
    return _none;
}

template <typename Key, typename Value, typename Pool, typename HashFunc, typename Comp, int Sparsity>
inline Value* HashMap<Key, Value, Pool, HashFunc, Comp, Sparsity>::FindWithLock(const KeyType& x) const
{      
    autil::ScopedReadWriteLock lock(mLock, 'r');
    return Find(x);
}


template <typename Key, typename Value, typename Pool, typename HashFunc, typename Comp, int Sparsity>
inline void HashMap<Key, Value, Pool, HashFunc, Comp, Sparsity>::Replace(const KeyType& x, const ValueType& value)
{
    assert(IsValid());
    KeyValuePair* p = Lookup(x);
    if (p != NULL) 
    {
        p->second = value;
    }
}

template <typename Key, typename Value, typename Pool, typename HashFunc, typename Comp, int Sparsity>
inline void HashMap<Key, Value, Pool, HashFunc, Comp, Sparsity>::Insert(const KeyType& key, const ValueType& value) 
{
    Insert(std::make_pair(key, value));
}

template <typename Key, typename Value, typename Pool, typename HashFunc, typename Comp, int Sparsity>
inline void HashMap<Key, Value, Pool, HashFunc, Comp, Sparsity>::FindAndInsert(const KeyType& key, const ValueType& value)
{
    ValueType *retValue = Find(key);
    if (retValue != NULL) 
    {
        *retValue = value;
    }
    else 
    {
        Insert(key, value);
    }
}

template <typename Key, typename Value, typename Pool, typename HashFunc, typename Comp, int Sparsity>
inline void HashMap<Key, Value, Pool, HashFunc, Comp, Sparsity>::Insert(const KeyValuePair& x) 
{      
    assert(IsValid());
    
    if (m_nElemCount >= Threshold()) 
    {
        m_pBucket->AddBlock();
    }

    FindAndInsertInLastBucket(x);
}

template <typename Key, typename Value, typename Pool, typename HashFunc, typename Comp, int Sparsity>
void HashMap<Key, Value, Pool, HashFunc, Comp, Sparsity>::Clear()
{
    m_pBucket->Clear();
    m_nElemCount = 0;
    if (m_bOwnPool && m_pMemPool)
    {
        m_pMemPool->reset();
    }
}
    
template <typename Key, typename Value, typename Pool, typename HashFunc, typename Comp, int Sparsity>
inline size_t HashMap<Key, Value, Pool, HashFunc, Comp, Sparsity>::Threshold() 
{
    return m_pBucket->Size() / Sparsity;
}

template <typename Key, typename Value, typename Pool, typename HashFunc, typename Comp, int Sparsity>
inline typename HashMap<Key, Value, Pool, HashFunc, Comp, Sparsity>::KeyValuePair* 
HashMap<Key, Value, Pool, HashFunc, Comp, Sparsity>::Lookup(const KeyType& x) const
{
    BlockType* buff = m_pBucket->GetLastBlock();
    while(buff)
    {
        Bitmap& bitmap = buff->bitmap;
        size_t pos = m_hasher(x) % buff->size;
        while(bitmap.Test(pos))
        {
            KeyValuePair* p = buff->begin + pos;
            if (m_comparator(p->first, x)) { return p; }
            if(++pos >= buff->size) { pos = 0; }
        }
        buff = buff->prev;
    }
    return NULL;
}

template <typename Key, typename Value, typename Pool, typename HashFunc, typename Comp, int Sparsity>
inline void HashMap<Key, Value, Pool, HashFunc, Comp, Sparsity>::FindAndInsertInLastBucket(
        const KeyValuePair& x) 
{
    BlockType* buff = m_pBucket->GetLastBlock();
    Bitmap& bitmap = buff->bitmap;
    size_t pos = m_hasher(x.first) % buff->size;

    KeyValuePair* p = buff->begin + pos;
    bool isFound = false;
    while (bitmap.Test(pos))
    {
        if (m_comparator(p->first, x.first)) 
        {
            isFound = true;
            break;
        }
        if (++pos >= buff->size) 
        { 
            pos = 0; 
        }
        p = buff->begin + pos;        
    }
    if (!isFound)
    {
        m_nElemCount++;
        m_pBucket->GetLastBlock()->capacity++;
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
    return allocElemSize * sizeof(KeyValuePair) 
        + sizeof(BlockType) 
        + slotCount * sizeof(uint32_t);
}

template <typename Key, typename Value, typename Pool, typename HashFunc, typename Comp, int Sparsity>
int64_t HashMap<Key, Value, Pool, HashFunc, Comp, Sparsity>::EstimateNeededMemory(
        int64_t keyCount) 
{
    int64_t memNeed = 0;
    int64_t allocElemSize = 0;
    int64_t capacity = 0;
    while (keyCount > (capacity / Sparsity))
    {
        allocElemSize = BucketType::GetNextBlockSize(allocElemSize);
        capacity += allocElemSize;
        int32_t slotCount = Bitmap::GetSlotCount(allocElemSize);
        memNeed += allocElemSize * sizeof(KeyValuePair) + sizeof(BlockType) 
                   + slotCount * sizeof(uint32_t);
    }
    return memNeed;
}

template <typename Key, typename Value, typename Pool, typename HashFunc, typename Comp, int Sparsity>
inline bool HashMap<Key, Value, Pool, HashFunc, Comp, Sparsity>::IsValid() const
{
    return (m_nElemCount >= 0) && (m_nElemCount <= (size_t)m_pBucket->Size() / Sparsity);
}

template <typename Key, typename Value, typename Pool, typename HashFunc, typename Comp, int Sparsity>
inline bool HashMap<Key, Value, Pool, HashFunc, Comp, Sparsity>::operator == (const HashMapType &otherMap) const
{
    if (m_nElemCount != otherMap.m_nElemCount) 
    {
        return false;
    }
    BlockType* buff = m_pBucket->GetLastBlock();
    BlockType* otherBuff = otherMap.m_pBucket->GetLastBlock();

    while (buff != NULL && otherBuff != NULL) 
    {
        //compare current block
        if (buff->size != otherBuff->size) {
            return false;
        }
        Bitmap& bitmap = buff->bitmap;
        Bitmap& otherBitmap = otherBuff->bitmap;
        if(bitmap != otherBitmap) { return false; }
        for (size_t i = 0; i < buff->size; ++i) {
            if(!bitmap.Test(i)) { continue; }
            if (buff->begin[i] != otherBuff->begin[i]) {
                return false;
            }
        }
        buff = buff->prev;
        otherBuff = otherBuff->prev;
    }
    if ((buff == NULL && otherBuff != NULL) ||
        (buff != NULL && otherBuff == NULL))
    {
        return false;
    }
    return true;
}

IE_NAMESPACE_END(util);

#endif

