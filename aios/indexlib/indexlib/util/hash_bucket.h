#ifndef __INDEXLIB_HASH_BUCKET_H
#define __INDEXLIB_HASH_BUCKET_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/util/bitmap.h"

IE_NAMESPACE_BEGIN(util);

static size_t HASHMAP_STEPTABLE[] = 
{
    6,1543,3079,6151,12289,24593,
    49157,98317,196613,393241,786433,
    1572869,3145739,6291469,12582917,
    12582919,12582929,12582967,12582971,
    12582991,12583003,12583007,12583027,
    12583031,12583037,12583061,12583063,
    12583091,12583099,12583121,12583157,
    12583159,12583171,12583177,12583183,
    12583211,12583217,12583237,12583273,
    12583297,12583327,12583379,12583397,
    12583409,12583423,12583429,12583463,
    12583469,12583481,12583499,12583511,
    12583513,12583517,12583523,12583559,
    12583561,12583573,12583577,12583583,
    12583591,12583601,12583603,12583607,
    12583609,12583657,12583661,12583693,
    12583699,12583721,12583741,12583759,
    12583777,12583787,12583789,12583817
};

static uint32_t HASHMAP_STEP_COUNT = sizeof(HASHMAP_STEPTABLE) / sizeof(HASHMAP_STEPTABLE[0]);

template <typename T, typename Pool>
class HashBucket
{
public:
    typedef T ValueType;
    typedef Pool PoolType;
    typedef HashBucket Self;

public:
    const static size_t FACTOR_N = 2;

public:
    struct Block  
    {
        Block(size_t n) 
            : size(n)
            , capacity(0)
            , prev(NULL)
            , next(NULL)
        { 
            end = begin + n;
        }

        void MountBitMap(uint32_t* data, uint32_t itemCount) 
        {
            bitmap.Mount(itemCount, data, true);
        }

        size_t size;
        size_t capacity;
        Bitmap bitmap;
        Block* prev;
        Block* next;
        T* end;
        T begin[0];
    };    

public:
    HashBucket(PoolType* pPool, size_t initSize) 
        : m_pBlocks(NULL), 
          m_nInitSize(initSize),
          m_nNumBlocks(0),
          mSize(0), 
          m_pMemPool(pPool)
    {
    }

    ~HashBucket() 
    { 
        Clear();
    } 

public:
    Block* GetFirstBlock()
    {
        Block* pCur = m_pBlocks;
        if (pCur == NULL) return NULL;
        while (pCur->prev != NULL) 
        {
            pCur = pCur->prev;
        }
        return pCur;
    }

    const Block* GetFirstBlock() const 
    {
        return const_cast<Self*>(this)->GetFirstBlock();
    }

    Block* GetLastBlock()
    {
        return m_pBlocks;
    }

    const Block* GetLastBlock() const 
    {
        return m_pBlocks;
    }

    size_t GetNumBlocks() const {return m_nNumBlocks;}

    size_t Size() const 
    {
        return mSize;     
    }

    static size_t GetNextBlockSize(size_t allocSize)
    {
        size_t *begin = HASHMAP_STEPTABLE;
        size_t *end = begin + HASHMAP_STEP_COUNT;
        size_t *iter = std::upper_bound(begin, end, allocSize);
        if (iter != end) {
            return *iter;
        } else {
            return HASHMAP_STEPTABLE[HASHMAP_STEP_COUNT - 1];
        }
    }

    void AddBlock() 
    {
        assert(m_nInitSize > 0);
        size_t allocElemSize = m_pBlocks == NULL ? m_nInitSize : m_pBlocks->size;
        allocElemSize = GetNextBlockSize(allocElemSize);
        int32_t slotCount = Bitmap::GetSlotCount(allocElemSize);
        char* p = (char*)m_pMemPool->allocate(
                allocElemSize * sizeof(T) 
                + sizeof(Block) 
                + slotCount * sizeof(uint32_t));
        Block* block = new(p) Block(allocElemSize);
        p +=  allocElemSize * sizeof(T) + sizeof(Block);
        memset(p, 0, slotCount * sizeof(uint32_t));
        block->MountBitMap((uint32_t*)p, (uint32_t)allocElemSize);

        MEMORY_BARRIER();
        AddBlock(block);
    }    

    void AddBlock(Block* x) 
    {
        assert(x != NULL);
        x->prev = m_pBlocks;
        if (m_pBlocks != NULL) m_pBlocks->next = x;

        MEMORY_BARRIER();
        m_pBlocks = x;
        mSize += m_pBlocks->size;
        ++m_nNumBlocks;
    }    

    void Clear() 
    {
        m_pBlocks = NULL;
        mSize = 0;
        m_nNumBlocks = 0;
    }

private:
    Block* m_pBlocks;
    size_t m_nInitSize;
    size_t m_nNumBlocks;
    size_t mSize;
    PoolType* m_pMemPool; 
};

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_HASH_BUCKET_H
