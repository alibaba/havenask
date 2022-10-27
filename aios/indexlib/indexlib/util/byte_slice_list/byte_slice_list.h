#ifndef __INDEXLIB_BYTESLICELIST_H
#define __INDEXLIB_BYTESLICELIST_H

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
#include "indexlib/misc/devirtual.h"
#include <stdint.h>
#include <memory>
#include <autil/mem_pool/Pool.h>

IE_NAMESPACE_BEGIN(util);

#pragma pack(push,1)
struct ByteSlice
{
private:
    ByteSlice() = default;

public:    
    static constexpr size_t GetHeadSize();

    /*dataSize is the size of the data*/
    static ByteSlice* CreateObject(size_t dataSize, 
                                   autil::mem_pool::Pool* pool = NULL);

    static void DestroyObject(ByteSlice* byteSlice, 
                              autil::mem_pool::Pool* pool = NULL);

    static ByteSlice* GetImmutableEmptyObject()
    {
        static ByteSlice slice;
        return &slice;
    }
    
public:
    //TODO: test perf for volatile in lookup
    ByteSlice* volatile next = nullptr;
    uint8_t* volatile data = nullptr;
    size_t volatile size = 0;

    // for BlockByteSliceList
    size_t volatile dataSize = 0; // actual block data size attached
    size_t volatile offset = 0; // offset of this slice in underlying file, needed by BlockByteSliceList
    // uint32_t volatile idx; // index of this slice in ByteSliceList, needed by BlockByteSlice
};

#pragma pack(pop)


IE_BASE_DECLARE_SUB_CLASS_BEGIN(ByteSliceList)
IE_BASE_DECLARE_SUB_CLASS(ByteSliceList, BlockByteSliceList)
IE_BASE_DECLARE_SUB_CLASS_END(ByteSliceList)

class ByteSliceList
{
public:
    ByteSliceList();
    ByteSliceList(ByteSlice* slice);
    virtual ~ByteSliceList();

public:
    void Add(ByteSlice* slice);
    size_t GetTotalSize() const {return mTotalSize;}
    size_t UpdateTotalSize();
    void IncrementTotalSize(size_t incValue) { mTotalSize += incValue;}
    ByteSlice* GetHead() const {return mHead;}
    ByteSlice* GetTail() const {return mTail;}

    void MergeWith(ByteSliceList& other);
    virtual void Clear(autil::mem_pool::Pool* pool);
    
    IE_BASE_CLASS_DECLARE(ByteSliceList);
protected:
    ByteSlice* mHead;
    ByteSlice* mTail;
    size_t volatile mTotalSize;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ByteSliceList);

inline constexpr size_t ByteSlice::GetHeadSize()
{
//    return sizeof(ByteSlice*) + sizeof(uint8_t*) + sizeof(uint32_t) + sizeof(uint32_t);
    return sizeof(ByteSlice);
}

/*dataSize is the size of the data*/
inline ByteSlice* ByteSlice::CreateObject(size_t dataSize, autil::mem_pool::Pool* pool)
{
    uint8_t* mem;
    size_t memSize = dataSize + GetHeadSize();
    if (pool == NULL)
    {
        mem = new uint8_t[memSize];
    }
    else
    {
        mem = (uint8_t*)pool->allocate(memSize);
    }
    ByteSlice* byteSlice =  new(mem)ByteSlice;
    byteSlice->data = mem + GetHeadSize();
    byteSlice->size = dataSize;
    byteSlice->dataSize = 0;
    byteSlice->offset = 0;
    return byteSlice;
}

inline void ByteSlice::DestroyObject(ByteSlice* byteSlice, autil::mem_pool::Pool* pool)
{
    byteSlice->~ByteSlice();

    uint8_t* mem= (uint8_t*)byteSlice;
    if (pool == NULL)
    {
        delete []mem;
    }
    else
    {
        pool->deallocate(mem, byteSlice->size + GetHeadSize());
    }
}

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_BYTESLICELIST_H
