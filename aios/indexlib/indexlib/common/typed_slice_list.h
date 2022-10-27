#ifndef __INDEXLIB_TYPED_SLICE_LIST_H
#define __INDEXLIB_TYPED_SLICE_LIST_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/common/radix_tree.h"

IE_NAMESPACE_BEGIN(common);

class TypedSliceListBase {
    
};

template<typename T>
class TypedSliceList : public TypedSliceListBase
{
public:
    typedef autil::mem_pool::Pool Pool;
    TypedSliceList(uint32_t slotNum, uint32_t sliceLength, Pool* pool)
        : mSize(0)
    {
        void* buffer = pool->allocate(sizeof(common::RadixTree));
        mData = new (buffer) common::RadixTree(slotNum, sliceLength, pool, sizeof(T));
    }

    ~TypedSliceList() {}

public:
    void PushBack(const T& value);
    void Update(uint32_t index, const T& value);
    void Read(uint32_t index, T& value) const;
    void Read(uint32_t index, T*& buffer) const;

    T& operator [](uint32_t index)
    {
        assert(index < mSize);
        T* buffer;
        Read(index, buffer);
        return *buffer;
    }

    uint64_t Size() const
    { return mSize; }

    uint32_t GetSliceNum() const
    { return mData->GetSliceNum(); }
    uint8_t* GetSlice(uint32_t sliceId, uint32_t& sliceLength) const
    {
        sliceLength = GetSliceSize(sliceId);
        return mData->GetSlice(sliceId); 
    }

private:
    uint32_t GetSliceSize(uint32_t sliceId) const;

private:
    common::RadixTree* mData;
    volatile uint64_t mSize;

private:
    friend class TypedSliceListTest;
    IE_LOG_DECLARE();
};

///////////////////////////////////////////////////////////
template <typename T>
inline void TypedSliceList<T>::Update(uint32_t index, const T& value)
{
    T* data = (T*)mData->Search(index);
    assert(data != NULL);
    *data = value;
}

template <typename T>
inline void TypedSliceList<T>::PushBack(const T& value)
{
    assert(mData);    
    T* data = (T*)mData->OccupyOneItem();
    *data = value;
    MEMORY_BARRIER();
    mSize++;
}

template <typename T>
inline void TypedSliceList<T>::Read(uint32_t index, T*& buffer) const
{
    buffer = (T*)mData->Search(index);
}

template <typename T>
inline void TypedSliceList<T>::Read(uint32_t index, T& value) const
{
    T* buffer;
    Read(index, buffer);
    value = *buffer;
}

template <typename T>
inline uint32_t TypedSliceList<T>::GetSliceSize(uint32_t sliceId) const
{
    return mData->GetSliceSize(sliceId);
}

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_TYPED_SLICE_LIST_H
