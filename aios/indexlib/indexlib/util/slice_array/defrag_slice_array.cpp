#include "indexlib/util/slice_array/defrag_slice_array.h"

using namespace std;

IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(util);
IE_LOG_SETUP(util, DefragSliceArray);


DefragSliceArray::SliceHeader::SliceHeader()
    : wastedSize(0)
{ }

DefragSliceArray::DefragSliceArray(const SliceArrayPtr& sliceArray)
    : mSliceArray(sliceArray)
{
    assert(mSliceArray);
}

DefragSliceArray::~DefragSliceArray()
{
}

uint64_t DefragSliceArray::Append(const void* value, size_t size)
{
    assert(value);

    if (IsOverLength(size, GetSliceLen()))
    {
        INDEXLIB_FATAL_ERROR(OutOfRange, 
                             "data [%ld] can not store in one slice [%ld],"
                             " slice header size [%ld]",
                             size, GetSliceLen(), GetHeaderSize());
    }

    if (!mSliceArray->IsCurrentSliceEnough(size))
    {
        int64_t lastSliceIdx = mSliceArray->GetCurrentSliceIdx();
        AllocateNewSlice();
        FreeLastSliceSpace(lastSliceIdx);
    }

    return mSliceArray->Insert(value, size);
}

void DefragSliceArray::AllocateNewSlice()
{
    mSliceArray->AllocateSlice();
    SliceHeader header;
    mSliceArray->Insert(&header, sizeof(header));
}

void DefragSliceArray::FreeLastSliceSpace(int64_t sliceIdx)
{
    if (sliceIdx < 0)
    {
        return;
    }

    uint64_t curSliceUsedBytes = mSliceArray->GetSliceUsedBytes(sliceIdx);
    int64_t freeSize = GetSliceLen() - curSliceUsedBytes;
    assert(freeSize >= 0);
    FreeBySliceIdx(sliceIdx, freeSize);
}

void DefragSliceArray::Free(uint64_t offset, size_t size)
{
    FreeBySliceIdx(mSliceArray->OffsetToSliceIdx(offset), size);
}

void DefragSliceArray::FreeBySliceIdx(int64_t sliceIdx, size_t size)
{
    IncreaseWastedSize(sliceIdx, size);
    DoFree(size);

    if (NeedDefrag(sliceIdx))
    {
        Defrag(sliceIdx);
    }
}

IE_NAMESPACE_END(util);

