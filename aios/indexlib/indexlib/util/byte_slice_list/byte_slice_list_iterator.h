#ifndef __INDEXLIB_BYTE_SLICE_LIST_ITERATOR_H
#define __INDEXLIB_BYTE_SLICE_LIST_ITERATOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/util/byte_slice_list/byte_slice_list.h"

IE_NAMESPACE_BEGIN(util);
class ByteSliceListIterator
{
public:
    ByteSliceListIterator(const ByteSliceList* sliceList);
    ByteSliceListIterator(const ByteSliceListIterator& other);
    ~ByteSliceListIterator() {}

public:
    // [beginPos, endPos)
    bool SeekSlice(size_t beginPos);
    bool HasNext(size_t endPos);
    void Next(void* &data, size_t &size);

private:
    const ByteSliceList* mSliceList;
    ByteSlice* mSlice;
    size_t mPosInSlice;
    size_t mSeekedSliceSize;
    size_t mEndPos;

private:
    friend class ByteSliceListIteratorTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ByteSliceListIterator);

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_BYTE_SLICE_LIST_ITERATOR_H
