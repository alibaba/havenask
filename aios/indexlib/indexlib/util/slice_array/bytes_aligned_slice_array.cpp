#include "indexlib/util/slice_array//bytes_aligned_slice_array.h"
#include "indexlib/util/mmap_pool.h"

using namespace std;
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(util);
IE_LOG_SETUP(util, BytesAlignedSliceArray);

BytesAlignedSliceArray::BytesAlignedSliceArray(
        const SimpleMemoryQuotaControllerPtr& memController,
        uint64_t sliceLen, uint32_t maxSliceNum)
    : mSliceArray(sliceLen, maxSliceNum, NULL, new MmapPool, true)
    , mMaxSliceNum(maxSliceNum)
    , mUsedBytes(0)
    , mCurSliceIdx(-1)
    , mMemController(memController)
{
    mCurCursor = mSliceArray.GetSliceLen() + 1;
}

BytesAlignedSliceArray::~BytesAlignedSliceArray()
{
}

IE_NAMESPACE_END(util);

