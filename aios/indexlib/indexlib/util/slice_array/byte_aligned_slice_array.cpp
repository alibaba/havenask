#include "indexlib/util/slice_array/byte_aligned_slice_array.h"

using namespace std;

IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(util);
IE_LOG_SETUP(util, ByteAlignedSliceArray);

ByteAlignedSliceArray::ByteAlignedSliceArray(
        uint32_t sliceLen, 
        uint32_t initSliceNum,
        util::Metrics *metrics,
        autil::mem_pool::PoolBase *pool,
        bool isOwner) 
    : AlignedSliceArray<char>(sliceLen, initSliceNum, 
                              metrics, pool, isOwner)
{
}

ByteAlignedSliceArray::ByteAlignedSliceArray(
        const ByteAlignedSliceArray& src)
    : AlignedSliceArray<char>(src)
{
}

ByteAlignedSliceArray::~ByteAlignedSliceArray() 
{
}

IE_NAMESPACE_END(util);

