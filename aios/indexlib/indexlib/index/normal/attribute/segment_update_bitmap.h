#ifndef __INDEXLIB_SEGMENT_UPDATE_BITMAP_H
#define __INDEXLIB_SEGMENT_UPDATE_BITMAP_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/util/bitmap.h"
#include "indexlib/misc/exception.h"

IE_NAMESPACE_BEGIN(index);

class SegmentUpdateBitmap
{
public:
    SegmentUpdateBitmap(size_t docCount,
                        autil::mem_pool::PoolBase* pool = NULL)
        : mDocCount(docCount)
        , mBitmap(false, pool)
    {}
    
    ~SegmentUpdateBitmap() {}
public:
    void Set(docid_t localDocId)
    {
        if(unlikely(size_t(localDocId) >= mDocCount))
        {
            INDEXLIB_THROW(misc::OutOfRangeException,
                           "local docId[%d] out of range[0:%lu)",
                           localDocId, mDocCount);
        }
        if (unlikely(mBitmap.GetItemCount() == 0))
        {
            mBitmap.Alloc(mDocCount, false);
        }
        
        mBitmap.Set(localDocId);
    }

    uint32_t GetUpdateDocCount() const
    {
        return mBitmap.GetSetCount();
    }
    
private:
    size_t mDocCount;
    util::Bitmap mBitmap;
};

DEFINE_SHARED_PTR(SegmentUpdateBitmap);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_SEGMENT_UPDATE_BITMAP_H
