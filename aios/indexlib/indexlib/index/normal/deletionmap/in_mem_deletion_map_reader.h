#ifndef __INDEXLIB_IN_MEM_DELETION_MAP_READER_H
#define __INDEXLIB_IN_MEM_DELETION_MAP_READER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/util/expandable_bitmap.h"
#include "indexlib/index_base/index_meta/segment_info.h"

IE_NAMESPACE_BEGIN(index);

class InMemDeletionMapReader
{
public:
    InMemDeletionMapReader(util::ExpandableBitmap* bitmap,
                           index_base::SegmentInfo* segmentInfo,
                           segmentid_t segId);
    ~InMemDeletionMapReader();

private:
    InMemDeletionMapReader(const InMemDeletionMapReader& bitmap);

public:
    bool IsDeleted(docid_t docId) const 
    {
        if (docId >= (docid_t)mSegmentInfo->docCount)
        {
            return true;
        }
        uint32_t currentDocCount = mBitmap->GetValidItemCount();
        if (docId >= (docid_t)currentDocCount)
        {
            return false;
        }
        return mBitmap->Test(docId); 
    }

    void Delete(docid_t docId)
    { mBitmap->Set(docId); }

    uint32_t GetDeletedDocCount() const 
    { return mBitmap->GetSetCount(); }

    segmentid_t GetInMemSegmentId() const
    { return mSegId; }

    util::ExpandableBitmap* GetBitmap() const { return mBitmap; }

private:
    util::ExpandableBitmap* mBitmap;
    index_base::SegmentInfo* mSegmentInfo;
    segmentid_t mSegId;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(InMemDeletionMapReader);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_IN_MEM_DELETION_MAP_READER_H
