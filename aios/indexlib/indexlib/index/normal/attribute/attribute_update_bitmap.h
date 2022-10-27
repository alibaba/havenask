#ifndef __INDEXLIB_ATTRIBUTE_UPDATE_BITMAP_H
#define __INDEXLIB_ATTRIBUTE_UPDATE_BITMAP_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/util/simple_pool.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/segment_update_bitmap.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index/normal/attribute/attribute_update_info.h"

DECLARE_REFERENCE_CLASS(util, BuildResourceMetrics);
DECLARE_REFERENCE_CLASS(util, BuildResourceMetricsNode);
IE_NAMESPACE_BEGIN(index);

class AttributeUpdateBitmap
{
private:
    struct SegmentBaseDocIdInfo
    {
        SegmentBaseDocIdInfo()
            : segId(INVALID_SEGMENTID)
            , baseDocId(INVALID_DOCID)
        {}
    
        SegmentBaseDocIdInfo(segmentid_t segmentId, docid_t docId)
            : segId(segmentId)
            , baseDocId(docId)
        {}
    
        segmentid_t segId;
        docid_t baseDocId;
    };

    typedef std::vector<SegmentBaseDocIdInfo> SegBaseDocIdInfoVect;
    typedef std::vector<SegmentUpdateBitmapPtr> SegUpdateBitmapVec;
    
public:
    AttributeUpdateBitmap()
        : mTotalDocCount(0)
        , mBuildResourceMetricsNode(NULL)
    {}
    
    ~AttributeUpdateBitmap() {}
    
public:
    void Init(const index_base::PartitionDataPtr& partitionData,
              util::BuildResourceMetrics* buildResourceMetrics = NULL);


    void Set(docid_t globalDocId);

    SegmentUpdateBitmapPtr GetSegmentBitmap(segmentid_t segId)
    {
        for (size_t i = 0; i < mDumpedSegmentBaseIdVec.size(); ++i)
        {
            if (segId == mDumpedSegmentBaseIdVec[i].segId)
            {
                return mSegUpdateBitmapVec[i];
            }
        }
        return SegmentUpdateBitmapPtr();
    }

    void Dump(const file_system::DirectoryPtr& dir) const;
    // void Dump(const std::string& dir) const;

private:
    int32_t GetSegmentIdx(docid_t globalDocId) const;
    AttributeUpdateInfo GetUpdateInfoFromBitmap() const;
    
private:
    util::SimplePool mPool;
    SegBaseDocIdInfoVect mDumpedSegmentBaseIdVec;
    SegUpdateBitmapVec mSegUpdateBitmapVec;
    docid_t mTotalDocCount;
    util::BuildResourceMetricsNode* mBuildResourceMetricsNode;

private:
    IE_LOG_DECLARE();
};

inline int32_t AttributeUpdateBitmap::GetSegmentIdx(docid_t globalDocId) const
{
    if (globalDocId >= mTotalDocCount)
    {
        return -1;
    }
    
    size_t i = 1;
    for (; i < mDumpedSegmentBaseIdVec.size(); ++i)
    {
        if (globalDocId < mDumpedSegmentBaseIdVec[i].baseDocId)
        {
            break;
        }
    }
    return int32_t(i)-1;
}

DEFINE_SHARED_PTR(AttributeUpdateBitmap);
IE_NAMESPACE_END(index);

#endif //__INDEXLIB_ATTRIBUTE_UPDATE_BITMAP_H
