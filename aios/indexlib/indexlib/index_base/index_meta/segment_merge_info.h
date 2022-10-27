#ifndef __INDEXLIB_SEGMENT_MERGE_INFO_H
#define __INDEXLIB_SEGMENT_MERGE_INFO_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_base/index_meta/segment_metrics.h"

DECLARE_REFERENCE_CLASS(index, InMemorySegmentReader);

IE_NAMESPACE_BEGIN(index_base);

struct DocRangeInfo
{
    exdocid_t mBeginDocId;
    exdocid_t mEndDocId;
    DocRangeInfo() {
        mBeginDocId = 0;
        mEndDocId = 0;
    }
    DocRangeInfo(exdocid_t beginDocId, exdocid_t endDocId) {
        mBeginDocId = beginDocId;
        mEndDocId = endDocId;
    }
};

struct SegmentMergeInfo : public autil::legacy::Jsonizable
{
    SegmentMergeInfo()
        : segmentId(INVALID_SEGMENTID)
        , deletedDocCount(0)
        , baseDocId(0)
        , segmentSize(0)
        , levelIdx(0)
        , inLevelIdx(0)
    {
    }

    //Attention:
    //baseId should be the base doc id in segmentMergeInfos,
    // started from 0.
    SegmentMergeInfo(segmentid_t segId, const SegmentInfo& segInfo,
                     uint32_t delDocCount, exdocid_t baseId, 
                     int64_t segSize = 0, uint32_t levelId = 0,
                     uint32_t inLevelId = 0)
        : segmentId(segId)
        , deletedDocCount(delDocCount)
        , baseDocId(baseId)
        , segmentSize(segSize)
        , segmentInfo(segInfo)
        , levelIdx(levelId)
        , inLevelIdx(inLevelId)
    {
    }

    void Jsonize(JsonWrapper &json) override
    {
        json.Jsonize("segment_id", segmentId, segmentId);
        json.Jsonize("deleted_doc_count", deletedDocCount, deletedDocCount);
        json.Jsonize("base_doc_id", baseDocId, baseDocId);
        json.Jsonize("segment_size", segmentSize, segmentSize);
        json.Jsonize("segment_info", segmentInfo, segmentInfo);
        json.Jsonize("levelIdx", levelIdx);
        json.Jsonize("inLevelIdx", inLevelIdx);
        json.Jsonize("segment_metrics", segmentMetrics, segmentMetrics);
    }

    bool operator < (const SegmentMergeInfo& other) const
    {
        if (levelIdx != other.levelIdx)
        {
            return levelIdx > other.levelIdx;
        }
        return inLevelIdx < other.inLevelIdx;
    }

    segmentid_t segmentId;
    uint32_t deletedDocCount;
    exdocid_t baseDocId;
    int64_t segmentSize;
    SegmentInfo segmentInfo;
    uint32_t levelIdx;
    uint32_t inLevelIdx;
    SegmentMetrics segmentMetrics;
    //TODO: legacy for reclaim map unittest
    index::InMemorySegmentReaderPtr inMemorySegmentReader;
};

typedef std::vector<SegmentMergeInfo> SegmentMergeInfos;

IE_NAMESPACE_END(index_base);

#endif //__INDEXLIB_SEGMENT_MERGE_INFO_H
 
