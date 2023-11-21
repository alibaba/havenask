#pragma once

#include <memory>

#include "autil/StringUtil.h"
#include "indexlib/common_define.h"
#include "indexlib/config/build_config.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_base/segment/building_segment_data.h"
#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/memory_control/MemoryQuotaControllerCreator.h"

namespace indexlib { namespace partition {

class SegmentIteratorHelper
{
public:
    SegmentIteratorHelper() {}
    ~SegmentIteratorHelper() {}

public:
    static void PrepareBuiltSegmentDatas(const std::string& inMemInfoStr,
                                         std::vector<index_base::SegmentData>& segmentDatas)
    {
        std::vector<std::vector<int32_t>> segmentInfoVec;
        autil::StringUtil::fromString(inMemInfoStr, segmentInfoVec, ":", ";");

        for (size_t i = 0; i < segmentInfoVec.size(); i++) {
            assert(segmentInfoVec[i].size() == 2);
            index_base::SegmentData segData;
            segData.SetSegmentId(segmentInfoVec[i][0]);

            index_base::SegmentInfo segInfo;
            segInfo.docCount = segmentInfoVec[i][1];
            segData.SetSegmentInfo(segInfo);
            segmentDatas.push_back(segData);
        }
    }

    static void PrepareInMemSegments(const std::string& inMemInfoStr,
                                     std::vector<index_base::InMemorySegmentPtr>& inMemSegments)
    {
        std::vector<std::vector<int32_t>> segmentInfoVec;
        autil::StringUtil::fromString(inMemInfoStr, segmentInfoVec, ":", ";");

        util::BlockMemoryQuotaControllerPtr memController =
            util::MemoryQuotaControllerCreator::CreateBlockMemoryController();
        for (size_t i = 0; i < segmentInfoVec.size(); i++) {
            assert(segmentInfoVec[i].size() == 2);
            index_base::BuildingSegmentData segData;
            segData.SetSegmentId(segmentInfoVec[i][0]);

            index_base::SegmentInfo segInfo;
            segInfo.docCount = segmentInfoVec[i][1];
            segData.SetSegmentInfo(segInfo);

            config::BuildConfig buildConfig;
            index_base::InMemorySegmentPtr inMemSegment(
                new index_base::InMemorySegment(buildConfig, memController, util::CounterMapPtr()));
            inMemSegment->Init(segData, false);
            inMemSegments.push_back(inMemSegment);
        }
    }

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SegmentIteratorHelper);
}} // namespace indexlib::partition
