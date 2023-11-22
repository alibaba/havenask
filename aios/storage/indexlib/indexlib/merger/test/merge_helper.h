#pragma once

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index_base/index_meta/segment_merge_info.h"
#include "indexlib/indexlib.h"
#include "indexlib/merger/merge_plan.h"

namespace indexlib { namespace merger {

class MergeHelper
{
public:
    MergeHelper();
    ~MergeHelper();

public:
    static void MakeSegmentMergeInfos(const std::string& str, index_base::SegmentMergeInfos& segMergeInfos);
    static void MakeSegmentMergeInfosWithSegId(const std::string& str, index_base::SegmentMergeInfos& segMergeInfos);
    static bool CheckMergePlan(std::string str, const MergePlan& plan);
    static bool CheckMergePlan(const MergePlan& plan, const std::vector<segmentid_t>& inPlanIds,
                               const std::vector<size_t>& targetPosition);
    static std::pair<index_base::SegmentMergeInfos, indexlibv2::framework::LevelInfo>
    PrepareMergeInfo(const std::string& levelStr);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MergeHelper);
}} // namespace indexlib::merger
