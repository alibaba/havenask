#ifndef __INDEXLIB_MERGE_HELPER_H
#define __INDEXLIB_MERGE_HELPER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index_base/index_meta/segment_merge_info.h"
#include "indexlib/merger/merge_plan.h"

IE_NAMESPACE_BEGIN(merger);

class MergeHelper
{
public:
    MergeHelper();
    ~MergeHelper();
public:
    static void MakeSegmentMergeInfos(const std::string& str, 
            index_base::SegmentMergeInfos& segMergeInfos);
    static void MakeSegmentMergeInfosWithSegId(const std::string& str, 
            index_base::SegmentMergeInfos& segMergeInfos);
    static bool CheckMergePlan(std::string str, const MergePlan& plan);
    static bool CheckMergePlan(const MergePlan& plan,
                               const std::vector<segmentid_t>& inPlanIds,
                               const std::vector<size_t>& targetPosition);
    static std::pair<index_base::SegmentMergeInfos, index_base::LevelInfo>
        PrepareMergeInfo(const std::string& levelStr);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MergeHelper);

IE_NAMESPACE_END(merger);

#endif //__INDEXLIB_MERGE_HELPER_H
