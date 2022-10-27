#include "indexlib/merger/test/merge_task_maker.h"
#include <autil/StringTokenizer.h>
#include <autil/StringUtil.h>
#include <algorithm>

using namespace std;
using namespace autil;

IE_NAMESPACE_BEGIN(merger);

MergeTaskMaker::MergeTaskMaker() 
{
}

MergeTaskMaker::~MergeTaskMaker() 
{
}

void MergeTaskMaker::CreateMergeTask(const string& mergeTaskStr,
                                     MergeTask& mergeTask,
                                     const index_base::SegmentMergeInfos& segMergeInfos)
{
    StringTokenizer st(mergeTaskStr, ";", StringTokenizer::TOKEN_TRIM
                       | StringTokenizer::TOKEN_IGNORE_EMPTY);
    for (size_t i = 0; i < st.getNumTokens(); ++i)
    {
        MergePlan plan;
        StringTokenizer st2(st[i], ",", StringTokenizer::TOKEN_TRIM
                            | StringTokenizer::TOKEN_IGNORE_EMPTY);
        for (uint32_t j = 0; j < st2.getNumTokens(); j++)
        {
            segmentid_t segId;
            StringUtil::strToInt32(st2[j].c_str(), segId);
            auto iter = std::find_if(segMergeInfos.begin(), segMergeInfos.end(),
                [segId](const index_base::SegmentMergeInfo& segMergeInfo) {
                    return segMergeInfo.segmentId == segId;
                });
            if (iter == segMergeInfos.end())
            {
                index_base::SegmentMergeInfo segMergeInfo;
                segMergeInfo.segmentId = segId;
                plan.AddSegment(segMergeInfo);
            }
            else
            { 
                plan.AddSegment(*iter);
            }
        }
        mergeTask.AddPlan(plan);
    }
}

IE_NAMESPACE_END(merger);

