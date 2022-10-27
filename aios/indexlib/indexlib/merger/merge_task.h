#ifndef __INDEXLIB_MERGE_TASK_H
#define __INDEXLIB_MERGE_TASK_H

#include <tr1/memory>
#include <vector>
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

#include "indexlib/merger/merge_plan.h"
#include "indexlib/index_base/index_meta/version.h"

IE_NAMESPACE_BEGIN(merger);

class MergeTask
{
public:
    MergeTask();
    ~MergeTask();

public:
    void AddPlan(const MergePlan& item)
    {
        mMergePlans.push_back(item);
    }

    const MergePlan& operator[] (size_t index) const
    {
        return mMergePlans[index];
    }

    MergePlan& operator[] (size_t index) 
    {
        return mMergePlans[index];
    }

    size_t GetPlanCount() const
    {
        return mMergePlans.size();
    }

private:
    std::vector<MergePlan> mMergePlans;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MergeTask);

IE_NAMESPACE_END(merger);

#endif //__INDEXLIB_MERGE_PLAN_H
