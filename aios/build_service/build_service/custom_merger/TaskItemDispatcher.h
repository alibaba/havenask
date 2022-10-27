#ifndef ISEARCH_BS_TASKITEMDISPATCHER_H
#define ISEARCH_BS_TASKITEMDISPATCHER_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/custom_merger/CustomMerger.h"
#include "build_service/custom_merger/CustomMergeMeta.h"
#include <indexlib/config/index_partition_schema.h>
#include <queue>

namespace build_service {
namespace custom_merger {

class TaskItemDispatcher
{
public:
    struct SegmentInfo
    {
        int32_t segmentId;
        size_t docCount;
    };

private:
    struct DispatchItem {
        double cost;
        int32_t segmentId;
        CustomMergerTaskItem taskItem;
        int32_t taskItemIdx;
    };
    class MergePlanComparator
    {
    public:
        bool operator() (const CustomMergePlanPtr& left,
                         const CustomMergePlanPtr& right)
        { return left->getCost() > right->getCost(); }
    };
    typedef std::priority_queue<CustomMergePlanPtr, std::vector<CustomMergePlanPtr>,
                                MergePlanComparator > MergePlanHeap;
public:
    TaskItemDispatcher();
    ~TaskItemDispatcher();
public:
    static void DispatchTasks(const std::vector<SegmentInfo>& segmentInfos,
                              const std::vector<CustomMergerTaskItem>& taskItems,
                              uint32_t totalInstanceCount,
                              CustomMergeMeta& mergeMeta);
private:
    TaskItemDispatcher(const TaskItemDispatcher &);
    TaskItemDispatcher& operator=(const TaskItemDispatcher &);
public:

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(TaskItemDispatcher);

}
}

#endif //ISEARCH_BS_TASKITEMDISPATCHER_H
