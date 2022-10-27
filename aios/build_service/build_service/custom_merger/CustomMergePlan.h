#ifndef ISEARCH_BS_CUSTOMMERGEPLAN_H
#define ISEARCH_BS_CUSTOMMERGEPLAN_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/custom_merger/CustomMergerTaskItem.h"

namespace build_service {
namespace custom_merger {

class CustomMergePlan : public autil::legacy::Jsonizable
{
public:
    struct TaskItem : public autil::legacy::Jsonizable{
        TaskItem()
            : taskIdx(-1)
        {}
        void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) 
        {
            json.Jsonize("task_item", taskItem, taskItem);
            json.Jsonize("segment_list", segments, segments);
            json.Jsonize("task_idx", taskIdx, taskIdx);
        }
        std::string getTaskCheckpointName() const
        { return std::string("custom_task_") + autil::StringUtil::toString(taskIdx); }
        CustomMergerTaskItem taskItem;
        std::vector<int32_t> segments;
        int32_t taskIdx;
    };
        
public:
    CustomMergePlan() 
        : _instanceId(-1)
        , _cost(0.0)
        , _taskCount(0)
    {}
    CustomMergePlan(int32_t instanceId);
    ~CustomMergePlan();
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void addTaskItem(int32_t segmentId, const CustomMergerTaskItem& taskItem, double cost);
    double getCost() const { return _cost; }
    int32_t getInstanceId() const { return _instanceId; }    
    void getTaskItems(std::vector<TaskItem>& tasks) const;
private:
    int32_t _instanceId;
    std::map<std::string, TaskItem> _taskItems;
    double _cost;
    int32_t _taskCount;
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(CustomMergePlan);

}
}

#endif //ISEARCH_BS_CUSTOMMERGEPLAN_H
