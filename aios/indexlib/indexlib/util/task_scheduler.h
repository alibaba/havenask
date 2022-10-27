#ifndef __INDEXLIB_TASK_SCHEDULER_H
#define __INDEXLIB_TASK_SCHEDULER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/util/task_group.h"

IE_NAMESPACE_BEGIN(util);

class TaskScheduler
{
public:
    TaskScheduler()
        : mTaskIdCounter(0)
        , mTaskCount(0)
    {}
    virtual ~TaskScheduler()
    {
        mTaskGroups.clear();
        mTaskToGroupMap.clear();
        mGroupMap.clear();
    }
public:
    static const int32_t INVALID_TASK_ID = -1;
    
public:
    bool DeclareTaskGroup(const std::string& taskGroupName,
                          uint64_t intervalInMicroSeconds = 0);
    virtual int32_t AddTask(const std::string& taskGroupName, const TaskItemPtr& taskItem);
    virtual bool DeleteTask(int32_t taskId);

public:
    int32_t GetTaskCount() const { return mTaskCount; }
    struct TaskGroupMetric
    {
        std::string groupName;
        uint64_t tasksExecuteUseTime;
        uint32_t executeTasksCount;
    };
    
    void GetTaskGroupMetrics(std::vector<TaskGroupMetric>& taskGroupMetrics);
        
private: 
    std::map<std::string, size_t> mGroupMap;
    std::map<int32_t, size_t> mTaskToGroupMap;
    std::vector<TaskGroupPtr> mTaskGroups;
    mutable autil::ThreadMutex mLock;
    
    int32_t mTaskIdCounter;
    int32_t mTaskCount;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TaskScheduler);

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_TASK_SCHEDULER_H
