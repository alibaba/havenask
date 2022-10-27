#include "indexlib/util/task_scheduler.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(util);
IE_LOG_SETUP(util, TaskScheduler);

bool TaskScheduler::DeclareTaskGroup(
    const string& taskGroupName, uint64_t intervalInMicroSeconds)
{
    ScopedLock lock(mLock); 
    auto it = mGroupMap.find(taskGroupName);
    
    if (it != mGroupMap.end())
    {
        int32_t groupIdx = it->second;
        if (mTaskGroups[groupIdx]->GetTimeInterval() != intervalInMicroSeconds)
        {
            IE_LOG(ERROR, "declare TaskGroup fail due to "
                   "different time interval setting");
            return false;
        }
        return true;
    }

    mTaskGroups.push_back(TaskGroupPtr(new TaskGroup(taskGroupName, intervalInMicroSeconds)));
    mGroupMap.insert(make_pair(taskGroupName, mTaskGroups.size() - 1));
    return true;
}

int32_t TaskScheduler::AddTask(
    const string& taskGroupName, const TaskItemPtr& taskItem)
{
    ScopedLock lock(mLock);
    auto it = mGroupMap.find(taskGroupName);
    if (it == mGroupMap.end())
    {
        return INVALID_TASK_ID;
    }
    int32_t groupIdx = it->second;
    int32_t taskId = mTaskIdCounter+1;

    if (!mTaskGroups[groupIdx]->AddTask(taskId, taskItem))
    {
        return INVALID_TASK_ID;
    }
    mTaskToGroupMap[taskId] = groupIdx;
    mTaskIdCounter++;
    mTaskCount++;
    return taskId;
}

bool TaskScheduler::DeleteTask(int32_t taskId)
{
    ScopedLock lock(mLock);
    auto it = mTaskToGroupMap.find(taskId);
    if (it == mTaskToGroupMap.end())
    {
        return false;
    }
    if (mTaskGroups[it->second]->DeleteTask(taskId))
    {
        mTaskCount--;
        return true;
    }
    return false;
}

void TaskScheduler::GetTaskGroupMetrics(
    vector<TaskGroupMetric>& taskGroupMetrics)
{
    map<std::string, size_t>::iterator iter = mGroupMap.begin();
    for(; iter != mGroupMap.end(); iter++)
    {
        TaskGroupMetric metric;
        metric.groupName = iter->first;
        metric.tasksExecuteUseTime = mTaskGroups[iter->second]->GetAllTaskExecuteTimeInterval();
        metric.executeTasksCount = mTaskGroups[iter->second]->GetOneLoopExecuteTaskCount();
        taskGroupMetrics.push_back(metric);
    }
}

IE_NAMESPACE_END(util);

