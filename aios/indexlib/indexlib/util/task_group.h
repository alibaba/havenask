#ifndef __INDEXLIB_TASK_GROUP_H
#define __INDEXLIB_TASK_GROUP_H

#include <tr1/memory>
#include <autil/Lock.h>
#include <autil/Thread.h>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/util/task_item.h"

IE_NAMESPACE_BEGIN(util);

class TaskGroup
{
public:
    TaskGroup(const std::string& groupName, uint64_t intervalInMicroSeconds)
        : mGroupName(groupName)
        , mTimeInterval(intervalInMicroSeconds)
        , mRunning(false)
        , mAllTaskExecutedTimeInterval(0)
        , mOneLoopExecuteTaskCount(0)
    {}
    
    ~TaskGroup()
    {
        mRunning = false;
        mBackGroundThreadPtr.reset();
        mIdToTaskMap.clear();
        mTaskItems.clear();
    }

public:
    bool AddTask(int32_t taskId, const TaskItemPtr& taskItem);

    bool DeleteTask(int32_t taskId);

    uint64_t GetTimeInterval() const {return mTimeInterval;}
    uint64_t GetAllTaskExecuteTimeInterval() const { return mAllTaskExecutedTimeInterval; }
    uint64_t GetOneLoopExecuteTaskCount() const { return mOneLoopExecuteTaskCount; }
    
private:
    void BackGroundThread();
    void ProcessItem(int32_t idx);
private:
    std::string mGroupName;
    uint64_t mTimeInterval;
    mutable autil::ThreadMutex mLock;
    std::map<int32_t, size_t> mIdToTaskMap;
    std::vector<TaskItemPtr> mTaskItems;
    autil::ThreadPtr mBackGroundThreadPtr;
    bool mRunning;
    uint64_t mAllTaskExecutedTimeInterval;
    uint32_t mOneLoopExecuteTaskCount;
    
private:
    IE_LOG_DECLARE();
    friend class TaskGroupTest;
};

DEFINE_SHARED_PTR(TaskGroup);

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_TASK_GROUP_H
