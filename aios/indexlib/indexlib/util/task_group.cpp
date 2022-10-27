#include "indexlib/util/task_group.h"
#include <unistd.h>
#include <autil/TimeUtility.h>

using namespace std;
using namespace autil;

IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(util);
IE_LOG_SETUP(util, TaskGroup);


bool TaskGroup::AddTask(int32_t taskId, const TaskItemPtr& taskItem)
{
    ScopedLock lock(mLock);
    if (!mBackGroundThreadPtr)
    {
        mRunning = true;
        mBackGroundThreadPtr = Thread::createThread(
                tr1::bind(&TaskGroup::BackGroundThread, this), "indexTask");
    }

    if (!mBackGroundThreadPtr)
    {
        IE_LOG(ERROR, "create thread fail for taskGroup[%s]", mGroupName.c_str());
        return false;
    }
    
    if (mIdToTaskMap.find(taskId) != mIdToTaskMap.end())
    {
        IE_LOG(ERROR, "add task to TaskGroup[%s] failed due to duplicate taskID",
               mGroupName.c_str());
        return false;
    }
    
    size_t idx = 0;
    for (; idx < mTaskItems.size(); idx++)
    {
        if (!mTaskItems[idx])
        {
            break;
        }
    }

    if (idx == mTaskItems.size())
    {
        mTaskItems.push_back(taskItem);
    }
    else
    {
        mTaskItems[idx] = taskItem;
    }
    
    mIdToTaskMap[taskId] = idx;
    return true;
}

void TaskGroup::BackGroundThread()
{
    size_t currentIdx = 0;
    int64_t beginTime = 0;
    int64_t endTime = 0;
    uint32_t executeTaskCount = 0;
    while(mRunning)
    {
        if (currentIdx == 0)
        {
            usleep(mTimeInterval);
            beginTime = TimeUtility::currentTimeInMicroSeconds();
            executeTaskCount = 0;
        }
        
        {
            ScopedLock lock(mLock);
            if (mTaskItems[currentIdx])
            {
                mTaskItems[currentIdx]->Run();
                executeTaskCount++;
            }
            
            if (currentIdx + 1 == mTaskItems.size())
            {
                endTime = TimeUtility::currentTimeInMicroSeconds();
                mAllTaskExecutedTimeInterval = endTime - beginTime;
                mOneLoopExecuteTaskCount = executeTaskCount;
                executeTaskCount = 0;
                if (mAllTaskExecutedTimeInterval >= 10000000) //interval more than 10 second
                {
                    IE_LOG(WARN, "TaskGroup[%s] execute [%d] tasks use time [%ld us]"
                           " more than 10 seconds", mGroupName.c_str(),
                           mOneLoopExecuteTaskCount, mAllTaskExecutedTimeInterval);
                }
            }
            
            currentIdx = (currentIdx + 1) % mTaskItems.size();
        }
    }
}

bool TaskGroup::DeleteTask(int32_t taskId)
{
    ScopedLock lock(mLock);
    auto it = mIdToTaskMap.find(taskId);
    if (it == mIdToTaskMap.end())
    {
        return false;
    }

    size_t taskIdx = it->second;
    mTaskItems[taskIdx].reset();
    mIdToTaskMap.erase(it);
    return true;
}

IE_NAMESPACE_END(util);

