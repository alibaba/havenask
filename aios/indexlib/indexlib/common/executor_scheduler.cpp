#include "indexlib/common/executor_scheduler.h"

using namespace std;

IE_NAMESPACE_BEGIN(common);
IE_LOG_SETUP(common, ExecutorScheduler);

ExecutorScheduler::ExecutorScheduler() 
{
}

ExecutorScheduler::~ExecutorScheduler() 
{
}

void ExecutorScheduler::Add(const ExecutorPtr& executor, SchedulerType type)
{
    if (type == ST_REPEATEDLY)
    {
        mRepeatedlyExecutors.push_back(executor);
    }
    else if (type == ST_ONCE)
    {
        mOnceExecutors.push_back(executor);
    }
}

void ExecutorScheduler::Execute()
{
    for (size_t i = 0; i < mRepeatedlyExecutors.size(); ++i)
    {
        mRepeatedlyExecutors[i]->Execute();
    }

    for (size_t i = 0; i < mOnceExecutors.size(); ++i)
    {
        mOnceExecutors[i]->Execute();
    }
    mOnceExecutors.clear();
}

IE_NAMESPACE_END(common);

