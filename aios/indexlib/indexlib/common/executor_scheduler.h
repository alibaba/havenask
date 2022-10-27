#ifndef __INDEXLIB_EXECUTOR_SCHEDULER_H
#define __INDEXLIB_EXECUTOR_SCHEDULER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/common/executor.h"

IE_NAMESPACE_BEGIN(common);

class ExecutorScheduler
{
public:
    enum SchedulerType { ST_REPEATEDLY, ST_ONCE };
public:
    ExecutorScheduler();
    ~ExecutorScheduler();

public:
    void Add(const ExecutorPtr& executor, SchedulerType type);
    void Execute();

    size_t GetRepeatedlyExecutorsCount()
    { return mRepeatedlyExecutors.size(); }
    size_t GetOnceExecutorsCount()
    { return mOnceExecutors.size(); }

private:
    std::vector<ExecutorPtr> mRepeatedlyExecutors;
    std::vector<ExecutorPtr> mOnceExecutors;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ExecutorScheduler);

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_EXECUTOR_SCHEDULER_H
