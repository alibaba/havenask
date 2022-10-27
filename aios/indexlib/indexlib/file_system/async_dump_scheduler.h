#ifndef __INDEXLIB_ASYNC_DUMP_SCHEDULER_H
#define __INDEXLIB_ASYNC_DUMP_SCHEDULER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/file_system/dump_scheduler.h"
#include <autil/Thread.h>
#include <autil/SynchronizedQueue.h>

IE_NAMESPACE_BEGIN(file_system);

class AsyncDumpScheduler : public DumpScheduler
{
public:
    AsyncDumpScheduler();
    ~AsyncDumpScheduler();
public:
    void Push(const DumpablePtr& dumpTask) override;
    void WaitFinished() override;
    bool HasDumpTask() override;
    void WaitDumpQueueEmpty() override;
    void SetDumpQueueCapacity(uint32_t capacity);

private:
    void Dump(const DumpablePtr& dumpTask);
    void DumpThread();

private:
    volatile bool mRunning;
    volatile bool mHadException;
    std::string mExceptionClassName;
    autil::ThreadPtr mDumpThread;
    autil::SynchronizedQueue<DumpablePtr> mDumpQueue;
    autil::ThreadCond mQueueEmptyCond;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(AsyncDumpScheduler);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_ASYNC_DUMP_SCHEDULER_H
