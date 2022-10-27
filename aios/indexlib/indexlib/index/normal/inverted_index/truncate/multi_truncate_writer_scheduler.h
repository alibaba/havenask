#ifndef __INDEXLIB_MULTI_TRUNCATE_WRITER_SCHEDULER_H
#define __INDEXLIB_MULTI_TRUNCATE_WRITER_SCHEDULER_H

#include <tr1/memory>
#include <autil/ThreadPool.h>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/truncate/truncate_writer_scheduler.h"
#include "indexlib/util/thread_pool.h"

IE_NAMESPACE_BEGIN(index);

class MultiTruncateWriterScheduler : public TruncateWriterScheduler
{
public:
    MultiTruncateWriterScheduler(uint32_t threadNum = autil::ThreadPool::DEFAULT_THREADNUM,
                                 uint32_t queueSize = autil::ThreadPool::DEFAULT_QUEUESIZE);
    ~MultiTruncateWriterScheduler();

public:
    void PushWorkItem(autil::WorkItem* workItem) override;
    void WaitFinished() override;
    uint32_t GetThreadCount() const { return mThreadPool.GetThreadNum(); }

private:
    util::ThreadPool mThreadPool;
    bool mStarted;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MultiTruncateWriterScheduler);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_MULTI_TRUNCATE_WRITER_SCHEDULER_H
