#include "indexlib/index/normal/inverted_index/truncate/multi_truncate_writer_scheduler.h"

using namespace std;

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, MultiTruncateWriterScheduler);

MultiTruncateWriterScheduler::MultiTruncateWriterScheduler(
        uint32_t threadNum, uint32_t queueSize) 
    : mThreadPool(threadNum, queueSize)
    , mStarted(false)
{
}

MultiTruncateWriterScheduler::~MultiTruncateWriterScheduler() 
{
}

void MultiTruncateWriterScheduler::PushWorkItem(autil::WorkItem* workItem)
{
    if (!mStarted)
    {
        mThreadPool.Start("indexTruncate");
        mStarted = true;
    }
    mThreadPool.PushWorkItem(workItem);
}

void MultiTruncateWriterScheduler::WaitFinished()
{
    if (mStarted)
    {
        mThreadPool.WaitFinish();
    }
}

IE_NAMESPACE_END(index);

