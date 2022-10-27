#include "indexlib/partition/segment/async_segment_dumper.h"
#include "indexlib/partition/segment/dump_segment_container.h"
#include "indexlib/partition/online_partition.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, AsyncSegmentDumper);

AsyncSegmentDumper::AsyncSegmentDumper(
    OnlinePartition* onlinePartition, autil::RecursiveThreadMutex& cleanerLock)
    : mOnlinePartition(onlinePartition)
    , mCleanerLock(cleanerLock)
    , mIsDumping(false)
    , mIsRunning(true)
{
    assert(onlinePartition);
}

AsyncSegmentDumper::~AsyncSegmentDumper() { mIsRunning = false; }

void AsyncSegmentDumper::TriggerDumpIfNecessary()
{
    if (mIsDumping)
    {
        return;
    }

    ScopedLock lock(mLock);
    mDumpSegmentThread.reset();
    const DumpSegmentContainerPtr& dumpSegContainer =
        mOnlinePartition->GetDumpSegmentContainer();
    if (!dumpSegContainer || dumpSegContainer->GetUnDumpedSegmentSize() == 0)
    {
        // no need dump
        return;
    }
    mIsDumping = true;
    mDumpSegmentThread = Thread::createThread(
            tr1::bind(&AsyncSegmentDumper::DumpSegmentThread, this), "indexAsyncFlush");
}

void AsyncSegmentDumper::DumpSegmentThread()
{
    while (mIsRunning)
    {
        if (mCleanerLock.trylock() != 0)
        {
            usleep(500 * 1000); // 500ms
            continue;
        }
        mOnlinePartition->FlushDumpSegmentInContainer();
        mCleanerLock.unlock();
        break;
    }
    mIsDumping = false;
}

IE_NAMESPACE_END(partition);

