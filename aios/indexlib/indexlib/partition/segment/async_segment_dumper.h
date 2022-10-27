#ifndef __INDEXLIB_ASYNC_SEGMENT_DUMPER_H
#define __INDEXLIB_ASYNC_SEGMENT_DUMPER_H

#include <tr1/memory>
#include <autil/Thread.h>
#include <autil/Lock.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"

DECLARE_REFERENCE_CLASS(partition, OnlinePartition);

IE_NAMESPACE_BEGIN(partition);

class AsyncSegmentDumper
{
public:
    AsyncSegmentDumper(OnlinePartition* onlinePartition, autil::RecursiveThreadMutex& cleanerLock);
    ~AsyncSegmentDumper();
    
public:
    void TriggerDumpIfNecessary();

private:
    void DumpSegmentThread();
    
private:
    OnlinePartition* mOnlinePartition;
    autil::ThreadPtr mDumpSegmentThread;
    autil::ThreadMutex mLock;
    autil::RecursiveThreadMutex& mCleanerLock;
    volatile bool mIsDumping;
    volatile bool mIsRunning;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(AsyncSegmentDumper);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_ASYNC_SEGMENT_DUMPER_H
