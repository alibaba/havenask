#include "indexlib/misc/exception.h"
#include "indexlib/file_system/async_dump_scheduler.h"
#include <beeper/beeper.h>

using namespace std;
using namespace std::tr1;
using namespace autil;
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, AsyncDumpScheduler);

AsyncDumpScheduler::AsyncDumpScheduler() 
    : mHadException(false)
{
    SetDumpQueueCapacity(1);
    mRunning = true;
    mDumpThread = Thread::createThread(tr1::bind(&AsyncDumpScheduler::DumpThread, this), "indexAsyncDump");
}

AsyncDumpScheduler::~AsyncDumpScheduler() 
{
    WaitFinished();
}

void AsyncDumpScheduler::Push(const DumpablePtr& dumpTask)
{
    mDumpQueue.push(dumpTask);
}

void AsyncDumpScheduler::WaitFinished()
{
    mRunning = false;
    mDumpQueue.wakeup();
    mDumpThread->join();
}

void AsyncDumpScheduler::SetDumpQueueCapacity(uint32_t capacity)
{
    mDumpQueue.setCapacity(capacity);
}

void AsyncDumpScheduler::Dump(const DumpablePtr& dumpTask)
{
    if (mHadException)
    {
        IE_LOG(ERROR, "Catch a exception before, drop this dump task");
        return;
    }
    try
    {
        dumpTask->Dump();
    }
    catch (const autil::legacy::ExceptionBase &e)
    {
        IE_LOG(ERROR, "Catch exception: %s", e.what());
        mHadException = true;
        mExceptionClassName = e.GetClassName();
        beeper::EventTags tags;        
        BEEPER_FORMAT_REPORT("bs_worker_error", tags,
                             "AsyncDump catch exception: %s", e.what());
    }
    catch (...)
    {
        IE_LOG(ERROR, "Catch unknown exception");
        mHadException = true;
        BEEPER_REPORT("bs_worker_error", "AsyncDump catch unknown exception");
    }
}

void AsyncDumpScheduler::DumpThread()
{
    while (mRunning || !mDumpQueue.isEmpty())
    {
        while (mDumpQueue.isEmpty() && mRunning)
        {
            mDumpQueue.waitNotEmpty();
        }
        while (!mDumpQueue.isEmpty())
        {
            DumpablePtr dumpTask = mDumpQueue.getFront();
            Dump(dumpTask);

            mQueueEmptyCond.lock();
            mDumpQueue.popFront();
            mQueueEmptyCond.signal();
            mQueueEmptyCond.unlock();
        }
    }
}

bool AsyncDumpScheduler::HasDumpTask()
{
    return !mDumpQueue.isEmpty();
}

void AsyncDumpScheduler::WaitDumpQueueEmpty()
{
    mQueueEmptyCond.lock();
    while(!mDumpQueue.isEmpty())
    {
        mQueueEmptyCond.wait(100000);
    }
    mQueueEmptyCond.unlock();
    if (mHadException)
    {
        if (mExceptionClassName == "NonExistException")
        {
            INDEXLIB_FATAL_ERROR(NonExist, "dump catch exception");
        }
        else if (mExceptionClassName == "ExistException")
        {
            INDEXLIB_FATAL_ERROR(Exist, "dump catch exception");
        }

        INDEXLIB_FATAL_ERROR(FileIO, "dump catch exception");
    }
}


IE_NAMESPACE_END(file_system);

