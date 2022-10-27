#include "indexlib/util/thread_pool.h"
#include <unistd.h>

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(util);
IE_LOG_SETUP(util, ThreadPool);

ThreadPool::ThreadPool(const size_t threadNum, const size_t queueSize)
    : mThreadNum(threadNum)
    , mQueueSize(queueSize)
    , mQueue(queueSize ? queueSize : DEFAULT_QUEUESIZE)
    , mPush(true) 
    , mRun(false)
    , mHasException(false)
    , mIsIOException(false)
    , mUnProcessedWorkItemCount(0)
{
    if (mThreadNum == 0) 
    {
        IE_LOG(WARN, "Thread number is zero, use default value 4");
        mThreadNum = DEFAULT_THREADNUM;
    }
    if (mQueueSize == 0) 
    {
        IE_LOG(WARN, "Queue size is zero, use default value 32");
        mQueueSize = DEFAULT_QUEUESIZE;
    }
}

ThreadPool::~ThreadPool() 
{
    Stop(STOP_AND_CLEAR_QUEUE_IGNORE_EXCEPTION);
}

size_t ThreadPool::GetItemCount() const 
{
    ScopedLock lock(mCond);
    return mQueue.size();
}

void ThreadPool::CheckException()
{
    ScopedLock lock(mCond); 
    if (mHasException)
    {
        if (mIsIOException)
        {
            INDEXLIB_FATAL_ERROR(FileIO, "%s", mException.GetMessage().c_str());
        }
        throw mException;
    }
}

void ThreadPool::WaitFinish()
{
    while (true && !mHasException) 
    {
        if (mUnProcessedWorkItemCount == 0)
        {
            break;
        }
        usleep(10000);
    }

    if (mHasException)
    {
        Stop(STOP_AND_CLEAR_QUEUE);
    }
}

ThreadPool::ERROR_TYPE ThreadPool::PushWorkItem(WorkItem *item, bool isBlocked) 
{
    if (NULL == item) 
    {
        IE_LOG(ERROR, "WorkItem is NULL");
        return ERROR_POOL_ITEM_IS_NULL;
    }    

    if (mHasException)
    {
        DropItemIgnoreException(item);
        Stop(STOP_AND_CLEAR_QUEUE);
        return ERROR_POOL_HAS_STOP;
    }

    if (!mPush) 
    {
        return ERROR_POOL_HAS_STOP;
    }
    
    ScopedLock lock(mCond);
    if (!isBlocked && mQueue.size() >= mQueueSize) 
    {
        return ERROR_POOL_QUEUE_FULL;        
    }
    
    while (mPush && mQueue.size() >= mQueueSize) 
    { 
        mCond.producerWait();
    }
    
    if (!mPush) 
    {
        return ERROR_POOL_HAS_STOP;
    }

    IncreaseUnProcessedWorkItemCount();
    mQueue.push_back(item);
    mCond.signalConsumer();
    return ERROR_NONE;
}

bool ThreadPool::Start(const std::string& name) 
{
    if (mRun) 
    {
        IE_LOG(ERROR, "Already started!");
        return false;
    }

    mPush = true;
    mRun = true;
 
    if (CreateThreads(name)) 
    {
        return true;
    }
    else 
    {
        Stop(STOP_THREAD_ONLY);
        return false;
    }
}

void ThreadPool::Stop(STOP_TYPE stopType) 
{
    if (STOP_THREAD_ONLY != stopType) 
    {
        {
            ScopedLock lock(mCond);
            mPush = false;
            mCond.broadcastProducer();
        }
    }

    if (STOP_AFTER_QUEUE_EMPTY == stopType) 
    {
        WaitQueueEmpty();
    }
    
    StopThread();
    
    if (STOP_THREAD_ONLY != stopType) 
    {
        ClearQueue();
    }

    if (stopType != STOP_AND_CLEAR_QUEUE_IGNORE_EXCEPTION)
    {
        CheckException();
    }
}

void ThreadPool::StopThread() 
{
    if (!mRun) 
    {
        return;
    }
    {
        ScopedLock lock(mCond);
        mRun = false;
        mCond.broadcastConsumer();
    }
    mThreads.clear();
}

void ThreadPool::WaitQueueEmpty() 
{
    while (true && !mHasException) 
    {
        if ((size_t)0 == GetItemCount()) 
        {
            break;
        }
        usleep(10000);
    }
}

bool ThreadPool::CreateThreads(const string& name) 
{
    size_t num = mThreadNum;
    while (num--) 
    {
        ThreadPtr tp = Thread::createThread(
                std::tr1::bind(&ThreadPool::WorkerLoop, this), name);
        mThreads.push_back(tp);
        if (!tp) 
        {
            IE_LOG(ERROR, "Create WorkerLoop thread fail!");
            return false;
        }
    }
    return true;
}

void ThreadPool::ClearQueue() 
{
    ScopedLock lock(mCond);
    while(!mQueue.empty()) 
    {
        WorkItem *item = mQueue.front();
        mQueue.pop_front();
        if (item) 
        {
            DropItemIgnoreException(item);
        }
    }
    mCond.broadcastProducer();
}

void ThreadPool::WorkerLoop() 
{
    while(mRun && !mHasException) 
    {
        WorkItem *item = NULL;        
        {
            ScopedLock lock(mCond);
            while(mRun && mQueue.empty()) 
            {
                mCond.consumerWait();
            }
            
            if (!mRun) 
            {
                return;
            }

            item = mQueue.front();
            mQueue.pop_front();
            mCond.signalProducer();
        }

        if (item) 
        {
            ConsumItem(item);
        }
        DecreaseUnProcessedWorkItemCount();
    }
}

void ThreadPool::ConsumItem(WorkItem * item)
{
    try
    {
        item->process();
        item->destroy();
    }
    catch(const misc::ExceptionBase& e)
    {
        IE_LOG(ERROR, "thread exception:[%s].", e.what());
        DestroyItemIgnoreException(item);
        {
            ScopedLock lock(mCond);
            if (!mHasException)
            {
                mHasException = true;
                mException = e;
                mIsIOException = (typeid(e) == typeid(misc::FileIOException));
            }
        }
    }
}

void ThreadPool::DestroyItemIgnoreException(autil::WorkItem *item)
{
    try
    {
        item->destroy();
    }
    catch(...)
    {
        IE_LOG(ERROR, "work item destroy exception ignore");
    }
}

void ThreadPool::DropItemIgnoreException(autil::WorkItem *item)
{
    try
    {
        item->drop();
    }
    catch(...)
    {
        IE_LOG(ERROR, "work item drop exception ignore");
    }
}

IE_NAMESPACE_END(util);
