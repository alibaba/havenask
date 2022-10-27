#ifndef INDEXLIB_UTIL_THREADPOOL_H
#define INDEXLIB_UTIL_THREADPOOL_H

#include <tr1/memory>
#include <autil/Thread.h>
#include <autil/Lock.h>
#include <autil/WorkItem.h>
#include <autil/CircularQueue.h>
#include <vector>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/misc/exception.h"

IE_NAMESPACE_BEGIN(util);
class ThreadPool
{
public:
    enum {
        DEFAULT_THREADNUM = 4,
        DEFAULT_QUEUESIZE = 32
    };

    enum STOP_TYPE {
        STOP_THREAD_ONLY = 0,
        STOP_AND_CLEAR_QUEUE,
        STOP_AFTER_QUEUE_EMPTY,
        STOP_AND_CLEAR_QUEUE_IGNORE_EXCEPTION
    };
    
    enum ERROR_TYPE {
        ERROR_NONE = 0,
        ERROR_POOL_HAS_STOP,
        ERROR_POOL_ITEM_IS_NULL,
        ERROR_POOL_QUEUE_FULL,
    };
public:
    ThreadPool(const size_t threadNum = DEFAULT_THREADNUM,
               const size_t queueSize = DEFAULT_QUEUESIZE);
    ~ThreadPool();
    
private:
    ThreadPool(const ThreadPool&);
    ThreadPool& operator = (const ThreadPool&);

public:
    ERROR_TYPE PushWorkItem(autil::WorkItem *item, bool isBlocked = true);
    bool Start(const std::string &name);
    void Stop(STOP_TYPE stopType = STOP_AFTER_QUEUE_EMPTY);
    void CheckException();

    size_t GetItemCount() const;
    size_t GetThreadNum() const {return mThreadNum;}
    size_t GetQueueSize() const {return mQueueSize;}
    void ClearQueue();
    void ConsumItem(autil::WorkItem * item);
    void WaitFinish();

private:
    void Push(autil::WorkItem *item);
    void PushFront(autil::WorkItem *item);
    bool TryPopItem(autil::WorkItem* &);
    bool CreateThreads(const std::string &name);
    void WorkerLoop();
    void WaitQueueEmpty();
    void StopThread();
    //Attention: workItem destroy and drop should not has exception
    void DestroyItemIgnoreException(autil::WorkItem *item);
    void DropItemIgnoreException(autil::WorkItem *item);
    void IncreaseUnProcessedWorkItemCount()
    {
        autil::ScopedLock lock(mUnProcessedWorkItemCountLock);
        mUnProcessedWorkItemCount++;
    }

    void DecreaseUnProcessedWorkItemCount()
    {
        autil::ScopedLock lock(mUnProcessedWorkItemCountLock);
        mUnProcessedWorkItemCount--;
    }

private:
    size_t mThreadNum;
    size_t mQueueSize;
    autil::CircularQueue<autil::WorkItem *> mQueue;
    std::vector<autil::ThreadPtr> mThreads;
    mutable autil::ProducerConsumerCond mCond;
    volatile bool mPush;
    volatile bool mRun;
    volatile bool mHasException;
    bool mIsIOException;
    autil::ThreadMutex mUnProcessedWorkItemCountLock;
    volatile int64_t mUnProcessedWorkItemCount;
    misc::ExceptionBase mException;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ThreadPool);

IE_NAMESPACE_END(util);

#endif //INDEXLIB_UTIL_THREADPOOL_H
