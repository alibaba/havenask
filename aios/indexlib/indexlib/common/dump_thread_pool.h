#ifndef __INDEXLIB_DUMP_THREAD_POOL_H
#define __INDEXLIB_DUMP_THREAD_POOL_H

#include <tr1/memory>
#include <autil/Thread.h>
#include <autil/Lock.h>
#include <autil/CircularQueue.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/common/dump_item.h"
#include "indexlib/misc/exception.h"
#include "indexlib/util/simple_pool.h"

IE_NAMESPACE_BEGIN(common);

class DumpThreadPool
{
    // TODO : change code style
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
    DumpThreadPool(const size_t threadNum = DEFAULT_THREADNUM,
               const size_t queueSize = DEFAULT_QUEUESIZE);
    ~DumpThreadPool();
    
private:
    DumpThreadPool(const DumpThreadPool&);
    DumpThreadPool& operator = (const DumpThreadPool&);
public:
    ERROR_TYPE pushWorkItem(DumpItem *item, bool isBlocked = true);
    size_t getItemCount() const;
    size_t getThreadNum() const {return _threadNum;}
    size_t getQueueSize() const {return _queueSize;}
    bool start(const std::string& name);
    void stop(STOP_TYPE stopType = STOP_AFTER_QUEUE_EMPTY);
    void clearQueue();
    size_t getPeakOfMemoryUse() const {return _peakMemoryUse;}
private:
    void push(DumpItem *item);
    void pushFront(DumpItem *item);
    bool tryPopItem(DumpItem* &);
    bool createThreads(const std::string& name);
    void workerLoop();
    void waitQueueEmpty();
    void stopThread();
    void consumItem(DumpItem * item, util::SimplePool* pool);
    void checkException();
private:
    size_t _threadNum;
    size_t _queueSize;
    autil::CircularQueue<DumpItem *> _queue;
    std::vector<autil::ThreadPtr> _threads;
    mutable autil::ProducerConsumerCond _cond;
    volatile bool _push;
    volatile bool _run;
    volatile bool _hasException;
    misc::ExceptionBase _exception;
    size_t _peakMemoryUse;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DumpThreadPool);

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_DUMP_THREAD_POOL_H
