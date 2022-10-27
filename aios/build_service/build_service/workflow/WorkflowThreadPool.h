#ifndef ISEARCH_BS_WORKFLOWTHREADPOOL_H
#define ISEARCH_BS_WORKFLOWTHREADPOOL_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include <tr1/memory>
#include <autil/Thread.h>
#include <autil/Lock.h>
#include <autil/CircularQueue.h>

namespace build_service {
namespace workflow {
class WorkflowItemBase;

class WorkflowThreadPool
{
public:
    enum {
        DEFAULT_THREADNUM = 4,
        DEFAULT_QUEUESIZE = 1024
    };
    
public:
    WorkflowThreadPool(const size_t threadNum = DEFAULT_THREADNUM,
                       const size_t queueSize = DEFAULT_QUEUESIZE);
    ~WorkflowThreadPool();
    
private:
    WorkflowThreadPool(const WorkflowThreadPool&);
    WorkflowThreadPool& operator = (const WorkflowThreadPool&);
public:
    bool pushWorkItem(WorkflowItemBase *item);
    size_t getItemCount() const;
    size_t getThreadNum() const {return _threadNum;}
    size_t getQueueSize() const {return _queueSize;}
    size_t getMaxItemCount() const { return _maxItemCount; }
    bool start();
    bool start(const std::string &name);
private:
    bool createThreads(const std::string &name);
    void workerLoop();
    void stopThread();
    void clearQueue();
    WorkflowItemBase* popOneItem();
private:
    size_t _threadNum;
    size_t _queueSize;
    size_t _maxItemCount;
    autil::CircularQueue<WorkflowItemBase *> _queue;
    std::vector<autil::ThreadPtr> _threads;
    mutable autil::ThreadMutex _mutex;
    volatile bool _run;
    
private:
    static const int64_t DEFAULT_WAIT_INTERVAL = 1000 * 200; // 200ms
private:
    BS_LOG_DECLARE();
};

typedef std::tr1::shared_ptr<WorkflowThreadPool> WorkflowThreadPoolPtr;

}
}

#endif //__INDEXLIB_DUMP_THREAD_POOL_H
