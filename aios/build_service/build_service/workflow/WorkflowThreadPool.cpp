#include "build_service/workflow/WorkflowThreadPool.h"
#include "build_service/workflow/WorkflowItem.h"

using namespace std;
using namespace autil;

namespace build_service {
namespace workflow {

BS_LOG_SETUP(workflow, WorkflowThreadPool);

WorkflowThreadPool::WorkflowThreadPool(const size_t threadNum, const size_t queueSize)
    : _threadNum(threadNum)
    , _queueSize(queueSize)
    , _maxItemCount(0)
    , _queue(queueSize ? queueSize : DEFAULT_QUEUESIZE)
    , _run(false)
{
    if (_threadNum == 0) {
        BS_LOG(WARN, "Thread number is zero, use default value 4");
        _threadNum = DEFAULT_THREADNUM;
    }
    if (_queueSize == 0) {
        BS_LOG(WARN, "Queue size is zero, use default value 32");
        _queueSize = DEFAULT_QUEUESIZE;
    }
}

WorkflowThreadPool::~WorkflowThreadPool() {
    stopThread();
    clearQueue();
}

size_t WorkflowThreadPool::getItemCount() const {
    ScopedLock lock(_mutex);
    return _queue.size();
}

bool WorkflowThreadPool::pushWorkItem(WorkflowItemBase *item) {
    assert(item);
    if (!_run) {
        BS_LOG(ERROR, "pushWorkItem fail because thread pool is not running.");
        return false;
    }
    
    ScopedLock lock(_mutex);
    if (_queue.size() >= _queueSize) {
        BS_LOG(ERROR, "pushWorkItem fail because queue size [%lu] is full.", _queue.size());
        return false;
    }
    _queue.push_back(item);
    _maxItemCount = max(_maxItemCount, _queue.size());
    return true;
}

bool WorkflowThreadPool::start() {
    return start(string());
}

bool WorkflowThreadPool::start(const std::string &name) {
    if (_run) {
        BS_LOG(ERROR, "Already started!");
        return false;
    }

    _run = true;
    if (createThreads(name)) {
        return true;
    } 
    stopThread();
    return false;
}

void WorkflowThreadPool::stopThread() {
    if (!_run) {
        return;
    }
    _run = false;
    _threads.clear();
}

bool WorkflowThreadPool::createThreads(const std::string &name) {
    size_t num = _threadNum;
    while (num--) {
        ThreadPtr tp = Thread::createThread(
                std::tr1::bind(&WorkflowThreadPool::workerLoop, this), name);
        _threads.push_back(tp);
        if (!tp) {
            BS_LOG(ERROR, "Create WorkerLoop thread fail!");
            return false;
        }
    }
    return true;
}

void WorkflowThreadPool::clearQueue() {
    ScopedLock lock(_mutex);
    while(!_queue.empty()) {
        _queue.pop_front();
    }
}

WorkflowItemBase* WorkflowThreadPool::popOneItem() {
    WorkflowItemBase *item = NULL;
    ScopedLock lock(_mutex);
    if (!_queue.empty()) {
        item = _queue.front();
        _queue.pop_front();
    }
    return item;
}
void WorkflowThreadPool::workerLoop() {
    BS_LOG(INFO, "workerLoop thread [%ld] begin.", (int64_t)pthread_self());
    /*
      when accumulate failed count reach limit, sleep some time
     */
    uint32_t accumulateFailedCount = 0;
    FlowError fe = FE_OK;
    while (_run) {
        WorkflowItemBase *item = popOneItem();
        // queue is empty, sleep and continue
        if (!item) {
            usleep(DEFAULT_WAIT_INTERVAL);
            continue;
        }
        if (unlikely(!item->isRunning())) {
            item->drop();
            continue;
        }
        if (!item->isSuspended()) {
            item->process();
            fe = item->getFlowError();
            if (fe == FE_OK || fe == FE_SKIP) {
                // do not sleep on FE_OK or FE_SKIP EVENTS
                accumulateFailedCount = 0;
            }
            else{
                accumulateFailedCount++;
            }
        }
        else {
            accumulateFailedCount++;
        }
        uint32_t queueSize = 0;
        {
            ScopedLock lock(_mutex);
            _queue.push_back(item);
            queueSize = _queue.size();
        }
        if (accumulateFailedCount >= queueSize) {
            usleep(DEFAULT_WAIT_INTERVAL);
            accumulateFailedCount = 0;
        }
    }
    BS_LOG(INFO, "workerLoop thread [%ld] end.", (int64_t)pthread_self());
}

}
}
