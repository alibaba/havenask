#include "indexlib/common/dump_thread_pool.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_BEGIN(common);
IE_LOG_SETUP(common, DumpThreadPool);

DumpThreadPool::DumpThreadPool(const size_t threadNum, const size_t queueSize)
    : _threadNum(threadNum)
    , _queueSize(queueSize)
    , _queue(queueSize ? queueSize : DEFAULT_QUEUESIZE)
    , _push(true) 
    , _run(false)
    , _hasException(false)
    , _peakMemoryUse(0)
{
    if (_threadNum == 0) {
        IE_LOG(WARN, "Thread number is zero, use default value 4");
        _threadNum = DEFAULT_THREADNUM;
    }
    if (_queueSize == 0) {
        IE_LOG(WARN, "Queue size is zero, use default value 32");
        _queueSize = DEFAULT_QUEUESIZE;
    }
}

DumpThreadPool::~DumpThreadPool() {
    stop(STOP_AND_CLEAR_QUEUE_IGNORE_EXCEPTION);
}

size_t DumpThreadPool::getItemCount() const {
    ScopedLock lock(_cond);
    return _queue.size();
}

DumpThreadPool::ERROR_TYPE DumpThreadPool::pushWorkItem(DumpItem *item, bool isBlocked) {
    if (NULL == item) {
        IE_LOG(ERROR, "WorkItem is NULL");
        return ERROR_POOL_ITEM_IS_NULL;
    }
    
    if (_hasException)
    {
        item->drop();
        stop(STOP_AND_CLEAR_QUEUE);
        return ERROR_POOL_HAS_STOP;
    }

    if (!_push) {
        return ERROR_POOL_HAS_STOP;
    }
    
    ScopedLock lock(_cond);
    if (!isBlocked && _queue.size() >= _queueSize) {
        return ERROR_POOL_QUEUE_FULL;        
    }
    
    while (_push && _queue.size() >= _queueSize) { 
        _cond.producerWait();
    }
    
    if (!_push) {
        return ERROR_POOL_HAS_STOP;
    }
    
    _queue.push_back(item);
    _cond.signalConsumer();
    return ERROR_NONE;
}

bool DumpThreadPool::start(const string& name) {
    if (_run) {
        IE_LOG(ERROR, "Already started!");
        return false;
    }

    _push = true;
    _run = true;
 
    if (createThreads(name)) {
        return true;
    } else {
        stop(STOP_THREAD_ONLY);
        return false;
    }
}

void DumpThreadPool::stop(STOP_TYPE stopType) 
{
    if (STOP_THREAD_ONLY != stopType) {
        {
            ScopedLock lock(_cond);
            _push = false;
            _cond.broadcastProducer();
        }
    }

    if (STOP_AFTER_QUEUE_EMPTY == stopType) {
        waitQueueEmpty();
    }
    
    stopThread();
    
    if (STOP_THREAD_ONLY != stopType) {
        clearQueue();
    }

    if (stopType != STOP_AND_CLEAR_QUEUE_IGNORE_EXCEPTION)
    {
        checkException();
    }
}

void DumpThreadPool::stopThread() {
    if (!_run) {
        return;
    }
    {
        ScopedLock lock(_cond);
        _run = false;
        _cond.broadcastConsumer();
    }
    _threads.clear();
}

void DumpThreadPool::waitQueueEmpty() {
    while (true && !_hasException) {
        if ((size_t)0 == getItemCount()) {
            break;
        }
        usleep(10000);
    }
}

bool DumpThreadPool::createThreads(const string& name) {
    size_t num = _threadNum;
    while (num--) {
        ThreadPtr tp = Thread::createThread(
                std::tr1::bind(&DumpThreadPool::workerLoop, this), name);
        _threads.push_back(tp);
        if (!tp) {
            IE_LOG(ERROR, "Create WorkerLoop thread fail!");
            return false;
        }
    }
    return true;
}

void DumpThreadPool::clearQueue() {
    ScopedLock lock(_cond);
    while(!_queue.empty()) {
        DumpItem *item = _queue.front();
        _queue.pop_front();
        if (item) {
            item->drop();
        }
    }
    _cond.broadcastProducer();
}

void DumpThreadPool::workerLoop() {
    util::SimplePool pool;
    while(_run && !_hasException) {
        DumpItem *item = NULL;        
        {
            ScopedLock lock(_cond);
            while(_run && _queue.empty()) {
                _cond.consumerWait();
            }
            
            if (!_run) {
                return;
            }

            item = _queue.front();
            _queue.pop_front();
            _cond.signalProducer();
        }
        
        if (item) {
            consumItem(item, &pool);
        }
        assert(pool.getUsedBytes() == 0);
    }
    size_t peakMemUse = pool.getPeakOfUsedBytes();
    IE_LOG(INFO, "dump thread memory pool used bytes peak is %lu", peakMemUse);
    ScopedLock lock(_cond);
    _peakMemoryUse += peakMemUse;
}

void DumpThreadPool::consumItem(DumpItem * item, util::SimplePool* pool)
{
    try
    {
        item->process(pool);
        item->destroy();
    }
    catch(const misc::ExceptionBase& e)
    {
        IE_LOG(ERROR, "dump thread exception:[%s].", e.what());
        try
        {
            item->destroy();
        }
        catch(...)
        {
            IE_LOG(ERROR, "dump item destroy exception ignore");
        }

        {
            ScopedLock lock(_cond);
            if (!_hasException)
            {
                _hasException = true;
                _exception = e;
            }
        }
    }
}

void DumpThreadPool::checkException()
{
    ScopedLock lock(_cond);    
    if (_hasException)
    {
        throw _exception;
    }
}

IE_NAMESPACE_END(common);

