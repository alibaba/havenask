#ifndef ISEARCH_BS_WORKFLOWITEM_H
#define ISEARCH_BS_WORKFLOWITEM_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/workflow/Producer.h"
#include "build_service/workflow/Consumer.h"

namespace build_service {
namespace workflow {

enum WorkflowStatus {
    WFS_START,
    WFS_STOPPING,
    WFS_STOPPED,
};

class WorkflowItemBase {
public:
    WorkflowItemBase()
        : _running(false)
        , _finished(false)
        , _suspended(false)
        , _hasFatalError(false)
        , _fe(FE_OK)
        , _status(WFS_STOPPED)
    {}
    
    virtual ~WorkflowItemBase() {}
public:
    bool isSuspended() const {
        return _suspended;
    }
    bool isRunning() const {
        return _running;
    }
    FlowError getFlowError() const {
        return _fe;
    }
    void suspend() {
        _suspended = true;
    }
    void resume() {
        _suspended = false;
    }
    bool isFinished() const {
        return _finished;
    }
    bool hasFatalError() const {
        return _hasFatalError;
    }
    
public:
    virtual void process() = 0;
    virtual void drop() = 0;
    
protected:
    volatile bool _running;
    volatile bool _finished;
    volatile bool _suspended;
    volatile bool _hasFatalError;
    volatile FlowError _fe;
    volatile WorkflowStatus _status;
};

enum WorkflowMode {
    JOB = 0,
    SERVICE = 1,
    REALTIME = 2,
};

template <typename T>
class WorkflowItem : public WorkflowItemBase
{
public:
    WorkflowItem(Producer<T> *producer, Consumer<T> *consumer);
    ~WorkflowItem();
private:
    WorkflowItem(const WorkflowItem &);
    WorkflowItem& operator=(const WorkflowItem &);
    
public:
    bool start(WorkflowMode mode = JOB);
    void stop();
    void drop() override;
    void process() override;
    
public:
    Producer<T> *getProducer() const {
        return _producer;
    }
    Consumer<T> *getConsumer() const {
        return _consumer;
    }
    const common::Locator &getStopLocator() const {
        return _stopLocator;
    }

private:
    void handleError(FlowError fe);
    void setFatalError();
    
private:
    WorkflowMode _mode;
    Producer<T> *_producer;
    Consumer<T> *_consumer;
    common::Locator _stopLocator;
    
private:
    static const int64_t DEFAULT_RETRY_INTERVAL = 200 * 1000; // 200 ms
    static const int64_t WAIT_STOP_SLEEP_TIME = 10 * 1000; // 10 ms
private:
    BS_LOG_DECLARE();
};

BS_LOG_SETUP_TEMPLATE(workflowitem, WorkflowItem, T);


template<typename T>
WorkflowItem<T>::WorkflowItem(Producer<T> *producer, Consumer<T> *consumer)
    : _mode(JOB)
    , _producer(producer)
    , _consumer(consumer)
{
}

template<typename T>
WorkflowItem<T>::~WorkflowItem() {
    stop();
    DELETE_AND_SET_NULL(_producer);
    DELETE_AND_SET_NULL(_consumer);
}

template<typename T>
bool WorkflowItem<T>::start(WorkflowMode mode) {
    _running = true;
    _status = WFS_START;
    _mode = mode;
    return true;
}

template<typename T>
void WorkflowItem<T>::stop() {
    if (!_running && _status == WFS_STOPPED) {
        BS_LOG(INFO, "workflow already stop.");
        return;
    }
    
    BS_LOG(INFO, "stop workflow begin.");
    _running = false;
    while (_status != WFS_STOPPED) {
        usleep(WAIT_STOP_SLEEP_TIME);
    }
    BS_LOG(INFO, "stop workflow end.");
}

template<typename T>
void WorkflowItem<T>::process() {
    T item;
    while (_running && (_fe = _producer->produce(item)) != FE_OK) {
        handleError(_fe);
        return;
    }
    while (_running && (_fe = _consumer->consume(item)) != FE_OK) {
        handleError(_fe);
    }
}

template<typename T>
void WorkflowItem<T>::drop() {
    assert(!_running);
    _status = WFS_STOPPING;
    if (FE_EOF == _fe && !_consumer->getLocator(_stopLocator)) {
        std::string errorMsg = "getLocator from consumer" + std::string(typeid(*_producer).name()) + " failed!";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        _hasFatalError = true;
    }

    if (!_producer->stop()) {
        std::string errorMsg = "stop producer " + std::string(typeid(*_producer).name()) + " failed!";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        _hasFatalError = true;
    }
    if (!_consumer->stop(_fe)) {
        std::string errorMsg = "stop consumer " + std::string(typeid(*_producer).name()) + " failed!";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        _hasFatalError = true;
    }
    _finished = !_hasFatalError && _fe == FE_EOF;
    BS_LOG(INFO, "End WorkflowItem, producer: %s consumer: %s.",
           typeid(*_producer).name(), typeid(*_consumer).name());
    _status = WFS_STOPPED;
}

template<typename T>
void WorkflowItem<T>::handleError(FlowError fe) {
    assert(FE_OK != fe);
    if (FE_EOF == fe) {
        BS_LOG(INFO, "EOF got, exit now");
        _running = false;
    } else if (FE_FATAL == fe) {
        std::string errorMsg = "fatal error happened! exit";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        _hasFatalError = true;
        _running = false;
    } else if (FE_EXCEPTION == fe && JOB == _mode) {
        std::string errorMsg = "exception happened! job will exit";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        _hasFatalError = true;
        _running = false;
    }
}

}
}

#endif //ISEARCH_BS_WORKFLOWITEM_H
