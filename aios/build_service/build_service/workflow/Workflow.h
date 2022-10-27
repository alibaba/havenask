#ifndef ISEARCH_BS_WORKFLOW_H
#define ISEARCH_BS_WORKFLOW_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include <autil/Thread.h>
#include <autil/Lock.h>
#include <tr1/functional>
#include <unistd.h>
#include "build_service/workflow/WorkflowItem.h"
#include "build_service/workflow/WorkflowThreadPool.h"
#include "build_service/document/RawDocument.h"

namespace build_service {
namespace document {

class ProcessedDocument;
BS_TYPEDEF_PTR(ProcessedDocument);
typedef std::vector<ProcessedDocumentPtr> ProcessedDocumentVec;
BS_TYPEDEF_PTR(ProcessedDocumentVec);

}
}

namespace build_service {
namespace workflow {

template <typename T>
class Workflow
{
public:
    Workflow(Producer<T> *producer, Consumer<T> *consumer);
    ~Workflow();
private:
    Workflow(const Workflow &);
    Workflow& operator=(const Workflow &);
public:
    bool start(WorkflowMode mode = JOB,
               const WorkflowThreadPoolPtr& pool = WorkflowThreadPoolPtr());
    void stop();
    bool isFinished() const;
    bool hasFatalError() const;
public:
    Producer<T> *getProducer() const {
        return _workflowItem.getProducer();
    }
    Consumer<T> *getConsumer() const {
        return _workflowItem.getConsumer();
    }
    void suspend() {
        _workflowItem.suspend();
    }
    void resume() {
        _workflowItem.resume();
    }

    bool isSuspended() const {
        return _workflowItem.isSuspended();
    }
    bool isRunning() const {
        return _workflowItem.isRunning();
    }

    const common::Locator &getStopLocator() const {
        return _workflowItem.getStopLocator();
    }

private:
    WorkflowItem<T> _workflowItem;
    WorkflowThreadPoolPtr _threadPool;
    
private:
    BS_LOG_DECLARE();
};
//////////////////////////////////////////////

typedef Workflow<document::RawDocumentPtr> RawDocWorkflow;
typedef Workflow<document::ProcessedDocumentVecPtr> ProcessedDocWorkflow;

BS_LOG_SETUP_TEMPLATE(workflow, Workflow, T);
template<typename T>
Workflow<T>::Workflow(Producer<T> *producer, Consumer<T> *consumer)
    : _workflowItem(producer, consumer)
{
}

template<typename T>
Workflow<T>::~Workflow() {
    stop();
}

template<typename T>
bool Workflow<T>::start(WorkflowMode mode,
                        const WorkflowThreadPoolPtr& pool) {
    _workflowItem.start(mode);
    if (pool && pool->pushWorkItem(&_workflowItem)) {
        BS_LOG(INFO, "use shared WorkflowThreadPool");
        _threadPool = pool;
        return true;
    } else if (pool) {
        BS_LOG(WARN, "pushWorkItem to shared WorkflowTheadPool fail!");
    }
    
    BS_LOG(INFO, "use self-own WorkflowThreadPool");
    _threadPool.reset(new WorkflowThreadPool(1, 1));
    if (!_threadPool->start("Bs" + std::to_string(int(mode))))
    {
        BS_LOG(ERROR, "start WorkflowThreadPool fail!");
        return false;
    }
    return _threadPool->pushWorkItem(&_workflowItem);
}

template<typename T>
void Workflow<T>::stop() {
    _workflowItem.stop();
}


template<typename T>
bool Workflow<T>::isFinished() const {
    return _workflowItem.isFinished();
}

template<typename T>
bool Workflow<T>::hasFatalError() const {
    return _workflowItem.hasFatalError();
}

}
}

#endif //ISEARCH_BS_WORKFLOW_H
