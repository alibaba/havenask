#include "build_service/processor/SingleThreadProcessorWorkItemExecutor.h"

using namespace std;

namespace build_service {
namespace processor {
BS_LOG_SETUP(processor, SingleThreadProcessorWorkItemExecutor);

SingleThreadProcessorWorkItemExecutor::SingleThreadProcessorWorkItemExecutor(uint32_t queueSize)
    : _running(false)
{
    _queue.reset(new Queue(queueSize));
}

SingleThreadProcessorWorkItemExecutor::~SingleThreadProcessorWorkItemExecutor() {
    stop();
    if (!_queue->empty()) {
        string errorMsg = "some processor work items still in queue, drop them";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
    }
    
    while (!_queue->empty()) {
        ProcessorWorkItem* workItem;
        if (_queue->pop(workItem)) {
            delete workItem;            
        }
    }
}

bool SingleThreadProcessorWorkItemExecutor::start() {
    _running = true;
    return true;
}

bool SingleThreadProcessorWorkItemExecutor::push(ProcessorWorkItem* workItem) {
    if (!_running || workItem == NULL)
    {
        return false;
    }
    workItem->process();
    _queue->push(workItem);
    return true;
}

ProcessorWorkItem* SingleThreadProcessorWorkItemExecutor::pop() {
    ProcessorWorkItem* item = NULL;
    if (_queue->pop(item)) {
        return item;
    }
    return NULL;
}

void SingleThreadProcessorWorkItemExecutor::stop() {
    _running = false;
    _queue->setFinish();
}

uint32_t SingleThreadProcessorWorkItemExecutor::getWaitItemCount() {
    return 0;
}

uint32_t SingleThreadProcessorWorkItemExecutor::getOutputItemCount() {
    return _queue->size();
}
    
}
}
