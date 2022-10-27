#include "build_service/processor/MultiThreadProcessorWorkItemExecutor.h"

using namespace std;

namespace build_service {
namespace processor {
BS_LOG_SETUP(processor, MultiThreadProcessorWorkItemExecutor);

MultiThreadProcessorWorkItemExecutor::MultiThreadProcessorWorkItemExecutor(
    uint32_t threadNum, uint32_t queueSize)
{
    _processThreadPool.reset(new autil::OutputOrderedThreadPool(threadNum, queueSize));
}

MultiThreadProcessorWorkItemExecutor::~MultiThreadProcessorWorkItemExecutor() {
    stop();

    ProcessorWorkItem* item = pop();
    if (item != NULL) {
        BS_LOG(ERROR, "some processor work itmes still in queue, drop them");
    }
    while (item != NULL) {
        DELETE_AND_SET_NULL(item);
        item = pop();
    } 
}

bool MultiThreadProcessorWorkItemExecutor::start() {
    return _processThreadPool->start();
}

bool MultiThreadProcessorWorkItemExecutor::push(ProcessorWorkItem* workItem) {
    return _processThreadPool->pushWorkItem(workItem);
}

ProcessorWorkItem* MultiThreadProcessorWorkItemExecutor::pop() {
    return static_cast<ProcessorWorkItem*>(_processThreadPool->popWorkItem());
}

void MultiThreadProcessorWorkItemExecutor::stop() {
    if (_processThreadPool) {
        _processThreadPool->waitStop();
    }
}

uint32_t MultiThreadProcessorWorkItemExecutor::getWaitItemCount() {
    return _processThreadPool->getOutputItemCount();
}

uint32_t MultiThreadProcessorWorkItemExecutor::getOutputItemCount() {
    return _processThreadPool->getOutputItemCount();
}
    
}
}
