#ifndef ISEARCH_BS_PROCESSORWORKITEMEXECUTOR_H
#define ISEARCH_BS_PROCESSORWORKITEMEXECUTOR_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/processor/ProcessorWorkItem.h"

namespace build_service {
namespace processor {

class ProcessorWorkItemExecutor
{
public:
    ProcessorWorkItemExecutor() {}
    virtual ~ProcessorWorkItemExecutor() {}
public:
    virtual bool start() = 0;
    virtual bool push(ProcessorWorkItem* workItem) = 0;
    virtual ProcessorWorkItem* pop() = 0;
    virtual void stop() = 0;
    virtual uint32_t getWaitItemCount() = 0;
    virtual uint32_t getOutputItemCount() = 0;
    
private:
    ProcessorWorkItemExecutor(const ProcessorWorkItemExecutor &);
    ProcessorWorkItemExecutor& operator=(const ProcessorWorkItemExecutor &);
public:
    
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(ProcessorWorkItemExecutor);

}
}

#endif //ISEARCH_BS_PROCESSORWORKITEMEXECUTOR_H
