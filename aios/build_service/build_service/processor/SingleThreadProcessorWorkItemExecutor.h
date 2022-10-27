#ifndef ISEARCH_BS_SINGLETHREADPROCESSORWORKITEMEXECUTOR_H
#define ISEARCH_BS_SINGLETHREADPROCESSORWORKITEMEXECUTOR_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/util/StreamQueue.h"
#include "build_service/processor/ProcessorWorkItemExecutor.h"

namespace build_service {
namespace processor {

class SingleThreadProcessorWorkItemExecutor : public ProcessorWorkItemExecutor
{
public:
    SingleThreadProcessorWorkItemExecutor(uint32_t queueSize);
    ~SingleThreadProcessorWorkItemExecutor();
private:
    SingleThreadProcessorWorkItemExecutor(const SingleThreadProcessorWorkItemExecutor &);
    SingleThreadProcessorWorkItemExecutor& operator=(const SingleThreadProcessorWorkItemExecutor &);
public:
    bool start() override;
    bool push(ProcessorWorkItem* workItem) override;
    ProcessorWorkItem* pop() override;
    void stop() override;
    uint32_t getWaitItemCount() override;
    uint32_t getOutputItemCount() override;
    
private:
    typedef util::StreamQueue<ProcessorWorkItem*> Queue;
    typedef std::unique_ptr<Queue> QueuePtr;
    
    bool _running;
    QueuePtr _queue;
    
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(SingleThreadProcessorWorkItemExecutor);

}
}

#endif //ISEARCH_BS_SINGLETHREADPROCESSORWORKITEMEXECUTOR_H
