#ifndef ISEARCH_BS_MULTITHREADPROCESSORWORKITEMEXECUTOR_H
#define ISEARCH_BS_MULTITHREADPROCESSORWORKITEMEXECUTOR_H

#include <atomic>
#include <autil/OutputOrderedThreadPool.h>
#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/processor/ProcessorWorkItemExecutor.h"

namespace build_service {
namespace processor {

class MultiThreadProcessorWorkItemExecutor : public ProcessorWorkItemExecutor
{
public:
    MultiThreadProcessorWorkItemExecutor(uint32_t threadNum, uint32_t queueSize);
    ~MultiThreadProcessorWorkItemExecutor();
private:
    MultiThreadProcessorWorkItemExecutor(const MultiThreadProcessorWorkItemExecutor &);
    MultiThreadProcessorWorkItemExecutor& operator=(const MultiThreadProcessorWorkItemExecutor &);
public:
    bool start() override;
    bool push(ProcessorWorkItem* workItem) override;
    ProcessorWorkItem* pop() override;
    void stop() override;
    uint32_t getWaitItemCount() override;
    uint32_t getOutputItemCount() override;
    
private:
    autil::OutputOrderedThreadPoolPtr _processThreadPool;
    std::atomic<uint32_t> _waitProcessCount;
    
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(MultiThreadProcessorWorkItemExecutor);

}
}

#endif //ISEARCH_BS_MULTITHREADPROCESSORWORKITEMEXECUTOR_H
