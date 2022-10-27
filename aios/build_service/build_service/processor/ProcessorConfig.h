#ifndef ISEARCH_BS_PROCESSORCONFIG_H
#define ISEARCH_BS_PROCESSORCONFIG_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include <autil/legacy/jsonizable.h>

namespace build_service {
namespace processor {

class ProcessorConfig : public autil::legacy::Jsonizable
{
public:
    ProcessorConfig();
    ~ProcessorConfig();
public:
    static const uint32_t DEFAULT_PROCESSOR_THREAD_NUM;
    static const uint32_t DEFAULT_PROCESSOR_QUEUE_SIZE;
    static const int64_t DEFAULT_CHECKPOINT_INTERVAL;
    static const std::string NORMAL_PROCESSOR_STRATEGY;
    static const std::string BATCH_PROCESSOR_STRATEGY;

public:
    /* override */ void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);
    bool validate() const;
public:
    uint32_t processorThreadNum;
    uint32_t processorQueueSize;
    // not public to user
    uint64_t checkpointInterval;
    
    std::string processorStrategyStr;
    std::string processorStrategyParameter;
    
private:
    BS_LOG_DECLARE();
};

}
}

#endif //ISEARCH_BS_PROCESSORCONFIG_H
