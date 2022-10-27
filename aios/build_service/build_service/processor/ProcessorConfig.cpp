#include "build_service/processor/ProcessorConfig.h"

using namespace std;

namespace build_service {
namespace processor {

BS_LOG_SETUP(processor, ProcessorConfig);

const uint32_t ProcessorConfig::DEFAULT_PROCESSOR_THREAD_NUM = 10;
const uint32_t ProcessorConfig::DEFAULT_PROCESSOR_QUEUE_SIZE = 1000;
const int64_t ProcessorConfig::DEFAULT_CHECKPOINT_INTERVAL = 30; // sec

const string ProcessorConfig::NORMAL_PROCESSOR_STRATEGY = "normal";
const string ProcessorConfig::BATCH_PROCESSOR_STRATEGY = "batch";

ProcessorConfig::ProcessorConfig()
    : processorThreadNum(DEFAULT_PROCESSOR_THREAD_NUM)
    , processorQueueSize(DEFAULT_PROCESSOR_QUEUE_SIZE)
    , checkpointInterval(DEFAULT_CHECKPOINT_INTERVAL)
    , processorStrategyStr(NORMAL_PROCESSOR_STRATEGY)
{
}

ProcessorConfig::~ProcessorConfig() {
}

void ProcessorConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) {
    json.Jsonize("processor_strategy", processorStrategyStr, processorStrategyStr);
    json.Jsonize("processor_strategy_parameter", processorStrategyParameter,
                 processorStrategyParameter);
    json.Jsonize("processor_thread_num", processorThreadNum, processorThreadNum);
    json.Jsonize("_bs_checkpoint_interval", checkpointInterval, checkpointInterval);
    
    if (json.GetMode() == autil::legacy::Jsonizable::FROM_JSON) {
        processorQueueSize = (processorStrategyStr == BATCH_PROCESSOR_STRATEGY) ?
                             100 : DEFAULT_PROCESSOR_QUEUE_SIZE;
        json.Jsonize("processor_queue_size", processorQueueSize, processorQueueSize);
    } else {
        json.Jsonize("processor_queue_size", processorQueueSize);
    }
}

bool ProcessorConfig::validate() const {
    if (processorThreadNum == 0) {
        string errorMsg = "processorThreadNum can't be 0";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    if (processorQueueSize == 0) {
        string errorMsg = "processorQueueSize can't be 0";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    if (checkpointInterval == 0) {
        string errorMsg = "checkpointInterval can't be 0";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }

    if (processorStrategyStr != NORMAL_PROCESSOR_STRATEGY &&
        processorStrategyStr != BATCH_PROCESSOR_STRATEGY)
    {
        BS_LOG(ERROR, "unknown processor_strategy [%s]", processorStrategyStr.c_str());
        return false;
    }
    return true;
}

}
}
