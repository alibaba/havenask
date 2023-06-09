/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "build_service/config/ProcessorConfig.h"

using namespace std;

namespace build_service { namespace config {

BS_LOG_SETUP(config, ProcessorConfig);

const uint32_t ProcessorConfig::DEFAULT_PROCESSOR_THREAD_NUM = 10;
const uint32_t ProcessorConfig::DEFAULT_PROCESSOR_QUEUE_SIZE = 1000;
const uint32_t ProcessorConfig::DEFAULT_SRC_THREAD_NUM = 5;
const uint32_t ProcessorConfig::DEFAULT_SRC_QUEUE_SIZE = 3000;
const int64_t ProcessorConfig::DEFAULT_CHECKPOINT_INTERVAL = 30; // sec

const string ProcessorConfig::NORMAL_PROCESSOR_STRATEGY = "normal";
const string ProcessorConfig::BATCH_PROCESSOR_STRATEGY = "batch";

ProcessorConfig::ProcessorConfig()
    : processorThreadNum(DEFAULT_PROCESSOR_THREAD_NUM)
    , processorQueueSize(DEFAULT_PROCESSOR_QUEUE_SIZE)
    , srcThreadNum(DEFAULT_SRC_THREAD_NUM)
    , srcQueueSize(DEFAULT_SRC_QUEUE_SIZE)
    , checkpointInterval(DEFAULT_CHECKPOINT_INTERVAL)
    , processorStrategyStr(NORMAL_PROCESSOR_STRATEGY)
    , enableRewriteDeleteSubDoc(false)
{
}

ProcessorConfig::~ProcessorConfig() {}

void ProcessorConfig::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("processor_strategy", processorStrategyStr, processorStrategyStr);
    json.Jsonize("processor_strategy_parameter", processorStrategyParameter, processorStrategyParameter);
    json.Jsonize("processor_thread_num", processorThreadNum, processorThreadNum);
    json.Jsonize("src_thread_num", srcThreadNum, srcThreadNum);
    json.Jsonize("_bs_checkpoint_interval", checkpointInterval, checkpointInterval);
    json.Jsonize("enable_rewrite_delete_sub_doc", enableRewriteDeleteSubDoc, enableRewriteDeleteSubDoc);

    if (json.GetMode() == autil::legacy::Jsonizable::FROM_JSON) {
        processorQueueSize = (processorStrategyStr == BATCH_PROCESSOR_STRATEGY) ? 100 : DEFAULT_PROCESSOR_QUEUE_SIZE;
        json.Jsonize("processor_queue_size", processorQueueSize, processorQueueSize);
        json.Jsonize("src_queue_size", srcQueueSize, srcQueueSize);
    } else {
        json.Jsonize("processor_queue_size", processorQueueSize);
        json.Jsonize("src_queue_size", srcQueueSize);
    }
}

bool ProcessorConfig::validate() const
{
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
    if (srcThreadNum == 0) {
        string errorMsg = "srcThreadNum can't be 0";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    if (srcQueueSize == 0) {
        string errorMsg = "srcQueueSize can't be 0";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    if (checkpointInterval == 0) {
        string errorMsg = "checkpointInterval can't be 0";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }

    if (processorStrategyStr != NORMAL_PROCESSOR_STRATEGY && processorStrategyStr != BATCH_PROCESSOR_STRATEGY) {
        BS_LOG(ERROR, "unknown processor_strategy [%s]", processorStrategyStr.c_str());
        return false;
    }
    if (processorStrategyStr != NORMAL_PROCESSOR_STRATEGY && enableRewriteDeleteSubDoc) {
        BS_LOG(ERROR, "batch processor_strategy not implement rewrite delete sub doc");
        return false;
    }
    return true;
}

}} // namespace build_service::config
