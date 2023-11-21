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
#pragma once

#include <stdint.h>
#include <string>

#include "autil/legacy/jsonizable.h"
#include "build_service/util/Log.h"

namespace build_service { namespace config {

class ProcessorConfig : public autil::legacy::Jsonizable
{
public:
    ProcessorConfig();
    ~ProcessorConfig();

public:
    static const uint32_t DEFAULT_PROCESSOR_THREAD_NUM;
    static const uint32_t DEFAULT_PROCESSOR_QUEUE_SIZE;
    static const uint32_t DEFAULT_SRC_THREAD_NUM;
    static const uint32_t DEFAULT_SRC_QUEUE_SIZE;
    static const int64_t DEFAULT_CHECKPOINT_INTERVAL;
    static const std::string NORMAL_PROCESSOR_STRATEGY;
    static const std::string BATCH_PROCESSOR_STRATEGY;

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    bool validate() const;

public:
    uint32_t processorThreadNum;
    uint32_t processorQueueSize;

    uint32_t srcThreadNum;
    uint32_t srcQueueSize;
    // not public to user
    uint64_t checkpointInterval;

    std::string processorStrategyStr;
    std::string processorStrategyParameter;

    bool enableRewriteDeleteSubDoc;

private:
    BS_LOG_DECLARE();
};

}} // namespace build_service::config
