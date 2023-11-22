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
#include <vector>

#include "autil/legacy/jsonizable.h"
#include "build_service/config/ProcessorAdaptiveScalingConfig.h"
#include "build_service/util/Log.h"

namespace build_service { namespace config {

class DetectSlowConfig : public autil::legacy::Jsonizable
{
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

public:
    std::string src;
    std::string condition;
    int64_t gap = 0;
};

typedef std::vector<DetectSlowConfig> DetectSlowConfigs;

class ProcessorRuleConfig : public autil::legacy::Jsonizable
{
public:
    ProcessorRuleConfig();
    ~ProcessorRuleConfig();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    bool validate() const;

public:
    uint32_t partitionCount;
    uint32_t parallelNum;
    uint32_t incParallelNum;
    int64_t incProcessorStartTs;
    DetectSlowConfigs detectSlowConfigs;
    ProcessorAdaptiveScalingConfig adaptiveScalingConfig;

private:
    BS_LOG_DECLARE();
};

}} // namespace build_service::config
