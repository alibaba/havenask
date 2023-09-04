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
#ifndef MULTI_CALL_FLOWCONTROLPARAM_H
#define MULTI_CALL_FLOWCONTROLPARAM_H

#include <limits>

#include "aios/network/gig/multi_call/common/common.h"
#include "aios/network/gig/multi_call/config/FlowControlConfig.h"
namespace multi_call {

class FlowControlParam
{
public:
    FlowControlParam(const FlowControlConfigPtr &flowControlConfig_)
        : flowControlConfig(flowControlConfig_)
        , partitionCount(0)
        , partitionIndex(0) {
    }
    ~FlowControlParam() {
    }

public:
    FlowControlParam(const FlowControlParam &other) {
        flowControlConfig = other.flowControlConfig;
        partitionCount = other.partitionCount;
        partitionIndex = other.partitionIndex;
    }

public:
    FlowControlConfigPtr flowControlConfig;
    uint32_t partitionCount;
    uint32_t partitionIndex;
    bool disableDegrade = false;
    bool ignoreWeightLabelInConsistentHash = false;

private:
    AUTIL_LOG_DECLARE();
};

MULTI_CALL_TYPEDEF_PTR(FlowControlParam);

} // namespace multi_call

#endif // ISEARCH_FLOWCONTROLPARAM_H
