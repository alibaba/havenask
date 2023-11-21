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

#include <deque>
#include <stddef.h>
#include <stdint.h>

#include "build_service/common/ResourceContainer.h"
#include "build_service/common_define.h"
#include "build_service/workflow/BuildFlow.h"
#include "build_service/workflow/FlowError.h"

namespace build_service { namespace workflow {

class ProcessorWorkflowErrorSampler
{
public:
    ProcessorWorkflowErrorSampler();
    ~ProcessorWorkflowErrorSampler();

public:
    bool workFlowErrorExceedThreadhold() const;
    void workLoop(BuildFlow* buildFlow);

private:
    const constexpr static size_t SAMPLE_WINDOWS_SIZE = 100;
    const constexpr static size_t CONTINEUOUS_NO_DATA_THREADHOLD = 500;
    const constexpr static int32_t WORKFLOW_ERROR_RATIO_THRESHOLD = 25;
    int32_t _ratio = 0;
    size_t _errorTimes = 0;
    std::deque<FlowError> _flowErrors;
    size_t _continuousNoData = 0;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(ProcessorWorkflowErrorSampler);

}} // namespace build_service::workflow
