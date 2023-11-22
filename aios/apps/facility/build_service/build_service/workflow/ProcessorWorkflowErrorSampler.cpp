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
#include "build_service/workflow/ProcessorWorkflowErrorSampler.h"

#include <algorithm>
#include <cstddef>

#include "alog/Logger.h"
#include "build_service/workflow/Workflow.h"

using namespace std;

namespace build_service { namespace workflow {
BS_LOG_SETUP(workflow, ProcessorWorkflowErrorSampler);

ProcessorWorkflowErrorSampler::ProcessorWorkflowErrorSampler()
{
    for (size_t i = 0; i < SAMPLE_WINDOWS_SIZE; ++i) {
        _flowErrors.push_back(FlowError::FE_OK);
    }
}

ProcessorWorkflowErrorSampler::~ProcessorWorkflowErrorSampler() {}

bool ProcessorWorkflowErrorSampler::workFlowErrorExceedThreadhold() const
{
    return _continuousNoData > CONTINEUOUS_NO_DATA_THREADHOLD || _ratio > WORKFLOW_ERROR_RATIO_THRESHOLD;
}

void ProcessorWorkflowErrorSampler::workLoop(BuildFlow* buildFlow)
{
    if (!buildFlow) {
        return;
    }
    FlowError error = FlowError::FE_OK;
    auto inputFlow = buildFlow->getInputFlow();
    if (inputFlow) {
        auto flowError = inputFlow->getLastFlowError();
        _continuousNoData = (flowError == FlowError::FE_WAIT ? _continuousNoData + 1 : 0);
        if (flowError != FlowError::FE_OK && flowError != FlowError::FE_WAIT) {
            error = flowError;
        }
    }

    auto outputFlow = buildFlow->getOutputFlow();
    if (outputFlow) {
        auto flowError = outputFlow->getLastFlowError();
        if (flowError != FlowError::FE_OK) {
            error = flowError;
        }
    }

    _errorTimes += error == FlowError::FE_OK ? 0 : 1;
    _flowErrors.push_back(error);
    while (_flowErrors.size() > SAMPLE_WINDOWS_SIZE) {
        FlowError flowError = _flowErrors.front();
        _flowErrors.pop_front();
        _errorTimes -= flowError == FlowError::FE_OK ? 0 : 1;
    }
    _ratio = _errorTimes * 100 / SAMPLE_WINDOWS_SIZE;
    BS_INTERVAL_LOG(100, INFO, "workflow error ratio is [%d], continuousNoDataSample [%lu]", _ratio, _continuousNoData);
}

}} // namespace build_service::workflow
