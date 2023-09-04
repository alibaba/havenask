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
#include "navi/ops/ControlMergeKernel.h"

namespace navi {

ControlMergeKernel::ControlMergeKernel()
{
}

ControlMergeKernel::~ControlMergeKernel() {
}

std::string ControlMergeKernel::getName() const {
    return CONTROL_MERGE_KERNEL;
}

std::string ControlMergeKernel::dataType() const {
    return "";
}

ErrorCode ControlMergeKernel::doCompute(const std::vector<StreamData> &dataVec,
                                        StreamData &outputData)
{
    for (const auto &streamData : dataVec) {
        if (!streamData.eof) {
            outputData.eof = false;
            return EC_NONE;
        }
    }
    outputData.eof = true;
    return EC_NONE;
}

REGISTER_KERNEL(ControlMergeKernel);

}

