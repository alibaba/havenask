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
#ifndef NAVI_CONTROLMERGEKERNEL_H
#define NAVI_CONTROLMERGEKERNEL_H

#include "navi/engine/PortMergeKernel.h"

namespace navi {

class ControlMergeKernel : public PortMergeKernel
{
public:
    ControlMergeKernel();
    ~ControlMergeKernel();
    ControlMergeKernel(const ControlMergeKernel &) = delete;
    ControlMergeKernel &operator=(const ControlMergeKernel &) = delete;
public:
    std::string getName() const override;
    std::string dataType() const override;
    ErrorCode doCompute(const std::vector<StreamData> &dataVec,
                        StreamData &outputData) override;
};

}

#endif //NAVI_CONTROLMERGEKERNEL_H
