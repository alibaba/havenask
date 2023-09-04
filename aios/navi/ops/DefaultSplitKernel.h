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
#ifndef NAVI_DEFAULTSPLITKERNEL_H
#define NAVI_DEFAULTSPLITKERNEL_H

#include "navi/engine/PortSplitKernel.h"

namespace navi {

class DefaultSplitKernel : public PortSplitKernel
{
public:
    DefaultSplitKernel();
    ~DefaultSplitKernel();
    DefaultSplitKernel(const DefaultSplitKernel &) = delete;
    DefaultSplitKernel &operator=(const DefaultSplitKernel &) = delete;
public:
    std::string getName() const override;
    std::string dataType() const override;
    ErrorCode doCompute(const StreamData &streamData,
                        std::vector<DataPtr> &dataVec) override;
    bool isExpensive() const override {
        return false;
    }
};

}

#endif //NAVI_DEFAULTSPLITKERNEL_H
