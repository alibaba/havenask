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
#ifndef NAVI_N2ONEKERNEL_H
#define NAVI_N2ONEKERNEL_H

#include "navi/engine/PipeKernel.h"

namespace navi {

class N2OneKernel : public PipeKernel
{
public:
    N2OneKernel(const std::string &name);
    ~N2OneKernel();
private:
    N2OneKernel(const N2OneKernel &);
    N2OneKernel &operator=(const N2OneKernel &);
public:
    ErrorCode compute(KernelComputeContext &ctx) override;
    virtual ErrorCode collect(const DataPtr &data) = 0;
    virtual ErrorCode finalize(DataPtr &data) = 0;
};

}

#endif //NAVI_N2ONEKERNEL_H
