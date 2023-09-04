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

#include "navi/engine/Kernel.h"
#include "navi/ops/ResourceFlushData.h"

namespace navi {

class ResourceFlushKernel : public Kernel
{
public:
    ResourceFlushKernel();
    ~ResourceFlushKernel();
    ResourceFlushKernel(const ResourceFlushKernel &) = delete;
    ResourceFlushKernel &operator=(const ResourceFlushKernel &) = delete;
public:
    void def(KernelDefBuilder &builder) const override;
    bool config(KernelConfigContext &ctx) override;
    ErrorCode init(KernelInitContext &ctx) override;
    ErrorCode compute(KernelComputeContext &ctx) override;
private:
    bool readInput();
    bool createData(KernelComputeContext &ctx);
private:
    ResourceFlushDataPtr _data;
};

}
