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

#include "autil/Log.h"
#include "navi/engine/Kernel.h"

namespace navi_ops {

class CoroutineKernel : public navi::Kernel {
public:
    CoroutineKernel();
    virtual ~CoroutineKernel();

private:
    CoroutineKernel(const CoroutineKernel &);
    CoroutineKernel &operator=(const CoroutineKernel &);

public:
    navi::ErrorCode init(navi::KernelInitContext &ctx) override;
    navi::ErrorCode compute(navi::KernelComputeContext &ctx) override final;
    void def(navi::KernelDefBuilder &builder) const override {}
    bool config(navi::KernelConfigContext &ctx) override {
        return true;
    }
    virtual navi::ErrorCode computeEnd(navi::KernelComputeContext &ctx) = 0;
protected:
    navi::AsyncPipePtr _asyncPipe;
private:
    AUTIL_LOG_DECLARE();
};

} // namespace navi_ops
