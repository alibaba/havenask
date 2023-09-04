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

#include <memory>

#include "navi/common.h"
#include "navi/engine/KernelConfigContext.h"
#include "navi_ops/coroutine/CoroutineKernel.h"

namespace navi {
class KernelComputeContext;
class KernelDefBuilder;
class KernelInitContext;
} // namespace navi

namespace sql {
class ScanBase;
class ScanPushDownR;

class AsyncScanKernel : public navi_ops::CoroutineKernel {
public:
    AsyncScanKernel();
    ~AsyncScanKernel();

private:
    AsyncScanKernel(const AsyncScanKernel &);
    AsyncScanKernel &operator=(const AsyncScanKernel &);

public:
    void def(navi::KernelDefBuilder &builder) const override;
    bool config(navi::KernelConfigContext &ctx) override;
    navi::ErrorCode init(navi::KernelInitContext &initContext) override;
    navi::ErrorCode computeEnd(navi::KernelComputeContext &runContext) override;

protected:
    ScanBase *_scanBase = nullptr;
    ScanPushDownR *_scanPushDownR = nullptr;
};

typedef std::shared_ptr<AsyncScanKernel> AsyncScanKernelPtr;

} // namespace sql
