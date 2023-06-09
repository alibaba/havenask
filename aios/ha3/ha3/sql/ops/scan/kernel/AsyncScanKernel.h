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

#include <any>
#include "autil/Log.h"
#include "ha3/sql/framework/PushDownOp.h"
#include "ha3/sql/ops/scan/ScanBase.h"
#include "ha3/sql/ops/scan/ScanKernelBase.h"
#include "navi/common.h"
#include "navi/engine/Kernel.h"
#include "navi/engine/KernelConfigContext.h"
#include "navi_ops/coroutine/CoroutineKernel.h"

namespace navi {
class KernelComputeContext;
class KernelDefBuilder;
class KernelInitContext;
class GigQuerySessionR;
} // namespace navi

namespace isearch {
namespace sql {

class AsyncScanKernel : public navi_ops::CoroutineKernel, public ScanKernelBase {
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

private:

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<AsyncScanKernel> AsyncScanKernelPtr;

} // namespace sql
} // namespace isearch
