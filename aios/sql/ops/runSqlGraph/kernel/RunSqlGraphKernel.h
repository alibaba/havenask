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
#include <stdint.h>

#include "autil/Log.h" // IWYU pragma: keep
#include "navi/common.h"
#include "navi/engine/Kernel.h"
#include "navi/engine/KernelConfigContext.h"
#include "navi/resource/GigClientR.h"
#include "sql/proto/SqlSearchInfoCollectorR.h"
#include "sql/resource/HashFunctionCacheR.h"

namespace navi {
class GraphBuilder;
class KernelComputeContext;
class KernelDefBuilder;
class KernelInitContext;
}; // namespace navi

namespace sql {

class RunSqlGraphKernel : public navi::Kernel {
public:
    RunSqlGraphKernel();
    ~RunSqlGraphKernel() = default;
    RunSqlGraphKernel(const RunSqlGraphKernel &) = delete;
    RunSqlGraphKernel &operator=(const RunSqlGraphKernel &) = delete;

public:
    void def(navi::KernelDefBuilder &builder) const override;
    bool config(navi::KernelConfigContext &ctx) override;
    navi::ErrorCode init(navi::KernelInitContext &initContext) override;
    navi::ErrorCode compute(navi::KernelComputeContext &runContext) override;

private:
    void addMetaCollectorOutput(navi::GraphBuilder &builder) const;

private:
    KERNEL_DEPEND_DECLARE();

private:
    KERNEL_DEPEND_ON_FALSE(navi::GigClientR, _gigClientR);
    KERNEL_DEPEND_ON_FALSE(HashFunctionCacheR, _hashFunctionCacheR);
    KERNEL_DEPEND_ON(SqlSearchInfoCollectorR, _sqlSearchInfoCollectorR);
    int64_t _oneBatch = 0;
};

typedef std::shared_ptr<RunSqlGraphKernel> RunSqlGraphKernelPtr;

} // namespace sql
