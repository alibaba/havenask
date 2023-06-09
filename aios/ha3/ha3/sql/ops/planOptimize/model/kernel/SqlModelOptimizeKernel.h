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

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/sql/ops/planOptimize/GraphOptimizer.h"
#include "navi/engine/Kernel.h"
#include "navi/engine/KernelConfigContext.h"

namespace navi {
class GraphMemoryPoolResource;
}
namespace isearch {
namespace sql {
class IquanResource;
class ModelConfigResource;
class SqlQueryResource;

class SqlModelOptimizeKernel : public navi::Kernel {
public:
    SqlModelOptimizeKernel();
    ~SqlModelOptimizeKernel();
    SqlModelOptimizeKernel(const SqlModelOptimizeKernel &) = delete;
    SqlModelOptimizeKernel &operator=(const SqlModelOptimizeKernel &) = delete;

public:
    void def(navi::KernelDefBuilder &builder) const override;
    bool config(navi::KernelConfigContext &ctx) override;
    navi::ErrorCode init(navi::KernelInitContext &initContext) override;
    navi::ErrorCode compute(navi::KernelComputeContext &runContext) override;
private:
    IquanResource *_iquanResource = nullptr;
    ModelConfigResource *_modelConfigResource = nullptr;
    SqlQueryResource *_queryResource = nullptr;
    navi::GraphMemoryPoolResource *_memoryPoolResource = nullptr;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<SqlModelOptimizeKernel> SqlModelOptimizeKernelPtr;

} // namespace sql
} // end namespace isearch
