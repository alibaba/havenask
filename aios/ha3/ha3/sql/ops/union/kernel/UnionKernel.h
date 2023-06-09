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

#include "autil/Log.h"
#include "navi/common.h"
#include "navi/engine/Data.h"
#include "navi/engine/Kernel.h"
#include "table/Table.h"

namespace navi {
class KernelComputeContext;
class KernelDefBuilder;
class GraphMemoryPoolResource;
} // namespace navi

namespace isearch {
namespace sql {
class SqlQueryResource;
} // namespace sql
} // namespace isearch

namespace isearch {
namespace sql {

class UnionKernel : public navi::Kernel {
public:
    UnionKernel();
    ~UnionKernel();

public:
    void def(navi::KernelDefBuilder &builder) const override;
    bool config(navi::KernelConfigContext &ctx) override;
    navi::ErrorCode compute(navi::KernelComputeContext &runContext) override;
    bool isExpensive() const override {
        return false;
    }

private:
    void outputResult(navi::KernelComputeContext &runContext);
    bool doCompute(const navi::DataPtr &data);

private:
    table::TablePtr _table;
    std::vector<int32_t> _reuseInputs;
    navi::GraphMemoryPoolResource *_memoryPoolResource = nullptr;
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<UnionKernel> UnionKernelPtr;
} // namespace sql
} // namespace isearch
