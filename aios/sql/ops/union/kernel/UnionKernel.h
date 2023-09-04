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
#include <vector>

#include "navi/common.h"
#include "navi/engine/Data.h"
#include "navi/engine/Kernel.h"
#include "navi/engine/KernelConfigContext.h"
#include "navi/resource/GraphMemoryPoolR.h"
#include "table/Table.h"

namespace navi {
class KernelComputeContext;
class KernelDefBuilder;
} // namespace navi

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
    KERNEL_DEPEND_DECLARE();

private:
    table::TablePtr _table;
    std::vector<int32_t> _reuseInputs;
    KERNEL_DEPEND_ON(navi::GraphMemoryPoolR, _graphMemoryPoolR);
};

typedef std::shared_ptr<UnionKernel> UnionKernelPtr;
} // namespace sql
