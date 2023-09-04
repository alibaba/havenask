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
#include <stddef.h>

#include "navi/common.h"
#include "navi/engine/Kernel.h"
#include "navi/engine/KernelConfigContext.h"
#include "sql/ops/join/JoinKernelBase.h"
#include "table/Table.h"

namespace navi {
class KernelComputeContext;
class KernelDefBuilder;
} // namespace navi

namespace sql {

class NestedLoopJoinKernel : public JoinKernelBase {
public:
    static const size_t DEFAULT_BUFFER_LIMIT_SIZE;

public:
    NestedLoopJoinKernel();
    ~NestedLoopJoinKernel();

public:
    void def(navi::KernelDefBuilder &builder) const override;
    bool config(navi::KernelConfigContext &ctx) override;
    navi::ErrorCode compute(navi::KernelComputeContext &runContext) override;

private:
    bool doCompute(table::TablePtr &outputTable);
    bool waitFullTable();
    size_t joinTable(const table::TablePtr &largeTable, const table::TablePtr &fullTable);

private:
    KERNEL_DEPEND_DECLARE_BASE(JoinKernelBase);

private:
    table::TablePtr _leftBuffer;
    table::TablePtr _rightBuffer;
    size_t _bufferLimitSize;
    bool _fullTableCreated;
    bool _leftFullTable;
    bool _leftEof;
    bool _rightEof;
};

typedef std::shared_ptr<NestedLoopJoinKernel> NestedLoopJoinKernelPtr;
} // namespace sql
