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

#include "navi/common.h"
#include "navi/engine/Kernel.h"
#include "navi/engine/KernelConfigContext.h"
#include "sql/ops/join/JoinKernelBase.h"
#include "sql/ops/join/LookupJoinBatch.h"
#include "table/Row.h"
#include "table/Table.h"

namespace navi {
class KernelComputeContext;
class KernelDefBuilder;
class KernelInitContext;
} // namespace navi
namespace table {
class Column;
} // namespace table

namespace sql {
class ScanBase;
class LookupR;
struct StreamQuery;

class LookupJoinKernel : public JoinKernelBase {
public:
    LookupJoinKernel();
    ~LookupJoinKernel();

public:
    void def(navi::KernelDefBuilder &builder) const override;
    navi::ErrorCode compute(navi::KernelComputeContext &runContext) override;

private:
    bool doInit() override;
    bool doConfig(navi::KernelConfigContext &ctx) override;
    navi::ErrorCode finishLastJoin(navi::KernelComputeContext &runContext);
    navi::ErrorCode startNewLookup(navi::KernelComputeContext &runContext);
    bool joinTable(const LookupJoinBatch &batch,
                   const table::TablePtr &streamOutput,
                   table::TablePtr &outputTable);
    void patchLookupHintInfo(const std::map<std::string, std::string> &hintsMap);
    bool scanAndJoin(const std::shared_ptr<StreamQuery> &streamQuery,
                     const LookupJoinBatch &batch,
                     table::TablePtr &outputTable,
                     bool &finished);

private:
    KERNEL_DEPEND_DECLARE_BASE(JoinKernelBase);

protected:
    ScanBase *_scanBase = nullptr;
    LookupR *_lookupR = nullptr;
    bool _inputEof;
    LookupJoinBatch _batch;
    std::shared_ptr<table::Table> _outputTable;
    size_t _lookupBatchSize;
    size_t _lookupTurncateThreshold;
    std::shared_ptr<StreamQuery> _streamQuery;
};

typedef std::shared_ptr<LookupJoinKernel> LookupJoinKernelPtr;
} // namespace sql
