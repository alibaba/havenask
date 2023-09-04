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

#include <stdint.h>
#include <string>

#include "navi/common.h"
#include "navi/engine/Data.h"
#include "navi/engine/KernelConfigContext.h"
#include "navi/ops/DefaultMergeKernel.h"
#include "navi/proto/KernelDef.pb.h"
#include "table/Table.h"

namespace navi {
class KernelComputeContext;
class KernelInitContext;
} // namespace navi

namespace sql {

class TableMergeKernel : public navi::DefaultMergeKernel {
public:
    TableMergeKernel() = default;
    ~TableMergeKernel() = default;
    TableMergeKernel(const TableMergeKernel &) = delete;
    TableMergeKernel &operator=(const TableMergeKernel &) = delete;

public:
    std::string getName() const override;
    std::string dataType() const override;
    navi::InputTypeDef inputType() const override;
    navi::ErrorCode doInit(navi::KernelInitContext &ctx) override;
    navi::ErrorCode compute(navi::KernelComputeContext &ctx) override;
    bool doConfig(navi::KernelConfigContext &ctx) override;

private:
    bool mergeTableData(navi::NaviPartId index, navi::DataPtr &data);
    bool outputTableData(navi::KernelComputeContext &ctx);

private:
    table::TablePtr _outputTable;
    int64_t _oneBatch = 0;
};

} // namespace sql
