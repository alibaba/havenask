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

#include <string>
#include <vector>

#include "navi/common.h"
#include "navi/engine/Data.h"
#include "navi/engine/KernelConfigContext.h"
#include "navi/ops/DefaultSplitKernel.h"
#include "sql/common/TableDistribution.h"
#include "sql/ops/tableSplit/TableSplit.h"
#include "table/Row.h"

namespace navi {
class KernelInitContext;
} // namespace navi
namespace table {
class Table;
} // namespace table

namespace sql {

class TableSplitKernel : public navi::DefaultSplitKernel {
public:
    std::string getName() const override;
    std::string dataType() const override;
    bool doConfig(navi::KernelConfigContext &ctx) override;
    navi::ErrorCode doInit(navi::KernelInitContext &ctx) override;
    navi::ErrorCode doCompute(const navi::StreamData &streamData,
                              std::vector<navi::DataPtr> &dataVec) override;

private:
    void initTableDistribution(const std::string &jsonConfig);
    void outputSplitData(table::Table &table,
                         const std::vector<std::vector<table::Row>> &partRows,
                         std::vector<navi::DataPtr> &dataVec);

private:
    TableDistribution _tableDist;
    TableSplit _tableSplit;
};

} // namespace sql
