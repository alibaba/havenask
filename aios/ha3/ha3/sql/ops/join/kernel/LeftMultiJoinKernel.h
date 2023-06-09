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

#include "ha3/sql/ops/scan/ScanBase.h"
#include "ha3/sql/common/IndexInfo.h"
#include "ha3/sql/ops/join/JoinKernelBase.h"
#include "table/Table.h"

namespace isearch {
namespace sql {

class LeftMultiJoinKernel : public JoinKernelBase {
public:
    LeftMultiJoinKernel();
    ~LeftMultiJoinKernel();

public:
    void def(navi::KernelDefBuilder &builder) const override;
    bool config(navi::KernelConfigContext &ctx) override;
    navi::ErrorCode init(navi::KernelInitContext &initContext) override;
    navi::ErrorCode compute(navi::KernelComputeContext &runContext) override;

private:
    bool doMultiJoin(const table::TablePtr &inputTable,
                     table::TablePtr &output);
    StreamQueryPtr genStreamQuery(const table::TablePtr &input, const std::string &joinColumnName);
    bool joinAndGather(const table::TablePtr &inputTable,
                       const table::TablePtr &rightTable,
                       table::TablePtr &output);
    bool initJoinedTable(const table::TablePtr &leftTable,
                         const table::TablePtr &rightTable,
                         table::TablePtr &output);
    bool declearTableColumn(const table::TablePtr &inputTable,
                            table::TablePtr &outputTable,
                            const std::string &inputField,
                            const std::string &outputField,
                            bool needExpand) const;
    bool getColumnInfo(const table::TablePtr &table, const std::string &field,
                       table::Column *&tableColumn, table::ValueType &vt) const;
    bool checkIndexInfo() const;
    bool createHashMap(const table::TablePtr &table,
                       const std::vector<std::string> &joinColumns,
                       std::unordered_map<size_t, size_t> &hashJoinMap);
    bool generateOutput(const table::TablePtr &leftTable,
                        const table::TablePtr &rightTable,
                        const std::vector<std::vector<size_t>> &resultIndexes,
                        table::TablePtr &outputTable);
private:
    ScanInitParam _initParam;
    ScanBasePtr _scanBase;

    std::map<std::string, sql::IndexInfo> _rightIndexInfos;
    TableMeta _rightTableMeta;
    std::string _joinColumnName;
};

} // sql
} // isearch
