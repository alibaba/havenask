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
#include "ha3/sql/ops/tableSplit/TableSplitKernel.h"
#include "navi/engine/PartInfo.h"
#include "ha3/sql/data/TableType.h"
#include "ha3/sql/ops/util/KernelUtil.h"
#include "ha3/sql/data/TableData.h"
#include "table/TableUtil.h"

using namespace std;
using namespace navi;
using namespace table;
using namespace autil;

namespace isearch {
namespace sql {

std::string TableSplitKernel::getName() const {
    return "TableSplitKernel";
}

std::string TableSplitKernel::dataType() const {
    return TableType::TYPE_ID;
}

bool TableSplitKernel::doConfig(navi::KernelConfigContext& ctx) {
    NAVI_KERNEL_LOG(DEBUG, "TableSplitKernel::doConfig");
    const auto binaryAttrs = ctx.getBinaryAttrs();
    const auto iterEnd = binaryAttrs.end();
    {
        auto iter = binaryAttrs.find("table_distribution");
        if (iter != iterEnd) {
            initTableDistribution(iter->second);
        }
    }
    return true;
}

navi::ErrorCode TableSplitKernel::doInit(navi::KernelInitContext& ctx) {
    NAVI_KERNEL_LOG(DEBUG, "TableSplitKernel::doInit");
    _tableSplit.init(_tableDist);
    return EC_NONE;
}

navi::ErrorCode TableSplitKernel::doCompute(
    const navi::StreamData& streamData,
    std::vector<navi::DataPtr>& dataVec)
{
    NAVI_KERNEL_LOG(DEBUG, "TableSplitKernel::doCompute");
    if (streamData.data == nullptr) {
        NAVI_KERNEL_LOG(DEBUG, "streamData.data == nullptr");
        return navi::EC_NONE;
    }
    auto table = KernelUtil::getTable(streamData.data);
    if (table == nullptr) {
        NAVI_KERNEL_LOG(ERROR, "get input table failed");
        return navi::EC_ABORT;
    }
    vector<vector<Row>> partRows;
    if (_tableSplit.split(*table, partRows)) {
        outputSplitData(*table, partRows, dataVec);
        return navi::EC_NONE;
    }
    NAVI_KERNEL_LOG(DEBUG, "split failed, use broadcast");
    return DefaultSplitKernel::doCompute(streamData, dataVec);
}

void TableSplitKernel::initTableDistribution(const std::string& jsonConfig) {
    try {
        FastFromJsonString(_tableDist, jsonConfig);
    }
    catch (const legacy::ExceptionBase &e) {
        NAVI_KERNEL_LOG(ERROR, "has error[%s]", e.GetMessage().c_str());
    }
}

void TableSplitKernel::outputSplitData(
    table::Table& table,
    const std::vector<std::vector<table::Row>>& partRows,
    std::vector<navi::DataPtr>& dataVec)
{
    const auto& partInfo = getPartInfo();
    for (size_t i = 0; i < dataVec.size(); ++i) {
        auto partId = partInfo.getUsedPartId(i);
        if (unlikely(partId < 0 || partId >= partRows.size())) {
            NAVI_KERNEL_LOG(WARN, "used part index:%ld partId:%d not valid", i, partId);
            continue;
        }
        const auto& rows = partRows[partId];
        TablePtr partTable(new Table(rows, table.getMatchDocAllocatorPtr()));
        dataVec[i].reset(new TableData(partTable));
        NAVI_KERNEL_LOG(DEBUG, "set dataVec[%ld], table row count:%ld",
            i, partTable->getRowCount());
        NAVI_KERNEL_LOG(TRACE3, "table:\n%s", TableUtil::toString(partTable, 10).c_str());
    }
}

REGISTER_KERNEL(TableSplitKernel);

} //end namespace kernel
} //end namespace isearch
