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
#include "ha3/sql/ops/correlate/ExecCorrelateKernel.h"

#include <algorithm>
#include <cstdint>
#include <iosfwd>
#include <memory>
#include <set>
#include <sstream>
#include <stddef.h>
#include <utility>

#include "alog/Logger.h"
#include "autil/HashAlgorithm.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "ha3/sql/common/Log.h"
#include "ha3/sql/common/common.h"
#include "ha3/sql/data/TableData.h"
#include "ha3/sql/data/TableType.h"
#include "ha3/sql/ops/correlate/CorrelateInfo.h"
#include "ha3/sql/ops/util/KernelUtil.h"
#include "ha3/sql/proto/SqlSearchInfo.pb.h"
#include "ha3/sql/proto/SqlSearchInfoCollector.h"
#include "ha3/sql/resource/SqlBizResource.h"
#include "ha3/sql/resource/SqlQueryResource.h"
#include "ha3/sql/resource/MetaInfoResource.h"
#include "kmonitor/client/MetricMacro.h"
#include "kmonitor/client/MetricsReporter.h"
#include "kmonitor/client/core/MutableMetric.h"
#include "matchdoc/ValueType.h"
#include "matchdoc/VectorDocStorage.h"
#include "navi/builder/KernelDefBuilder.h"
#include "navi/common.h"
#include "navi/engine/CreatorRegistry.h"
#include "navi/engine/Data.h"
#include "navi/engine/Kernel.h"
#include "navi/engine/KernelComputeContext.h"
#include "navi/engine/KernelConfigContext.h"
#include "navi/engine/Resource.h"
#include "navi/resource/GraphMemoryPoolResource.h"
#include "table/Column.h"
#include "table/ColumnData.h"
#include "table/ColumnSchema.h"
#include "table/DataCommon.h"
#include "table/Row.h"
#include "table/Table.h"
#include "table/TableUtil.h"
#include "table/ValueTypeSwitch.h"

namespace navi {
class KernelInitContext;
} // namespace navi

namespace kmonitor {
class MetricsTags;
} // namespace kmonitor

using namespace std;
using namespace matchdoc;
using namespace navi;
using namespace autil;
using namespace table;
using namespace suez::turing;
using namespace kmonitor;

namespace isearch {
namespace sql {
AUTIL_LOG_SETUP(sql, ExecCorrelateKernel);

class ExecCorrelateOpMetrics : public MetricsGroup {
public:
    bool init(kmonitor::MetricsGroupManager *manager) override {
        REGISTER_GAUGE_MUTABLE_METRIC(_totalComputeTimes, "TotalComputeTimes");
        REGISTER_GAUGE_MUTABLE_METRIC(_totalOutputCount, "TotalOutputCount");
        REGISTER_LATENCY_MUTABLE_METRIC(_totalUsedTime, "TotalUsedTime");
        return true;
    }
    void report(const kmonitor::MetricsTags *tags, CalcInfo *calcInfo) {
        REPORT_MUTABLE_METRIC(_totalComputeTimes, calcInfo->totalcomputetimes());
        REPORT_MUTABLE_METRIC(_totalOutputCount, calcInfo->totaloutputcount());
        REPORT_MUTABLE_METRIC(_totalUsedTime, calcInfo->totalusetime() / 1000);
    }

private:
    MutableMetric *_totalComputeTimes = nullptr;
    MutableMetric *_totalOutputCount = nullptr;
    MutableMetric *_totalUsedTime = nullptr;
};

ExecCorrelateKernel::ExecCorrelateKernel()
    : _opId(-1) {}

ExecCorrelateKernel::~ExecCorrelateKernel() {
    reportMetrics();
}

auto singleFunc = [](auto a) -> bool { return false; };

void ExecCorrelateKernel::def(navi::KernelDefBuilder &builder) const {
    builder.name("ExecCorrelateKernel")
        .input("input0", TableType::TYPE_ID)
        .output("output0", TableType::TYPE_ID)
        .resource(SqlBizResource::RESOURCE_ID, true, BIND_RESOURCE_TO(_bizResource))
        .resource(SqlQueryResource::RESOURCE_ID, true, BIND_RESOURCE_TO(_queryResource))
        .resource(navi::GRAPH_MEMORY_POOL_RESOURCE_ID, true, BIND_RESOURCE_TO(_memoryPoolResource))
        .resource(navi::META_INFO_RESOURCE_ID, false, BIND_RESOURCE_TO(_metaInfoResource));
}

bool ExecCorrelateKernel::config(navi::KernelConfigContext &ctx) {
    std::vector<std::string> outputFields, outputFieldsType;
    std::vector<CorrelateInfo> infos;
    ctx.Jsonize("output_fields", outputFields, outputFields);
    ctx.Jsonize("output_fields_type", outputFieldsType, outputFieldsType);
    ctx.Jsonize("uncollect_attrs", infos, infos);
    ctx.Jsonize("reuse_inputs", _reuseInputs, _reuseInputs);
    ctx.Jsonize(IQUAN_OP_ID, _opId, _opId);
    if (infos.size() == 0) {
        SQL_LOG(ERROR, "can not found uncollect_attrs");
        return false;
    }
    CorrelateInfo &info = infos[0];
    if (outputFields.size() != outputFieldsType.size()) {
        SQL_LOG(ERROR, "output field size not equal output field type size");
        return false;
    }

    KernelUtil::stripName(outputFields);
    KernelUtil::stripName(info._nestFieldNames);
    KernelUtil::stripName(info._outputFields);

    {
        UNNESTINFO_ERRNO flag = info.check();
        if (flag != UE_OK) {
            SQL_LOG(ERROR,
                    "parse attribute failed [%s], invalid unnest info [%s]",
                    unnestInfoErrno2String(flag).c_str(),
                    info.toString().c_str());
            return false;
        }
        _hasOffset = info._withOrdinality;
        if (_hasOffset) {
            if (info._outputFields.size() <= 1) {
                SQL_LOG(ERROR,
                        "uncollect_attrs -> output_fields invalid size [%lu]",
                        info._outputFields.size());
                return false;
            }
            // we think offset is the lase one
            _offsetField = info._outputFields[info._outputFields.size() - 1];
        }
        // size_t offset = 0;
        for (size_t i = 0; i < info._nestFieldNames.size(); i++) {
            if (info._nestFieldCounts[i] != 1) {
                SQL_LOG(ERROR, "uncollect_attrs -> nest_field_counts element \
                        is not 1, not support this pattern");
                return false;
            }
            _unnestFieldMapper.insert(
                std::make_pair(info._nestFieldNames[i], info._outputFields[i]));
        }
        std::set<std::string> unnestOrOffsetFields;
        for (std::string &field : info._outputFields) {
            unnestOrOffsetFields.insert(field);
        }

        for (std::string &field : outputFields) {
            if (unnestOrOffsetFields.count(field)) {
                continue;
            }
            _copyFields.push_back(field);
        }

        // valid the unnest field type, it must be ARRAY
        for (size_t i = 0; i < outputFields.size(); i++) {
            if (_unnestFieldMapper.find(outputFields[i]) != _unnestFieldMapper.end()) {
                if (outputFieldsType[i].size() < 5
                    || outputFieldsType[i].compare(0, 5, "ARRAY") != 0) {
                    SQL_LOG(ERROR,
                            "nest field type not array [%s], field [%s]",
                            outputFieldsType[i].c_str(),
                            outputFields[i].c_str());
                    return false;
                }
            }
        }
    }
    SQL_LOG(INFO,
            "copy fields [%lu], unnest field [%lu], offset[%s]",
            _copyFields.size(),
            _unnestFieldMapper.size(),
            (_hasOffset ? "true" : "false"));

    return true;
}

navi::ErrorCode ExecCorrelateKernel::init(navi::KernelInitContext &context) {
    if (_metaInfoResource) {
        _sqlSearchInfoCollector = _metaInfoResource->getOverwriteInfoCollector();
    }
    const std::string &kernelName = getKernelName();
    auto queryMetricsReporter = _queryResource->getQueryMetricsReporter();
    if (queryMetricsReporter != nullptr) {
        std::string &&pathName = "sql.user.ops." + kernelName;
        _opMetricReporter = queryMetricsReporter->getSubReporter(pathName, {}).get();
    }
    _calcInfo.set_kernelname(kernelName);
    _calcInfo.set_nodename(getNodeName());
    if (_opId < 0) {
        std::string &&keyStr = kernelName + "__" + StringUtil::toString(_copyFields);
        _calcInfo.set_hashkey(autil::HashAlgorithm::hashString(keyStr.c_str(), keyStr.size(), 1));
    } else {
        _calcInfo.set_hashkey(_opId);
    }
    return navi::EC_NONE;
}

navi::ErrorCode ExecCorrelateKernel::compute(KernelComputeContext &ctx) {
    incComputeTimes();
    uint64_t beginTime = TimeUtility::currentTime();
    navi::PortIndex inputIndex(0, navi::INVALID_INDEX);
    bool isEof = false;
    navi::DataPtr data;
    ctx.getInput(inputIndex, data, isEof);
    TablePtr outputTable;

    if (data) {
        auto inputTable = KernelUtil::getTable(data, _memoryPoolResource, false);
        if (!inputTable) {
            SQL_LOG(ERROR, "invalid input table");
            return EC_ABORT;
        }
        outputTable = doUnnestTable(inputTable);
        if (!outputTable) {
            return EC_ABORT;
        }
        outputTable->mergeDependentPools(inputTable);
        incOutputCount(outputTable->getRowCount());
    }

    uint64_t totalComputeTimes = _calcInfo.totalcomputetimes();
    if (totalComputeTimes < 5) {
        SQL_LOG(TRACE1,
                "calc batch [%lu] output table: [%s]",
                totalComputeTimes,
                TableUtil::toString(outputTable, 10).c_str());
    }

    navi::PortIndex outputIndex(0, navi::INVALID_INDEX);
    TableDataPtr tableData(new TableData(outputTable));
    ctx.setOutput(outputIndex, tableData, isEof);
    uint64_t endTime = TimeUtility::currentTime();
    incTotalTime(endTime - beginTime);

    if (isEof) {
        SQL_LOG(DEBUG, "calc info: [%s]", _calcInfo.ShortDebugString().c_str());
    }
    if (_sqlSearchInfoCollector) {
        _sqlSearchInfoCollector->overwriteCalcInfo(_calcInfo);
    }
    return EC_NONE;
}

bool ExecCorrelateKernel::initOutputTable(const TablePtr inputTable, TablePtr outputTable) {
    // based on input table to declare output table column
    for (size_t i = 0; i < _copyFields.size(); i++) {
        const string &copyField = _copyFields[i];
        Column *tmpPtr = inputTable->getColumn(copyField);
        if (tmpPtr == nullptr) {
            SQL_LOG(ERROR, "can not found copy column [%s] in input table", copyField.c_str());
            return false;
        }
        ColumnSchema *tmpSchema = tmpPtr->getColumnSchema();
        if (tmpSchema == nullptr) {
            SQL_LOG(ERROR, "can not fetch copy column schema from column [%s]", copyField.c_str());
            return false;
        }
        ValueType valueType = tmpSchema->getType();
        outputTable->declareColumn(copyField, valueType, false);
    }

    // declare unnest field to output table
    for (auto &kv : _unnestFieldMapper) {
        Column *tmpPtr = inputTable->getColumn(kv.first);
        if (tmpPtr == nullptr) {
            SQL_LOG(ERROR, "can not found nest column [%s] in input table", kv.first.c_str());
            return false;
        }
        ColumnSchema *tmpSchema = tmpPtr->getColumnSchema();
        if (tmpSchema == nullptr) {
            SQL_LOG(ERROR, "can not fetch nest column schema from column [%s]", kv.second.c_str());
            return false;
        }
        ValueType valueType = tmpSchema->getType();
        valueType.setMultiValue(false);
        outputTable->declareColumn(kv.second, valueType, false);
    }

    if (_hasOffset) {
        outputTable->declareColumn(_offsetField, ValueTypeHelper<int32_t>::getValueType(), false);
    }
    outputTable->endGroup();

    return true;
}

bool ExecCorrelateKernel::calcOutputTableMultiRowOffset(const TablePtr &inputTable,
                                                        std::vector<uint32_t> *offset2InputTable) {
    auto debugStr = [&offset2InputTable]() -> std::string {
        std::stringstream ss;
        for (size_t i = 0; i < offset2InputTable->size(); i++) {
            ss << (*offset2InputTable)[i] << ",";
        }
        return ss.str();
    };
    for (auto &kv : _unnestFieldMapper) {
        Column *unnestColumnPtr = inputTable->getColumn(kv.first);
        ColumnSchema *unnestColumnSchema = unnestColumnPtr->getColumnSchema();
        if (unnestColumnSchema == nullptr) {
            SQL_LOG(ERROR, "can not get unnest column schema");
            return false;
        }
        if (!calcOutputTableOneRowOffset(
                *unnestColumnPtr, *unnestColumnSchema, offset2InputTable)) {
            return false;
        }
        SQL_LOG(DEBUG, "field [%s], offset2InputTable [%s]", kv.first.c_str(), debugStr().c_str())
    }

    return true;
}

bool ExecCorrelateKernel::calcOutputTableOneRowOffset(Column &unnestColumn,
                                                      const ColumnSchema &unnestColumnSchema,
                                                      std::vector<uint32_t> *offset2InputTable) {
    auto multiFunc = [&](auto a) -> bool {
        using Multi = typename decltype(a)::value_type;

        ColumnData<Multi> *inputDatas = unnestColumn.getColumnData<Multi>();
        for (size_t i = 0; i < unnestColumn.getRowCount(); i++) {
            const Multi &multiData = inputDatas->get(i);
            (*offset2InputTable)[i] = std::max(multiData.size(), (*offset2InputTable)[i]);
        }

        return true;
    };
    if (!ValueTypeSwitch::switchType(unnestColumnSchema.getType(), singleFunc, multiFunc)) {
        SQL_LOG(ERROR, "failed to calc table row offset");
        return false;
    }

    return true;
}

bool ExecCorrelateKernel::copyUnnestFields(const TablePtr &inputTable,
                                           TablePtr outputTable,
                                           const std::vector<uint32_t> &offset2InputTable) {
    auto cit = _unnestFieldMapper.cbegin();

    Column *unnestColumnPtr = nullptr;
    ColumnSchema *unnestColumnSchema = nullptr;
    auto multiFunc = [&](auto a) -> bool {
        using Multi = typename decltype(a)::value_type;
        using Single = typename Multi::value_type;

        ColumnData<Multi> *inputDatas = unnestColumnPtr->getColumnData<Multi>();
        static Single defaultValue;
        Column *outputColumnPtr = outputTable->getColumn(cit->second);
        Column *offsetColumnPtr = nullptr;
        ColumnData<int32_t> *offsetDatas = nullptr;

        ColumnData<Single> *outputDatas = outputColumnPtr->getColumnData<Single>();
        if (_hasOffset && cit == _unnestFieldMapper.cbegin()) {
            offsetColumnPtr = outputTable->getColumn(_offsetField);
            offsetDatas = offsetColumnPtr->getColumnData<int32_t>();
        }
        uint32_t currentOffset = 0;
        for (size_t i = 0; i < unnestColumnPtr->getRowCount(); i++) {
            const Multi &multiData = inputDatas->get(i);

            for (size_t j = 0; j < multiData.size(); j++) {
                outputDatas->set(currentOffset + j, multiData[j]);
                if (_hasOffset && cit == _unnestFieldMapper.cbegin()) {
                    // index start from 1
                    offsetDatas->set(currentOffset + j, j + 1);
                }
            }
            for (size_t j = multiData.size(); j < offset2InputTable[i]; j++) {
                outputDatas->set(currentOffset + j, defaultValue);
                if (_hasOffset && cit == _unnestFieldMapper.cbegin()) {
                    offsetDatas->set(currentOffset + j, j + 1);
                }
            }
            currentOffset += offset2InputTable[i];
        }

        return true;
    };
    while (cit != _unnestFieldMapper.cend()) {
        unnestColumnPtr = inputTable->getColumn(cit->first);
        unnestColumnSchema = unnestColumnPtr->getColumnSchema();
        if (unnestColumnSchema == nullptr) {
            SQL_LOG(ERROR, "can not get unnest column schema");
            return false;
        }
        if (!ValueTypeSwitch::switchType(unnestColumnSchema->getType(), singleFunc, multiFunc)) {
            SQL_LOG(ERROR, "failed to copy unnest data");
            return false;
        }

        cit++;
    }

    return true;
}

bool ExecCorrelateKernel::copyOtherFields(const TablePtr &inputTable,
                                          TablePtr outputTable,
                                          const std::vector<uint32_t> &offset2InputTable) {
    Column *columnPtr = nullptr;
    auto otherCopyFunc = [&](auto a) -> bool {
        using T = typename decltype(a)::value_type;

        ColumnData<T> *inputDatas = columnPtr->getColumnData<T>();
        Column *outputColumnPtr = outputTable->getColumn(columnPtr->getColumnSchema()->getName());
        ColumnData<T> *outputDatas = outputColumnPtr->getColumnData<T>();

        uint32_t currentOffset = 0;
        for (size_t i = 0; i < columnPtr->getRowCount(); i++) {
            const T &data = inputDatas->get(i);
            for (size_t j = 0; j < offset2InputTable[i]; j++) {
                outputDatas->set(currentOffset + j, data);
            }
            currentOffset += offset2InputTable[i];
        }

        return true;
    };
    for (size_t i = 0; i < _copyFields.size(); i++) {
        columnPtr = inputTable->getColumn(_copyFields[i]);
        if (columnPtr == nullptr) {
            SQL_LOG(
                ERROR, "can not found copy field [%s] from input table", _copyFields[i].c_str());
            return false;
        }

        if (!ValueTypeSwitch::switchType(
                columnPtr->getColumnSchema()->getType(), otherCopyFunc, otherCopyFunc)) {
            SQL_LOG(ERROR,
                    "failed to copy column [%s] data",
                    columnPtr->getColumnSchema()->getName().c_str());
            return false;
        }
    }

    return true;
}

TablePtr ExecCorrelateKernel::doUnnestTable(const TablePtr &inputTable) {
    std::vector<uint32_t> offset2InputTable(inputTable->getRowCount(), 0);
    TablePtr nullTable;
    TablePtr outputTable(new Table(_memoryPoolResource->getPool()));
    // init output table
    if (!initOutputTable(inputTable, outputTable)) {
        SQL_LOG(ERROR, "init output table failed");
        return nullTable;
    }

    if (!calcOutputTableMultiRowOffset(inputTable, &offset2InputTable)) {
        SQL_LOG(ERROR, "pre calculate table row offset failed");
        return nullTable;
    }

    uint32_t totalRow = 0;
    for (size_t i = 0; i < offset2InputTable.size(); i++) {
        totalRow += offset2InputTable[i];
    }
    outputTable->batchAllocateRow(totalRow);
    if (!copyUnnestFields(inputTable, outputTable, offset2InputTable)) {
        SQL_LOG(ERROR, "copy unnest field failed");
        return nullTable;
    }

    if (!copyOtherFields(inputTable, outputTable, offset2InputTable)) {
        SQL_LOG(ERROR, "copy the other field failed");
        return nullTable;
    }

    return outputTable;
}

void ExecCorrelateKernel::reportMetrics() {
    if (_opMetricReporter != nullptr) {
        _opMetricReporter->report<ExecCorrelateOpMetrics, CalcInfo>(nullptr, &_calcInfo);
    }
}

void ExecCorrelateKernel::incTotalTime(int64_t time) {
    _calcInfo.set_totalusetime(_calcInfo.totalusetime() + time);
}

void ExecCorrelateKernel::incOutputCount(int64_t count) {
    _calcInfo.set_totaloutputcount(_calcInfo.totaloutputcount() + count);
}

void ExecCorrelateKernel::incComputeTimes() {
    _calcInfo.set_totalcomputetimes(_calcInfo.totalcomputetimes() + 1);
}

REGISTER_KERNEL(ExecCorrelateKernel);

} // namespace sql
} // namespace isearch
