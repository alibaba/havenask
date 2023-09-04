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
#include "sql/ops/join/JoinKernelBase.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <engine/KernelInitContext.h>
#include <engine/NaviConfigContext.h>
#include <map>
#include <memory>
#include <stdint.h>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "autil/HashAlgorithm.h"
#include "autil/HashUtil.h"
#include "autil/MultiValueType.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/mem_pool/Pool.h"
#include "kmonitor/client/MetricLevel.h"
#include "kmonitor/client/MetricMacro.h"
#include "kmonitor/client/MetricsReporter.h"
#include "kmonitor/client/core/MutableMetric.h"
#include "matchdoc/ValueType.h"
#include "navi/builder/KernelDefBuilder.h"
#include "navi/common.h"
#include "navi/engine/Data.h"
#include "navi/engine/KernelComputeContext.h"
#include "navi/engine/KernelConfigContext.h"
#include "navi/resource/GraphMemoryPoolR.h"
#include "sql/common/Log.h"
#include "sql/common/common.h"
#include "sql/ops/calc/CalcTableR.h"
#include "sql/ops/condition/Condition.h"
#include "sql/ops/condition/ConditionParser.h"
#include "sql/ops/join/AntiJoin.h"
#include "sql/ops/join/HashJoinConditionVisitor.h"
#include "sql/ops/join/InnerJoin.h"
#include "sql/ops/join/JoinBase.h"
#include "sql/ops/join/JoinInfoCollector.h"
#include "sql/ops/join/LeftJoin.h"
#include "sql/ops/join/SemiJoin.h"
#include "sql/ops/util/KernelUtil.h"
#include "sql/proto/SqlSearchInfo.pb.h"
#include "table/Column.h"
#include "table/ColumnData.h"
#include "table/ColumnSchema.h"
#include "table/Table.h"

namespace kmonitor {
class MetricsTags;
} // namespace kmonitor

using namespace std;
using namespace autil;
using namespace matchdoc;
using namespace table;
using namespace kmonitor;

namespace sql {

const int JoinKernelBase::DEFAULT_BATCH_SIZE = 100000;
static const size_t HASH_VALUE_BUFFER_LIMIT = 100000;

class JoinOpMetrics : public MetricsGroup {
public:
    bool init(kmonitor::MetricsGroupManager *manager) override {
        REGISTER_LATENCY_MUTABLE_METRIC(_totalEvaluateTime, "TotalEvaluateTime");
        REGISTER_LATENCY_MUTABLE_METRIC(_totalJoinTime, "TotalJoinTime");
        REGISTER_LATENCY_MUTABLE_METRIC(_totalHashTime, "TotalHashTime");
        REGISTER_LATENCY_MUTABLE_METRIC(_totalTime, "TotalTime");
        REGISTER_GAUGE_MUTABLE_METRIC(_totalJoinCount, "TotalJoinCount");
        REGISTER_GAUGE_MUTABLE_METRIC(_totalRightHashCount, "TotalRightHashCount");
        REGISTER_GAUGE_MUTABLE_METRIC(_totalLeftHashCount, "TotalLeftHashCount");
        REGISTER_GAUGE_MUTABLE_METRIC(_hashMapSize, "HashMapSize");
        REGISTER_GAUGE_MUTABLE_METRIC(_totalComputeTimes, "TotalComputeTimes");
        REGISTER_GAUGE_MUTABLE_METRIC(_rightScanTime, "RightScanTime");
        REGISTER_GAUGE_MUTABLE_METRIC(_rightUpdateQueryTime, "RightUpdateQueryTime");
        return true;
    }

    void report(const kmonitor::MetricsTags *tags, JoinInfo *joinInfo) {
        REPORT_MUTABLE_METRIC(_totalEvaluateTime, joinInfo->totalevaluatetime() / 1000.0);
        REPORT_MUTABLE_METRIC(_totalJoinTime, joinInfo->totaljointime() / 1000.0);
        REPORT_MUTABLE_METRIC(_totalHashTime, joinInfo->totalhashtime() / 1000.0);
        REPORT_MUTABLE_METRIC(_totalTime, joinInfo->totalusetime() / 1000.0);
        REPORT_MUTABLE_METRIC(_totalJoinCount, joinInfo->totaljoincount());
        REPORT_MUTABLE_METRIC(_totalRightHashCount, joinInfo->totalrighthashcount());
        REPORT_MUTABLE_METRIC(_totalLeftHashCount, joinInfo->totallefthashcount());
        REPORT_MUTABLE_METRIC(_hashMapSize, joinInfo->hashmapsize());
        REPORT_MUTABLE_METRIC(_totalComputeTimes, joinInfo->totalcomputetimes());
        REPORT_MUTABLE_METRIC(_rightScanTime, joinInfo->rightscantime() / 1000.0);
        REPORT_MUTABLE_METRIC(_rightUpdateQueryTime, joinInfo->rightupdatequerytime() / 1000.0);
    }

private:
    MutableMetric *_totalEvaluateTime = nullptr;
    MutableMetric *_totalJoinTime = nullptr;
    MutableMetric *_totalHashTime = nullptr;
    MutableMetric *_totalTime = nullptr;
    MutableMetric *_totalJoinCount = nullptr;
    MutableMetric *_rightScanTime = nullptr;
    MutableMetric *_rightUpdateQueryTime = nullptr;
    MutableMetric *_totalRightHashCount = nullptr;
    MutableMetric *_totalLeftHashCount = nullptr;
    MutableMetric *_hashMapSize = nullptr;
    MutableMetric *_totalComputeTimes = nullptr;
};

JoinKernelBase::JoinKernelBase()
    : _joinIndex(0)
    , _systemFieldNum(0)
    , _batchSize(DEFAULT_BATCH_SIZE)
    , _truncateThreshold(0)
    , _opId(-1)
    , _isEquiJoin(true)
    , _shouldClearTable(false) {}

JoinKernelBase::~JoinKernelBase() {}

void JoinKernelBase::addDepend(navi::KernelDefBuilder &builder) const {
    builder.enableBuildR({CalcTableR::RESOURCE_ID});
}

bool JoinKernelBase::config(navi::KernelConfigContext &ctx) {
    ctx.JsonizeAsString("condition", _conditionJson, "");
    ctx.JsonizeAsString("equi_condition", _equiConditionJson, "");
    ctx.JsonizeAsString("remaining_condition", _remainConditionJson, "");
    NAVI_JSONIZE(ctx, "join_type", _joinType);
    NAVI_JSONIZE(ctx, "semi_join_type", _semiJoinType, _semiJoinType);
    NAVI_JSONIZE(ctx, "reuse_inputs", _reuseInputs, _reuseInputs);
    // for compatible
    if (_joinType == SQL_SEMI_JOIN_TYPE || _semiJoinType == SQL_SEMI_JOIN_TYPE) {
        _joinType = _semiJoinType = SQL_SEMI_JOIN_TYPE;
    }
    NAVI_JSONIZE(ctx, "system_field_num", _systemFieldNum, _systemFieldNum);
    NAVI_JSONIZE(ctx, "is_equi_join", _isEquiJoin, _isEquiJoin);
    NAVI_JSONIZE(ctx, IQUAN_OP_ID, _opId, _opId);

    // join parameters
    NAVI_JSONIZE(
        ctx, "left_input_fields", _joinBaseParam._leftInputFields, _joinBaseParam._leftInputFields);
    NAVI_JSONIZE(ctx,
                 "right_input_fields",
                 _joinBaseParam._rightInputFields,
                 _joinBaseParam._rightInputFields);
    NAVI_JSONIZE(
        ctx, "output_fields_internal", _joinBaseParam._outputFields, _joinBaseParam._outputFields);
    NAVI_JSONIZE(ctx, "output_fields", _outputFields, _outputFields);
    KernelUtil::stripName(_joinBaseParam._leftInputFields);
    KernelUtil::stripName(_joinBaseParam._rightInputFields);
    KernelUtil::stripName(_outputFields);
    KernelUtil::stripName(_joinBaseParam._outputFields);
    if (!convertFields()) {
        SQL_LOG(ERROR, "convert fields failed.");
        return false;
    }

    // hints config
    std::map<std::string, std::map<std::string, std::string>> hintsMap;
    NAVI_JSONIZE(ctx, "hints", hintsMap, hintsMap);
    if (hintsMap.find(SQL_JOIN_HINT) != hintsMap.end()) {
        _hashHints.swap(hintsMap[SQL_JOIN_HINT]);
    }
    NAVI_JSONIZE(ctx, "batch_size", _batchSize, _batchSize);
    if (_batchSize == 0) {
        _batchSize = DEFAULT_BATCH_SIZE;
    }
    patchHintInfo(_hashHints);
    return true;
}

navi::ErrorCode JoinKernelBase::init(navi::KernelInitContext &context) {
    _joinBaseParam._pool = _queryMemPoolR->getPool().get();
    navi::ErrorCode ec = initCalcTableAndJoinColumns(context);
    if (navi::EC_NONE != ec) {
        return ec;
    }
    _joinInfo.set_kernelname(getKernelName());
    _joinInfo.set_nodename(getNodeName());
    if (_opId < 0) {
        const string &keyStr
            = getKernelName() + "__" + _conditionJson + "__" + StringUtil::toString(_outputFields);
        _joinInfo.set_hashkey(autil::HashAlgorithm::hashString(keyStr.c_str(), keyStr.size(), 1));
    } else {
        _joinInfo.set_hashkey(_opId);
    }
    _joinBaseParam._joinInfo = &_joinInfo;
    _joinBaseParam._poolPtr = _graphMemoryPoolR->getPool();

    if (_joinType == SQL_INNER_JOIN_TYPE) {
        _joinPtr.reset(new InnerJoin(_joinBaseParam));
    } else if (_joinType == SQL_LEFT_JOIN_TYPE) {
        _joinPtr.reset(new LeftJoin(_joinBaseParam));
    } else if (_joinType == SQL_SEMI_JOIN_TYPE) {
        _joinPtr.reset(new SemiJoin(_joinBaseParam));
    } else if (_joinType == SQL_ANTI_JOIN_TYPE) {
        _joinPtr.reset(new AntiJoin(_joinBaseParam));
    } else if (_joinType == SQL_LEFT_MULTI_JOIN_TYPE) {
        //  pass, left multi join dont need joinptr
    } else {
        SQL_LOG(ERROR, "join type [%s] is not supported", _joinType.c_str());
        return navi::EC_ABORT;
    }
    return navi::EC_NONE;
}

navi::ErrorCode JoinKernelBase::initCalcTableAndJoinColumns(navi::KernelInitContext &ctx) {
    ConditionParser parser(_joinBaseParam._poolPtr.get());
    ConditionPtr condition;
    if (!_remainConditionJson.empty()) {
        if (!parser.parseCondition(_equiConditionJson, condition)) {
            SQL_LOG(ERROR, "parse condition [%s] failed", _equiConditionJson.c_str());
            return navi::EC_ABORT;
        }
        auto calcTableR = CalcTableR::buildOne(ctx, {}, {}, _remainConditionJson, "");
        if (!calcTableR) {
            SQL_LOG(WARN, "build CalcTableR failed");
            return navi::EC_ABORT;
        }
        _joinBaseParam._calcTableR = calcTableR;
    } else {
        if (!parser.parseCondition(_conditionJson, condition)) {
            SQL_LOG(ERROR, "parse condition [%s] failed", _conditionJson.c_str());
            return navi::EC_ABORT;
        }
    }

    HashJoinConditionVisitor visitor(_output2InputMap);
    if (condition) {
        condition->accept(&visitor);
    }
    if (visitor.isError()) {
        SQL_LOG(ERROR, "visit condition failed, error info [%s]", visitor.errorInfo().c_str());
        return navi::EC_ABORT;
    }

    _leftJoinColumns = visitor.getLeftJoinColumns();
    _rightJoinColumns = visitor.getRightJoinColumns();
    if (_leftJoinColumns.size() != _rightJoinColumns.size()) {
        SQL_LOG(ERROR,
                "left join column size [%zu] not match right one [%zu]",
                _leftJoinColumns.size(),
                _rightJoinColumns.size());
        return navi::EC_ABORT;
    }

    return navi::EC_NONE;
}

navi::ErrorCode JoinKernelBase::compute(navi::KernelComputeContext &runContext) {
    return navi::EC_ABORT;
}

bool JoinKernelBase::convertFields() {
    if (_systemFieldNum != 0) {
        return false;
    }
    if (_joinBaseParam._outputFields.empty()) {
        _joinBaseParam._outputFields = _outputFields;
    }
    size_t joinFiledSize
        = _joinBaseParam._leftInputFields.size() + _joinBaseParam._rightInputFields.size();
    if (_semiJoinType == SQL_SEMI_JOIN_TYPE || _joinType == SQL_ANTI_JOIN_TYPE) {
        if (_outputFields.size() != _joinBaseParam._leftInputFields.size()) {
            SQL_LOG(ERROR,
                    "semi join or anti join output fields [%ld] not equal left input fields [%ld]",
                    _outputFields.size(),
                    _joinBaseParam._leftInputFields.size());
            return false;
        }
    } else {
        if (_outputFields.size() != joinFiledSize) {
            return false;
        }
    }

    if (_joinBaseParam._outputFields.size() != joinFiledSize) {
        return false;
    }

    for (size_t i = 0; i < _joinBaseParam._outputFields.size(); i++) {
        if (i < _joinBaseParam._leftInputFields.size()) {
            _output2InputMap[_joinBaseParam._outputFields[i]]
                = make_pair(_joinBaseParam._leftInputFields[i], true);
        } else {
            size_t offset = i - _joinBaseParam._leftInputFields.size();
            _output2InputMap[_joinBaseParam._outputFields[i]]
                = make_pair(_joinBaseParam._rightInputFields[offset], false);
        }
    }
    return true;
}

bool JoinKernelBase::createHashMap(const table::TablePtr &table,
                                   size_t offset,
                                   size_t count,
                                   bool hashLeftTable) {
    _hashJoinMap.clear();
    uint64_t beginHash = TimeUtility::currentTime();
    const auto &joinColumns = hashLeftTable ? _leftJoinColumns : _rightJoinColumns;
    HashValues values;
    if (!getHashValues(table, offset, count, joinColumns, values)) {
        return false;
    }
    if (hashLeftTable) {
        JoinInfoCollector::incLeftHashCount(&_joinInfo, values.size());
    } else {
        JoinInfoCollector::incRightHashCount(&_joinInfo, values.size());
    }
    uint64_t afterHash = TimeUtility::currentTime();
    JoinInfoCollector::incHashTime(&_joinInfo, afterHash - beginHash);
    for (const auto &valuePair : values) {
        const auto &hashKey = valuePair.second;
        const auto &hashValue = valuePair.first;
        auto iter = _hashJoinMap.find(hashKey);
        if (iter == _hashJoinMap.end()) {
            vector<size_t> hashValues = {hashValue};
            _hashJoinMap.insert(make_pair(hashKey, std::move(hashValues)));
        } else {
            iter->second.push_back(hashValue);
        }
    }
    JoinInfoCollector::incHashMapSize(&_joinInfo, _hashJoinMap.size());
    uint64_t endHash = TimeUtility::currentTime();
    JoinInfoCollector::incCreateTime(&_joinInfo, endHash - afterHash);
    return true;
}

bool JoinKernelBase::getHashValues(const table::TablePtr &table,
                                   size_t offset,
                                   size_t count,
                                   const vector<string> &joinColumns,
                                   HashValues &values) {
    if (!getColumnHashValues(table, offset, count, joinColumns[0], values)) {
        return false;
    }
    for (size_t i = 1; i < joinColumns.size(); ++i) {
        HashValues tmpValues;
        if (!getColumnHashValues(table, offset, count, joinColumns[i], tmpValues)) {
            return false;
        }
        combineHashValues(tmpValues, values);
    }
    return true;
}

bool JoinKernelBase::getColumnHashValues(const table::TablePtr &table,
                                         size_t offset,
                                         size_t count,
                                         const string &columnName,
                                         HashValues &values) {
    auto joinColumn = table->getColumn(columnName);
    if (joinColumn == nullptr) {
        SQL_LOG(ERROR, "invalid join column name [%s]", columnName.c_str());
        return false;
    }
    auto schema = joinColumn->getColumnSchema();
    if (schema == nullptr) {
        SQL_LOG(ERROR, "get join column [%s] schema failed", columnName.c_str());
        return false;
    }
    size_t rowCount = min(offset + count, table->getRowCount());
    auto vt = schema->getType();
    bool isMulti = vt.isMultiValue();
    switch (vt.getBuiltinType()) {
#define CASE_MACRO(ft)                                                                             \
    case ft: {                                                                                     \
        if (isMulti) {                                                                             \
            typedef MatchDocBuiltinType2CppType<ft, true>::CppType T;                              \
            auto columnData = joinColumn->getColumnData<T>();                                      \
            if (unlikely(!columnData)) {                                                           \
                SQL_LOG(ERROR, "impossible cast column data failed");                              \
                return false;                                                                      \
            }                                                                                      \
            values.reserve(std::max(rowCount * 4, HASH_VALUE_BUFFER_LIMIT));                       \
            size_t dataSize = 0;                                                                   \
            for (size_t i = offset; i < rowCount; i++) {                                           \
                const auto &datas = columnData->get(i);                                            \
                dataSize = datas.size();                                                           \
                for (size_t k = 0; k < dataSize; k++) {                                            \
                    auto hashKey = HashUtil::calculateHashValue(datas[k]);                         \
                    values.emplace_back(i, hashKey);                                               \
                }                                                                                  \
            }                                                                                      \
        } else {                                                                                   \
            typedef MatchDocBuiltinType2CppType<ft, false>::CppType T;                             \
            auto columnData = joinColumn->getColumnData<T>();                                      \
            if (unlikely(!columnData)) {                                                           \
                SQL_LOG(ERROR, "impossible cast column data failed");                              \
                return false;                                                                      \
            }                                                                                      \
            values.reserve(rowCount);                                                              \
            for (size_t i = offset; i < rowCount; i++) {                                           \
                const auto &data = columnData->get(i);                                             \
                auto hashKey = HashUtil::calculateHashValue(data);                                 \
                values.emplace_back(i, hashKey);                                                   \
            }                                                                                      \
        }                                                                                          \
        break;                                                                                     \
    }
        BUILTIN_TYPE_MACRO_HELPER(CASE_MACRO);
#undef CASE_MACRO
    default: {
        SQL_LOG(ERROR, "impossible reach this branch");
        return false;
    }
    }
    if (values.empty()) {
        _shouldClearTable = true; // for multi-type datas may be empty
        SQL_LOG(WARN,
                "table size[%zu], hash values size[%zu], should clear",
                table->getRowCount(),
                values.size());
    }
    return true;
}

void JoinKernelBase::combineHashValues(const HashValues &valuesA, HashValues &valuesB) {
    HashValues tmpValues;
    size_t beginA = 0;
    size_t beginB = 0;
    while (beginA < valuesA.size() && beginB < valuesB.size()) {
        size_t rowIndexA = valuesA[beginA].first;
        size_t rowIndexB = valuesB[beginB].first;
        if (rowIndexA < rowIndexB) {
            ++beginA;
        } else if (rowIndexA == rowIndexB) {
            size_t offsetB = beginB;
            for (; offsetB < valuesB.size() && rowIndexA == valuesB[offsetB].first; offsetB++) {
                size_t seed = valuesA[beginA].second;
                HashUtil::combineHash(seed, valuesB[offsetB].second);
                tmpValues.emplace_back(rowIndexA, seed);
            }
            ++beginA;
        } else {
            ++beginB;
        }
    }
    valuesB = std::move(tmpValues);
}

bool JoinKernelBase::getLookupInput(navi::KernelComputeContext &runContext,
                                    const navi::PortIndex &portIndex,
                                    bool leftTable,
                                    table::TablePtr &inputTable,
                                    bool &eof) {
    static const size_t EmptyBufferLimitSize = 1;
    return getInput(runContext, portIndex, leftTable, EmptyBufferLimitSize, inputTable, eof);
}

bool JoinKernelBase::getInput(navi::KernelComputeContext &runContext,
                              const navi::PortIndex &portIndex,
                              bool leftTable,
                              size_t bufferLimitSize,
                              table::TablePtr &inputTable,
                              bool &eof) {
    if (inputTable == nullptr || inputTable->getRowCount() < bufferLimitSize) {
        navi::DataPtr data;
        runContext.getInput(portIndex, data, eof);
        if (data != nullptr) {
            auto table = KernelUtil::getTable(data, _graphMemoryPoolR, !_reuseInputs.empty());
            if (!table) {
                SQL_LOG(ERROR, "invalid input table");
                return false;
            }
            if (leftTable) {
                incTotalLeftInputTable(table->getRowCount());
            } else {
                incTotalRightInputTable(table->getRowCount());
            }
            if (!inputTable) {
                inputTable = table;
            } else {
                if (!inputTable->merge(table)) {
                    SQL_LOG(ERROR, "merge input failed");
                    return false;
                }
            }
        }
    }
    return true;
}

bool JoinKernelBase::canTruncate(size_t joinedCount, size_t truncateThreshold) {
    if (truncateThreshold > 0 && joinedCount >= truncateThreshold) {
        return true;
    }
    return false;
}

void JoinKernelBase::reserveJoinRow(size_t rowCount) {
    _tableAIndexes.reserve(rowCount);
    _tableBIndexes.reserve(rowCount);
}

void JoinKernelBase::patchHintInfo(const map<string, string> &hintsMap) {
    if (hintsMap.empty()) {
        return;
    }
    auto iter = hintsMap.find("batchSize");
    if (iter != hintsMap.end()) {
        uint32_t batchSize = 0;
        StringUtil::fromString(iter->second, batchSize);
        if (batchSize > 0) {
            _batchSize = batchSize;
        }
    }
    iter = hintsMap.find("truncateThreshold");
    if (iter != hintsMap.end()) {
        uint32_t truncateThreshold = 0;
        StringUtil::fromString(iter->second, truncateThreshold);
        if (truncateThreshold > 0) {
            _truncateThreshold = truncateThreshold;
            SQL_LOG(DEBUG, "truncate threshold [%ld]", _truncateThreshold);
        }
    }
    iter = hintsMap.find("defaultValue");
    if (iter != hintsMap.end()) {
        vector<string> values;
        StringUtil::fromString(iter->second, values, ",");
        for (auto &value : values) {
            auto pair = StringUtil::split(value, ":");
            StringUtil::toUpperCase(pair[0]);
            _joinBaseParam._defaultValue[pair[0]] = pair[1];
            SQL_LOG(DEBUG, "join use default value [%s] : [%s]", pair[0].c_str(), pair[1].c_str());
        }
    }
}

void JoinKernelBase::reportMetrics() {
    if (_queryMetricReporterR) {
        string pathName = "sql.user.ops." + getKernelName();
        auto opMetricsReporter = _queryMetricReporterR->getReporter()->getSubReporter(pathName, {});
        opMetricsReporter->report<JoinOpMetrics, JoinInfo>(nullptr, &_joinInfo);
    }
}

void JoinKernelBase::incTotalLeftInputTable(size_t count) {
    _joinInfo.set_totalleftinputcount(_joinInfo.totalleftinputcount() + count);
}

void JoinKernelBase::incTotalRightInputTable(size_t count) {
    _joinInfo.set_totalrightinputcount(_joinInfo.totalrightinputcount() + count);
}

} // namespace sql
