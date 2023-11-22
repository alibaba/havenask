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
#include "sql/ops/agg/Aggregator.h"

#include <algorithm>
#include <assert.h>
#include <cstddef>
#include <cstdint>
#include <ext/alloc_traits.h>
#include <memory>
#include <utility>

#include "autil/CommonMacros.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "autil/mem_pool/Pool.h"
#include "autil/mem_pool/PoolBase.h"
#include "autil/mem_pool/PoolVector.h"
#include "navi/resource/GraphMemoryPoolR.h"
#include "sql/common/Log.h"
#include "sql/ops/agg/AggFunc.h"
#include "sql/ops/agg/AggFuncDesc.h"
#include "sql/ops/agg/AggFuncFactoryR.h"
#include "table/Column.h"
#include "table/ColumnData.h"
#include "table/ColumnSchema.h"
#include "table/Table.h"

using namespace std;
using namespace autil;
using namespace table;

namespace sql {

static constexpr size_t kCheckIntervalMask = (1 << 7) - 1;
#define UPDATE_AND_CHECK_AGG_POOL()                                                                \
    _aggPoolSize = _aggregatorPoolPtr->getAllocatedSize();                                         \
    if (unlikely(_aggPoolSize > _aggHints.memoryLimit)) {                                          \
        SQL_LOG(ERROR,                                                                             \
                "agg pool used too many bytes, limit=[%lu], actual=[%lu]",                         \
                _aggHints.memoryLimit,                                                             \
                _aggregatorPoolPtr->getAllocatedSize());                                           \
        return false;                                                                              \
    }

Aggregator::Aggregator(AggFuncMode mode,
                       navi::GraphMemoryPoolR *graphMemoryPoolR,
                       AggHints aggHints)
    : _graphMemoryPoolR(graphMemoryPoolR)
    , _aggHints(aggHints)
    , _aggregateTime(0)
    , _getTableTime(0)
    , _aggPoolSize(0)
    , _aggregateCnt(0)
    , _mode(mode)
    , _aggregatorPoolPtr(_graphMemoryPoolR->getPool())
    , _dataPoolPtr(_graphMemoryPoolR->getPool()) {}

Aggregator::~Aggregator() {
    assert(_accumulatorVec.size() == _aggFuncVec.size());
    for (size_t i = 0; i < _accumulatorVec.size(); ++i) {
        auto *func = _aggFuncVec[i];
        func->destroyAccumulator(_accumulatorVec[i].data(), _accumulatorVec[i].size());
    }

    for (auto aggFunc : _aggFuncVec) {
        DELETE_AND_SET_NULL(aggFunc);
    }
    _aggFuncVec.clear();
    _aggFilterArgs.clear();

    _table.reset();
}

bool Aggregator::init(const AggFuncFactoryR *aggFuncFactoryR,
                      const vector<AggFuncDesc> &aggFuncDesc,
                      const vector<string> &groupKey,
                      const vector<string> &outputFields,
                      const TablePtr &table) {
    SQL_LOG(DEBUG,
            "init aggregator with groupKey [%s] outputField [%s] aggFuncDesc [%s]",
            StringUtil::toString(groupKey).c_str(),
            StringUtil::toString(outputFields).c_str(),
            FastToJsonString(aggFuncDesc, true).c_str());
    _table.reset(new Table(_graphMemoryPoolR->getPool()));
    _accumulatorVec.reserve(aggFuncDesc.size() + groupKey.size());
    for (const auto &iter : aggFuncDesc) {
        if (!createAggFunc(
                aggFuncFactoryR, table, iter.funcName, iter.inputs, iter.outputs, iter.filterArg)) {
            SQL_LOG(ERROR, "create agg function [%s] failed", iter.funcName.c_str());
            return false;
        }
    }
    for (const string &key : groupKey) {
        if (find(outputFields.begin(), outputFields.end(), key) != outputFields.end()) {
            if (!createAggFunc(aggFuncFactoryR, table, "IDENTITY", {key}, {key})) {
                SQL_LOG(ERROR, "create identity function [%s] failed", key.c_str());
                return false;
            }
        } else if (_mode == AggFuncMode::AGG_FUNC_MODE_LOCAL) {
            SQL_LOG(ERROR,
                    "group key column not found in outputFields for local aggregator, "
                    "key [%s] outputFields [%s]",
                    key.c_str(),
                    StringUtil::toString(outputFields).c_str());
            return false;
        }
    }
    for (auto func : _aggFuncVec) {
        if (!func->initOutput(_table)) {
            SQL_LOG(ERROR, "create agg output [%s] failed", func->getName().c_str());
            return false;
        }
    }
    return true;
}

bool Aggregator::createAggFunc(const AggFuncFactoryR *aggFuncFactoryR,
                               const TablePtr &table,
                               const std::string &funcName,
                               const vector<std::string> &inputs,
                               const vector<std::string> &outputs,
                               const int32_t filterArg) {
    vector<ValueType> types;
    for (const string &param : inputs) {
        auto column = table->getColumn(param);
        if (column == nullptr) {
            SQL_LOG(ERROR, "column [%s] does not exist", param.c_str());
            return false;
        }
        auto schema = column->getColumnSchema();
        if (schema == nullptr) {
            SQL_LOG(ERROR, "column schema [%s] does not exist", param.c_str());
            return false;
        }
        types.push_back(schema->getType());
    }
    auto func = aggFuncFactoryR->createAggFunction(funcName, types, inputs, outputs, _mode);
    if (func == nullptr) {
        SQL_LOG(ERROR, "create agg function [%s] failed", funcName.c_str());
        return false;
    }
    if (!func->init(_dataPoolPtr.get(), _aggregatorPoolPtr.get(), _aggHints.funcHint)) {
        SQL_LOG(ERROR,
                "agg function [%s] init failed, hint [%s]",
                funcName.c_str(),
                _aggHints.funcHint.c_str());
        return false;
    }
    _aggFuncVec.emplace_back(func);
    _accumulatorVec.emplace_back(_aggregatorPoolPtr.get());
    if (filterArg >= 0 && _mode != AggFuncMode::AGG_FUNC_MODE_GLOBAL) {
        _aggFilterArgs.emplace_back(filterArg);
    } else {
        _aggFilterArgs.emplace_back(-1);
    }
    return true;
}

bool Aggregator::aggregate(const TablePtr &table, const vector<size_t> &groupKeys) {
    autil::ScopedTime2 aggregatorTimer;

    std::vector<table::ColumnData<bool> *> aggFilterColumn(_aggFilterArgs.size(), nullptr);
    for (size_t i = 0; i < _aggFuncVec.size(); ++i) {
        if (!_aggFuncVec[i]->initInput(table)) {
            SQL_LOG(ERROR, "init input [%s] failed", _aggFuncVec[i]->getName().c_str());
            return false;
        }
        if (unlikely(_aggFilterArgs[i] >= 0)) {
            auto column = table->getColumn(_aggFilterArgs[i])->getColumnData<bool>();
            if (column == nullptr) {
                SQL_LOG(ERROR, "column [%d] is not bool type", _aggFilterArgs[i]);
                return false;
            }
            aggFilterColumn[i] = column;
        }
    }
    size_t rowCount = table->getRowCount();
    for (size_t i = 0; i < rowCount; i++) {
        if (!doAggregate(table->getRow(i), groupKeys[i], aggFilterColumn)) {
            return false;
        }
    }
    UPDATE_AND_CHECK_AGG_POOL();

    _aggregateTime += aggregatorTimer.done_us();
    return true;
}

bool Aggregator::doAggregate(const Row &row,
                             size_t groupKey,
                             const std::vector<table::ColumnData<bool> *> &aggFilterColumn) {
    size_t accIdx;
    auto it = _accumulatorIdxMap.find(groupKey);
    if (it == _accumulatorIdxMap.end()) {
        accIdx = _accumulatorIdxMap.size();
        if (accIdx >= _aggHints.groupKeyLimit) {
            if (_aggHints.stopExceedLimit) {
                SQL_LOG(ERROR, "group key size large than limit[%lu]", _aggHints.groupKeyLimit);
                return false;
            } else {
                return true;
            }
        }
        for (size_t i = 0; i < _aggFuncVec.size(); i++) {
            // IMPORTANT: use independent pool for each thread
            auto acc = _aggFuncVec[i]->createAccumulator(_aggregatorPoolPtr.get());
            if (acc == nullptr) {
                SQL_LOG(ERROR, "create accumulator failed");
                return false;
            }
            assert(i < _accumulatorVec.size());
            assert(_accumulatorVec[i].size() == accIdx);
            _accumulatorVec[i].push_back(acc);
        }
        _accumulatorIdxMap.insert({groupKey, accIdx});
    } else {
        accIdx = it->second;
    }
    for (size_t i = 0; i < _aggFuncVec.size(); i++) {
        if (aggFilterColumn[i] != nullptr && aggFilterColumn[i]->get(row) == false) {
            continue;
        }
        assert(i < _accumulatorVec.size());
        if (!_aggFuncVec[i]->aggregate(row, _accumulatorVec[i][accIdx])) {
            return false;
        }
    }
    if ((++_aggregateCnt & kCheckIntervalMask) == 0) {
        UPDATE_AND_CHECK_AGG_POOL();
    }
    return true;
}

TablePtr Aggregator::getTable() {
    if (_table == nullptr) {
        return _table;
    }

    autil::ScopedTime2 getTableTimer;
    _table->batchAllocateRow(_accumulatorIdxMap.size());
    size_t rowIndex = 0;
    for (auto &pair : _accumulatorIdxMap) {
        Row row = _table->getRow(rowIndex++);
        size_t accIdx = pair.second;
        for (size_t i = 0; i < _aggFuncVec.size(); i++) {
            auto *acc = _accumulatorVec[i][accIdx];
            if (!_aggFuncVec[i]->setResult(acc, row)) {
                SQL_LOG(ERROR, "set agg result [%s] failed", _aggFuncVec[i]->getName().c_str());
                return nullptr;
            }
        }
    }

    _getTableTime += getTableTimer.done_us();
    _aggPoolSize = _aggregatorPoolPtr->getAllocatedSize();
    return _table;
}

void Aggregator::getStatistics(uint64_t &aggregateTime,
                               uint64_t &getTableTime,
                               uint64_t &aggPoolSize) const {
    aggregateTime = _aggregateTime;
    getTableTime = _getTableTime;
    aggPoolSize = _aggPoolSize;
}

} // namespace sql
