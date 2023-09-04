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
#include <stdint.h>
#include <string>
#include <unordered_map>
#include <vector>

#include "autil/mem_pool/PoolVector.h"
#include "sql/common/common.h"
#include "sql/ops/agg/AggFuncMode.h"
#include "table/Row.h"
#include "table/Table.h"

namespace table {
template <typename T>
class ColumnData;
} // namespace table

namespace autil {
namespace mem_pool {
class Pool;
} // namespace mem_pool
} // namespace autil
namespace sql {
class Accumulator;
class AggFunc;
class AggFuncDesc;
class AggFuncFactoryR;
} // namespace sql

namespace navi {
class GraphMemoryPoolR;
} // namespace navi

namespace sql {
struct AggHints {
    AggHints()
        : memoryLimit(DEFAULT_AGG_MEMORY_LIMIT)
        , groupKeyLimit(DEFAULT_GROUP_KEY_COUNT)
        , stopExceedLimit(true) {}
    size_t memoryLimit;
    size_t groupKeyLimit;
    std::string funcHint;
    bool stopExceedLimit;
};

class Aggregator {
public:
    Aggregator(AggFuncMode mode,
               navi::GraphMemoryPoolR *graphMemoryPoolR,
               AggHints aggHints = AggHints());
    ~Aggregator();

private:
    Aggregator(const Aggregator &);
    Aggregator &operator=(const Aggregator &);

public:
    bool init(const AggFuncFactoryR *aggFuncFactoryR,
              const std::vector<AggFuncDesc> &aggFuncDesc,
              const std::vector<std::string> &groupKey,
              const std::vector<std::string> &outputFields,
              const table::TablePtr &table);
    bool aggregate(const table::TablePtr &table, const std::vector<size_t> &groupKeys);
    table::TablePtr getTable();
    void
    getStatistics(uint64_t &aggregateTime, uint64_t &getTableTime, uint64_t &aggPoolSize) const;

private:
    bool createAggFunc(const AggFuncFactoryR *aggFuncFactoryR,
                       const table::TablePtr &table,
                       const std::string &funcName,
                       const std::vector<std::string> &inputs,
                       const std::vector<std::string> &outputs,
                       const int32_t filterArg = -1);
    bool doAggregate(const table::Row &row,
                     size_t groupKey,
                     const std::vector<table::ColumnData<bool> *> &aggFilterColumn);
    bool needDependInputTablePools() const;

private:
    table::TablePtr _table;
    navi::GraphMemoryPoolR *_graphMemoryPoolR;
    AggHints _aggHints;
    uint64_t _aggregateTime;
    uint64_t _getTableTime;
    uint64_t _aggPoolSize;
    size_t _aggregateCnt;
    AggFuncMode _mode;

    std::vector<AggFunc *> _aggFuncVec;
    std::vector<int32_t> _aggFilterArgs;
    std::shared_ptr<autil::mem_pool::Pool> _aggregatorPoolPtr;
    std::vector<autil::mem_pool::PoolVector<Accumulator *>> _accumulatorVec;
    std::unordered_map<size_t, size_t> _accumulatorIdxMap;
};

typedef std::shared_ptr<Aggregator> AggregatorPtr;
} // namespace sql
