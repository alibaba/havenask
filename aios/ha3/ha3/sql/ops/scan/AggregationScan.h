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

#include "autil/result/Result.h"
#include "ha3/sql/ops/condition/ExprUtil.h"
#include "ha3/sql/ops/scan/ScanBase.h"
#include "ha3/sql/ops/scan/Collector.h"
#include "ha3/sql/ops/scan/AggIndexKeyCollector.h"
#include "table/Table.h"

namespace navi {
class AsyncPipe;
typedef std::shared_ptr<AsyncPipe> AsyncPipePtr;
} // namespace navi

namespace indexlibv2 {
namespace index {
class AggregationReader;
}
}

namespace isearch {
namespace sql {
class CalcTable;
}
}

namespace isearch {
namespace sql {

class AggregationScan : public ScanBase {
public:
    AggregationScan(const ScanInitParam &param, navi::AsyncPipePtr pipe);
    virtual ~AggregationScan();

private:
    AggregationScan(const AggregationScan &);
    AggregationScan &operator=(const AggregationScan &);

private:
    bool doInit() override;
    bool createMatchDocAllocator(const std::shared_ptr<indexlibv2::config::ITabletSchema> &schema);
    bool doBatchScan(table::TablePtr &table, bool &eof) override;
    autil::Result<table::TablePtr> countAggFunc();
    autil::Result<table::TablePtr> sumAggFunc();
    autil::Result<table::TablePtr> lookupFunc();
    template <typename T>
    autil::Result<table::TablePtr> createTable(T result, const std::string &fieldName,
            const std::string &fieldType,
            const std::shared_ptr<autil::mem_pool::Pool> &pool);
    bool appendMatchdoc(const autil::StringView &view, std::vector<matchdoc::MatchDoc> &matchdocs);
    
private:
    navi::AsyncPipePtr _asyncPipe;
    uint64_t _hashKey;
    std::shared_ptr<indexlibv2::index::AggregationReader> _aggReader;
    std::shared_ptr<AggIndexKeyCollector> _keyCollector;
    ValueCollectorPtr _valueCollector;
    std::unique_ptr<CalcTable> _calcTable;

private:
    AUTIL_LOG_DECLARE();
};

template <typename T>
autil::Result<table::TablePtr> AggregationScan::createTable(T result,
        const std::string &fieldName,
        const std::string &fieldType,
        const std::shared_ptr<autil::mem_pool::Pool> &pool) {
    auto table = std::make_shared<table::Table>(pool);
    table->batchAllocateRow(1);

    auto type = ExprUtil::transSqlTypeToVariableType(fieldType);
    if (type.second) {
        return autil::result::RuntimeError::make("not support multi value now, fieldType: %s",
                fieldType.c_str());
    }
    auto column = table->declareColumn(fieldName, type.first, type.second);
    if (column == nullptr) {
        return autil::result::RuntimeError::make("declare column %s failed", fieldName.c_str());
    }
    switch (type.first) {
#define CASE_MACRO(ft)                                                  \
        case ft: {                                                      \
            typedef matchdoc::MatchDocBuiltinType2CppType<ft, false>::CppType TT; \
            auto columnData = column->getColumnData<TT>();              \
            columnData->set(0, result);                                 \
            break;                                                      \
        }
        NUMBER_BUILTIN_TYPE_MACRO_HELPER(CASE_MACRO);
#undef CASE
    default:
        return autil::result::RuntimeError::make("not support type: %s, bt: %d", fieldType.c_str(),
                                                 type.first);
    }

    return {table};
}


} // namespace sql
} // namespace isearch
