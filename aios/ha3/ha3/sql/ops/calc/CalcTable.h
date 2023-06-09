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

#include <map>
#include <memory>
#include <stddef.h>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>
#include <set>

#include "autil/Log.h"
#include "autil/legacy/RapidJsonCommon.h"
#include "ha3/sql/common/Log.h" // IWYU pragma: keep
#include "ha3/sql/ops/condition/Condition.h"
#include "ha3/sql/ops/condition/ExprUtil.h"
#include "matchdoc/MatchDocAllocator.h"
#include "table/Table.h"
#include "navi/engine/KernelConfigContext.h"
#include "suez/turing/expression/provider/FunctionProvider.h"

namespace autil {
namespace mem_pool {
class Pool;
} // namespace mem_pool
} // namespace autil
namespace matchdoc {
struct ValueType;
} // namespace matchdoc
namespace navi {
class GraphMemoryPoolResource;
} // namespace navi
namespace suez {
namespace turing {
class AttributeExpression;
class CavaPluginManager;
class FunctionInterfaceCreator;
class MatchDocsExpressionCreator;
class SuezCavaAllocator;
class Tracer;
class IndexInfoHelper;
class MetaInfo;
} // namespace turing
} // namespace suez
namespace table {
class Column;
class ColumnDataBase;
} // namespace table
namespace kmonitor {
class MetricsReporter;
};

namespace indexlib {
namespace index {
class InvertedIndexReader;
} // namespace index
} // namespace indexlib

namespace isearch {
namespace sql {
class AliasConditionVisitor;

class CalcInitParam {
public:
    CalcInitParam();
    CalcInitParam(const std::vector<std::string> &outputFields,
                  const std::vector<std::string> &outputFieldsType,
                  const std::string &conditionJson = "",
                  const std::string &outputExprsJson = "");
    bool initFromJson(navi::KernelConfigContext &ctx);

public:
    int32_t opId;
    std::vector<std::string> outputFields;
    std::vector<std::string> outputFieldsType;
    std::string conditionJson;
    std::string outputExprsJson;
    std::vector<int32_t> reuseInputs;
    std::set<std::string> matchType;
    std::string opName;
    std::string nodeName;
};

struct CalcResource {
    navi::GraphMemoryPoolResource *memoryPoolResource = nullptr;
    suez::turing::Tracer *tracer = nullptr;
    suez::turing::SuezCavaAllocator *cavaAllocator = nullptr;
    const suez::turing::CavaPluginManager *cavaPluginManager = nullptr;
    suez::turing::FunctionInterfaceCreator *funcInterfaceCreator = nullptr;
    kmonitor::MetricsReporter *metricsReporter = nullptr;
    std::shared_ptr<suez::turing::MetaInfo> metaInfo;
    const suez::turing::IndexInfoHelper *indexInfoHelper = nullptr;
    std::shared_ptr<indexlib::index::InvertedIndexReader> indexReader;
};

class CalcTable {
public:
    CalcTable(const CalcInitParam &param, CalcResource resource);
    ~CalcTable();

public:
    bool init();
    void setMatchInfo(std::shared_ptr<suez::turing::MetaInfo> metaInfo,
                      const suez::turing::IndexInfoHelper *indexInfoHelper,
                      std::shared_ptr<indexlib::index::InvertedIndexReader> indexReader);
    bool calcTable(table::TablePtr &table);
    bool needCalc(const table::TablePtr &table);
    bool filterTable(const table::TablePtr &table, size_t startIdx, size_t endIdx, bool lazyDelete);
    bool projectTable(table::TablePtr &table);
    void setFilterFlag(bool filterFlag) {
        _filterFlag = filterFlag;
    }
    void setReuseTable(bool reuse) {
        _reuseTable = reuse;
    }
    bool isReuseTable() const {
        return _reuseTable;
    }

public:
    typedef std::pair<suez::turing::AttributeExpression *, table::ColumnDataBase *> ExprColumnType;
    typedef std::tuple<table::ColumnDataBase *, table::ColumnDataBase *, matchdoc::ValueType>
        ColumnDataTuple;
    static void addAliasMap(matchdoc::MatchDocAllocator *allocator,
                            const matchdoc::MatchDocAllocator::ReferenceAliasMap &aliasMap);

private:
    void prepareWithMatchInfo(suez::turing::FunctionProvider &provider);
    bool filterTable(const table::TablePtr &table);
    bool doFilterTable(const table::TablePtr &table,
                       size_t startIdx,
                       size_t endIdx,
                       bool lazyDelete,
                       suez::turing::MatchDocsExpressionCreator *exprCreator);
    bool doProjectTable(table::TablePtr &table,
                        suez::turing::MatchDocsExpressionCreator *exprCreator);
    bool doProjectReuseTable(table::TablePtr &table,
                             suez::turing::MatchDocsExpressionCreator *exprCreator);
    bool declareTable(const table::TablePtr &inputTable,
                      table::TablePtr &outputTable,
                      suez::turing::MatchDocsExpressionCreator *exprCreator,
                      std::vector<ExprColumnType> &exprVec,
                      std::vector<ColumnDataTuple> &columnDataTupleVec);
    bool calcTableExpr(const table::TablePtr &inputTable,
                       table::TablePtr &outputTable,
                       std::vector<ExprColumnType> &exprVec,
                       suez::turing::AttributeExpression *optimizeReCalcExpression);
    bool calcTableCloumn(table::TablePtr &outputTable,
                         std::vector<ColumnDataTuple> &columnDataTupleVec);
    bool cloneColumnData(table::Column *originColumn,
                         table::TablePtr &table,
                         const std::string &name,
                         std::vector<ColumnDataTuple> &columnDataTupleVec);
    suez::turing::AttributeExpression *
    createAttributeExpression(const std::string &outputType,
                              const std::string &exprStr,
                              suez::turing::MatchDocsExpressionCreator *exprCreator,
                              autil::mem_pool::Pool *pool);
    bool declareExprColumn(const std::string &name,
                           const std::string &outputType,
                           const ExprEntity &exprEntity,
                           table::TablePtr &outputTable,
                           suez::turing::MatchDocsExpressionCreator *exprCreator,
                           std::vector<ExprColumnType> &exprVec);
    bool needCopyTable(const table::TablePtr &table);

private:
    CalcInitParam _param;
    CalcResource _resource;
    std::shared_ptr<autil::mem_pool::Pool> _initPool; // only used for init
    autil::AutilPoolAllocator *_allocator = nullptr;
    autil::SimpleDocument *_simpleDoc = nullptr;
    AliasConditionVisitor *_aliasConditionVisitor = nullptr;
    std::map<std::string, ExprEntity> _exprsMap;
    std::unordered_map<std::string, std::string> _exprsAliasMap;
    ConditionPtr _condition;
    bool _filterFlag;
    bool _needDestructJson;
    bool _reuseTable;

private:
    static const std::string DEFAULT_NULL_NUMBER_VALUE;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<CalcTable> CalcTablePtr;
} // namespace sql
} // namespace isearch
