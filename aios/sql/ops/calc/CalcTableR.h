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

#include <algorithm>
#include <map>
#include <memory>
#include <stddef.h>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "autil/legacy/RapidJsonCommon.h"
#include "matchdoc/MatchDocAllocator.h"
#include "navi/common.h"
#include "navi/engine/Resource.h"
#include "navi/engine/ResourceConfigContext.h"
#include "navi/engine/ResourceMap.h"
#include "navi/resource/GraphMemoryPoolR.h"
#include "sql/common/Log.h" // IWYU pragma: keep
#include "sql/ops/calc/CalcInitParamR.h"
#include "sql/ops/condition/Condition.h"
#include "sql/ops/condition/ExprUtil.h"
#include "sql/resource/QueryMetricReporterR.h"
#include "sql/resource/TraceAdapterR.h"
#include "suez/turing/expression/cava/common/CavaPluginManagerR.h"
#include "suez/turing/expression/function/FunctionInterfaceCreatorR.h"
#include "suez/turing/navi/SuezCavaAllocatorR.h"
#include "table/Table.h"

namespace kmonitor {
class MetricsReporter;
} // namespace kmonitor
namespace navi {
class ResourceDefBuilder;
class ResourceInitContext;
} // namespace navi

namespace autil {
namespace mem_pool {
class Pool;
} // namespace mem_pool
} // namespace autil
namespace matchdoc {
struct ValueType;
} // namespace matchdoc
namespace suez {
namespace turing {
class AttributeExpression;
class FunctionProvider;
class IndexInfoHelper;
class MatchDocsExpressionCreator;
class MetaInfo;
} // namespace turing
} // namespace suez
namespace table {
class Column;
class ColumnDataBase;
} // namespace table

namespace indexlib {
namespace index {
class InvertedIndexReader;
} // namespace index
} // namespace indexlib

namespace sql {
class AliasConditionVisitor;

class CalcTableR : public navi::Resource {
public:
    CalcTableR();
    ~CalcTableR();

public:
    void def(navi::ResourceDefBuilder &builder) const override;
    bool config(navi::ResourceConfigContext &ctx) override;
    navi::ErrorCode init(navi::ResourceInitContext &ctx) override;

public:
    static constexpr char RESOURCE_ID[] = "calc_table_r";

public:
    template <typename Context>
    static std::shared_ptr<CalcTableR> buildOne(Context &ctx,
                                                const std::vector<std::string> &outputFields,
                                                const std::vector<std::string> &outputFieldsType,
                                                const std::string &conditionJson,
                                                const std::string &outputExprsJson);

public:
    void setMatchInfo(std::shared_ptr<suez::turing::MetaInfo> metaInfo,
                      const suez::turing::IndexInfoHelper *indexInfoHelper,
                      const std::shared_ptr<indexlib::index::InvertedIndexReader> &indexReader);
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
    kmonitor::MetricsReporter *getOpMetricReporter() const {
        return _opMetricReporter;
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
    RESOURCE_DEPEND_DECLARE();

private:
    RESOURCE_DEPEND_ON(CalcInitParamR, _calcInitParamR);
    RESOURCE_DEPEND_ON(navi::GraphMemoryPoolR, _graphMemoryPoolR);
    RESOURCE_DEPEND_ON(suez::turing::CavaPluginManagerR, _cavaPluginManagerR);
    RESOURCE_DEPEND_ON(suez::turing::FunctionInterfaceCreatorR, _functionInterfaceCreatorR);
    RESOURCE_DEPEND_ON(QueryMetricReporterR, _queryMetricReporterR);
    RESOURCE_DEPEND_ON(suez::turing::SuezCavaAllocatorR, _suezCavaAllocatorR);
    RESOURCE_DEPEND_ON(TraceAdapterR, _traceAdapterR);
    kmonitor::MetricsReporter *_opMetricReporter = nullptr;
    std::string _opName;
    std::shared_ptr<suez::turing::MetaInfo> _metaInfo;
    const suez::turing::IndexInfoHelper *_indexInfoHelper = nullptr;
    std::shared_ptr<indexlib::index::InvertedIndexReader> _indexReader;

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
};

template <typename Context>
std::shared_ptr<CalcTableR> CalcTableR::buildOne(Context &ctx,
                                                 const std::vector<std::string> &outputFields,
                                                 const std::vector<std::string> &outputFieldsType,
                                                 const std::string &conditionJson,
                                                 const std::string &outputExprsJson) {
    auto calcParamR = new CalcInitParamR();
    calcParamR->outputFields = outputFields;
    calcParamR->outputFieldsType = outputFieldsType;
    calcParamR->conditionJson = conditionJson;
    calcParamR->outputExprsJson = outputExprsJson;
    navi::ResourceMap inputResourceMap;
    inputResourceMap.addResource(navi::ResourcePtr(calcParamR));
    return std::dynamic_pointer_cast<CalcTableR>(
        ctx.buildR(CalcTableR::RESOURCE_ID, ctx.getConfigContext(), inputResourceMap));
}

NAVI_TYPEDEF_PTR(CalcTableR);

} // namespace sql
