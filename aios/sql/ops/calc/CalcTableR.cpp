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
#include "sql/ops/calc/CalcTableR.h"

#include <cstddef>
#include <engine/NaviConfigContext.h>
#include <memory>
#include <stdint.h>
#include <utility>

#include "autil/CommonMacros.h"
#include "autil/StringUtil.h"
#include "kmonitor/client/MetricsReporter.h"
#include "matchdoc/ValueType.h"
#include "matchdoc/VectorDocStorage.h"
#include "navi/builder/ResourceDefBuilder.h"
#include "navi/common.h"
#include "navi/proto/KernelDef.pb.h"
#include "navi/resource/GraphMemoryPoolR.h"
#include "sql/common/Log.h"
#include "sql/common/TracerAdapter.h"
#include "sql/ops/calc/CalcConditionVisitor.h"
#include "sql/ops/condition/AliasConditionVisitor.h"
#include "sql/ops/condition/ConditionParser.h"
#include "sql/ops/condition/ExprUtil.h"
#include "sql/ops/util/KernelUtil.h"
#include "suez/turing/expression/cava/common/CavaPluginManager.h"
#include "suez/turing/expression/cava/common/SuezCavaAllocator.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/framework/AttributeExpression.h"
#include "suez/turing/expression/framework/ConstAttributeExpression.h"
#include "suez/turing/expression/framework/MatchDocsExpressionCreator.h"
#include "suez/turing/expression/framework/OptimizeReCalcExpression.h"
#include "suez/turing/expression/function/FunctionInterfaceCreator.h"
#include "suez/turing/expression/provider/FunctionProvider.h"
#include "suez/turing/expression/syntax/AtomicSyntaxExpr.h"
#include "suez/turing/expression/syntax/SyntaxExpr.h"
#include "suez/turing/expression/syntax/SyntaxParser.h"
#include "table/Column.h"
#include "table/ColumnSchema.h"
#include "table/Row.h"
#include "table/Table.h"
#include "table/TableUtil.h"
#include "table/ValueTypeSwitch.h"

namespace navi {
class ResourceInitContext;
} // namespace navi

namespace table {
class BaseColumnData;
template <typename T>
class ColumnData;
} // namespace table

namespace autil {
namespace mem_pool {
class Pool;
} // namespace mem_pool
} // namespace autil

using namespace std;
using namespace matchdoc;
using namespace table;
using namespace navi;
using namespace suez::turing;

namespace sql {

const string CalcTableR::DEFAULT_NULL_NUMBER_VALUE = "0";

static constexpr size_t TRACE_ROW_COUNT = 10;

CalcTableR::CalcTableR()
    : _filterFlag(true)
    , _needDestructJson(false)
    , _reuseTable(false) {}

CalcTableR::~CalcTableR() {
    if (_needDestructJson) {
        DELETE_AND_SET_NULL(_simpleDoc);
        DELETE_AND_SET_NULL(_allocator);
    }
    DELETE_AND_SET_NULL(_aliasConditionVisitor);
}

void CalcTableR::def(navi::ResourceDefBuilder &builder) const {
    builder.name(RESOURCE_ID, navi::RS_KERNEL);
}

bool CalcTableR::config(navi::ResourceConfigContext &ctx) {
    NAVI_JSONIZE(ctx, navi::NAVI_BUILDIN_ATTR_KERNEL, _opName);
    return true;
}

navi::ErrorCode CalcTableR::init(navi::ResourceInitContext &ctx) {
    string opName = _opName;
    KernelUtil::stripKernelNamePrefix(opName);
    string pathName = "sql.user.ops." + std::move(opName);
    _opMetricReporter = _queryMetricReporterR->getReporter()->getSubReporter(pathName, {}).get();
    _initPool = _graphMemoryPoolR->getPool();
    if (!ExprUtil::parseOutputExprs(
            _initPool.get(), _calcInitParamR->outputExprsJson, _exprsMap, _exprsAliasMap)) {
        SQL_LOG(WARN,
                "parse output exprs json failed,  json str [%s].",
                _calcInitParamR->outputExprsJson.c_str());
        return navi::EC_ABORT;
    }
    SQL_LOG(TRACE3, "expr alias map[%s]", autil::StringUtil::toString(_exprsAliasMap).c_str());
    ConditionParser parser(_initPool.get());
    if (!parser.parseCondition(_calcInitParamR->conditionJson, _condition)) {
        SQL_LOG(ERROR, "parse condition [%s] failed", _calcInitParamR->conditionJson.c_str());
        return navi::EC_ABORT;
    }
    if (_condition) {
        _simpleDoc = _condition->getJsonDoc();
        _aliasConditionVisitor = new AliasConditionVisitor();
        _condition->accept(_aliasConditionVisitor);
    } else {
        _allocator = new autil::AutilPoolAllocator(_initPool.get());
        _simpleDoc = new autil::SimpleDocument(_allocator);
        _needDestructJson = true;
    }
    SQL_LOG(
        DEBUG,
        "outputFields[%s] outputFieldsType[%s] conditionJson[%s] outputExprsJson[%s] tracer[%p]",
        autil::StringUtil::toString(_calcInitParamR->outputFields).c_str(),
        autil::StringUtil::toString(_calcInitParamR->outputFieldsType).c_str(),
        _calcInitParamR->conditionJson.c_str(),
        _calcInitParamR->outputExprsJson.c_str(),
        _traceAdapterR->getTracer().get());
    return navi::EC_NONE;
}

void CalcTableR::setMatchInfo(
    std::shared_ptr<suez::turing::MetaInfo> metaInfo,
    const IndexInfoHelper *indexInfoHelper,
    const std::shared_ptr<indexlib::index::InvertedIndexReader> &indexReader) {
    _metaInfo = std::move(metaInfo);
    _indexInfoHelper = indexInfoHelper;
    _indexReader = indexReader;
}

void CalcTableR::prepareWithMatchInfo(FunctionProvider &provider) {
    if (_metaInfo) {
        provider.initMatchInfoReader(_metaInfo);
        provider.setIndexInfoHelper(_indexInfoHelper);
        provider.setIndexReaderPtr(_indexReader);
    }
}

bool CalcTableR::calcTable(table::TablePtr &table) {
    bool doFilter = _condition && _filterFlag && _aliasConditionVisitor;
    if (doFilter) {
        SQL_LOG(TRACE3,
                "before filter: %s",
                table::TableUtil::toString(table, TRACE_ROW_COUNT).c_str());
        if (!filterTable(table)) {
            SQL_LOG(ERROR, "filter table failed.");
            return false;
        }
        SQL_LOG(
            TRACE3, "after filter: %s", table::TableUtil::toString(table, TRACE_ROW_COUNT).c_str());
    }
    if (_calcInitParamR->outputFields.empty() || !needCopyTable(table)) {
        return true;
    }
    SQL_LOG(
        TRACE3, "before project: %s", table::TableUtil::toString(table, TRACE_ROW_COUNT).c_str());
    if (!projectTable(table)) {
        SQL_LOG(ERROR, "project table failed.");
        return false;
    }
    SQL_LOG(
        TRACE3, "after project: %s", table::TableUtil::toString(table, TRACE_ROW_COUNT).c_str());
    return true;
}

bool CalcTableR::needCalc(const table::TablePtr &table) {
    return _condition != nullptr || needCopyTable(table);
}

bool CalcTableR::filterTable(const table::TablePtr &table) {
    return filterTable(table, 0, table->getRowCount(), false);
}

bool CalcTableR::filterTable(const table::TablePtr &table,
                             size_t startIdx,
                             size_t endIdx,
                             bool lazyDelete) {
    if (_condition == nullptr || !_filterFlag) {
        return true;
    }
    AliasConditionVisitor aliasVisitor;
    _condition->accept(&aliasVisitor);
    const auto &aliasMap = aliasVisitor.getAliasMap();
    auto allocator = table::Table::toMatchDocs(*table);
    addAliasMap(allocator.get(), aliasMap);

    // ATTENTION:
    // filter should not create extra multi value for input table, so temp pool is enough
    auto tempPool = _graphMemoryPoolR->getPool();
    FunctionProvider functionProvider(allocator.get(),
                                      tempPool.get(),
                                      _suezCavaAllocatorR->getAllocator().get(),
                                      _traceAdapterR->getTracer().get(),
                                      nullptr,
                                      nullptr,
                                      _opMetricReporter);
    prepareWithMatchInfo(functionProvider);
    MatchDocsExpressionCreator exprCreator(tempPool.get(),
                                           allocator.get(),
                                           _functionInterfaceCreatorR->getCreator().get(),
                                           _cavaPluginManagerR->getManager().get(),
                                           &functionProvider,
                                           _suezCavaAllocatorR->getAllocator().get());
    return doFilterTable(table, startIdx, endIdx, lazyDelete, &exprCreator);
}

bool CalcTableR::doFilterTable(const table::TablePtr &table,
                               size_t startIdx,
                               size_t endIdx,
                               bool lazyDelete,
                               MatchDocsExpressionCreator *exprCreator) {
    CalcConditionVisitor visitor(exprCreator);
    _condition->accept(&visitor);
    if (visitor.isError()) {
        SQL_LOG(WARN, "visit calc condition failed, error info [%s]", visitor.errorInfo().c_str());
        return false;
    }
    AttributeExpression *attriExpr = visitor.stealAttributeExpression();
    if (attriExpr == nullptr) {
        SQL_LOG(WARN, "create attribute expr failed, expr string");
        return false;
    }
    AttributeExpressionTyped<bool> *boolExpr
        = dynamic_cast<AttributeExpressionTyped<bool> *>(attriExpr);
    if (!boolExpr) {
        auto constExpr = dynamic_cast<ConstAttributeExpression<int64_t> *>(attriExpr);
        if (constExpr) {
            auto val = constExpr->getValue();
            SQL_LOG(INFO, "expr value is [%ld]", val);
            if (val <= 0) {
                for (size_t i = startIdx; i < endIdx; i++) {
                    table->markDeleteRow(i);
                }
                if (!lazyDelete) {
                    table->deleteRows();
                }
            }
            return true;
        }
        SQL_LOG(WARN, "[%s] not bool expr", attriExpr->getOriginalString().c_str());
        return false;
    }
    for (size_t i = startIdx; i < endIdx; i++) {
        auto row = table->getRow(i);
        if (!boolExpr->evaluateAndReturn(row)) {
            table->markDeleteRow(i);
        }
    }
    if (!lazyDelete) {
        table->deleteRows();
    }
    return true;
}

void CalcTableR::addAliasMap(matchdoc::MatchDocAllocator *allocator,
                             const matchdoc::MatchDocAllocator::ReferenceAliasMap &aliasMap) {
    auto originalMap = allocator->getReferenceAliasMap();
    if (!originalMap) {
        originalMap.reset(new matchdoc::MatchDocAllocator::ReferenceAliasMap(aliasMap));
        allocator->setReferenceAliasMap(originalMap);
    } else {
        for (auto alias : aliasMap) {
            originalMap->insert(make_pair(alias.first, alias.second));
        }
    }
}

bool CalcTableR::needCopyTable(const table::TablePtr &table) {
    if (!_exprsMap.empty()) {
        return true;
    }
    if (_calcInitParamR->outputFields.size() != table->getColumnCount()) {
        return true;
    }
    for (size_t i = 0; i < _calcInitParamR->outputFields.size(); ++i) {
        if (_calcInitParamR->outputFields[i] != table->getColumn(i)->getName()) {
            return true;
        }
    }
    return false;
}

bool CalcTableR::doProjectReuseTable(table::TablePtr &table,
                                     suez::turing::MatchDocsExpressionCreator *exprCreator) {
    vector<ExprColumnType> exprVec;
    vector<ColumnDataTuple> columnDataTupleVec;
    if (!declareTable(table, table, exprCreator, exprVec, columnDataTupleVec)) {
        SQL_LOG(WARN, "declare output table field failed.");
        return false;
    }
    auto optimizeReCalcExpression = exprCreator->createOptimizeReCalcExpression();
    if (!calcTableExpr(exprCreator->getAllocator(),
                       table->getRows(),
                       table,
                       exprVec,
                       optimizeReCalcExpression)) {
        SQL_LOG(WARN, "fill output table expression failed.");
        return false;
    }

    return true;
}

bool CalcTableR::doProjectTable(table::TablePtr &table,
                                suez::turing::MatchDocsExpressionCreator *exprCreator) {
    if (_reuseTable) {
        return doProjectReuseTable(table, exprCreator);
    }
    table::TablePtr output(new table::Table(_graphMemoryPoolR->getPool()));
    vector<ExprColumnType> exprVec;
    vector<ColumnDataTuple> columnDataTupleVec;
    if (!declareTable(table, output, exprCreator, exprVec, columnDataTupleVec)) {
        SQL_LOG(WARN, "declare output table field failed.");
        return false;
    }

    auto optimizeReCalcExpression = exprCreator->createOptimizeReCalcExpression();
    size_t rowCount = table->getRowCount();
    output->batchAllocateRow(rowCount);
    if (!calcTableExpr(exprCreator->getAllocator(),
                       table->getRows(),
                       output,
                       exprVec,
                       optimizeReCalcExpression)) {
        SQL_LOG(WARN, "fill output table expression failed.");
        return false;
    }

    if (!calcTableColumn(output, columnDataTupleVec)) {
        SQL_LOG(WARN, "project output table failed.");
        return false;
    }
    table = output;
    return true;
}

bool CalcTableR::projectTable(table::TablePtr &table) {
    if (!needCopyTable(table)) {
        return true;
    }
    auto allocator = table::Table::toMatchDocs(*table);
    addAliasMap(allocator.get(), _exprsAliasMap);
    auto functionInterfaceCreator = _functionInterfaceCreatorR->getCreator().get();
    auto cavaPluginManager = _cavaPluginManagerR->getManager().get();
    auto cavaAllocator = _suezCavaAllocatorR->getAllocator().get();
    auto tracer = _traceAdapterR->getTracer().get();
    FunctionProvider functionProvider(allocator.get(),
                                      table->getPool(),
                                      cavaAllocator,
                                      tracer,
                                      nullptr,
                                      nullptr,
                                      _opMetricReporter);
    prepareWithMatchInfo(functionProvider);

    auto tempPool = _graphMemoryPoolR->getPool();
    MatchDocsExpressionCreator exprCreator(tempPool.get(),
                                           allocator.get(),
                                           functionInterfaceCreator,
                                           cavaPluginManager,
                                           &functionProvider,
                                           cavaAllocator);
    return doProjectTable(table, &exprCreator);
}

bool CalcTableR::declareTable(const table::TablePtr &inputTable,
                              table::TablePtr &outputTable,
                              MatchDocsExpressionCreator *exprCreator,
                              vector<ExprColumnType> &exprVec,
                              vector<ColumnDataTuple> &columnDataTupleVec) {
    SQL_LOG(TRACE3, "step1: declare table");
    for (size_t i = 0; i < _calcInitParamR->outputFields.size(); i++) {
        const string &newName = _calcInitParamR->outputFields[i];
        auto iter = _exprsMap.find(newName);
        if (iter != _exprsMap.end()) {
            string outputType;
            if (_calcInitParamR->outputFields.size() == _calcInitParamR->outputFieldsType.size()) {
                outputType = _calcInitParamR->outputFieldsType[i];
            }
            if (!declareExprColumn(
                    newName, outputType, iter->second, outputTable, exprCreator, exprVec)) {
                SQL_LOG(WARN,
                        "output expr column[%s] exprStr[%s] exprJson[%s] failed",
                        newName.c_str(),
                        iter->second.exprStr.c_str(),
                        iter->second.exprJson.c_str());
                return false;
            }
            continue;
        }
        if (_reuseTable) {
            continue;
        }
        table::Column *originColumn = inputTable->getColumn(newName);
        if (originColumn == nullptr) {
            SQL_LOG(WARN, "column [%s] not found, may not exist in attribute", newName.c_str());
            continue;
        }
        if (!cloneColumnData(originColumn, outputTable, newName, columnDataTupleVec)) {
            SQL_LOG(ERROR, "clone column[%s] failed", newName.c_str());
            return false;
        }
    }
    return true;
}

AttributeExpression *
CalcTableR::createAttributeExpression(const std::string &outputType,
                                      const std::string &exprStr,
                                      suez::turing::MatchDocsExpressionCreator *exprCreator,
                                      autil::mem_pool::Pool *pool) {
    AttributeExpression *attriExpr = nullptr;
    if (exprStr.empty()) {
        if (outputType.empty()) {
            SQL_LOG(WARN, "expr string and output type both empty");
            return nullptr;
        }
        ExprResultType resultType = ExprUtil::transSqlTypeToVariableType(outputType).first;
        if (resultType == vt_unknown) {
            SQL_LOG(WARN,
                    "output type [%s] can not be unknown when expr string is empty",
                    outputType.c_str());
            return nullptr;
        }
        string constValue;
        if (resultType != vt_string) {
            constValue = DEFAULT_NULL_NUMBER_VALUE;
        }
        attriExpr = ExprUtil::createConstExpression(pool, constValue, constValue, resultType);
        if (attriExpr) {
            SQL_LOG(TRACE3, "create const expression for null value, result type [%d]", resultType);
            exprCreator->addNeedDeleteExpr(attriExpr);
        }
        return attriExpr;
    }

    SyntaxExpr *syntaxExpr = SyntaxParser::parseSyntax(exprStr);
    if (!syntaxExpr) {
        SQL_LOG(WARN, "parse syntax [%s] failed", exprStr.c_str());
        return nullptr;
    }
    if (syntaxExpr->getSyntaxExprType() == suez::turing::SYNTAX_EXPR_TYPE_CONST_VALUE) {
        AtomicSyntaxExpr *atomicExpr = dynamic_cast<AtomicSyntaxExpr *>(syntaxExpr);
        if (atomicExpr) {
            ExprResultType resultType = vt_unknown;
            if (!outputType.empty()) {
                resultType = ExprUtil::transSqlTypeToVariableType(outputType).first;
            }
            if (resultType != vt_unknown) {
                attriExpr = ExprUtil::createConstExpression(pool, atomicExpr, resultType);
                if (attriExpr) {
                    SQL_LOG(TRACE3,
                            "create const expression [%s], result type [%d]",
                            exprStr.c_str(),
                            resultType);
                    exprCreator->addNeedDeleteExpr(attriExpr);
                }
            }
        }
    }
    if (attriExpr) {
        DELETE_AND_SET_NULL(syntaxExpr);
        return attriExpr;
    } else {
        attriExpr = exprCreator->createAttributeExpression(syntaxExpr, true);
        if (!attriExpr) {
            SQL_LOG(WARN, "create attribute expression [%s] failed", exprStr.c_str());
        }
        exprCreator->addNeedOptimizeReCalcSyntaxExpr(syntaxExpr);
        return attriExpr;
    }
}

bool CalcTableR::declareExprColumn(const std::string &name,
                                   const std::string &outputType,
                                   const ExprEntity &exprEntity,
                                   table::TablePtr &outputTable,
                                   MatchDocsExpressionCreator *exprCreator,
                                   vector<ExprColumnType> &exprVec) {
    SQL_LOG(TRACE3, "declare expr column [%s]", name.c_str());
    AttributeExpression *attriExpr = nullptr;
    const std::string &exprJson = exprEntity.exprJson;
    if (!exprJson.empty() && ExprUtil::parseExprsJson(exprJson, *_simpleDoc)
        && ExprUtil::isCaseOp(*_simpleDoc)) {
        bool hasError = false;
        std::string errorInfo;
        attriExpr = ExprUtil::CreateCaseExpression(
            exprCreator, outputType, *_simpleDoc, hasError, errorInfo, outputTable->getPool());
        if (hasError || attriExpr == nullptr) {
            SQL_LOG(WARN, "create case expression failed [%s]", errorInfo.c_str());
            return false;
        }
    } else {
        const std::string &exprStr = exprEntity.exprStr;
        attriExpr
            = createAttributeExpression(outputType, exprStr, exprCreator, outputTable->getPool());
        if (attriExpr == nullptr) {
            SQL_LOG(WARN, "create attribute expr failed, expr string [%s]", exprStr.c_str());
            return false;
        }
    }
    auto bt = attriExpr->getType();
    bool isMulti = attriExpr->isMultiValue();
    auto func = [&](auto a) {
        typedef typename decltype(a)::value_type T;
        BaseColumnData *newColumnData
            = table::TableUtil::declareAndGetColumnData<T>(outputTable, name, true);
        if (newColumnData == nullptr) {
            SQL_LOG(ERROR, "can not declare column [%s]", name.c_str());
            return false;
        }
        exprVec.emplace_back(std::make_pair(attriExpr, newColumnData));
        return true;
    };
    if (!table::ValueTypeSwitch::switchType(bt, isMulti, func, func)) {
        SQL_LOG(ERROR, "output expr column [%s] failed", name.c_str());
        return false;
    }
    return true;
}

bool CalcTableR::calcTableExpr(matchdoc::MatchDocAllocator *allocator,
                               const std::vector<table::Row> &rows,
                               table::TablePtr &outputTable,
                               std::vector<ExprColumnType> &exprVec,
                               AttributeExpression *optimizeReCalcExpression) {
    SQL_LOG(TRACE3, "step2.1: project table fill optimize recalc expression value");
    if (optimizeReCalcExpression) {
        SQL_LOG(TRACE3, "step2.1: start fill optimize recalc expression value");
        auto expr = dynamic_cast<OptimizeReCalcExpression *>(optimizeReCalcExpression);
        if (expr) {
            SQL_LOG(
                TRACE3, "optimize recalc expression is [%s]", expr->getOriginalString().c_str());
            if (!expr->allocate(allocator)) {
                SQL_LOG(ERROR, "optimize recalc expression allocate failed");
                return false;
            }
            allocator->extend();
            expr->batchAndSetEvaluate((table::Row *)rows.data(), rows.size());
        }
        SQL_LOG(TRACE3, "step2.1: end fill optimize recalc expression value");
    } else {
        SQL_LOG(TRACE3, "step2.1: no optimize recalc expression value");
    }
    SQL_LOG(TRACE3, "step2.2: project table fill expression value");

    AttributeExpression *attriExpr = nullptr;
    table::BaseColumnData *originColumnData = nullptr;

    auto func = [&](auto a) {
        typedef typename decltype(a)::value_type T;
        auto attriExprTyped = static_cast<AttributeExpressionTyped<T> *>(attriExpr);
        auto newColumnData = static_cast<ColumnData<T> *>(originColumnData);
        if (attriExprTyped == nullptr || newColumnData == nullptr) {
            return false;
        }
        for (size_t i = 0; i < outputTable->getRowCount(); i++) {
            newColumnData->set(i, attriExprTyped->evaluateAndReturn(rows[i]));
        }
        return true;
    };

    for (size_t i = 0; i < exprVec.size(); ++i) {
        attriExpr = exprVec[i].first;
        originColumnData = exprVec[i].second;
        auto bt = attriExpr->getType();
        bool isMulti = attriExpr->isMultiValue();
        if (!table::ValueTypeSwitch::switchType(bt, isMulti, func, func)) {
            SQL_LOG(ERROR, "evaluate expr column failed");
            return false;
        }
    }
    return true;
}

bool CalcTableR::cloneColumnData(table::Column *originColumn,
                                 table::TablePtr &table,
                                 const string &name,
                                 vector<ColumnDataTuple> &columnDataTupleVec) {
    auto schema = originColumn->getColumnSchema();
    if (schema == nullptr) {
        SQL_LOG(ERROR, "get old column schema failed, new column name [%s]", name.c_str());
        return false;
    }
    auto vt = schema->getType();
    auto func = [&](auto a) {
        typedef typename decltype(a)::value_type T;
        BaseColumnData *originColumnData = originColumn->getColumnData<T>();
        BaseColumnData *newColumnData
            = table::TableUtil::declareAndGetColumnData<T>(table, name, true);
        if (newColumnData == nullptr) {
            SQL_LOG(ERROR, "can not declare column [%s]", name.c_str());
            return false;
        }
        columnDataTupleVec.emplace_back(std::make_tuple(originColumnData, newColumnData, vt));
        return true;
    };
    if (!ValueTypeSwitch::switchType(vt, func, func)) {
        SQL_LOG(ERROR, "clone column [%s] failed", name.c_str());
        return false;
    }
    return true;
}

bool CalcTableR::calcTableColumn(table::TablePtr &outputTable,
                                 std::vector<ColumnDataTuple> &columnDataTupleVec) {
    SQL_LOG(TRACE3, "step3: project table  fill colunm value");
    table::BaseColumnData *newColumnData = nullptr;
    table::BaseColumnData *originColumnData = nullptr;
    matchdoc::ValueType vt;
    auto calcFunc = [&](auto a) {
        typedef typename decltype(a)::value_type T;
        auto originColumnDataTyped = static_cast<ColumnData<T> *>(originColumnData);
        auto newColumnDataTyped = static_cast<ColumnData<T> *>(newColumnData);
        for (size_t i = 0; i < outputTable->getRowCount(); i++) {
            newColumnDataTyped->set(i, originColumnDataTyped->get(i));
        }
        return true;
    };
    for (size_t i = 0; i < columnDataTupleVec.size(); ++i) {
        std::tie(originColumnData, newColumnData, vt) = columnDataTupleVec[i];
        if (!ValueTypeSwitch::switchType(vt, calcFunc, calcFunc)) {
            SQL_LOG(ERROR, "evaluate column value failed");
            return false;
        }
    }
    return true;
}

REGISTER_RESOURCE(CalcTableR);

} // namespace sql
