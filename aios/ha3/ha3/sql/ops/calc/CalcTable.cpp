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
#include "ha3/sql/ops/calc/CalcTable.h"

#include <cstddef>
#include <memory>
#include <stdint.h>
#include <utility>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "autil/Scope.h"
#include "ha3/sql/common/Log.h"
#include "ha3/sql/ops/calc/CalcConditionVisitor.h"
#include "ha3/sql/ops/condition/AliasConditionVisitor.h"
#include "ha3/sql/ops/condition/ConditionParser.h"
#include "ha3/sql/ops/condition/ExprUtil.h"
#include "ha3/sql/ops/util/KernelUtil.h"
#include "matchdoc/ValueType.h"
#include "matchdoc/flatbuffer/MatchDoc_generated.h"
#include "navi/common.h"
#include "navi/resource/GraphMemoryPoolResource.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/framework/AttributeExpression.h"
#include "suez/turing/expression/framework/ConstAttributeExpression.h"
#include "suez/turing/expression/framework/MatchDocsExpressionCreator.h"
#include "suez/turing/expression/framework/OptimizeReCalcExpression.h"
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

namespace table {
class ColumnDataBase;
template <typename T>
class ColumnData;
} // namespace table

namespace autil {
namespace mem_pool {
class Pool;
} // namespace mem_pool
} // namespace autil
namespace suez {
namespace turing {
class CavaPluginManager;
class FunctionInterfaceCreator;
class SuezCavaAllocator;
class Tracer;
} // namespace turing
} // namespace suez

using namespace std;
using namespace matchdoc;
using namespace table;
using namespace navi;
using namespace suez::turing;

namespace isearch {
namespace sql {
AUTIL_LOG_SETUP(sql, CalcTable);

CalcInitParam::CalcInitParam()
    : opId(-1)
{}

CalcInitParam::CalcInitParam(const std::vector<std::string> &outputFields_,
                             const std::vector<std::string> &outputFieldsType_,
                             const std::string &conditionJson_,
                             const std::string &outputExprsJson_)
    : opId(-1)
    , outputFields(outputFields_)
    , outputFieldsType(outputFieldsType_)
    , conditionJson(conditionJson_)
    , outputExprsJson(outputExprsJson_)
{}

bool CalcInitParam::initFromJson(KernelConfigContext &ctx) {
    ctx.JsonizeAsString("condition", conditionJson, "");
    ctx.JsonizeAsString("output_field_exprs", outputExprsJson, "");
    ctx.Jsonize("output_fields", outputFields);
    ctx.Jsonize("output_fields_type", outputFieldsType, outputFieldsType);
    ctx.Jsonize("reuse_inputs", reuseInputs, reuseInputs);
    ctx.Jsonize("match_type", matchType, matchType);
    ctx.Jsonize(IQUAN_OP_ID, opId, opId);
    KernelUtil::stripName(outputFields);
    return true;
}

const string CalcTable::DEFAULT_NULL_NUMBER_VALUE = "0";

CalcTable::CalcTable(const CalcInitParam &param, CalcResource resource)
    : _param(param)
    , _resource(std::move(resource))
    , _filterFlag(true)
    , _needDestructJson(false)
    , _reuseTable(false) {
    SQL_LOG(
        TRACE3,
        "outputFields[%s] outputFieldsType[%s] conditionJson[%s] outputExprsJson[%s] tracer[%p]",
        autil::StringUtil::toString(_param.outputFields).c_str(),
        autil::StringUtil::toString(_param.outputFieldsType).c_str(),
        _param.conditionJson.c_str(),
        _param.outputExprsJson.c_str(),
        _resource.tracer);
}

CalcTable::~CalcTable() {
    if (_needDestructJson) {
        DELETE_AND_SET_NULL(_simpleDoc);
        DELETE_AND_SET_NULL(_allocator);
    }
    DELETE_AND_SET_NULL(_aliasConditionVisitor);
}

bool CalcTable::init() {
    _initPool = _resource.memoryPoolResource->getPool();
    if (!ExprUtil::parseOutputExprs(
            _initPool.get(), _param.outputExprsJson, _exprsMap, _exprsAliasMap)) {
        SQL_LOG(WARN, "parse output exprs json failed,  json str [%s].", _param.outputExprsJson.c_str());
        return false;
    }
    SQL_LOG(TRACE1, "expr alias map[%s]", autil::StringUtil::toString(_exprsAliasMap).c_str());
    ConditionParser parser(_initPool.get());
    if (!parser.parseCondition(_param.conditionJson, _condition)) {
        SQL_LOG(ERROR, "parse condition [%s] failed", _param.conditionJson.c_str());
        return false;
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
    return true;
}

void CalcTable::setMatchInfo(std::shared_ptr<suez::turing::MetaInfo> metaInfo,
                             const IndexInfoHelper *indexInfoHelper,
                             std::shared_ptr<indexlib::index::InvertedIndexReader> indexReader) {
    _resource.metaInfo = std::move(metaInfo);
    _resource.indexInfoHelper = indexInfoHelper;
    _resource.indexReader = indexReader;
}

void CalcTable::prepareWithMatchInfo(FunctionProvider &provider) {
    if (_resource.metaInfo) {
        provider.initMatchInfoReader(_resource.metaInfo);
        provider.setIndexInfoHelper(_resource.indexInfoHelper);
        provider.setIndexReaderPtr(_resource.indexReader);
    }
}

bool CalcTable::calcTable(table::TablePtr &table) {
    bool doFilter = _condition && _filterFlag && _aliasConditionVisitor;
    if (doFilter) {
        if (!filterTable(table)) {
            SQL_LOG(ERROR, "filter table failed.");
            return false;
        }
    }
    if (_param.outputFields.empty() || !needCopyTable(table)) {
        return true;
    }
    if (!projectTable(table)) {
        SQL_LOG(ERROR, "project table failed.");
        return false;
    }
    return true;
}

bool CalcTable::needCalc(const table::TablePtr &table) {
    return _condition != nullptr || needCopyTable(table);
}

bool CalcTable::filterTable(const table::TablePtr &table) {
    return filterTable(table, 0, table->getRowCount(), false);
}

bool CalcTable::filterTable(const table::TablePtr &table,
                            size_t startIdx,
                            size_t endIdx,
                            bool lazyDelete) {
    if (_condition == nullptr || !_filterFlag) {
        return true;
    }
    AliasConditionVisitor aliasVisitor;
    _condition->accept(&aliasVisitor);
    const auto &aliasMap = aliasVisitor.getAliasMap();
    addAliasMap(table->getMatchDocAllocator(), aliasMap);

    // ATTENTION:
    // filter should not create extra multi value for input table, so temp pool is enough
    auto tempPool = _resource.memoryPoolResource->getPool();
    FunctionProvider functionProvider(table->getMatchDocAllocator(),
                                      tempPool.get(),
                                      _resource.cavaAllocator,
                                      _resource.tracer,
                                      nullptr,
                                      nullptr,
                                      _resource.metricsReporter);
    prepareWithMatchInfo(functionProvider);
    MatchDocsExpressionCreator exprCreator(tempPool.get(),
                                           table->getMatchDocAllocator(),
                                           _resource.funcInterfaceCreator,
                                           _resource.cavaPluginManager,
                                           &functionProvider,
                                           _resource.cavaAllocator);
    return doFilterTable(table, startIdx, endIdx, lazyDelete, &exprCreator);
}

bool CalcTable::doFilterTable(const table::TablePtr &table,
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

void CalcTable::addAliasMap(matchdoc::MatchDocAllocator *allocator,
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

bool CalcTable::needCopyTable(const table::TablePtr &table) {
    if (table->getMatchDocAllocator()->hasSubDocAllocator()) {
        return true;
    }
    if (!_exprsMap.empty()) {
        return true;
    }
    if (_param.outputFields.size() != table->getColumnCount()) {
        return true;
    }
    for (size_t i = 0; i < _param.outputFields.size(); ++i) {
        if (_param.outputFields[i] != table->getColumnName(i)) {
            return true;
        }
    }
    return false;
}

bool CalcTable::doProjectReuseTable(table::TablePtr &table,
                                    suez::turing::MatchDocsExpressionCreator *exprCreator) {
    vector<ExprColumnType> exprVec;
    vector<ColumnDataTuple> columnDataTupleVec;
    if (!declareTable(table, table, exprCreator, exprVec, columnDataTupleVec)) {
        SQL_LOG(WARN, "declare output table field failed.");
        return false;
    }
    table->endGroup();
    auto optimizeReCalcExpression = exprCreator->createOptimizeReCalcExpression();
    if (!calcTableExpr(table, table, exprVec, optimizeReCalcExpression)) {
        SQL_LOG(WARN, "fill output table expression failed.");
        return false;
    }

    return true;
}

bool CalcTable::doProjectTable(table::TablePtr &table,
                               suez::turing::MatchDocsExpressionCreator *exprCreator) {
    if (_reuseTable) {
        return doProjectReuseTable(table, exprCreator);
    }
    table::TablePtr output(new table::Table(_resource.memoryPoolResource->getPool()));
    vector<ExprColumnType> exprVec;
    vector<ColumnDataTuple> columnDataTupleVec;
    if (!declareTable(table, output, exprCreator, exprVec, columnDataTupleVec)) {
        SQL_LOG(WARN, "declare output table field failed.");
        return false;
    }

    auto optimizeReCalcExpression = exprCreator->createOptimizeReCalcExpression();
    size_t rowCount = table->getRowCount();
    output->batchAllocateRow(rowCount);
    if (!calcTableExpr(table, output, exprVec, optimizeReCalcExpression)) {
        SQL_LOG(WARN, "fill output table expression failed.");
        return false;
    }

    if (!calcTableCloumn(output, columnDataTupleVec)) {
        SQL_LOG(WARN, "project output table failed.");
        return false;
    }
    output->mergeDependentPools(table);
    table = output;
    return true;
}

bool CalcTable::projectTable(table::TablePtr &table) {
    if (!needCopyTable(table)) {
        return true;
    }
    addAliasMap(table->getMatchDocAllocator(), _exprsAliasMap);
    FunctionProvider functionProvider(table->getMatchDocAllocator(),
                                      table->getDataPool(),
                                      _resource.cavaAllocator,
                                      _resource.tracer,
                                      nullptr,
                                      nullptr,
                                      _resource.metricsReporter);
    prepareWithMatchInfo(functionProvider);

    auto tempPool = _resource.memoryPoolResource->getPool();
    MatchDocsExpressionCreator exprCreator(tempPool.get(),
                                           table->getMatchDocAllocator(),
                                           _resource.funcInterfaceCreator,
                                           _resource.cavaPluginManager,
                                           &functionProvider,
                                           _resource.cavaAllocator);
    return doProjectTable(table, &exprCreator);
}

bool CalcTable::declareTable(const table::TablePtr &inputTable,
                             table::TablePtr &outputTable,
                             MatchDocsExpressionCreator *exprCreator,
                             vector<ExprColumnType> &exprVec,
                             vector<ColumnDataTuple> &columnDataTupleVec) {
    SQL_LOG(TRACE2, "step1: declare table");
    for (size_t i = 0; i < _param.outputFields.size(); i++) {
        const string &newName = _param.outputFields[i];
        auto iter = _exprsMap.find(newName);
        if (iter != _exprsMap.end()) {
            string outputType;
            if (_param.outputFields.size() == _param.outputFieldsType.size()) {
                outputType = _param.outputFieldsType[i];
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
CalcTable::createAttributeExpression(const std::string &outputType,
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
            SQL_LOG(DEBUG, "create const expression for null value, result type [%d]", resultType);
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
                    SQL_LOG(TRACE1,
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

bool CalcTable::declareExprColumn(const std::string &name,
                                  const std::string &outputType,
                                  const ExprEntity &exprEntity,
                                  table::TablePtr &outputTable,
                                  MatchDocsExpressionCreator *exprCreator,
                                  vector<ExprColumnType> &exprVec) {
    AttributeExpression *attriExpr = nullptr;
    const std::string &exprJson = exprEntity.exprJson;
    if (!exprJson.empty() && ExprUtil::parseExprsJson(exprJson, *_simpleDoc)
        && ExprUtil::isCaseOp(*_simpleDoc)) {
        bool hasError = false;
        std::string errorInfo;
        attriExpr = ExprUtil::CreateCaseExpression(
            exprCreator, outputType, *_simpleDoc, hasError, errorInfo, outputTable->getDataPool());
        if (hasError || attriExpr == nullptr) {
            SQL_LOG(WARN, "create case expression failed [%s]", errorInfo.c_str());
            return false;
        }
    } else {
        const std::string &exprStr = exprEntity.exprStr;
        attriExpr = createAttributeExpression(
            outputType, exprStr, exprCreator, outputTable->getDataPool());
        if (attriExpr == nullptr) {
            SQL_LOG(WARN, "create attribute expr failed, expr string [%s]", exprStr.c_str());
            return false;
        }
    }
    auto bt = attriExpr->getType();
    bool isMulti = attriExpr->isMultiValue();
    auto func = [&](auto a) {
        typedef typename decltype(a)::value_type T;
        ColumnDataBase *newColumnData
            = table::TableUtil::declareAndGetColumnData<T>(outputTable, name, false, true);
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

bool CalcTable::calcTableExpr(const table::TablePtr &inputTable,
                              table::TablePtr &outputTable,
                              std::vector<ExprColumnType> &exprVec,
                              AttributeExpression *optimizeReCalcExpression) {
    SQL_LOG(TRACE2, "step2.1: project table fill optimize recalc expression value");
    if (optimizeReCalcExpression) {
        SQL_LOG(TRACE2, "step2.1: start fill optimize recalc expression value");
        auto expr = dynamic_cast<OptimizeReCalcExpression *>(optimizeReCalcExpression);
        if (expr) {
            SQL_LOG(
                TRACE2, "optimize recalc expression is [%s]", expr->getOriginalString().c_str());
            auto rows = inputTable->getRows();
            auto allocator = inputTable->getMatchDocAllocator();
            if (!expr->allocate(allocator)) {
                SQL_LOG(ERROR, "optimize recalc expression allocate failed");
                return false;
            }
            allocator->extend();
            expr->batchAndSetEvaluate(rows.data(), inputTable->getRowCount());
        }
        SQL_LOG(TRACE2, "step2.1: end fill optimize recalc expression value");
    } else {
        SQL_LOG(TRACE2, "step2.1: no optimize recalc expression value");
    }
    SQL_LOG(TRACE2, "step2.2: project table fill expression value");

    AttributeExpression *attriExpr = nullptr;
    table::ColumnDataBase *originColumnDataBase = nullptr;

    auto func = [&](auto a) {
        typedef typename decltype(a)::value_type T;
        auto attriExprTyped = static_cast<AttributeExpressionTyped<T> *>(attriExpr);
        auto newColumnData = static_cast<ColumnData<T> *>(originColumnDataBase);
        if (attriExprTyped == nullptr || newColumnData == nullptr) {
            return false;
        }
        for (size_t i = 0; i < outputTable->getRowCount(); i++) {
            newColumnData->set(i, attriExprTyped->evaluateAndReturn(inputTable->getRow(i)));
        }
        return true;
    };

    for (size_t i = 0; i < exprVec.size(); ++i) {
        attriExpr = exprVec[i].first;
        originColumnDataBase = exprVec[i].second;
        auto bt = attriExpr->getType();
        bool isMulti = attriExpr->isMultiValue();
        if (!table::ValueTypeSwitch::switchType(bt, isMulti, func, func)) {
            SQL_LOG(ERROR, "evaluate expr column failed");
            return false;
        }
    }
    return true;
}

bool CalcTable::cloneColumnData(table::Column *originColumn,
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
        ColumnDataBase *originColumnData = originColumn->getColumnData<T>();
        ColumnDataBase *newColumnData
            = table::TableUtil::declareAndGetColumnData<T>(table, name, false, true);
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

bool CalcTable::calcTableCloumn(table::TablePtr &outputTable,
                                std::vector<ColumnDataTuple> &columnDataTupleVec) {
    SQL_LOG(TRACE2, "step3: project table  fill colunm value");
    table::ColumnDataBase *newColumnDataBase = nullptr;
    table::ColumnDataBase *originColumnDataBase = nullptr;
    matchdoc::ValueType vt;
    auto calcFunc = [&](auto a) {
        typedef typename decltype(a)::value_type T;
        auto originColumnData = static_cast<ColumnData<T> *>(originColumnDataBase);
        auto newColumnData = static_cast<ColumnData<T> *>(newColumnDataBase);
        for (size_t i = 0; i < outputTable->getRowCount(); i++) {
            newColumnData->set(i, originColumnData->get(i));
        }
        return true;
    };
    for (size_t i = 0; i < columnDataTupleVec.size(); ++i) {
        std::tie(originColumnDataBase, newColumnDataBase, vt) = columnDataTupleVec[i];
        if (!ValueTypeSwitch::switchType(vt, calcFunc, calcFunc)) {
            SQL_LOG(ERROR, "evaluate column value failed");
            return false;
        }
    }
    return true;
}

} // namespace sql
} // namespace isearch
