#include <ha3/sql/ops/calc/CalcTable.h>
#include <ha3/sql/ops/condition/ConstExpression.h>
#include <ha3/sql/data/TableUtil.h>
#include <ha3/sql/ops/util/KernelRegister.h>
#include <ha3/sql/ops/util/KernelUtil.h>
#include <ha3/sql/resource/SqlQueryResource.h>
#include <autil/TimeUtility.h>
#include <autil/StringUtil.h>
#include <autil/legacy/RapidJsonCommon.h>
#include <autil/legacy/RapidJsonHelper.h>
#include <ha3/sql/ops/util/SqlJsonUtil.h>
#include <ha3/sql/ops/condition/ConditionParser.h>
#include <ha3/sql/ops/condition/AliasConditionVisitor.h>
#include <ha3/sql/ops/calc/CalcConditionVisitor.h>
#include "suez/turing/expression/function/FunctionInterfaceCreator.h"
#include <suez/turing/expression/syntax/SyntaxParser.h>
#include <ha3/sql/util/ValueTypeSwitch.h>
#include <suez/turing/expression/framework/ConstAttributeExpression.h>

using namespace std;
using namespace matchdoc;
using namespace navi;
using namespace autil_rapidjson;
using namespace suez::turing;

BEGIN_HA3_NAMESPACE(sql);
HA3_LOG_SETUP(sql, CalcTable);

const string CalcTable::DEFAULT_NULL_NUMBER_VALUE = "0";

CalcTable::CalcTable(autil::mem_pool::Pool *pool,
                     navi::MemoryPoolResource *memoryPoolResource,
                     const std::vector<std::string> &outputFields,
                     const std::vector<std::string> &outputFieldsType,
                     suez::turing::Tracer *tracer,
                     suez::turing::SuezCavaAllocator *cavaAllocator,
                     suez::turing::CavaPluginManagerPtr cavaPluginManager,
                     suez::turing::FunctionInterfaceCreator *funcInterfaceCreator)
    : _filterFlag(true)
    , _pool(pool)
    , _memoryPoolResource(memoryPoolResource)
    , _outputFields(outputFields)
    , _outputFieldsType(outputFieldsType)
    , _tracer(tracer)
    , _cavaAllocator(cavaAllocator)
    , _cavaPluginManager(cavaPluginManager)
    , _funcInterfaceCreator(funcInterfaceCreator)
{}

bool CalcTable::init(const std::string &outputExprsJson, const std::string &conditionJson) {
    if (!ExprUtil::parseOutputExprs(_pool, outputExprsJson, _exprsMap, _exprsAliasMap)) {
        SQL_LOG(WARN, "parse output exprs json failed,  json str [%s].", outputExprsJson.c_str());
        return false;
    }
    _conditionJson = conditionJson;
    ConditionParser parser(_pool);
    if (!parser.parseCondition(_conditionJson, _condition)) {
        SQL_LOG(ERROR, "parse condition [%s] failed", _conditionJson.c_str());
        return false;
    }
    return true;
}

bool CalcTable::calcTable(TablePtr &table) {
    if (!filterTable(table)) {
        SQL_LOG(ERROR, "filter table failed.");
        return false;
    }
    if (!_outputFields.empty() && !projectTable(table)) {
        SQL_LOG(ERROR, "project table failed.");
        return false;
    }
    return true;
}

bool CalcTable::needCalc(const TablePtr &table) {
    return _condition != nullptr || needCopyTable(table);
}

bool CalcTable::filterTable(const TablePtr &table) {
    return filterTable(table, 0, table->getRowCount(), false);
}

bool CalcTable::filterTable(const TablePtr &table, size_t startIdx, size_t endIdx, bool lazyDelete) {
    if (_condition == nullptr || !_filterFlag) {
        return true;
    }
    AliasConditionVisitor aliasVisitor;
    _condition->accept(&aliasVisitor);
    const auto &aliasMap = aliasVisitor.getAliasMap();
    addAliasMap(table->getMatchDocAllocator(), aliasMap);

    FunctionProviderPtr functionProvider(new FunctionProvider(
                    table->getMatchDocAllocator(), _pool, _cavaAllocator, _tracer, nullptr, nullptr));
    MatchDocsExpressionCreator exprCreator(_pool, table->getMatchDocAllocator(),
            _funcInterfaceCreator, _cavaPluginManager, functionProvider.get(), _cavaAllocator);
    CalcConditionVisitor visitor(&exprCreator, _pool);
    _condition->accept(&visitor);
    if (visitor.isError()) {
        SQL_LOG(WARN, "visit calc condition failed, error info [%s]",
                        visitor.errorInfo().c_str());
        return false;
    }
    AttributeExpression *attriExpr = visitor.stealAttributeExpression();
    if (attriExpr == nullptr) {
        SQL_LOG(WARN, "create attribute expr failed, expr string [%s]", _conditionJson.c_str());
        return false;
    }
    AttributeExpressionTyped<bool> *boolExpr =
        dynamic_cast<AttributeExpressionTyped<bool> *>(attriExpr);
    if (!boolExpr) {
        auto constExpr = dynamic_cast<ConstAttributeExpression<int64_t> *>(attriExpr);
        if (constExpr) {
            auto val = constExpr->getValue();
            if (val > 0) {
                SQL_LOG(INFO, "expr [%ld] is true", val);
                return true;
            } else {
                SQL_LOG(INFO, "expr [%ld] is false", val);
                for (size_t i = startIdx; i < endIdx; i++) {
                    table->markDeleteRow(i);
                }
                if (!lazyDelete) {
                    table->deleteRows();
                }
                return true;
            }
        }
        SQL_LOG(WARN, "[%s] not bool expr", attriExpr->getOriginalString().c_str());
        return false;
    }
    for (size_t i = startIdx; i < endIdx; i++) {
        Row row = table->getRow(i);
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
                             const map<string, string> &aliasMap)
{
    auto originalMap = allocator->getReferenceAliasMap();
    if (!originalMap) {
        originalMap.reset(new map<string, string>(aliasMap));
        allocator->setReferenceAliasMap(originalMap);
    } else {
        for (auto alias : aliasMap) {
            originalMap->insert(make_pair(alias.first, alias.second));
        }
    }
}

bool CalcTable::needCopyTable(const TablePtr &table) {
    if (table->getMatchDocAllocator()->hasSubDocAllocator()) {
        return true;
    }
    if (!_exprsMap.empty()) {
        return true;
    }
    if (_outputFields.size() != table->getColumnCount()) {
        return true;
    }
    for (size_t i = 0; i < _outputFields.size(); ++i) {
        if (_outputFields[i] != table->getColumnName(i)) {
            return true;
        }
    }
    return false;
}

bool CalcTable::projectTable(const TablePtr &inputTable, TablePtr &outputTable, bool declareOnly) {
    SQL_LOG(TRACE2, "project table %s",
            declareOnly ? "step1: declare table" : "step2: fill value");
    addAliasMap(inputTable->getMatchDocAllocator(), _exprsAliasMap);
    FunctionProviderPtr functionProvider(new FunctionProvider(
                    inputTable->getMatchDocAllocator(), _pool, _cavaAllocator, _tracer, nullptr, nullptr));
    MatchDocsExpressionCreator exprCreator(_pool, inputTable->getMatchDocAllocator(),
            _funcInterfaceCreator, _cavaPluginManager, functionProvider.get(), _cavaAllocator);
    for (size_t i = 0; i < _outputFields.size(); i++) {
        const string &newName = _outputFields[i];
        auto iter = _exprsMap.find(newName);
        string oldName;
        if (iter == _exprsMap.end()) {
            oldName = newName;
        } else {
            string outputType;
            if (_outputFields.size() == _outputFieldsType.size()) {
                outputType = _outputFieldsType[i];
            }
            if (!outputExprColumn(outputTable, newName, outputType,
                            inputTable, iter->second, declareOnly, &exprCreator))
            {
                SQL_LOG(WARN, "output expr column[%s] exprStr[%s] exprJson[%s] failed",
                        newName.c_str(), iter->second.exprStr.c_str(), iter->second.exprJson.c_str());
                return false;
            }
            continue;
        }
        ColumnPtr originColumn = inputTable->getColumn(oldName);
        if (originColumn == nullptr) {
            SQL_LOG(WARN, "column [%s] not found, may not exist in attribute", oldName.c_str());
            continue;
        }
        if (!cloneColumn(originColumn, outputTable, newName, declareOnly)) {
            SQL_LOG(ERROR, "clone column [%s] to [%s] failed",
                            oldName.c_str(), newName.c_str());
            return false;
        }
    }
    return true;
}

bool CalcTable::projectTable(TablePtr &table) {
    if (!needCopyTable(table)) {
        return true;
    }
    TablePtr output(new Table(_memoryPoolResource->getPool()));
    if (!projectTable(table, output, true)) {
        SQL_LOG(WARN, "declare output table field failed.");
        return false;
    }
    size_t rowCount = table->getRowCount();
    output->batchAllocateRow(rowCount);
    if (!projectTable(table, output, false)) {
        SQL_LOG(WARN, "project output table failed.");
        return false;
    }
    table = output;
    return true;
}

AttributeExpression *CalcTable::createAttributeExpression(
        const std::string &outputType,
        suez::turing::MatchDocsExpressionCreator &exprCreator,
        const std::string &exprStr)
{
    AttributeExpression* attriExpr = nullptr;
    if (exprStr.empty()) {
        if (outputType.empty()) {
            SQL_LOG(WARN, "expr string and output type both empty");
            return nullptr;
        }
        ExprResultType resultType = ExprUtil::transSqlTypeToVariableType(outputType);
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
        attriExpr = ExprUtil::createConstExpression(
            _pool, constValue, constValue, resultType);
        if (attriExpr) {
            SQL_LOG(DEBUG,
                "create const expression for null value, result type [%d]",
                 resultType);
            exprCreator.addNeedDeleteExpr(attriExpr);
        }
        return attriExpr;
    }

    SyntaxExpr *syntaxExpr = SyntaxParser::parseSyntax(exprStr);
    if (!syntaxExpr) {
        SQL_LOG(WARN, "parse syntax [%s] failed", exprStr.c_str());
        return nullptr;
    }
    if (syntaxExpr->getSyntaxExprType() == suez::turing::SYNTAX_EXPR_TYPE_CONST_VALUE) {
        AtomicSyntaxExpr *atomicExpr = dynamic_cast<AtomicSyntaxExpr*>(syntaxExpr);
        if (atomicExpr) {
            ExprResultType resultType = vt_unknown;
            if (!outputType.empty()) {
                resultType = ExprUtil::transSqlTypeToVariableType(outputType);
            } else {
                AtomicSyntaxExprType type = atomicExpr->getAtomicSyntaxExprType();
                if (type == FLOAT_VALUE) {
                    resultType = vt_float;
                } else if (type == INTEGER_VALUE) {
                    resultType = vt_int32;
                } else if (type == STRING_VALUE) {
                    resultType = vt_string;
                }
            }
            if (resultType != vt_unknown) {
                attriExpr = ExprUtil::createConstExpression(_pool, atomicExpr, resultType);
                if (attriExpr) {
                    SQL_LOG(TRACE1, "create const expression [%s], result type [%d]",
                            exprStr.c_str(), resultType);
                    exprCreator.addNeedDeleteExpr(attriExpr);
                }
            }
        }
    }
    if (!attriExpr) {
        attriExpr = exprCreator.createAttributeExpression(syntaxExpr, true);
    }
    DELETE_AND_SET_NULL(syntaxExpr);
    return attriExpr;
}

bool CalcTable::outputExprColumn(const TablePtr &table, const std::string &name, const std::string &outputType,
                                 const TablePtr &inputTable, const ExprEntity &exprEntity,
                                 bool declareOnly, MatchDocsExpressionCreator *exprCreator)
{
    AttributeExpression* attriExpr = NULL;
    AutilPoolAllocator allocator(_pool);
    SimpleDocument simpleDoc(&allocator);
    const std::string &exprJson = exprEntity.exprJson;
    if (!exprJson.empty() && ExprUtil::parseExprsJson(exprJson, simpleDoc) && ExprUtil::isCaseOp(simpleDoc)) {
        bool hasError = false;
        std::string errorInfo;
        std::map<std::string, std::string> renameMap;
        attriExpr = ExprUtil::CreateCaseExpression(
                exprCreator, outputType, simpleDoc,  hasError, errorInfo, renameMap, _pool);
        if (hasError || attriExpr == NULL)
        {
            SQL_LOG(WARN, "create case expression failed [%s]", errorInfo.c_str());
            return false;
        }
    } else {
        const std::string &exprStr = exprEntity.exprStr;
        attriExpr = createAttributeExpression(outputType, *exprCreator, exprStr);
        if (attriExpr == nullptr) {
            SQL_LOG(WARN, "create attribute expr failed, expr string [%s]", exprStr.c_str());
            return false;
        }
    }
    auto bt = attriExpr->getType();
    bool isMulti = attriExpr->isMultiValue();
    auto func = [&](auto a) {
        typedef typename decltype(a)::value_type T;
        auto newColumnData = TableUtil::declareAndGetColumnData<T>(
                table, name, !declareOnly, declareOnly);
        if (newColumnData == nullptr) {
            SQL_LOG(ERROR, "can not declare column [%s]", name.c_str());
            return false;
        }
        if (!declareOnly) {
            AttributeExpressionTyped<T>* attriExprTyped =
                dynamic_cast<AttributeExpressionTyped<T>*>(attriExpr);
            if (attriExprTyped == nullptr) { return false; }
            for (size_t i = 0; i < table->getRowCount(); i++) {
                newColumnData->set(i,
                        attriExprTyped->evaluateAndReturn(inputTable->getRow(i)));
            }
        }
        return true;
    };
    if (!ValueTypeSwitch::switchType(bt, isMulti, func, func)) {
        SQL_LOG(ERROR, "output expr column [%s] failed", name.c_str());
        return false;
    }
    if (attriExpr) {
        attriExpr->setEvaluated();
    }
    return true;
}

bool CalcTable::cloneColumn(ColumnPtr originColumn, TablePtr table,
                             const string &name, bool declareOnly)
{
    if (originColumn == nullptr) {
        SQL_LOG(ERROR, "input column is empty.");
        return false;
    }
    auto schema = originColumn->getColumnSchema();
    if (schema == nullptr) {
        SQL_LOG(ERROR, "get old column schema failed, new column name [%s]",
                name.c_str());
        return false;
    }
    auto vt = schema->getType();
    auto func = [&](auto a) {
        typedef typename decltype(a)::value_type T;
        auto originColumnData = originColumn->getColumnData<T>();
        auto newColumnData = TableUtil::declareAndGetColumnData<T>(
                table, name, !declareOnly, declareOnly);
        if (newColumnData == nullptr) {
            SQL_LOG(ERROR, "can not declare column [%s]", name.c_str());
            return false;
        }
        if (!declareOnly) {
            for (size_t i = 0; i < table->getRowCount(); i++) {
                newColumnData->set(i, originColumnData->get(i));
            }
        }
        return true;
    };
    if (!ValueTypeSwitch::switchType(vt, func, func)) {
        SQL_LOG(ERROR, "clone column [%s] failed", name.c_str());
        return false;
    }
    return true;
}

END_HA3_NAMESPACE(sql);
