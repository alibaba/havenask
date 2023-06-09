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
#include "ha3/sql/ops/scan/Ha3ScanConditionVisitor.h"

#include <assert.h>
#include <ext/alloc_traits.h>
#include <stdint.h>
#include <cstddef>
#include <functional>
#include <memory>
#include <utility>
#include <vector>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/RapidJsonCommon.h"
#include "autil/legacy/RapidJsonHelper.h"
#include "autil/mem_pool/Pool.h"
#include "autil/mem_pool/PoolBase.h"
#include "ha3/common/AndQuery.h"
#include "ha3/common/ErrorDefine.h"
#include "ha3/common/NumberQuery.h"
#include "ha3/common/NumberTerm.h"
#include "ha3/common/OrQuery.h"
#include "ha3/common/Query.h"
#include "ha3/common/Term.h"
#include "ha3/common/TermQuery.h"
#include "ha3/common/TimeoutTerminator.h"
#include "ha3/search/IndexPartitionReaderWrapper.h"
#include "ha3/search/QueryExecutor.h"
#include "ha3/sql/common/IndexInfo.h"
#include "ha3/sql/common/Log.h"
#include "ha3/sql/common/common.h"
#include "ha3/sql/ops/condition/Condition.h"
#include "ha3/sql/ops/condition/ExprUtil.h"
#include "ha3/sql/ops/condition/NotExpressionWrapper.h"
#include "ha3/sql/ops/condition/SqlJsonUtil.h"
#include "ha3/sql/ops/scan/QueryExecutorExpressionWrapper.h"
#include "ha3/sql/ops/scan/UdfToQuery.h"
#include "ha3/sql/ops/scan/UdfToQueryManager.h"
#include "indexlib/misc/common.h"
#include "indexlib/util/Exception.h"
#include "rapidjson/document.h"
#include "suez/turing/expression/cava/common/CavaConfig.h"
#include "suez/turing/expression/framework/AttributeExpression.h"
#include "suez/turing/expression/framework/AttributeExpressionCreator.h"
#include "suez/turing/expression/framework/BinaryAttributeExpression.h"

namespace isearch {
namespace search {
class LayerMeta;
} // namespace search
} // namespace isearch

using namespace std;
using namespace autil;
using namespace suez::turing;

using namespace isearch::common;
using namespace isearch::search;

namespace isearch {
namespace sql {
AUTIL_LOG_SETUP(sql, Ha3ScanConditionVisitor);

Ha3ScanConditionVisitor::Ha3ScanConditionVisitor(const Ha3ScanConditionVisitorParam &param)
    : _query(nullptr)
    , _attrExpr(nullptr)
    , _param(param) {}

Ha3ScanConditionVisitor::~Ha3ScanConditionVisitor() {
    DELETE_AND_SET_NULL(_query);
    _attrExpr = nullptr; // delete by attribute expression pool
}

void Ha3ScanConditionVisitor::visitAndCondition(AndCondition *condition) {
    if (isError()) {
        return;
    }
    const vector<ConditionPtr> &children = condition->getChildCondition();
    vector<Query *> childQuery;
    vector<AttributeExpression *> childExpr;
    for (size_t i = 0; i < children.size(); i++) {
        children[i]->accept(this);
        if (getQuery()) {
            childQuery.push_back(stealQuery());
        }
        if (getAttributeExpression()) {
            childExpr.push_back(stealAttributeExpression());
        }
        if (isError()) {
            break;
        }
    }
    if (isError()) {
        for (auto query : childQuery) {
            delete query;
        }
        return;
    }
    if (childQuery.size() == 1) {
        _query = childQuery[0];
    } else if (childQuery.size() > 1) {
        _query = new AndQuery("");
        for (auto query : childQuery) {
            _query->addQuery(QueryPtr(query));
        }
    }
    if (childExpr.size() == 1) {
        _attrExpr = childExpr[0];
    } else if (childExpr.size() > 1) {
        AttributeExpression *leftExpr = childExpr[0];
        AttributeExpression *rightExpr = nullptr;
        typedef BinaryAttributeExpression<std::logical_and, bool> AndAttrExpr;
        for (size_t i = 1; i < childExpr.size(); i++) {
            rightExpr = childExpr[i];
            string oriString
                = leftExpr->getOriginalString() + " AND " + rightExpr->getOriginalString();
            bool isSubExpr = leftExpr->isSubExpression() || rightExpr->isSubExpression();
            leftExpr = POOL_NEW_CLASS(_param.pool, AndAttrExpr, leftExpr, rightExpr);
            leftExpr->setOriginalString(oriString);
            leftExpr->setIsSubExpression(isSubExpr);
            _param.attrExprCreator->addNeedDeleteExpr(leftExpr);
        }
        _attrExpr = leftExpr;
    }
}

void Ha3ScanConditionVisitor::visitOrCondition(OrCondition *condition) {
    if (isError()) {
        return;
    }
    const vector<ConditionPtr> &children = condition->getChildCondition();
    vector<Query *> childQuery;
    vector<AttributeExpression *> childExpr;
    bool transQuery = false;
    for (size_t i = 0; i < children.size(); i++) {
        children[i]->accept(this);
        if (getQuery() != nullptr
            && getAttributeExpression() != nullptr) { // TODO optimize by translate tree
            Query *query = stealQuery();
            AttributeExpression *attrExpr
                = POOL_NEW_CLASS(_param.pool, QueryExecutorExpressionWrapper, query);
            _queryExprs.push_back(attrExpr);
            attrExpr->setOriginalString(query->toString());
            _param.attrExprCreator->addNeedDeleteExpr(attrExpr);

            AttributeExpression *leftExpr = attrExpr;
            AttributeExpression *rightExpr = stealAttributeExpression();
            typedef BinaryAttributeExpression<std::logical_and, bool> AndAttrExpr;
            string oriString = "(" + leftExpr->getOriginalString() + " AND "
                               + rightExpr->getOriginalString() + ")";
            bool isSubExpr = leftExpr->isSubExpression() || rightExpr->isSubExpression();
            AttributeExpression *newExpr
                = POOL_NEW_CLASS(_param.pool, AndAttrExpr, leftExpr, rightExpr);
            newExpr->setOriginalString(oriString);
            newExpr->setIsSubExpression(isSubExpr);
            _param.attrExprCreator->addNeedDeleteExpr(newExpr);
            childExpr.push_back(newExpr);
            transQuery = true;
        } else if (getQuery() != nullptr) {
            childQuery.push_back(stealQuery());
        } else if (getAttributeExpression() != nullptr) {
            transQuery = true;
            childExpr.push_back(stealAttributeExpression());
        }
        if (isError()) {
            break;
        }
    }
    if (isError()) {
        for (auto query : childQuery) {
            delete query;
        }
        return;
    }
    if (childQuery.size() == 1) {
        _query = childQuery[0];
    } else if (childQuery.size() > 1) {
        _query = new OrQuery("");
        for (auto query : childQuery) {
            _query->addQuery(QueryPtr(query));
        }
    }
    if (transQuery && _query) {
        AttributeExpression *attrExpr
            = POOL_NEW_CLASS(_param.pool, QueryExecutorExpressionWrapper, _query);
        _queryExprs.push_back(attrExpr);
        attrExpr->setOriginalString(_query->toString());
        _param.attrExprCreator->addNeedDeleteExpr(attrExpr);
        childExpr.push_back(attrExpr);
        _query = nullptr;
    }
    if (childExpr.size() == 1) {
        _attrExpr = childExpr[0];
    } else if (childExpr.size() > 1) {
        AttributeExpression *leftExpr = childExpr[0];
        AttributeExpression *rightExpr = nullptr;
        typedef BinaryAttributeExpression<std::logical_or, bool> OrAttrExpr;
        for (size_t i = 1; i < childExpr.size(); i++) {
            rightExpr = childExpr[i];
            string oriString = "(" + leftExpr->getOriginalString() + " OR "
                               + rightExpr->getOriginalString() + ")";
            bool isSubExpr = leftExpr->isSubExpression() || rightExpr->isSubExpression();
            leftExpr = POOL_NEW_CLASS(_param.pool, OrAttrExpr, leftExpr, rightExpr);
            leftExpr->setOriginalString(oriString);
            leftExpr->setIsSubExpression(isSubExpr);
            _param.attrExprCreator->addNeedDeleteExpr(leftExpr);
        }
        _attrExpr = leftExpr;
    }
}

void Ha3ScanConditionVisitor::visitNotCondition(NotCondition *condition) {
    if (isError()) {
        return;
    }
    const vector<ConditionPtr> &children = condition->getChildCondition();
    assert(children.size() == 1);
    children[0]->accept(this);
    Query *childQuery = stealQuery();
    AttributeExpression *childExpr = stealAttributeExpression();
    if (isError()) {
        DELETE_AND_SET_NULL(childQuery);
        return;
    }
    if (childQuery != nullptr && childExpr == nullptr) {
        AttributeExpressionTyped<bool> *expr
            = POOL_NEW_CLASS(_param.pool, QueryExecutorExpressionWrapper, childQuery);
        _queryExprs.push_back(expr);
        expr->setOriginalString(childQuery->toString());
        _param.attrExprCreator->addNeedDeleteExpr(expr);
        _attrExpr = POOL_NEW_CLASS(_param.pool, NotExpressionWrapper, expr);
        string oriString = "not (" + childQuery->toString() + ")";
        _attrExpr->setOriginalString(oriString);
        _attrExpr->setIsSubExpression(expr->isSubExpression());
        _param.attrExprCreator->addNeedDeleteExpr(_attrExpr);
        childQuery = nullptr;
    } else if (childQuery == nullptr && childExpr != nullptr) {
        AttributeExpressionTyped<bool> *childExprTyped
            = dynamic_cast<AttributeExpressionTyped<bool> *>(childExpr);
        if (childExprTyped == nullptr) {
            string errorInfo = "dynamic cast expr[" + childExpr->getOriginalString()
                               + "] to bool expression failed.";
            setErrorInfo(errorInfo);
            return;
        }
        _attrExpr = POOL_NEW_CLASS(_param.pool, NotExpressionWrapper, childExprTyped);
        string oriString = "not (" + childExprTyped->getOriginalString() + ")";
        _attrExpr->setOriginalString(oriString);
        _attrExpr->setIsSubExpression(childExprTyped->isSubExpression());
        _param.attrExprCreator->addNeedDeleteExpr(_attrExpr);
    } else if (childQuery != nullptr && childExpr != nullptr) {
        AttributeExpression *leftExpr
            = POOL_NEW_CLASS(_param.pool, QueryExecutorExpressionWrapper, childQuery);
        _queryExprs.push_back(leftExpr);
        leftExpr->setOriginalString(childQuery->toString());
        _param.attrExprCreator->addNeedDeleteExpr(leftExpr);
        childQuery = nullptr;
        AttributeExpression *rightExpr = childExpr;
        string oriString = leftExpr->getOriginalString() + " AND " + rightExpr->getOriginalString();
        typedef BinaryAttributeExpression<std::logical_and, bool> AndAttrExpr;
        bool isSubExpr = leftExpr->isSubExpression() || rightExpr->isSubExpression();
        leftExpr = POOL_NEW_CLASS(_param.pool, AndAttrExpr, leftExpr, rightExpr);
        leftExpr->setOriginalString(oriString);
        leftExpr->setIsSubExpression(isSubExpr);
        _param.attrExprCreator->addNeedDeleteExpr(leftExpr);

        AttributeExpressionTyped<bool> *leftExprTyped
            = dynamic_cast<AttributeExpressionTyped<bool> *>(leftExpr);
        if (leftExprTyped == nullptr) {
            string errorInfo = "dynamic cast expr[" + leftExpr->getOriginalString()
                               + "] to bool expression failed.";
            setErrorInfo(errorInfo);
            return;
        }

        _attrExpr = POOL_NEW_CLASS(_param.pool, NotExpressionWrapper, leftExprTyped);
        oriString = "not (" + leftExprTyped->getOriginalString() + ")";
        _attrExpr->setOriginalString(oriString);
        _attrExpr->setIsSubExpression(leftExprTyped->isSubExpression());
        _param.attrExprCreator->addNeedDeleteExpr(_attrExpr);
    }
}

#define RETURN_AND_SET_ERROR(errorInfo)                                                            \
    SQL_LOG(WARN, "%s", errorInfo.c_str());                                                        \
    setErrorInfo(errorInfo);                                                                       \
    return;

void Ha3ScanConditionVisitor::visitLeafCondition(LeafCondition *condition) {
    if (isError()) {
        return;
    }
    const SimpleValue &leafCondition = condition->getCondition();
    if (ExprUtil::isUdf(leafCondition) || ExprUtil::isInOp(leafCondition)) {
        SQL_LOG(TRACE2, "to udf `%s`",
                RapidJsonHelper::SimpleValue2Str(leafCondition).c_str())
        if (!visitUdfCondition(leafCondition)) {
            return;
        }
    } else {
        SQL_LOG(TRACE2, "to term query `%s`",
                RapidJsonHelper::SimpleValue2Str(leafCondition).c_str())
        _query = toTermQuery(leafCondition);
    }
    if (_query) {
        SQL_LOG(DEBUG,
                "table [%s], leaf query [%s]",
                _param.mainTableName.c_str(),
                _query->toString().c_str());
        return;
    }

    bool hasError = false;
    string errorInfo = "";
    if (ExprUtil::isCaseOp(leafCondition)) {
        _attrExpr = ExprUtil::CreateCaseExpression(_param.attrExprCreator,
                                                   "CONDITION_BOOLEAN",
                                                   leafCondition,
                                                   hasError,
                                                   errorInfo,
                                                   _param.pool);
    } else {
        std::string exprStr = "";
        if (ExprUtil::createExprString(leafCondition, exprStr) && !exprStr.empty()) {
            SQL_LOG(DEBUG,
                    "table [%s] leaf exprStr [%s]",
                    _param.mainTableName.c_str(),
                    exprStr.c_str());
            _attrExpr = _param.attrExprCreator->createAttributeExpression(exprStr);
            if (_attrExpr != nullptr && dynamic_cast<AttributeExpressionTyped<bool>* >(_attrExpr) == nullptr ) {
                SQL_LOG(ERROR,
                    "table [%s] leaf exprStr [%s] is not bool expression",
                    _param.mainTableName.c_str(),
                    exprStr.c_str());
                _attrExpr = nullptr;
                errorInfo = "table [" + _param.mainTableName + "] parse condition failed";
                RETURN_AND_SET_ERROR(errorInfo);
            }
        }
    }
    if (_attrExpr == nullptr) {
        errorInfo = "table [" + _param.mainTableName + "] parse condition failed";
        RETURN_AND_SET_ERROR(errorInfo);
    }
}

bool Ha3ScanConditionVisitor::visitUdfCondition(const SimpleValue &leafCondition) {
    string op(leafCondition[SQL_CONDITION_OPERATOR].GetString());
    auto *udfToQuery = _param.udfToQueryManager->get<UdfToQuery>(op);
    if (!udfToQuery) {
        return true;
    }
    _query = udfToQuery->toQuery(leafCondition, _param);

    if (_query == nullptr) {
        if (udfToQuery->onlyQuery()) {
            string errorInfo = "convert udf [" + op + "] to query failed ["
                               + RapidJsonHelper::SimpleValue2Str(leafCondition)
                               + "]. table: " + _param.mainTableName;
            SQL_LOG(WARN, "%s", errorInfo.c_str());
            setErrorInfo(errorInfo);
            return false;
        } else {
            return true;
        }
    }
    SQL_LOG(DEBUG,
            "convert udf [%s] to query [%s]",
            RapidJsonHelper::SimpleValue2Str(leafCondition).c_str(),
            _query->toString().c_str());
    return true;
}

Query *Ha3ScanConditionVisitor::stealQuery() {
    Query *query = _query;
    _query = nullptr;
    return query;
}

suez::turing::AttributeExpression *Ha3ScanConditionVisitor::stealAttributeExpression() {
    suez::turing::AttributeExpression *attrExpr = _attrExpr;
    _attrExpr = nullptr;
    return attrExpr;
}

const Query *Ha3ScanConditionVisitor::getQuery() const {
    return _query;
}

const suez::turing::AttributeExpression *Ha3ScanConditionVisitor::getAttributeExpression() const {
    return _attrExpr;
}

vector<suez::turing::AttributeExpression *> Ha3ScanConditionVisitor::getQueryExprs() const {
    return _queryExprs;
}

Query *Ha3ScanConditionVisitor::toTermQuery(const SimpleValue &condition) {
    if (!condition.IsObject() || !condition.HasMember(SQL_CONDITION_OPERATOR)
        || !condition.HasMember(SQL_CONDITION_PARAMETER)) {
        SQL_LOG(TRACE2, "`%s` is not object to term query",
                RapidJsonHelper::SimpleValue2Str(condition).c_str());
        return nullptr;
    }
    string op(condition[SQL_CONDITION_OPERATOR].GetString());
    if (op != SQL_LIKE_OP && op != SQL_EQUAL_OP) {
        SQL_LOG(TRACE2, "`%s` op is not like or =",
                RapidJsonHelper::SimpleValue2Str(condition).c_str());
        return nullptr;
    }
    const SimpleValue &param = condition[SQL_CONDITION_PARAMETER];
    if ((!param.IsArray()) || param.Size() != 2) {
        SQL_LOG(TRACE2, "`%s` params size is not 2",
                RapidJsonHelper::SimpleValue2Str(condition).c_str());
        return nullptr;
    }
    const SimpleValue &left = param[0];
    const SimpleValue &right = param[1];
    bool leftIsCast = ExprUtil::isSameUdf(left, SQL_UDF_CAST_OP);
    bool rightIsCast = ExprUtil::isSameUdf(right, SQL_UDF_CAST_OP);
    if (leftIsCast && rightIsCast) {
        const SimpleValue &paramLeft = left[SQL_CONDITION_PARAMETER];
        if ((!paramLeft.IsArray()) || paramLeft.Size() == 0) {
            SQL_LOG(TRACE2, "`%s` is invalid",
                    RapidJsonHelper::SimpleValue2Str(paramLeft).c_str());
            return nullptr;
        }
        const SimpleValue &paramRight = right[SQL_CONDITION_PARAMETER];
        if ((!paramRight.IsArray()) || paramRight.Size() == 0) {
            SQL_LOG(TRACE2, "`%s` is invalid",
                    RapidJsonHelper::SimpleValue2Str(paramRight).c_str());
            return nullptr;
        }
        return createTermQuery(paramLeft[0], paramRight[0], op);
    } else if (leftIsCast) {
        const SimpleValue &param = left[SQL_CONDITION_PARAMETER];
        if ((!param.IsArray()) || param.Size() == 0) {
            SQL_LOG(TRACE2, "`%s` is invalid",
                    RapidJsonHelper::SimpleValue2Str(param).c_str());
            return nullptr;
        }
        return createTermQuery(param[0], right, op);
    } else if (rightIsCast) {
        const SimpleValue &param = right[SQL_CONDITION_PARAMETER];
        if ((!param.IsArray()) || param.Size() == 0) {
            SQL_LOG(TRACE2, "`%s` is invalid",
                    RapidJsonHelper::SimpleValue2Str(param).c_str());
            return nullptr;
        }
        return createTermQuery(left, param[0], op);
    }
    return createTermQuery(left, right, op);
}

Query *Ha3ScanConditionVisitor::createTermQuery(const SimpleValue &left,
                                                const SimpleValue &right,
                                                const string &op) {
    if ((!left.IsNumber() && !left.IsString()) || (!right.IsNumber() && !right.IsString())) {
        SQL_LOG(TRACE2, "`%s`, `%s` is not string or number type",
                RapidJsonHelper::SimpleValue2Str(left).c_str(),
                RapidJsonHelper::SimpleValue2Str(right).c_str());
        return nullptr;
    }
    bool leftHasColumn = SqlJsonUtil::isColumn(left);
    bool rightHasColumn = SqlJsonUtil::isColumn(right);
    if (leftHasColumn && rightHasColumn) {
        SQL_LOG(TRACE2, "`%s`, `%s` is both column",
                RapidJsonHelper::SimpleValue2Str(left).c_str(),
                RapidJsonHelper::SimpleValue2Str(right).c_str());
        return nullptr;
    } else if (leftHasColumn) {
        return doCreateTermQuery(left, right, op);
    } else if (rightHasColumn) {
        return doCreateTermQuery(right, left, op);
    } else {
        SQL_LOG(TRACE2, "`%s`, `%s` is not column",
                RapidJsonHelper::SimpleValue2Str(left).c_str(),
                RapidJsonHelper::SimpleValue2Str(right).c_str());
        return nullptr;
    }
}

Query *Ha3ScanConditionVisitor::doCreateTermQuery(const SimpleValue &attr,
                                                  const SimpleValue &value,
                                                  const string &op) {
    string attrName = SqlJsonUtil::getColumnName(attr);
    auto iter = _param.indexInfo->find(attrName);
    if (iter == _param.indexInfo->end()) {
        SQL_LOG(TRACE2, "`%s` do not build index, skip", attrName.c_str());
        return nullptr;
    }
    const string &indexName = iter->second.name;
    const string &indexType = iter->second.type;
    if (indexType == "number") {
        if (op != SQL_EQUAL_OP) {
            SQL_LOG(TRACE2, "`%s %s %s` : number type index need op `=`",
                    RapidJsonHelper::SimpleValue2Str(attr).c_str(),
                    RapidJsonHelper::SimpleValue2Str(value).c_str(),
                    op.c_str());
            return nullptr;
        }
        if (value.IsInt() || value.IsInt64()) {
            NumberTerm term(value.GetInt64(), indexName.c_str(), RequiredFields());
            return new NumberQuery(term, "");
        } else if (value.IsString()) {
            int64_t numVal = 0;
            if (StringUtil::numberFromString(value.GetString(), numVal)) {
                NumberTerm term(numVal, indexName.c_str(), RequiredFields());
                return new NumberQuery(term, "");
            } else {
                SQL_LOG(TRACE2, "`%s` : cast number type from string failed",
                        RapidJsonHelper::SimpleValue2Str(value).c_str());
                return nullptr;
            }
        }
    } else {
        string termStr;
        if (value.IsInt() || value.IsInt64()) {
            termStr = StringUtil::toString(value.GetInt64());
        } else if (value.IsString()) {
            termStr = value.GetString();
        }
        if (!termStr.empty()) {
            Term term;
            term.setIndexName(indexName.c_str());
            term.setWord(termStr.c_str());
            return new TermQuery(term, "");
        } else {
            SQL_LOG(TRACE2, "`%s` : cast string type index failed",
                    RapidJsonHelper::SimpleValue2Str(value).c_str());
        }
    }
    return nullptr;
}

} // namespace sql
} // namespace isearch
