#include <ha3/sql/ops/scan/Ha3ScanConditionVisitor.h>
#include <ha3/sql/common/common.h>
#include <ha3/search/QueryExecutorCreator.h>
#include <ha3/common/Term.h>
#include <ha3/common/NumberTerm.h>
#include <ha3/common/NumberQuery.h>
#include <ha3/common/TermQuery.h>
#include <ha3/common/AndQuery.h>
#include <ha3/common/OrQuery.h>
#include <ha3/queryparser/QueryParser.h>
#include <ha3/queryparser/ParserContext.h>
#include <ha3/qrs/StopWordsCleaner.h>
#include <ha3/qrs/QueryTokenizer.h>
#include <ha3/qrs/QueryValidator.h>
#include <suez/turing/expression/framework/BinaryAttributeExpression.h>
#include <ha3/sql/ops/scan/QueryExecutorExpressionWrapper.h>
#include <ha3/sql/ops/scan/NotExpressionWrapper.h>
#include <ha3/sql/ops/scan/SpQueryUdf.h>
#include <autil/legacy/RapidJsonHelper.h>
#include <ha3/sql/ops/util/SqlJsonUtil.h>
#include <memory>
#include <ha3/sql/ops/condition/ExprUtil.h>

using namespace std;
using namespace autil;
using namespace autil_rapidjson;
using namespace suez::turing;

USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(search);

BEGIN_HA3_NAMESPACE(sql);
HA3_LOG_SETUP(search, Ha3ScanConditionVisitor);

Ha3ScanConditionVisitor::Ha3ScanConditionVisitor(const Ha3ScanConditionVisitorParam &param)
    : _query(NULL)
    , _attrExpr(NULL)
    , _param(param)
{}

Ha3ScanConditionVisitor::~Ha3ScanConditionVisitor() {
    DELETE_AND_SET_NULL(_query);
    _attrExpr = NULL; // delete by attribute expression pool
}

void Ha3ScanConditionVisitor::visitAndCondition(AndCondition *condition) {
    if (isError()) {
        return;
    }
    vector<ConditionPtr> children = condition->getChildCondition();
    vector<Query*> childQuery;
    vector<AttributeExpression*> childExpr;
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
        for (auto query : childQuery ) {
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
        AttributeExpression *rightExpr = NULL;
        typedef BinaryAttributeExpression<std::logical_and, bool> AndAttrExpr;
        for (size_t i = 1; i < childExpr.size() ; i++ ) {
            rightExpr = childExpr[i];
            string oriString = leftExpr->getOriginalString() + " AND " + rightExpr->getOriginalString();
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
    vector<ConditionPtr> children = condition->getChildCondition();
    vector<Query*> childQuery;
    vector<AttributeExpression*> childExpr;
    bool transQuery = false;
    for (size_t i = 0; i < children.size(); i++) {
        children[i]->accept(this);
        if (getQuery() != NULL && getAttributeExpression() != NULL) { // TODO optimize by translate tree
            Query *query = stealQuery();
            QueryExecutor *queryExecutor = createQueryExecutor(query, _param.indexPartitionReaderWrapper,
                    _param.mainTableName, _param.pool, _param.timeoutTerminator, _param.layerMeta);
            if (!queryExecutor) {
                string errorInfo = "create query executor failed, query [" + query->toString() +"].";
                SQL_LOG(INFO, "table [%s] %s", _param.mainTableName.c_str(), errorInfo.c_str());
            }
            AttributeExpression *attrExpr = POOL_NEW_CLASS(_param.pool, QueryExecutorExpressionWrapper, queryExecutor);
            attrExpr->setOriginalString(query->toString());
            _param.attrExprCreator->addNeedDeleteExpr(attrExpr);
            DELETE_AND_SET_NULL(query);

            AttributeExpression *leftExpr = attrExpr;
            AttributeExpression *rightExpr = stealAttributeExpression();
            typedef BinaryAttributeExpression<std::logical_and, bool> AndAttrExpr;
            string oriString = "(" + leftExpr->getOriginalString() + " AND " + rightExpr->getOriginalString() + ")";
            bool isSubExpr = leftExpr->isSubExpression() || rightExpr->isSubExpression();
            AttributeExpression *newExpr = POOL_NEW_CLASS(_param.pool, AndAttrExpr, leftExpr, rightExpr);
            newExpr->setOriginalString(oriString);
            newExpr->setIsSubExpression(isSubExpr);
            _param.attrExprCreator->addNeedDeleteExpr(newExpr);
            childExpr.push_back(newExpr);
            transQuery = true;
        } else if (getQuery() != NULL) {
            childQuery.push_back(stealQuery());
        } else if (getAttributeExpression() != NULL) {
            transQuery = true;
            childExpr.push_back(stealAttributeExpression());
        }
        if (isError()) {
            break;
        }
    }
    if (isError()) {
        for (auto query : childQuery ) {
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
        QueryExecutor *queryExecutor = createQueryExecutor(_query, _param.indexPartitionReaderWrapper,
                _param.mainTableName, _param.pool, _param.timeoutTerminator, _param.layerMeta);
        if (!queryExecutor) {
            string errorInfo = "create query executor failed, query [" + _query->toString() +"].";
            SQL_LOG(INFO, "table [%s] %s", _param.mainTableName.c_str(), errorInfo.c_str());
        }
        AttributeExpression *attrExpr = POOL_NEW_CLASS(_param.pool, QueryExecutorExpressionWrapper, queryExecutor);
        attrExpr->setOriginalString(_query->toString());
        _param.attrExprCreator->addNeedDeleteExpr(attrExpr);
        childExpr.push_back(attrExpr);
        DELETE_AND_SET_NULL(_query);
    }
    if (childExpr.size() == 1) {
        _attrExpr = childExpr[0];
    } else if (childExpr.size() > 1) {
        AttributeExpression *leftExpr = childExpr[0];
        AttributeExpression *rightExpr = NULL;
        typedef BinaryAttributeExpression<std::logical_or, bool> OrAttrExpr;
        for (size_t i = 1; i < childExpr.size() ; i++ ) {
            rightExpr = childExpr[i];
            string oriString = "(" + leftExpr->getOriginalString() + " OR " + rightExpr->getOriginalString() + ")";
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
    vector<ConditionPtr> children = condition->getChildCondition();
    assert(children.size() == 1);
    children[0]->accept(this);
    Query *childQuery = stealQuery();
    AttributeExpression *childExpr = stealAttributeExpression();
    if (isError()) {
        DELETE_AND_SET_NULL(childQuery);
        return;
    }
    if (childQuery != NULL && childExpr == NULL) {
        QueryExecutor *queryExecutor = createQueryExecutor(childQuery, _param.indexPartitionReaderWrapper,
                _param.mainTableName, _param.pool, _param.timeoutTerminator, _param.layerMeta);
        if (!queryExecutor) {
            string errorInfo = "create query executor failed, query [" + childQuery->toString() +"].";
            SQL_LOG(INFO, "table [%s] %s", _param.mainTableName.c_str(), errorInfo.c_str());
        }
        AttributeExpressionTyped<bool> *expr = POOL_NEW_CLASS(_param.pool, QueryExecutorExpressionWrapper, queryExecutor);
        expr->setOriginalString(childQuery->toString());
        _param.attrExprCreator->addNeedDeleteExpr(expr);
        _attrExpr = POOL_NEW_CLASS(_param.pool, NotExpressionWrapper, expr);
        string oriString = "not (" + childQuery->toString() + ")";
        _attrExpr->setOriginalString(oriString);
        _attrExpr->setIsSubExpression(expr->isSubExpression());
        _param.attrExprCreator->addNeedDeleteExpr(_attrExpr);
        DELETE_AND_SET_NULL(childQuery);

    } else if (childQuery == NULL && childExpr != NULL) {
        AttributeExpressionTyped<bool> *childExprTyped = dynamic_cast<AttributeExpressionTyped<bool> *>(childExpr);
        if (childExprTyped == NULL) {
            string errorInfo = "dynamic cast expr[" + childExpr->getOriginalString() + "] to bool expression failed.";
            setErrorInfo(errorInfo);
            return;
        }
        _attrExpr = POOL_NEW_CLASS(_param.pool, NotExpressionWrapper, childExprTyped);
        string oriString = "not (" + childExprTyped->getOriginalString() + ")";
        _attrExpr->setOriginalString(oriString);
        _attrExpr->setIsSubExpression(childExprTyped->isSubExpression());
        _param.attrExprCreator->addNeedDeleteExpr(_attrExpr);
    } else if (childQuery != NULL && childExpr != NULL) {
        QueryExecutor *queryExecutor = createQueryExecutor(childQuery, _param.indexPartitionReaderWrapper,
                _param.mainTableName, _param.pool, _param.timeoutTerminator, _param.layerMeta);
        if (!queryExecutor) {
            string errorInfo = "create query executor failed, query [" + childQuery->toString() +"].";
            SQL_LOG(INFO, "table [%s] %s", _param.mainTableName.c_str(), errorInfo.c_str());
        }
        AttributeExpression *leftExpr = POOL_NEW_CLASS(_param.pool, QueryExecutorExpressionWrapper, queryExecutor);
        leftExpr->setOriginalString(childQuery->toString());
        _param.attrExprCreator->addNeedDeleteExpr(leftExpr);
        DELETE_AND_SET_NULL(childQuery);

        AttributeExpression *rightExpr = childExpr;
        string oriString = leftExpr->getOriginalString() + " AND " + rightExpr->getOriginalString();
        typedef BinaryAttributeExpression<std::logical_and, bool> AndAttrExpr;
        bool isSubExpr = leftExpr->isSubExpression() || rightExpr->isSubExpression();
        leftExpr = POOL_NEW_CLASS(_param.pool, AndAttrExpr, leftExpr, rightExpr);
        leftExpr->setOriginalString(oriString);
        leftExpr->setIsSubExpression(isSubExpr);
        _param.attrExprCreator->addNeedDeleteExpr(leftExpr);

        AttributeExpressionTyped<bool> *leftExprTyped = dynamic_cast<AttributeExpressionTyped<bool> *>(leftExpr);
        if (leftExprTyped == NULL) {
            string errorInfo = "dynamic cast expr[" + leftExpr->getOriginalString() + "] to bool expression failed.";
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

void Ha3ScanConditionVisitor::visitLeafCondition(LeafCondition *condition) {
    if (isError()) {
        return;
    }
    const SimpleValue &leafCondition = condition->getCondition();
    if (ExprUtil::isUdf(leafCondition)) {
        if (isMatchUdf(leafCondition)) {
            _query = toMatchQuery(leafCondition);
            if (_query == NULL) {
                string errorInfo = _param.mainTableName + " toMatchQuery failed ["
                                   + RapidJsonHelper::SimpleValue2Str(leafCondition)  +"].";
                SQL_LOG(WARN, "%s", errorInfo.c_str());
                setErrorInfo(errorInfo);
                return;
            }
        } else if (isQueryUdf(leafCondition)) {
            _query = toQuery(leafCondition);
            if (_query == NULL) {
                string errorInfo = _param.mainTableName + " toQuery failed ["
                                   + RapidJsonHelper::SimpleValue2Str(leafCondition)  +"].";
                SQL_LOG(WARN, "%s", errorInfo.c_str());
                setErrorInfo(errorInfo);
                return;
            }
        } else if (isSpQueryMatchUdf(leafCondition)) {
            _query = spToQuery(leafCondition);
            if (_query == NULL) {
                string errorInfo = _param.mainTableName + " spToQuery failed ["
                                   + RapidJsonHelper::SimpleValue2Str(leafCondition)  +"].";
                SQL_LOG(WARN, "%s", errorInfo.c_str());
                setErrorInfo(errorInfo);
                return;
            }
        } else if (ExprUtil::isContainUdf(leafCondition)) {
            _query = udfToQuery(leafCondition);
            if (_query != NULL) {
                SQL_LOG(TRACE2, "convert contain [%s] to query [%s]",
                        RapidJsonHelper::SimpleValue2Str(leafCondition).c_str(),
                        _query->toString().c_str());
            }
        } else if (ExprUtil::isHaInUdf(leafCondition)) {
            _query = udfToQuery(leafCondition);
            if (_query != NULL) {
                SQL_LOG(TRACE2, "convert ha_in [%s] to query [%s]",
                        RapidJsonHelper::SimpleValue2Str(leafCondition).c_str(),
                        _query->toString().c_str());
            }
        }
    } else {
        _query = toTermQuery(leafCondition);
    }
    if (_query) {
        SQL_LOG(TRACE2, "table [%s], leaf query [%s]",
                        _param.mainTableName.c_str(), _query->toString().c_str());
        return;
    } else {
        bool hasError = isError();
        string errorInfo;
        std::map<std::string, std::string> renameMap;
        if (ExprUtil::isCaseOp(leafCondition))
        {
            _attrExpr = ExprUtil::CreateCaseExpression(_param.attrExprCreator, "CONDITION_BOOLEAN",
                                                       leafCondition, hasError, errorInfo, renameMap, _param.pool);
        } else {
            std::string exprStr = ExprUtil::toExprString(leafCondition, hasError, errorInfo, renameMap);
            if (!hasError)
            {
                _attrExpr = _param.attrExprCreator->createAttributeExpression(exprStr);
                if (_attrExpr != NULL)
                {
                    SQL_LOG(DEBUG, "table [%s] leaf exprStr [%s]", _param.mainTableName.c_str(), exprStr.c_str());
                } else {
                    SQL_LOG(TRACE2, "table [%s] leaf exprStr [%s]", _param.mainTableName.c_str(), exprStr.c_str());
                }
            }
        }
        if (_attrExpr == NULL) {
            hasError = true;
            errorInfo = _param.mainTableName + " parse condition failed:" + errorInfo;
            SQL_LOG(WARN, "%s", errorInfo.c_str());
            setErrorInfo(errorInfo);
        }
    }
}

Query *Ha3ScanConditionVisitor::stealQuery() {
    Query *query = _query;
    _query = NULL;
    return query;
}

suez::turing::AttributeExpression *Ha3ScanConditionVisitor::stealAttributeExpression() {
    suez::turing::AttributeExpression *attrExpr = _attrExpr;
    _attrExpr = NULL;
    return attrExpr;
}

const Query *Ha3ScanConditionVisitor::getQuery() const {
    return _query;
}

const suez::turing::AttributeExpression *Ha3ScanConditionVisitor::getAttributeExpression() const {
    return _attrExpr;
}

Query *Ha3ScanConditionVisitor::toTermQuery(const SimpleValue &condition) {
    if (!condition.IsObject() || !condition.HasMember(SQL_CONDITION_OPERATOR)
        || !condition.HasMember(SQL_CONDITION_PARAMETER))
    {
        return NULL;
    }
    string op(condition[SQL_CONDITION_OPERATOR].GetString());
    if (op != SQL_LIKE_OP && op != SQL_EQUAL_OP) {
        return NULL;
    }
    const SimpleValue &param = condition[SQL_CONDITION_PARAMETER];
    if ((!param.IsArray()) || param.Size() != 2) {
        return NULL;
    }
    const SimpleValue &left = param[0];
    const SimpleValue &right = param[1];
    bool leftIsCast = ExprUtil::isCastUdf(left);
    bool rightIsCast = ExprUtil::isCastUdf(right);
    if (leftIsCast && rightIsCast) {
        const SimpleValue &paramLeft = left[SQL_CONDITION_PARAMETER];
        if ((!paramLeft.IsArray()) || paramLeft.Size() == 0) {
            return NULL;
        }
        const SimpleValue &paramRight = right[SQL_CONDITION_PARAMETER];
        if ((!paramRight.IsArray()) || paramRight.Size() == 0) {
            return NULL;
        }
        return createTermQuery(paramLeft[0], paramRight[0], op);
    } else if (leftIsCast) {
        const SimpleValue &param = left[SQL_CONDITION_PARAMETER];
        if ((!param.IsArray()) || param.Size() == 0) {
            return NULL;
        }
        return createTermQuery(param[0], right, op);
    } else if (rightIsCast) {
        const SimpleValue &param = right[SQL_CONDITION_PARAMETER];
        if ((!param.IsArray()) || param.Size() == 0) {
            return NULL;
        }
        return createTermQuery(left, param[0], op);
    }
    return createTermQuery(left, right, op);
}

Query *Ha3ScanConditionVisitor::toMatchQuery(const SimpleValue &condition) {
    if (!condition.IsObject() || !condition.HasMember(SQL_CONDITION_OPERATOR)
        || !condition.HasMember(SQL_CONDITION_PARAMETER))
    {
        return NULL;
    }
    string op(condition[SQL_CONDITION_OPERATOR].GetString());
    if (op != SQL_UDF_MATCH_OP) {
        return NULL;
    }
    const SimpleValue &param = condition[SQL_CONDITION_PARAMETER];
    if ((!param.IsArray()) || param.Size() == 0) {
        return NULL;
    }
    string queryStr;
    QueryOperator defaultOp = _param.queryInfo->getDefaultOperator();
    string defaultIndex = _param.queryInfo->getDefaultIndexName();
    set<string> noTokenIndexs;
    map<string, string> indexAnalyzerMap;
    string globalAnalyzer;
    bool tokenQuery = true;
    bool removeStopWords = true;
    if (param.Size() == 1) {
        queryStr = param[0].GetString();
    } else if (param.Size() == 2) {
        if (SqlJsonUtil::isColumn(param[0])) {
            defaultIndex = SqlJsonUtil::getColumnName(param[0]);
        } else {
            defaultIndex = param[0].GetString();
        }
        queryStr = param[1].GetString();
    } else {
        if (SqlJsonUtil::isColumn(param[0])) {
            defaultIndex = SqlJsonUtil::getColumnName(param[0]);
        } else {
            defaultIndex = param[0].GetString();
        }
        queryStr = param[1].GetString();
        string defaultOpStr;
        parseKvPairInfo(param[2].GetString(), indexAnalyzerMap, globalAnalyzer,
                        defaultOpStr, noTokenIndexs, tokenQuery, removeStopWords);
        if (defaultOpStr == "AND" || defaultOpStr == "and") {
            defaultOp = OP_AND;
        } else if (defaultOpStr == "OR" || defaultOpStr == "or") {
            defaultOp = OP_OR;
        }
    }
    Term term;
    term.setIndexName(defaultIndex.c_str());
    term.setWord(queryStr.c_str());
    Query *query = new TermQuery(term, "");
    return postProcessQuery(tokenQuery, removeStopWords, defaultOp,
                            queryStr, globalAnalyzer, noTokenIndexs,
                            indexAnalyzerMap, query);
}

Query *Ha3ScanConditionVisitor::toQuery(const SimpleValue &condition) {
    if (!condition.IsObject() || !condition.HasMember(SQL_CONDITION_OPERATOR)
        || !condition.HasMember(SQL_CONDITION_PARAMETER))
    {
        return NULL;
    }
    string op(condition[SQL_CONDITION_OPERATOR].GetString());
    if (op != SQL_UDF_QUERY_OP) {
        return NULL;
    }
    const SimpleValue &param = condition[SQL_CONDITION_PARAMETER];
    if ((!param.IsArray())) {
        return NULL;
    }
    string queryStr;
    QueryOperator defaultOp = _param.queryInfo->getDefaultOperator();
    string defaultIndex = _param.queryInfo->getDefaultIndexName();
    set<string> noTokenIndexs;
    string globalAnalyzer;
    map<string, string> indexAnalyzerMap;
    bool tokenQuery = true;
    bool removeStopWords = true;
    if (param.Size() == 1) {
        queryStr = param[0].GetString();
    } else if (param.Size() == 2) {
        if (SqlJsonUtil::isColumn(param[0])) {
            defaultIndex = SqlJsonUtil::getColumnName(param[0]);
        } else {
            defaultIndex = param[0].GetString();
        }
        queryStr = param[1].GetString();
    } else {
        if (SqlJsonUtil::isColumn(param[0])) {
            defaultIndex = SqlJsonUtil::getColumnName(param[0]);
        } else {
            defaultIndex = param[0].GetString();
        }
        queryStr = param[1].GetString();
        string defaultOpStr;
        parseKvPairInfo(param[2].GetString(), indexAnalyzerMap, globalAnalyzer, defaultOpStr,
                        noTokenIndexs, tokenQuery, removeStopWords);
        if (defaultOpStr == "AND" || defaultOpStr == "and") {
            defaultOp = OP_AND;
        } else if (defaultOpStr == "OR" || defaultOpStr == "or") {
            defaultOp = OP_OR;
        }
    }
    queryparser::QueryParser queryParser(defaultIndex.c_str(), defaultOp, false);
    SQL_LOG(TRACE2, "QueryText: [%s]", queryStr.c_str());
    unique_ptr<queryparser::ParserContext> context(queryParser.parse(queryStr.c_str()));
    if (queryparser::ParserContext::OK != context->getStatus()){
        SQL_LOG(WARN, "parse query failed: [%s]", queryStr.c_str());
        return NULL;
    }

    vector<Query*> querys = context->stealQuerys();
    return postProcessQuery(tokenQuery || _param.analyzerFactory,
                            removeStopWords, defaultOp,
                            queryStr, globalAnalyzer, noTokenIndexs,
                            indexAnalyzerMap, reserveFirstQuery(querys));
}

Query* Ha3ScanConditionVisitor::postProcessQuery(bool tokenQuery,
        bool removeStopWords, QueryOperator op,
        const string &queryStr,
        const string &globalAnalyzer,
        const set<string> &noTokenIndexs,
        const map<string, string> &indexAnalyzerMap,
        Query *query) {

    if (tokenQuery) {
        Query *tQuery = tokenizeQuery(query, op, noTokenIndexs,
                globalAnalyzer, indexAnalyzerMap);
        DELETE_AND_SET_NULL(query);
        query = tQuery;
    }
    if (removeStopWords) {
        Query *stopWordQuery = cleanStopWords(query);
        if (stopWordQuery != NULL) {
            DELETE_AND_SET_NULL(query);
            query = stopWordQuery;
        }
    }
    if (!validateQuery(query, _param.indexInfos)) {
        SQL_LOG(WARN, "validate query failed: [%s]", queryStr.c_str());
        DELETE_AND_SET_NULL(query);
        return NULL;
    }
    return query;
}

Query* Ha3ScanConditionVisitor::reserveFirstQuery(vector<Query*> &querys) {
    Query *query = NULL;
    if (querys.size() == 0) {
        return NULL;
    } else {
        query = querys[0];
        for (size_t i = 1; i < querys.size(); i++) {
            delete querys[i];
        }
    }
    return query;
}

bool Ha3ScanConditionVisitor::validateQuery(Query* query,
        const suez::turing::IndexInfos *indexInfos)
{
    if (!query) {
        return false;
    }
    qrs::QueryValidator queryValidator(indexInfos);
    query->accept(&queryValidator);
    auto errorCode = queryValidator.getErrorCode();
    if (errorCode != ERROR_NONE) {
        SQL_LOG(WARN, "validate query failed, error msg [%s]",
                queryValidator.getErrorMsg().c_str());
        return false;
    }
    return true;
}

Query *Ha3ScanConditionVisitor::udfToQuery(const SimpleValue &condition) {
    if (!condition.IsObject() || !condition.HasMember(SQL_CONDITION_OPERATOR)
        || !condition.HasMember(SQL_CONDITION_PARAMETER))
    {
        return NULL;
    }
    string op(condition[SQL_CONDITION_OPERATOR].GetString());
    if (op != SQL_UDF_CONTAIN_OP && op != SQL_UDF_HA_IN_OP) {
        return NULL;
    }
    const SimpleValue &param = condition[SQL_CONDITION_PARAMETER];
    if (!param.IsArray() || param.Size() < 2 || param.Size() > 3) {
        return NULL;
    }
    if (!SqlJsonUtil::isColumn(param[0])) {
        return NULL;
    }
    string columnName = SqlJsonUtil::getColumnName(param[0]);
    auto iter = _param.indexInfo->find(columnName);
    if (iter == _param.indexInfo->end()) {
        return NULL;
    }
    const string &indexName = iter->second.name;
    const string &indexType = iter->second.type;
    string queryTerms = param[1].GetString();
    string sepStr = "|";
    if (param.Size() == 3) {
        sepStr = param[2].GetString();
    }
    const vector<string> &termVec = StringUtil::split(queryTerms, sepStr, true);
    if (termVec.empty()) {
        return NULL;
    }
    unique_ptr<MultiTermQuery> query(new MultiTermQuery("", OP_OR));
    if (indexType == "number") {
        for (auto termStr : termVec) {
            int64_t numVal = 0;
            if (StringUtil::numberFromString(termStr, numVal)) {
                TermPtr term(new NumberTerm(numVal, indexName.c_str(), RequiredFields()));
                query->addTerm(term);
            } else {
                return NULL;
            }
        }
    } else {
        for (auto termStr : termVec) {
            TermPtr term(new Term(termStr, indexName.c_str(), RequiredFields()));
            query->addTerm(term);
        }
    }
    return query.release();
}

Query *Ha3ScanConditionVisitor::spToQuery(const SimpleValue &condition) {
    if (!condition.IsObject() || !condition.HasMember(SQL_CONDITION_OPERATOR)
        || !condition.HasMember(SQL_CONDITION_PARAMETER))
    {
        return NULL;
    }
    string op(condition[SQL_CONDITION_OPERATOR].GetString());
    if (op != SQL_UDF_SP_QUERY_MATCH_OP) {
        return NULL;
    }
    const SimpleValue &param = condition[SQL_CONDITION_PARAMETER];
    if ((!param.IsArray())) {
        return NULL;
    }

    string queryStr = param[0].GetString();
    QueryOperator defaultOp = _param.queryInfo->getDefaultOperator();
    string defaultIndex = _param.queryInfo->getDefaultIndexName();
    set<string> noTokenIndexs;
    string globalAnalyzer;
    map<string, string> indexAnalyzerMap;
    bool tokenQuery = true;
    bool removeStopWords = true;

    SPQueryUdf queryParser(defaultIndex.c_str());
    SQL_LOG(TRACE2, "QueryText: [%s]", queryStr.c_str());
    unique_ptr<queryparser::ParserContext> context(queryParser.parse(queryStr.c_str()));
    if (queryparser::ParserContext::OK != context->getStatus()){
        SQL_LOG(WARN, "parse query failed: [%s]", queryStr.c_str());
        return NULL;
    }

    vector<Query*> querys = context->stealQuerys();
    return postProcessQuery(tokenQuery,
                            removeStopWords, defaultOp,
                            queryStr, globalAnalyzer, noTokenIndexs,
                            indexAnalyzerMap, reserveFirstQuery(querys));
}

Query *Ha3ScanConditionVisitor::createTermQuery(const SimpleValue &left,
        const SimpleValue &right, const string &op)
{
    if ((!left.IsNumber() && !left.IsString()) || (!right.IsNumber() && !right.IsString())) {
        return NULL;
    }
    bool leftHasColumn = SqlJsonUtil::isColumn(left);
    bool rightHasColumn = SqlJsonUtil::isColumn(right);
    if (leftHasColumn && rightHasColumn) {
        return NULL;
    } else if (leftHasColumn) {
        return doCreateTermQuery(left, right, op);
    } else if (rightHasColumn) {
        return doCreateTermQuery(right, left, op);
    } else {
        return NULL;
    }
}

Query *Ha3ScanConditionVisitor::doCreateTermQuery(const SimpleValue &attr,
        const SimpleValue &value,
        const string &op)
{
    string attrName = SqlJsonUtil::getColumnName(attr);
    auto iter = _param.indexInfo->find(attrName);
    if (iter == _param.indexInfo->end()) {
        return NULL;
    }
    const string &indexName = iter->second.name;
    const string &indexType = iter->second.type;
    if (indexType == "number") {
        if (op != SQL_EQUAL_OP) {
            return NULL;
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
                return NULL;
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
        }
    }
    return NULL;
}

Query *Ha3ScanConditionVisitor::tokenizeQuery(common::Query*query, QueryOperator qp,
        const std::set<std::string> &noTokenIndexes,
        const std::string &globalAnalyzer,
        const std::map<std::string, std::string> &indexAnalyzerMap)
{
    if (query == NULL) {
        return NULL;
    }
    qrs::QueryTokenizer queryTokenizer(_param.analyzerFactory);
    queryTokenizer.setNoTokenizeIndexes(noTokenIndexes);
    queryTokenizer.setGlobalAnalyzerName(globalAnalyzer);
    queryTokenizer.setIndexAnalyzerNameMap(indexAnalyzerMap);
    SQL_LOG(DEBUG, "before tokenize query:%s", query->toString().c_str());
    if (!queryTokenizer.tokenizeQuery(query, _param.indexInfos, qp)) {
        auto errorResult = queryTokenizer.getErrorResult();
        SQL_LOG(WARN, "tokenize query failed [%s], detail: %s",
                query->toString().c_str(),
                errorResult.getErrorDescription().c_str());
        return NULL;
    }
    common::Query *tokenizedQuery = queryTokenizer.stealQuery();
    if (tokenizedQuery) {
        SQL_LOG(DEBUG, "tokenized query:%s", tokenizedQuery->toString().c_str());
    }
    return tokenizedQuery;
}

Query *Ha3ScanConditionVisitor::cleanStopWords(common::Query*query) {
    if (query == NULL) {
        return NULL;
    }
    qrs::StopWordsCleaner stopWordsCleaner;
    SQL_LOG(DEBUG, "tokenized query:%s", query->toString().c_str());
    bool ret = stopWordsCleaner.clean(query);
    if (!ret) {
        SQL_LOG(WARN, "clean query failed [%s]", haErrorCodeToString(
                        stopWordsCleaner.getErrorCode()).c_str());
        return NULL;
    }
    return stopWordsCleaner.stealQuery();
}

bool Ha3ScanConditionVisitor::isMatchUdf(const SimpleValue &simpleVal) {
    if (!ExprUtil::isUdf(simpleVal)) {
        return false;
    }
    string op(simpleVal[SQL_CONDITION_OPERATOR].GetString());
    return op == SQL_UDF_MATCH_OP;
}

bool Ha3ScanConditionVisitor::isQueryUdf(const SimpleValue &simpleVal) {
    if (!ExprUtil::isUdf(simpleVal)) {
        return false;
    }
    string op(simpleVal[SQL_CONDITION_OPERATOR].GetString());
    return op == SQL_UDF_QUERY_OP ;
}

bool Ha3ScanConditionVisitor::isSpQueryMatchUdf(const SimpleValue &simpleVal) {
    if (!ExprUtil::isUdf(simpleVal)) {
        return false;
    }
    string op(simpleVal[SQL_CONDITION_OPERATOR].GetString());
    return op == SQL_UDF_SP_QUERY_MATCH_OP ;
}

void Ha3ScanConditionVisitor::parseKvPairInfo(const string &kvStr,
        map<string, string> &indexAnalyzerMap,
        string &globalAnalyzerName, string &defaultOpStr, set<string> &noTokenIndexes,
        bool &tokenizeQuery, bool &removeStopWords)
{
    map<string, string> kvPairs;
    vector<string> kvVec;
    StringUtil::split(kvVec, kvStr, SQL_FUNC_KVPAIR_SEP);
    vector<string> valueVec;
    for (auto kv : kvVec) {
        valueVec.clear();
        StringUtil::split(valueVec, kv, SQL_FUNC_KVPAIR_KV_SEP);
        if (valueVec.size() != 2) {
            continue;
        }
        kvPairs[valueVec[0]] = valueVec[1];
    }
    globalAnalyzerName = kvPairs["global_analyzer"];
    string specialAnalyzerNames = kvPairs["specific_index_analyzer"];
        if (!specialAnalyzerNames.empty()) {
            valueVec.clear();
            StringUtil::split(valueVec, specialAnalyzerNames, SQL_FUNC_KVPAIR_VALUE_SEP);
            vector<string> valueValueVec;
            for (auto kv : valueVec) {
                valueValueVec.clear();
                StringUtil::split(valueValueVec, kv, SQL_FUNC_KVPAIR_VALUE_VALUE_SEP);
                if (valueValueVec.size() != 2) {
                    continue;
                }
                indexAnalyzerMap[valueValueVec[0]] = valueValueVec[1];
            }
    }
    string noTokenStr = kvPairs["no_token_indexes"];
    if (!noTokenStr.empty()) {
        valueVec.clear();
        StringUtil::split(valueVec, noTokenStr, SQL_FUNC_KVPAIR_VALUE_SEP);
        noTokenIndexes.insert(valueVec.begin(), valueVec.end());
    }
    string tokenizeQueryStr = kvPairs["tokenize_query"];
    if (!tokenizeQueryStr.empty()) {
        StringUtil::parseTrueFalse(tokenizeQueryStr, tokenizeQuery);
    }
    string removeStopWordsStr = kvPairs["remove_stopwords"];
    if (!removeStopWordsStr.empty()) {
        StringUtil::parseTrueFalse(removeStopWordsStr, removeStopWords);
    }
    defaultOpStr = kvPairs["default_op"];
}

QueryExecutor *Ha3ScanConditionVisitor::createQueryExecutor(const Query *query,
        IndexPartitionReaderWrapperPtr &indexPartitionReader,
        const std::string &mainTableName,
        autil::mem_pool::Pool *pool,
        TimeoutTerminator *timeoutTerminator,
        LayerMeta *layerMeta)
{
    if (query == NULL) {
        return NULL;
    }
    QueryExecutor *queryExecutor = NULL;
    ErrorCode errorCode = ERROR_NONE;
    std::string errorMsg;
    try {
        QueryExecutorCreator qeCreator(NULL, indexPartitionReader.get(),
                pool, timeoutTerminator, layerMeta);
        query->accept(&qeCreator);
        queryExecutor = qeCreator.stealQuery();
        if (queryExecutor->isEmpty()) {
            POOL_DELETE_CLASS(queryExecutor);
            queryExecutor = NULL;
        }
    } catch (const IE_NAMESPACE(misc)::ExceptionBase &e) {
        SQL_LOG(WARN, "lookup exception: %s", e.what());
        errorMsg = "ExceptionBase: " + e.GetClassName();
        errorCode = ERROR_SEARCH_LOOKUP;
        if (e.GetClassName() == "FileIOException") {
            errorCode = ERROR_SEARCH_LOOKUP_FILEIO_ERROR;
        }
    } catch (const std::exception &e) {
        errorMsg = e.what();
        errorCode = ERROR_SEARCH_LOOKUP;
    } catch (...) {
        errorMsg = "Unknown Exception";
        errorCode = ERROR_SEARCH_LOOKUP;
    }
    if (errorCode != ERROR_NONE) {
        SQL_LOG(WARN, "Create query executor failed, query [%s], Exception: [%s]",
                        query->toString().c_str(), errorMsg.c_str());
    }
    return queryExecutor;
}


END_HA3_NAMESPACE(sql);
