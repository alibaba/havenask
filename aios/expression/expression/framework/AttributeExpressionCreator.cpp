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
#include "expression/framework/AttributeExpressionPool.h"
#include "expression/framework/AttributeExpressionCreator.h"
#include "expression/framework/SyntaxExpr2AttrExpr.h"
#include "expression/framework/AtomicAttributeExpressionCreatorBase.h"
#include "expression/syntax/SyntaxExpr.h"
#include "expression/syntax/SyntaxParser.h"
#include "expression/framework/VirtualAttributeContainer.h"
#include "expression/framework/MultiLayerBottomUpEvaluator.h"
#include "resource_reader/ResourceReader.h"
#include "autil/MultiValueType.h"
#include <tr1/unordered_set>

using namespace autil;
using namespace autil::mem_pool;
using namespace std;
using namespace matchdoc;

namespace expression {
AUTIL_LOG_SETUP(expression, AttributeExpressionCreator);

const string AttributeExpressionCreator::CONTEXT_TABLE_NAME = "context";
ExpressionGroupNameAssigner AttributeExpressionCreator::TmpGroupNameAssigner;

AttributeExpressionCreator::AttributeExpressionCreator(
        FunctionInterfaceManager *funcManager,
        bool useSub, std::function<AtomicAttributeExpressionCreatorBase *(AttributeExpressionPool*, autil::mem_pool::Pool*, bool)> creator, Pool* pool)
    : _creator(creator)
    ,  _pool(pool)
    , _errorCode(ERROR_NONE)
    , _defaultSerializeLevel(SL_PROXY)
    , _useSub(useSub)
    , _isPoolOwner(false)
    , _enableSyntaxParseOpt(false)
{
    if (!_pool)
    {
        _pool = new Pool(128 * 1024);  // 128K
        _isPoolOwner = true;
    }

    _exprPool = new AttributeExpressionPool();
    _atomicExprCreator = _creator(_exprPool, _pool, _useSub);
    assert(_atomicExprCreator);
    _funcExprCreator = NULL;
    if (funcManager != NULL) {
        _funcExprCreator = new FunctionExpressionCreator(_pool, funcManager, this, useSub);
    }
    _virtualAttributes = new VirtualAttributeContainer;
}

AttributeExpressionCreator::~AttributeExpressionCreator() {
    DELETE_AND_SET_NULL(_exprPool);
    DELETE_AND_SET_NULL(_funcExprCreator);
    DELETE_AND_SET_NULL(_atomicExprCreator);
    DELETE_AND_SET_NULL(_virtualAttributes);
    if (_isPoolOwner) {
        DELETE_AND_SET_NULL(_pool);
    }
}

bool AttributeExpressionCreator::registerVirtualAttribute(
        const string &name, const string &exprStr)
{
    if (_virtualAttributes->findVirtualAttribute(name)) {
        AUTIL_LOG(WARN, "regisiter duplicate virtual attribute[%s]", name.c_str());
        return false;
    }
    
    SyntaxExpr *syntaxExpr = SyntaxParser::parseSyntax(exprStr);
    if (syntaxExpr == NULL) {
        AUTIL_LOG(WARN, "parse virtual attribute [%s:%s] failed",
                  name.c_str(), exprStr.c_str());
        return false;
    }

    if (!registerVirtualAttribute(name, syntaxExpr)) {
        delete syntaxExpr;
        return false;
    }
    delete syntaxExpr;
    return true;
}

bool AttributeExpressionCreator::registerVirtualAttribute(
        const string &name, SyntaxExpr *syntaxExpr)
{
    assert(syntaxExpr);
    AttributeExpression *expr = create(syntaxExpr, false);
    if (!expr) {
        AUTIL_LOG(WARN, "register virtual attribute [%s] failed", name.c_str());
        return false;
    }
    return _virtualAttributes->addVirtualAttribute(name, expr);
}

AttributeExpression *AttributeExpressionCreator::getAttributeExpression(
        const std::string &syntaxStr) const {
    return _exprPool->tryGetAttributeExpression(syntaxStr);
}

AttributeExpression* AttributeExpressionCreator::create(const string &syntaxStr) {
    AttributeExpression *expr = tryGetFromContextExpr(syntaxStr);
    if (expr != NULL) {
        _curSessionExprs.push_back(expr);
        addFunctionDependentExpression(expr);
        return expr;
    }

    expr = _exprPool->tryGetAttributeExpression(syntaxStr);
    if (expr != NULL) {
        _curSessionExprs.push_back(expr);
        addFunctionDependentExpression(expr);
        return expr;
    }

    unique_ptr<SyntaxExpr> syntaxExpr(SyntaxParser::parseSyntax(syntaxStr, _enableSyntaxParseOpt));
    if (syntaxExpr.get() == NULL) {
        AUTIL_LOG(WARN, "parse syntax[%s] failed", syntaxStr.c_str());
        return NULL;
    }

    expr = create(syntaxExpr.get(), false);
    if (expr == NULL) {
        AUTIL_LOG(WARN, "create attribute expression from [%s] failed", syntaxStr.c_str());
    } else {
        _exprPool->addPair(syntaxStr, expr);
        AUTIL_LOG(DEBUG, "create attribute expression from [%s] success.", syntaxStr.c_str());
    }
    return expr;
}

AttributeExpression *AttributeExpressionCreator::tryGetFromContextExpr(const string &name) const {
    AttributeExpression* expr = _exprPool->tryGetAttributeExpression(name);
    if (!expr) {
        size_t pos = name.find(":");
        if (pos != string::npos) {
            string tableName = name.substr(0, pos);
            string exprName = name.substr(pos+1);
            if (tableName == CONTEXT_TABLE_NAME) {
                expr = _exprPool->tryGetAttributeExpression(exprName);
            }
        }
    }
    if (expr && expr->getType() == ET_CONTEXT && expr->check()) {
        return expr;
    }
    return NULL;
}

bool AttributeExpressionCreator::beginRequest(SessionResource*resource) {
    if (!resource) {
        return false;
    }
    if (!hasValidExpression())
    {
        return true;
    }
    
    if (_funcExprCreator != NULL) {
        FunctionResource functinResource(*resource, this);
        if (!_funcExprCreator->beginRequest(functinResource)) {
            AUTIL_LOG(WARN, "function begin request failed");
            return false;
        }
    }
    
    if (resource->location == FL_UNKNOWN) {
        return false;
    }
    
    vector<AttributeExpression*> allExprInSession;
    if (!getAllExpressionInCurrentSession(allExprInSession)) {
        return false;
    }

    if (!resource->allocator) {
        AUTIL_LOG(WARN, "MatchDocAllocator should not be NULL");
        return false;
    }

    if (!allocateReference(allExprInSession, resource->allocator)) {
        AUTIL_LOG(WARN, "attribute expression allocate failed");
        return false;
    }

    if (!_atomicExprCreator->beginRequest(allExprInSession, resource))
    {
        AUTIL_LOG(WARN, "init atomic expression failed");
        return false;
    }

    return true;
}

void AttributeExpressionCreator::endRequest() {
    if (_funcExprCreator != NULL) {
        _funcExprCreator->endRequest();
    }
    const vector<AttributeExpression*> &allExpressions = _exprPool->getAllExpressions();
    for (vector<AttributeExpression*>::const_iterator it = allExpressions.begin();
         it != allExpressions.end(); it++)
    {
        (*it)->reset();
    }
    _atomicExprCreator->endRequest();
    _needEvalExprs.clear();
    _curSessionExprs.clear();
}

void AttributeExpressionCreator::reset() {
    assert(_needEvalExprs.empty());
    assert(_curSessionExprs.empty());

    _needEvalExprs.clear();
    _curSessionExprs.clear();

    if (_funcExprCreator != NULL) {
        _funcExprCreator->reset();
    }
    _virtualAttributes->reset();
    DELETE_AND_SET_NULL(_exprPool);
    _exprPool = new AttributeExpressionPool();
    DELETE_AND_SET_NULL(_atomicExprCreator);
    ResetPool();
    _atomicExprCreator = _creator(_exprPool, _pool, _useSub);
}

void AttributeExpressionCreator::batchEvaluate(matchdoc::MatchDoc *matchDocs,
        uint32_t docCount, const matchdoc::MatchDocAllocatorPtr& allocator)
{
    BottomUpEvaluator bottomUpEvaluator;
    assert(allocator);
    bottomUpEvaluator.init(_needEvalExprs, allocator->getSubDocAccessor());
    bottomUpEvaluator.batchEvaluate(matchDocs, docCount);
}

void AttributeExpressionCreator::evaluateAllExpressions(
        matchdoc::MatchDoc matchDoc,
        const matchdoc::MatchDocAllocatorPtr& allocator)
{
    vector<AttributeExpressionVec> exprLayers;
    exprLayers.push_back(getNeedEvalExpressions());
    exprLayers.push_back(_curSessionExprs);
    MultiLayerBottomUpEvaluator evaluator;
    evaluator.init(exprLayers, allocator->getSubDocAccessor());
    evaluator.evaluate(matchDoc);
}

void AttributeExpressionCreator::addNeedEvalAttributeExpression(AttributeExpression *expr) {
    _needEvalExprs.push_back(expr);
}

void AttributeExpressionCreator::getExpressionsByType(
        ExpressionType type, vector<AttributeExpression*>& exprVec) const
{
    const vector<AttributeExpression*> &allExpressions = _exprPool->getAllExpressions();
    for (auto it = allExpressions.begin(); it != allExpressions.end(); it++) {
        if ((*it)->getType() == type) {
            exprVec.push_back(*it);
        }
    }
}

AttributeExpression* AttributeExpressionCreator::create(
        const SyntaxExpr *syntaxExpr, bool tryPool) {
    assert(syntaxExpr);
    if (tryPool) {
        const string& syntaxStr = syntaxExpr->getExprString();
        AttributeExpression *expr = tryGetFromContextExpr(syntaxStr);
        if (expr != NULL) {
            _curSessionExprs.push_back(expr);
            addFunctionDependentExpression(expr);
            return expr;
        }
        expr = _exprPool->tryGetAttributeExpression(syntaxStr);
        if (expr != NULL) {
            _curSessionExprs.push_back(expr);
            addFunctionDependentExpression(expr);
            return expr;
        }
    }
    
    SyntaxExpr2AttrExpr visitor(_atomicExprCreator, _funcExprCreator,
                                _exprPool, _virtualAttributes, _pool);
    syntaxExpr->accept(&visitor);
    AttributeExpression *expr = visitor.stealAttrExpr();
    if (!expr) {
        AUTIL_LOG(WARN, "create attribute expression [%s] fail",
                  syntaxExpr->getExprString().c_str());
        _errorCode = visitor.getErrorCode();
        _errorMsg = visitor.getErrorMsg();
        return expr;
    }
    _curSessionExprs.push_back(expr);
    addFunctionDependentExpression(expr);
    return expr;
}

bool AttributeExpressionCreator::getAllExpressionInCurrentSession(
        vector<AttributeExpression*>& attrExpr)
{
    tr1::unordered_set<expressionid_t> uniqExprIdSet;
    for (size_t i = 0; i < _curSessionExprs.size(); ++i)
    {
        AttributeExpression *expr = _curSessionExprs[i];
        assert(expr);

        const vector<expressionid_t>& exprIds = 
            expr->getDependentExpressionIds();
        uniqExprIdSet.insert(exprIds.begin(), exprIds.end());
    }

    attrExpr.reserve(uniqExprIdSet.size());
    tr1::unordered_set<expressionid_t>::const_iterator iter = uniqExprIdSet.begin(); 
    for (; iter != uniqExprIdSet.end(); ++iter)
    {
        assert(_exprPool);
        AttributeExpression* expr = _exprPool->tryGetAttributeExpression(*iter);
        if (expr == NULL)
        {
            AUTIL_LOG(ERROR, "get attribute expression[id:%d] failed", *iter);
            return false;
        }
        attrExpr.push_back(expr);
    }
    return true;
}

bool AttributeExpressionCreator::allocateReference(
        const std::vector<AttributeExpression*>& attrExpr,
        const matchdoc::MatchDocAllocatorPtr& allocator)
{
    string tmpExprGroupName = GetTempExprGroupName(allocator);
    assert(allocator);
    for (size_t i = 0; i < attrExpr.size(); ++i) {
        if (attrExpr[i]->getReferenceBase()) {
            continue;
        }

        if (!attrExpr[i]->allocate(allocator.get(),
                        tmpExprGroupName, _defaultSerializeLevel)) {
            AUTIL_LOG(WARN, "attribute expression[%s] allocate failed",
                      attrExpr[i]->getExprString().c_str());
            return false;
        }
    }
    return true;
}

void AttributeExpressionCreator::addFunctionDependentExpression(
        AttributeExpression* expr)
{
    if (!expr || !_funcExprCreator ||
        !_funcExprCreator->needCollectDependentExpression())
    {
        return;
    }
    _funcExprCreator->addDependentExpression(expr);
}

string AttributeExpressionCreator::GetTempExprGroupName(
        const MatchDocAllocatorPtr& matchDocAllocator)
{
    assert(matchDocAllocator);
    const MatchDocAllocator::FieldGroups& fieldGroups = matchDocAllocator->getFieldGroups();
    size_t tmpGroupId = 0;
    for (;;tmpGroupId++) {
        string groupName = TmpGroupNameAssigner.getGroupName(tmpGroupId);
        if (fieldGroups.find(groupName) == fieldGroups.end()) {
            return groupName;
        }
        AUTIL_LOG(DEBUG, "tmp group [%s] for AllocateExpression already in use, try next one",
                  groupName.c_str());
    }
    return "";
}

}
