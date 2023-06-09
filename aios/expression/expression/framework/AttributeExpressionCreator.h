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
#ifndef ISEARCH_EXPRESSION_ATTRIBUTEEXPRESSIONCREATOR_H
#define ISEARCH_EXPRESSION_ATTRIBUTEEXPRESSIONCREATOR_H

#include "expression/common.h"
#include "expression/framework/AttributeExpression.h"
#include "expression/framework/AttributeExpressionTyped.h"
#include "expression/framework/ContextAttributeExpression.h"
#include "expression/function/FunctionConfig.h"
#include "expression/util/ExpressionGroupNameAssigner.h"
#include "expression/common/SessionResource.h"
#include "expression/common/ErrorDefine.h"
#include "autil/mem_pool/Pool.h"
#include "matchdoc/MatchDocAllocator.h"

DECLARE_RESOURCE_READER

//namespace matchdoc {
//class MatchDocAllocator;
//typedef std::shared_ptr<MatchDocAllocator> MatchDocAllocatorPtr;
//}

namespace expression {

class AtomicAttributeExpressionCreatorBase;
class FunctionExpressionCreator;
class SyntaxExpr;
class AttributeExpressionPool;
class FunctionInterfaceManager;
class VirtualAttributeContainer;
class AnyTypeExpression;
class JoinDocIdConverterCreator;
typedef std::shared_ptr<JoinDocIdConverterCreator> JoinDocIdConverterCreatorPtr;

class AttributeExpressionCreator
{
public:
    AttributeExpressionCreator(
        FunctionInterfaceManager *functionManager,
        bool useSub,
        std::function<AtomicAttributeExpressionCreatorBase *(AttributeExpressionPool *, autil::mem_pool::Pool *, bool)>
            creator,
        autil::mem_pool::Pool *pool = NULL);
    virtual ~AttributeExpressionCreator();
private:
    AttributeExpressionCreator(const AttributeExpressionCreator &);
    AttributeExpressionCreator& operator=(const AttributeExpressionCreator &);
public:
    bool registerVirtualAttribute(const std::string &name, const std::string &exprStr);
    bool registerVirtualAttribute(const std::string &name, SyntaxExpr *syntaxExpr);

    template <typename T>
    bool registerContextExpr(const std::string &name,
                             uint32_t docCount,
                             docid_t *docIds,
                             T *values)
    {
        typedef ContextAttributeExpression<T> ContextAttributeExpressionTyped;
        ContextAttributeExpressionTyped *expr = NULL;
        AttributeExpression* attrExpr = _exprPool->tryGetAttributeExpression(name);
        if (attrExpr)
        {
            expr = dynamic_cast<ContextAttributeExpressionTyped*>(attrExpr);
        }
        else
        {
            expr = POOL_NEW_CLASS(_pool, ContextAttributeExpressionTyped, name);
            std::vector<expressionid_t> exprIds;
            exprIds.push_back((expressionid_t)_exprPool->getAttributeExpressionCount()); 
            expr->init(exprIds, _exprPool); 
            _exprPool->addPair(name, expr); 
            
        }
        if (expr == NULL)
        {
            AUTIL_LOG(WARN, "register const expression [%s] failed", name.c_str());
            return false;
        }
        expr->setValue(docCount, docIds, values);
        return true;
    }

    AttributeExpression *tryGetFromContextExpr(const std::string &name) const;
    AttributeExpression *create(const std::string &syntaxStr);
    AttributeExpression *create(const SyntaxExpr *syntaxExpr, bool tryPool = true);
    AttributeExpression *getAttributeExpression(const std::string &syntaxStr) const;
    bool beginRequest(SessionResource *resouce);
    void endRequest();
    void reset();

    void batchEvaluate(matchdoc::MatchDoc *matchDocs, uint32_t docCount,
                       const matchdoc::MatchDocAllocatorPtr& allocator);
    void addNeedEvalAttributeExpression(AttributeExpression *expr);

    autil::mem_pool::Pool *getPool() { return _pool; }

    ErrorCode getErrorCode() const {
        return _errorCode;
    }
    const std::string& getErrorMsg() const {
        return _errorMsg;
    }
    const std::vector<AttributeExpression*>& getNeedEvalExpressions() const {
        return _needEvalExprs;
    }

    void getExpressionsByType(ExpressionType type,
                              std::vector<AttributeExpression*>& exprVec) const;

    void setDefaultSerializeLevel(uint8_t serializeLevel) {
        _defaultSerializeLevel = serializeLevel;
    }

    bool hasValidExpression() const {
        return !_curSessionExprs.empty() || !_needEvalExprs.empty();
    }

    bool hasExpresssion() const
    { return _exprPool->getAttributeExpressionCount() > 0; }

    void setEnableSyntaxParseOpt(bool enableSyntaxParseOpt) {
        _enableSyntaxParseOpt = enableSyntaxParseOpt;
    }
    
public:
    // for test
    void evaluateAllExpressions(matchdoc::MatchDoc matchDoc,
                                const matchdoc::MatchDocAllocatorPtr& allocator);

private:
    SyntaxExpr *parseSyntax(const std::string &syntaxStr) const;


    bool allocateReference(const std::vector<AttributeExpression*>& attrExpr,
                           const matchdoc::MatchDocAllocatorPtr& allocator);

    void addFunctionDependentExpression(AttributeExpression* expr);

    static const size_t POOL_MAX_USE_LIMIT = 20 * 1024 * 1024; // 20M
    
    void ResetPool()
    {
        if (!_isPoolOwner)
        {
            return;
        }
        
        if (_pool->getTotalBytes() > POOL_MAX_USE_LIMIT)
        {
            _pool->release();
        }
        else
        {
            _pool->reset();
        }
    }

    bool getAllExpressionInCurrentSession(
            std::vector<AttributeExpression*>& attrExpr);

    std::string GetTempExprGroupName(const matchdoc::MatchDocAllocatorPtr& matchDocAllocator);
    
private:
    std::function<AtomicAttributeExpressionCreatorBase *(AttributeExpressionPool*, autil::mem_pool::Pool*, bool)> _creator;
    autil::mem_pool::Pool *_pool;
    AttributeExpressionPool *_exprPool;
    AtomicAttributeExpressionCreatorBase *_atomicExprCreator;
    FunctionExpressionCreator *_funcExprCreator;
    VirtualAttributeContainer *_virtualAttributes;
    std::vector<AttributeExpression*> _needEvalExprs;
    std::vector<AttributeExpression*> _curSessionExprs;
    ErrorCode _errorCode;
    std::string _errorMsg;
    uint8_t _defaultSerializeLevel;
    bool _useSub;
    bool _isPoolOwner;
    bool _enableSyntaxParseOpt;

private:
    static const std::string CONTEXT_TABLE_NAME;
    static ExpressionGroupNameAssigner TmpGroupNameAssigner;
    
private:
    friend class AttributeExpressionCreatorTest;
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<AttributeExpressionCreator> AttributeExpressionCreatorPtr;

}

#endif //ISEARCH_EXPRESSION_ATTRIBUTEEXPRESSIONCREATOR_H
