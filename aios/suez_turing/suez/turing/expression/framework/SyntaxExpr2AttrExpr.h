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

#include <string>

#include "autil/Log.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/framework/AttributeExpressionPool.h"
#include "suez/turing/expression/syntax/SyntaxExprVisitor.h"

namespace autil {
namespace mem_pool {
class Pool;
} // namespace mem_pool
} // namespace autil

namespace suez {
namespace turing {
class AddSyntaxExpr;
class AndSyntaxExpr;
class AtomicSyntaxExpr;
class AttributeExpression;
class AttributeExpressionFactory;
class BinarySyntaxExpr;
class BitAndSyntaxExpr;
class BitOrSyntaxExpr;
class BitXorSyntaxExpr;
class DivSyntaxExpr;
class EqualSyntaxExpr;
class FuncSyntaxExpr;
class FunctionManager;
class GreaterEqualSyntaxExpr;
class GreaterSyntaxExpr;
class LessEqualSyntaxExpr;
class LessSyntaxExpr;
class LogSyntaxExpr;
class MinusSyntaxExpr;
class MulSyntaxExpr;
class MultiSyntaxExpr;
class NotEqualSyntaxExpr;
class OrSyntaxExpr;
class PowerSyntaxExpr;
class RankSyntaxExpr;
class SyntaxExpr;
class VirtualAttrConvertor;

class SyntaxExpr2AttrExpr : public SyntaxExprVisitor {
public:
    SyntaxExpr2AttrExpr(AttributeExpressionFactory *attrFactory,
                        FunctionManager *funcManager,
                        AttributeExpressionPool *exprPool,
                        VirtualAttrConvertor *convertor,
                        autil::mem_pool::Pool *pool);

    virtual ~SyntaxExpr2AttrExpr();

public:
    void visitAtomicSyntaxExpr(const AtomicSyntaxExpr *syntaxExpr) override;
    void visitAddSyntaxExpr(const AddSyntaxExpr *syntaxExpr) override;
    void visitMinusSyntaxExpr(const MinusSyntaxExpr *syntaxExpr) override;
    void visitMulSyntaxExpr(const MulSyntaxExpr *syntaxExpr) override;
    void visitDivSyntaxExpr(const DivSyntaxExpr *syntaxExpr) override;
    void visitPowerSyntaxExpr(const PowerSyntaxExpr *syntaxExpr) override;
    void visitLogSyntaxExpr(const LogSyntaxExpr *syntaxExpr) override;
    void visitEqualSyntaxExpr(const EqualSyntaxExpr *syntaxExpr) override;
    void visitNotEqualSyntaxExpr(const NotEqualSyntaxExpr *syntaxExpr) override;
    void visitLessSyntaxExpr(const LessSyntaxExpr *syntaxExpr) override;
    void visitGreaterSyntaxExpr(const GreaterSyntaxExpr *syntaxExpr) override;
    void visitLessEqualSyntaxExpr(const LessEqualSyntaxExpr *syntaxExpr) override;
    void visitGreaterEqualSyntaxExpr(const GreaterEqualSyntaxExpr *syntaxExpr) override;
    void visitAndSyntaxExpr(const AndSyntaxExpr *syntaxExpr) override;
    void visitOrSyntaxExpr(const OrSyntaxExpr *syntaxExpr) override;
    void visitFuncSyntaxExpr(const FuncSyntaxExpr *syntaxExpr) override;

    void visitBitAndSyntaxExpr(const BitAndSyntaxExpr *syntaxExpr) override;
    void visitBitXorSyntaxExpr(const BitXorSyntaxExpr *syntaxExpr) override;
    void visitBitOrSyntaxExpr(const BitOrSyntaxExpr *syntaxExpr) override;
    void visitRankSyntaxExpr(const RankSyntaxExpr *syntaxExpr) override;
    void visitMultiSyntaxExpr(const MultiSyntaxExpr *syntaxExpr) override;

    AttributeExpression *stealAttrExpr();

    void addNeedDeleteExpr(AttributeExpression *attrExpr) { _exprPool->addNeedDeleteExpr(attrExpr); }

public:
    AttributeExpression *createAtomicExpr(const std::string &attrName);
    virtual AttributeExpression *
    createAtomicExpr(const std::string &attrName, const std::string &prefixName, const std::string &completeAttrName);

private:
    template <template <class T> class BinaryOperator>
    void visitBinarySyntaxExpr(const BinarySyntaxExpr *syntaxExpr);

    template <template <class T> class BinaryOperator>
    void visitMultiEqualBinarySyntaxExpr(const BinarySyntaxExpr *syntaxExpr);

    AttributeExpression *createConstAttrExpr(const AtomicSyntaxExpr *atomicSyntaxExpr, VariableType type) const;
    AttributeExpression *createConstStringAttrExpr(const std::string &constValue) const;

    AttributeExpression *createArgAttrExpr(const AtomicSyntaxExpr *atomicSyntaxExpr, VariableType type) const;
    AttributeExpression *createArgStringAttrExpr(const std::string &argValue) const;

    bool visitChildrenExprs(const BinarySyntaxExpr *syntaxExpr,
                            AttributeExpression *&leftAttrExpr,
                            AttributeExpression *&rightAttrExpr);

protected:
    bool tryGetFromPool(const SyntaxExpr *syntaxExpr);

protected:
    AttributeExpressionFactory *_attrFactory;
    FunctionManager *_funcManager;
    AttributeExpression *_attrExpr;
    AttributeExpressionPool *_exprPool;
    VirtualAttrConvertor *_convertor;
    autil::mem_pool::Pool *_pool;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace turing
} // namespace suez
