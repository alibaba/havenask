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

#include <assert.h>
#include <string>
#include <vector>

#include "autil/Log.h"
#include "suez/turing/expression/framework/AttributeExpression.h"
#include "suez/turing/expression/framework/AttributeExpressionPool.h"
#include "suez/turing/expression/syntax/SyntaxExpr.h"
#include "suez/turing/expression/syntax/SyntaxExprVisitor.h"
#include "suez/turing/expression/util/VirtualAttribute.h"

namespace suez {
namespace turing {
class AddSyntaxExpr;
class AndSyntaxExpr;
class AtomicSyntaxExpr;
class AttributeInfos;
class BinarySyntaxExpr;
class BitAndSyntaxExpr;
class BitOrSyntaxExpr;
class BitXorSyntaxExpr;
class DivSyntaxExpr;
class EqualSyntaxExpr;
class FuncSyntaxExpr;
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

class ReCalcFetcherVisitor : public SyntaxExprVisitor {
public:
    ReCalcFetcherVisitor(std::unordered_map<std::string, int32_t> *cnt,
                         std::unordered_map<std::string, bool> *needOptimize,
                         AttributeExpressionPool *exprPool);
    ~ReCalcFetcherVisitor();

public:
    void fetch(const SyntaxExpr *expr, const std::string &fatherExprStr = "");

    ExpressionVector getAttrExprs() { return _attrExprs; }

    virtual void visitAtomicSyntaxExpr(const AtomicSyntaxExpr *syntaxExpr);
    virtual void visitAddSyntaxExpr(const AddSyntaxExpr *syntaxExpr);
    virtual void visitMinusSyntaxExpr(const MinusSyntaxExpr *syntaxExpr);
    virtual void visitMulSyntaxExpr(const MulSyntaxExpr *syntaxExpr);
    virtual void visitDivSyntaxExpr(const DivSyntaxExpr *syntaxExpr);
    virtual void visitPowerSyntaxExpr(const PowerSyntaxExpr *syntaxExpr);
    virtual void visitLogSyntaxExpr(const LogSyntaxExpr *syntaxExpr);
    virtual void visitEqualSyntaxExpr(const EqualSyntaxExpr *syntaxExpr);
    virtual void visitNotEqualSyntaxExpr(const NotEqualSyntaxExpr *syntaxExpr);
    virtual void visitLessSyntaxExpr(const LessSyntaxExpr *syntaxExpr);
    virtual void visitGreaterSyntaxExpr(const GreaterSyntaxExpr *syntaxExpr);
    virtual void visitLessEqualSyntaxExpr(const LessEqualSyntaxExpr *syntaxExpr);
    virtual void visitGreaterEqualSyntaxExpr(const GreaterEqualSyntaxExpr *syntaxExpr);
    virtual void visitAndSyntaxExpr(const AndSyntaxExpr *syntaxExpr);
    virtual void visitOrSyntaxExpr(const OrSyntaxExpr *syntaxExpr);
    virtual void visitRankSyntaxExpr(const RankSyntaxExpr *syntaxExpr);
    virtual void visitFuncSyntaxExpr(const FuncSyntaxExpr *syntaxExpr);
    virtual void visitBitAndSyntaxExpr(const BitAndSyntaxExpr *syntaxExpr);
    virtual void visitBitXorSyntaxExpr(const BitXorSyntaxExpr *syntaxExpr);
    virtual void visitBitOrSyntaxExpr(const BitOrSyntaxExpr *syntaxExpr);
    virtual void visitMultiSyntaxExpr(const MultiSyntaxExpr *syntaxExpr);

private:
    void visitBinarySyntaxExpr(const BinarySyntaxExpr *syntaxExpr);
    void visitLeftSyntaxExpr(const BinarySyntaxExpr *syntaxExpr);

private:
    std::unordered_map<std::string, int32_t> *_cnt;
    std::unordered_map<std::string, bool> *_needOptimize;
    AttributeExpressionPool *_exprPool;
    ExpressionVector _attrExprs;
    std::unordered_set<std::string> _attrExprSet;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace turing
} // namespace suez
