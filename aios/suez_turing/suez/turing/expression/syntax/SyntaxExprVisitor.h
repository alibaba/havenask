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
#include <cassert>

namespace suez {
namespace turing {

class MinusSyntaxExpr;
class AtomicSyntaxExpr;
class AddSyntaxExpr;
class DivSyntaxExpr;
class MulSyntaxExpr;
class PowerSyntaxExpr;
class LogSyntaxExpr;
class EqualSyntaxExpr;
class NotEqualSyntaxExpr;
class LessSyntaxExpr;
class GreaterSyntaxExpr;
class LessEqualSyntaxExpr;
class GreaterEqualSyntaxExpr;
class ConditionalSyntaxExpr;
class AndSyntaxExpr;
class OrSyntaxExpr;
class FuncSyntaxExpr;
class BitAndSyntaxExpr;
class BitXorSyntaxExpr;
class BitOrSyntaxExpr;
class RankSyntaxExpr;
class MultiSyntaxExpr;

class SyntaxExprVisitor {
public:
    SyntaxExprVisitor() {}
    virtual ~SyntaxExprVisitor() {}

public:
    virtual void visitAtomicSyntaxExpr(const AtomicSyntaxExpr *syntaxExpr) = 0;
    virtual void visitAddSyntaxExpr(const AddSyntaxExpr *syntaxExpr) = 0;
    virtual void visitMinusSyntaxExpr(const MinusSyntaxExpr *syntaxExpr) = 0;
    virtual void visitMulSyntaxExpr(const MulSyntaxExpr *syntaxExpr) = 0;
    virtual void visitDivSyntaxExpr(const DivSyntaxExpr *syntaxExpr) = 0;
    virtual void visitPowerSyntaxExpr(const PowerSyntaxExpr *syntaxExpr) = 0;
    virtual void visitLogSyntaxExpr(const LogSyntaxExpr *syntaxExpr) = 0;
    virtual void visitEqualSyntaxExpr(const EqualSyntaxExpr *syntaxExpr) = 0;
    virtual void visitNotEqualSyntaxExpr(const NotEqualSyntaxExpr *syntaxExpr) = 0;
    virtual void visitLessSyntaxExpr(const LessSyntaxExpr *syntaxExpr) = 0;
    virtual void visitGreaterSyntaxExpr(const GreaterSyntaxExpr *syntaxExpr) = 0;
    virtual void visitLessEqualSyntaxExpr(const LessEqualSyntaxExpr *syntaxExpr) = 0;
    virtual void visitGreaterEqualSyntaxExpr(const GreaterEqualSyntaxExpr *syntaxExpr) = 0;
    virtual void visitAndSyntaxExpr(const AndSyntaxExpr *syntaxExpr) = 0;
    virtual void visitOrSyntaxExpr(const OrSyntaxExpr *syntaxExpr) = 0;
    virtual void visitFuncSyntaxExpr(const FuncSyntaxExpr *syntaxExpr) = 0;
    virtual void visitBitAndSyntaxExpr(const BitAndSyntaxExpr *syntaxExpr) = 0;
    virtual void visitBitXorSyntaxExpr(const BitXorSyntaxExpr *syntaxExpr) = 0;
    virtual void visitBitOrSyntaxExpr(const BitOrSyntaxExpr *syntaxExpr) = 0;
    virtual void visitRankSyntaxExpr(const RankSyntaxExpr *syntaxExpr) = 0;
    virtual void visitMultiSyntaxExpr(const MultiSyntaxExpr *syntaxExpr) { assert(false); }
    virtual void visitConditionalSyntaxExpr(const ConditionalSyntaxExpr *syntaxExpr) { assert(false); }
};

} // namespace turing
} // namespace suez
