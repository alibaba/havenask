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
#include "suez/turing/expression/syntax/SyntaxExpr.h"
#include "suez/turing/expression/syntax/SyntaxExprVisitor.h"

namespace autil {
class DataBuffer;
} // namespace autil

namespace suez {
namespace turing {

class BinarySyntaxExpr : public SyntaxExpr {
public:
    BinarySyntaxExpr(SyntaxExpr *a, SyntaxExpr *b, SyntaxExprType syntaxExprType);
    virtual ~BinarySyntaxExpr();

public:
    const SyntaxExpr *getLeftExpr() const { return _exprA; }
    const SyntaxExpr *getRightExpr() const { return _exprB; }

    bool operator==(const SyntaxExpr *expr) const;

    virtual std::string getExprString() const { return std::string("(") + _exprString + std::string(")"); }

    void serialize(autil::DataBuffer &dataBuffer) const;
    void deserialize(autil::DataBuffer &dataBuffer);

private:
    void setExprString(SyntaxExpr *a, SyntaxExpr *b, SyntaxExprType syntaxExprType);

private:
    SyntaxExpr *_exprA;
    SyntaxExpr *_exprB;

private:
    AUTIL_LOG_DECLARE();
};

class AddSyntaxExpr : public BinarySyntaxExpr {
public:
    AddSyntaxExpr(SyntaxExpr *a, SyntaxExpr *b) : BinarySyntaxExpr(a, b, SYNTAX_EXPR_TYPE_ADD) {}
    void accept(SyntaxExprVisitor *visitor) const { visitor->visitAddSyntaxExpr(this); }

private:
    AUTIL_LOG_DECLARE();
};

class MinusSyntaxExpr : public BinarySyntaxExpr {
public:
    MinusSyntaxExpr(SyntaxExpr *a, SyntaxExpr *b) : BinarySyntaxExpr(a, b, SYNTAX_EXPR_TYPE_MINUS) {}
    void accept(SyntaxExprVisitor *visitor) const { visitor->visitMinusSyntaxExpr(this); }

private:
    AUTIL_LOG_DECLARE();
};

class MulSyntaxExpr : public BinarySyntaxExpr {
public:
    MulSyntaxExpr(SyntaxExpr *a, SyntaxExpr *b) : BinarySyntaxExpr(a, b, SYNTAX_EXPR_TYPE_MUL) {}
    void accept(SyntaxExprVisitor *visitor) const { visitor->visitMulSyntaxExpr(this); }

private:
    AUTIL_LOG_DECLARE();
};

class DivSyntaxExpr : public BinarySyntaxExpr {
public:
    DivSyntaxExpr(SyntaxExpr *a, SyntaxExpr *b) : BinarySyntaxExpr(a, b, SYNTAX_EXPR_TYPE_DIV) {}

    void accept(SyntaxExprVisitor *visitor) const { visitor->visitDivSyntaxExpr(this); }

private:
    AUTIL_LOG_DECLARE();
};

class PowerSyntaxExpr : public BinarySyntaxExpr {
public:
    PowerSyntaxExpr(SyntaxExpr *a, SyntaxExpr *b) : BinarySyntaxExpr(a, b, SYNTAX_EXPR_TYPE_POWER) {}
    void accept(SyntaxExprVisitor *visitor) const { visitor->visitPowerSyntaxExpr(this); }

private:
    AUTIL_LOG_DECLARE();
};

class LogSyntaxExpr : public BinarySyntaxExpr {
public:
    LogSyntaxExpr(SyntaxExpr *a, SyntaxExpr *b) : BinarySyntaxExpr(a, b, SYNTAX_EXPR_TYPE_LOG) {}
    void accept(SyntaxExprVisitor *visitor) const { visitor->visitLogSyntaxExpr(this); }

private:
    AUTIL_LOG_DECLARE();
};

class EqualSyntaxExpr : public BinarySyntaxExpr {
public:
    EqualSyntaxExpr(SyntaxExpr *a, SyntaxExpr *b) : BinarySyntaxExpr(a, b, SYNTAX_EXPR_TYPE_EQUAL) {

        setExprResultType(vt_bool);
    }
    void accept(SyntaxExprVisitor *visitor) const { visitor->visitEqualSyntaxExpr(this); }

private:
    AUTIL_LOG_DECLARE();
};

class NotEqualSyntaxExpr : public BinarySyntaxExpr {
public:
    NotEqualSyntaxExpr(SyntaxExpr *a, SyntaxExpr *b) : BinarySyntaxExpr(a, b, SYNTAX_EXPR_TYPE_NOTEQUAL) {
        setExprResultType(vt_bool);
    }
    void accept(SyntaxExprVisitor *visitor) const { visitor->visitNotEqualSyntaxExpr(this); }

private:
    AUTIL_LOG_DECLARE();
};

class LessSyntaxExpr : public BinarySyntaxExpr {
public:
    LessSyntaxExpr(SyntaxExpr *a, SyntaxExpr *b) : BinarySyntaxExpr(a, b, SYNTAX_EXPR_TYPE_LESS) {
        setExprResultType(vt_bool);
    }
    void accept(SyntaxExprVisitor *visitor) const { visitor->visitLessSyntaxExpr(this); }

private:
    AUTIL_LOG_DECLARE();
};

class GreaterSyntaxExpr : public BinarySyntaxExpr {
public:
    GreaterSyntaxExpr(SyntaxExpr *a, SyntaxExpr *b) : BinarySyntaxExpr(a, b, SYNTAX_EXPR_TYPE_GREATERTHAN) {
        setExprResultType(vt_bool);
    }
    void accept(SyntaxExprVisitor *visitor) const { visitor->visitGreaterSyntaxExpr(this); }

private:
    AUTIL_LOG_DECLARE();
};

class LessEqualSyntaxExpr : public BinarySyntaxExpr {
public:
    LessEqualSyntaxExpr(SyntaxExpr *a, SyntaxExpr *b) : BinarySyntaxExpr(a, b, SYNTAX_EXPR_TYPE_LESSEQUAL) {
        setExprResultType(vt_bool);
    }
    void accept(SyntaxExprVisitor *visitor) const { visitor->visitLessEqualSyntaxExpr(this); }

private:
    AUTIL_LOG_DECLARE();
};

class GreaterEqualSyntaxExpr : public BinarySyntaxExpr {
public:
    GreaterEqualSyntaxExpr(SyntaxExpr *a, SyntaxExpr *b) : BinarySyntaxExpr(a, b, SYNTAX_EXPR_TYPE_GREATEREQUAL) {
        setExprResultType(vt_bool);
    }
    void accept(SyntaxExprVisitor *visitor) const { visitor->visitGreaterEqualSyntaxExpr(this); }

private:
    AUTIL_LOG_DECLARE();
};

class AndSyntaxExpr : public BinarySyntaxExpr {
public:
    AndSyntaxExpr(SyntaxExpr *a, SyntaxExpr *b) : BinarySyntaxExpr(a, b, SYNTAX_EXPR_TYPE_AND) {
        setExprResultType(vt_bool);
    }
    void accept(SyntaxExprVisitor *visitor) const { visitor->visitAndSyntaxExpr(this); }

private:
    AUTIL_LOG_DECLARE();
};

class OrSyntaxExpr : public BinarySyntaxExpr {
public:
    OrSyntaxExpr(SyntaxExpr *a, SyntaxExpr *b) : BinarySyntaxExpr(a, b, SYNTAX_EXPR_TYPE_OR) {
        setExprResultType(vt_bool);
    }
    void accept(SyntaxExprVisitor *visitor) const { visitor->visitOrSyntaxExpr(this); }

private:
    AUTIL_LOG_DECLARE();
};

class BitOrSyntaxExpr : public BinarySyntaxExpr {
public:
    BitOrSyntaxExpr(SyntaxExpr *a, SyntaxExpr *b) : BinarySyntaxExpr(a, b, SYNTAX_EXPR_TYPE_BITOR) {}
    void accept(SyntaxExprVisitor *visitor) const { visitor->visitBitOrSyntaxExpr(this); }

private:
    AUTIL_LOG_DECLARE();
};

class BitAndSyntaxExpr : public BinarySyntaxExpr {
public:
    BitAndSyntaxExpr(SyntaxExpr *a, SyntaxExpr *b) : BinarySyntaxExpr(a, b, SYNTAX_EXPR_TYPE_BITAND) {}
    void accept(SyntaxExprVisitor *visitor) const { visitor->visitBitAndSyntaxExpr(this); }

private:
    AUTIL_LOG_DECLARE();
};

class BitXorSyntaxExpr : public BinarySyntaxExpr {
public:
    BitXorSyntaxExpr(SyntaxExpr *a, SyntaxExpr *b) : BinarySyntaxExpr(a, b, SYNTAX_EXPR_TYPE_BITXOR) {}
    void accept(SyntaxExprVisitor *visitor) const { visitor->visitBitXorSyntaxExpr(this); }

private:
    AUTIL_LOG_DECLARE();
};

} // namespace turing
} // namespace suez
