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
#include "expression/syntax/SyntaxFactory.h"
#include "expression/util/SyntaxStringConvertor.h"

using namespace std;

namespace expression {
AUTIL_LOG_SETUP(expression, SyntaxFactory);

SyntaxFactory::SyntaxFactory() {
}

SyntaxFactory::~SyntaxFactory() {
}

SyntaxExpr* SyntaxFactory::createAndExpr(SyntaxExpr *exprA, SyntaxExpr *exprB, bool enableSyntaxParseOpt) {
    if (enableSyntaxParseOpt) {
        SyntaxExpr* ret = createSyntaxExprWithOpt(exprA, exprB, OPTIMIZE_MODE_AND);
        if (ret) {
            delete exprA;
            delete exprB;
            return ret;
        }
    }
    AUTIL_LOG(TRACE2, "createAndExpr(%p, %p)", exprA, exprB);
    return new AndSyntaxExpr(exprA, exprB);
}

SyntaxExpr* SyntaxFactory::createOrExpr(SyntaxExpr *exprA, SyntaxExpr *exprB, bool enableSyntaxParseOpt) {
    if (enableSyntaxParseOpt) {
        SyntaxExpr* ret = createSyntaxExprWithOpt(exprA, exprB, OPTIMIZE_MODE_OR);
        if (ret) {
            delete exprA;
            delete exprB;
            return ret;
        }
    }
    
    AUTIL_LOG(TRACE2, "createOrExpr(%p, %p)", exprA, exprB);
    return new OrSyntaxExpr(exprA, exprB);
}

SyntaxExpr* SyntaxFactory::createSyntaxExprWithOpt(SyntaxExpr *exprA, SyntaxExpr *exprB, OptimizeMode optmizeMode) {
    std::string valueStrA, valueStrB;
    AttributeSyntaxExpr *attrExprA, *attrExprB;
    if (!getSyntaxParams(exprA, valueStrA, attrExprA, optmizeMode) || !getSyntaxParams(exprB, valueStrB, attrExprB, optmizeMode)) {
        return nullptr;
    }
    return optimize(valueStrA, attrExprA, valueStrB, attrExprB, optmizeMode);
}

inline bool SyntaxFactory::getSyntaxParams(SyntaxExpr *expr, std::string &valueStr, AttributeSyntaxExpr *&attrExpr, OptimizeMode optmizeMode) {
    if (optmizeMode == OPTIMIZE_MODE_AND) {
        if (expr->getSyntaxType() == SYNTAX_EXPR_TYPE_NOTEQUAL) {
            return getEqualSyntaxParams(expr, valueStr, attrExpr);
        } else if (expr->getSyntaxType() == SYNTAX_EXPR_TYPE_FUNC) {
            return getFuncSyntaxParams(expr, valueStr, attrExpr, "notin");
        }
    } else if (optmizeMode == OPTIMIZE_MODE_OR) {
        if (expr->getSyntaxType() == SYNTAX_EXPR_TYPE_EQUAL) {
            return getEqualSyntaxParams(expr, valueStr, attrExpr);
        } else if (expr->getSyntaxType() == SYNTAX_EXPR_TYPE_FUNC) {
            return getFuncSyntaxParams(expr, valueStr, attrExpr, "in");
        }
    }
    return false;
}


inline bool SyntaxFactory::getEqualSyntaxParams(SyntaxExpr *expr, std::string &valueStr, AttributeSyntaxExpr *&attrExpr) {
    BinarySyntaxExpr* binaryExpr = static_cast<BinarySyntaxExpr*>(expr); 
    const ConstSyntaxExpr* constExpr = nullptr;                      //指向的内存空间不可更改
    if (binaryExpr->getLeftExpr()->getSyntaxType() == SYNTAX_EXPR_TYPE_ATOMIC_ATTR && binaryExpr->getRightExpr()->getSyntaxType() == SYNTAX_EXPR_TYPE_CONST_VALUE) {
        attrExpr = const_cast<AttributeSyntaxExpr*>(static_cast<const AttributeSyntaxExpr*>(binaryExpr->getLeftExpr()));
        constExpr = static_cast<const ConstSyntaxExpr*>(binaryExpr->getRightExpr());
    } else if (binaryExpr->getLeftExpr()->getSyntaxType() == SYNTAX_EXPR_TYPE_CONST_VALUE && binaryExpr->getRightExpr()->getSyntaxType() == SYNTAX_EXPR_TYPE_ATOMIC_ATTR) {
        attrExpr = const_cast<AttributeSyntaxExpr*>(static_cast<const AttributeSyntaxExpr*>(binaryExpr->getRightExpr()));
        constExpr = static_cast<const ConstSyntaxExpr*>(binaryExpr->getLeftExpr());
    }
    if (constExpr && constExpr->getValueType() != STRING_VALUE) {
        valueStr = constExpr->getValueString();
        return true;
    }
    return false;
}

inline bool SyntaxFactory::getFuncSyntaxParams(SyntaxExpr *expr, std::string &valueStr, AttributeSyntaxExpr *&attrExpr, std::string funcName) {
    FuncSyntaxExpr* funcExpr = static_cast<FuncSyntaxExpr*>(expr);
    if (funcExpr->getFunctionName() == funcName) {
        const FuncSyntaxExpr::SubExprType &argSyntaxExprs = funcExpr->getArgumentSyntaxExprs();
        if (argSyntaxExprs[0]->getSyntaxType() == SYNTAX_EXPR_TYPE_ATOMIC_ATTR) {
            attrExpr = static_cast<AttributeSyntaxExpr*>(argSyntaxExprs[0]);
            ConstSyntaxExpr* constExpr = static_cast<ConstSyntaxExpr*>(argSyntaxExprs[1]);
            if (constExpr->getNeedEncode()) {
                valueStr = constExpr->getValueString();
            } else {
                valueStr = SyntaxStringConvertor::decodeEscapeString(constExpr->getValueString());
            }
            return true;
        }
    }
    return false;
}

inline SyntaxExpr* SyntaxFactory::optimize(std::string &valueStrA, AttributeSyntaxExpr *attrExprA,
                                    std::string &valueStrB, AttributeSyntaxExpr *attrExprB,
                                    OptimizeMode optmizeMode) {
    if (attrExprA->getExprString() == attrExprB->getExprString()) {
        FuncSyntaxExpr::SubExprType *funcParam = new FuncSyntaxExpr::SubExprType;
        AttributeSyntaxExpr* attrExpr = new AttributeSyntaxExpr(*attrExprA);
        funcParam->push_back(attrExpr);
        ConstSyntaxExpr* expr = new ConstSyntaxExpr(valueStrA + "|" + valueStrB, STRING_VALUE, SYNTAX_EXPR_TYPE_CONST_VALUE, true);
        funcParam->push_back(expr);
        std::string funcName;
        if (optmizeMode == OPTIMIZE_MODE_AND) {
            funcName = "notin";
        } else if (optmizeMode == OPTIMIZE_MODE_OR) {
            funcName = "in";
        }
        FuncSyntaxExpr* ret = new FuncSyntaxExpr(funcName, *funcParam);
        AUTIL_LOG(TRACE2, "createOptFuncExpr(%s), ret(%p)", funcName.c_str(), ret);
        delete funcParam;
        return ret;
    }
    return nullptr;
}

EqualSyntaxExpr* SyntaxFactory::createEqualExpr(SyntaxExpr *exprA, SyntaxExpr *exprB) {
    AUTIL_LOG(TRACE2, "createEqualExpr(%p, %p)", exprA, exprB);
    return new EqualSyntaxExpr(exprA, exprB);
}

LessSyntaxExpr* SyntaxFactory::createLessExpr(SyntaxExpr *exprA, SyntaxExpr *exprB) {
    LessSyntaxExpr *ret =  new LessSyntaxExpr(exprA, exprB);
    AUTIL_LOG(TRACE2, "createLessExpr(%p, %p), ret(%p)", exprA, exprB, ret);
    return ret;
}

GreaterSyntaxExpr* SyntaxFactory::createGreaterExpr(SyntaxExpr *exprA, SyntaxExpr *exprB) {
    GreaterSyntaxExpr *ret =  new GreaterSyntaxExpr(exprA, exprB);
    AUTIL_LOG(TRACE2, "createGreaterExpr(%p, %p), ret(%p)", exprA, exprB, ret);
    return ret;
}

LessEqualSyntaxExpr* SyntaxFactory::createLessEqualExpr(SyntaxExpr *exprA, SyntaxExpr *exprB) {
    LessEqualSyntaxExpr *ret =  new LessEqualSyntaxExpr(exprA, exprB);
    AUTIL_LOG(TRACE2, "createLessEqualExpr(%p, %p), ret(%p)", exprA, exprB, ret);
    return ret;
}

GreaterEqualSyntaxExpr* SyntaxFactory::createGreaterEqualExpr(SyntaxExpr *exprA, SyntaxExpr *exprB) {
    GreaterEqualSyntaxExpr *ret =  new GreaterEqualSyntaxExpr(exprA, exprB);
    AUTIL_LOG(TRACE2, "createGreaterEqualExpr(%p, %p), ret(%p)", exprA, exprB, ret);
    return ret;
}

NotEqualSyntaxExpr* SyntaxFactory::createNotEqualExpr(SyntaxExpr *exprA, SyntaxExpr *exprB) {
    NotEqualSyntaxExpr *ret =  new NotEqualSyntaxExpr(exprA, exprB);
    AUTIL_LOG(TRACE2, "createNotEqualExpr(%p, %p), ret(%p)", exprA, exprB, ret);
    return ret;
}

ConstSyntaxExpr* SyntaxFactory::createConstExpr(string* expr, 
        ConstExprType type, bool isPositive)
{
    const string &exprStr = isPositive ? *expr : ("-" + *expr);
    ConstSyntaxExpr *ret =  new ConstSyntaxExpr(exprStr, type);
    AUTIL_LOG(TRACE2, "createConstExpr: %s, ret(%p)", exprStr.c_str(), ret);
    delete expr;
    return ret;
}

AttributeSyntaxExpr* SyntaxFactory::createAttributeExpr(std::string* attrName, std::string *tableName) {
    string table = tableName ? *tableName : "";
    AttributeSyntaxExpr *ret =  new AttributeSyntaxExpr(*attrName, table);
    delete tableName;
    delete attrName;
    return ret;
}

AddSyntaxExpr* SyntaxFactory::createAddExpr(SyntaxExpr *exprA, SyntaxExpr *exprB) {
    AddSyntaxExpr *ret =  new AddSyntaxExpr(exprA, exprB);
    AUTIL_LOG(TRACE2, "createAddExpr(%p, %p), ret(%p)", exprA, exprB, ret);
    return ret;
}

MinusSyntaxExpr* SyntaxFactory::createMinusExpr(SyntaxExpr *exprA, SyntaxExpr *exprB) {
    MinusSyntaxExpr *ret =  new MinusSyntaxExpr(exprA, exprB);
    AUTIL_LOG(TRACE2, "createMinusExpr(%p, %p), ret(%p)", exprA, exprB, ret);
    return ret;
}

MulSyntaxExpr* SyntaxFactory::createMulExpr(SyntaxExpr *exprA, SyntaxExpr *exprB) {
    MulSyntaxExpr *ret =  new MulSyntaxExpr(exprA, exprB);
    AUTIL_LOG(TRACE2, "createMulExpr(%p, %p), ret(%p)", exprA, exprB, ret);
    return ret;
}
DivSyntaxExpr* SyntaxFactory::createDivExpr(SyntaxExpr *exprA, SyntaxExpr *exprB) {
    DivSyntaxExpr *ret =  new DivSyntaxExpr(exprA, exprB);
    AUTIL_LOG(TRACE2, "createDivExpr(%p, %p), ret(%p)", exprA, exprB, ret);
    return ret;
}

BitOrSyntaxExpr* SyntaxFactory::createBitOrExpr(SyntaxExpr *exprA, SyntaxExpr *exprB) {
    BitOrSyntaxExpr *ret =  new BitOrSyntaxExpr(exprA, exprB);
    AUTIL_LOG(TRACE2, "createBitOrExpr(%p, %p), ret(%p)", exprA, exprB, ret);
    return ret;
}

BitXorSyntaxExpr* SyntaxFactory::createBitXorExpr(SyntaxExpr *exprA, SyntaxExpr *exprB) {
    BitXorSyntaxExpr *ret =  new BitXorSyntaxExpr(exprA, exprB);
    AUTIL_LOG(TRACE2, "createBitXorExpr(%p, %p), ret(%p)", exprA, exprB, ret);
    return ret;
}

BitAndSyntaxExpr* SyntaxFactory::createBitAndExpr(SyntaxExpr *exprA, SyntaxExpr *exprB) {
    BitAndSyntaxExpr *ret =  new BitAndSyntaxExpr(exprA, exprB);
    AUTIL_LOG(TRACE2, "createBitAndExpr(%p, %p), ret(%p)", exprA, exprB, ret);
    return ret;
}

FuncSyntaxExpr* SyntaxFactory::createFuncExpr(std::string *funcName,
        const vector<SyntaxExpr*> *param)
{
    FuncSyntaxExpr *ret = NULL;
    if (!param) {
        ret = new FuncSyntaxExpr(*funcName, vector<SyntaxExpr*>());
    } else {
        ret = new FuncSyntaxExpr(*funcName, *param);
    }
    AUTIL_LOG(TRACE2, "createFuncExpr(%s), ret(%p)", funcName->c_str(), ret);
    delete funcName;
    delete param;
    return ret;
}

}
