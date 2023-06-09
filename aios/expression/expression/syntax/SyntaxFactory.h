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
#ifndef ISEARCH_EXPRESSION_SYNTAXFACTORY_H
#define ISEARCH_EXPRESSION_SYNTAXFACTORY_H

#include "expression/common.h"
#include "expression/syntax/SyntaxExpr.h"
#include "expression/syntax/ConstSyntaxExpr.h"
#include "expression/syntax/AttributeSyntaxExpr.h"
#include "expression/syntax/BinarySyntaxExpr.h"
#include "expression/syntax/FuncSyntaxExpr.h"
namespace expression {

enum OptimizeMode {
    OPTIMIZE_MODE_UNKNOWN,
    OPTIMIZE_MODE_AND,
    OPTIMIZE_MODE_OR,
};

class SyntaxFactory
{
public:
    SyntaxFactory();
    ~SyntaxFactory();
private:
    SyntaxFactory(const SyntaxFactory &);
    SyntaxFactory& operator=(const SyntaxFactory &);
public:
    static ConstSyntaxExpr* createConstExpr(std::string* expr, 
            ConstExprType valueType, bool isPositive = true);
    static AttributeSyntaxExpr* createAttributeExpr(std::string* attrName, std::string *tableName);

    static SyntaxExpr* createAndExpr(SyntaxExpr *exprA, SyntaxExpr *exprB, bool enableSyntaxParseOpt = false);
    static SyntaxExpr* createOrExpr(SyntaxExpr *exprA, SyntaxExpr *exprB, bool enableSyntaxParseOpt = false);

    static EqualSyntaxExpr* createEqualExpr(SyntaxExpr *exprA, SyntaxExpr *exprB);
    static NotEqualSyntaxExpr* createNotEqualExpr(SyntaxExpr *exprA, SyntaxExpr *exprB);

    static LessSyntaxExpr* createLessExpr(SyntaxExpr *exprA, SyntaxExpr *exprB);
    static LessEqualSyntaxExpr* createLessEqualExpr(SyntaxExpr *exprA, SyntaxExpr *exprB);

    static GreaterSyntaxExpr* createGreaterExpr(SyntaxExpr *exprA, SyntaxExpr *exprB);
    static GreaterEqualSyntaxExpr* createGreaterEqualExpr(SyntaxExpr *exprA, SyntaxExpr *exprB);

    static AddSyntaxExpr* createAddExpr(SyntaxExpr *exprA, SyntaxExpr *exprB);
    static MinusSyntaxExpr* createMinusExpr(SyntaxExpr *exprA, SyntaxExpr *exprB);
    static MulSyntaxExpr* createMulExpr(SyntaxExpr *exprA, SyntaxExpr *exprB);
    static DivSyntaxExpr* createDivExpr(SyntaxExpr *exprA, SyntaxExpr *exprB);

    static BitOrSyntaxExpr* createBitOrExpr(SyntaxExpr *exprA, SyntaxExpr *exprB);
    static BitXorSyntaxExpr* createBitXorExpr(SyntaxExpr *exprA, SyntaxExpr *exprB);
    static BitAndSyntaxExpr* createBitAndExpr(SyntaxExpr *exprA, SyntaxExpr *exprB);
    static FuncSyntaxExpr* createFuncExpr(std::string *funcName, 
            const std::vector<SyntaxExpr*> *param = NULL);
private:
    AUTIL_LOG_DECLARE();
    static SyntaxExpr* createSyntaxExprWithOpt(SyntaxExpr *exprA, SyntaxExpr *exprB, OptimizeMode optmizeMode);
    static bool getSyntaxParams(SyntaxExpr *expr, std::string &valueStr, AttributeSyntaxExpr *&attrexpr, OptimizeMode optmizeMode);
    static bool getEqualSyntaxParams(SyntaxExpr *expr, std::string &valueStr, AttributeSyntaxExpr *&attrexpr);
    static bool getFuncSyntaxParams(SyntaxExpr *expr, std::string &valueStr, AttributeSyntaxExpr *&attrexpr, std::string funcname);
    static SyntaxExpr* optimize(std::string &valueStrA, AttributeSyntaxExpr *attrexprA,
                                std::string &valueStrB, AttributeSyntaxExpr *attrexprB,
                                OptimizeMode optmizeMode);

};

}

#endif //ISEARCH_EXPRESSION_SYNTAXFACTORY_H
