#ifndef ISEARCH_SYNTAXEXPRPARSER_H
#define ISEARCH_SYNTAXEXPRPARSER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <string>
#include <vector>
#include <suez/turing/expression/syntax/SyntaxExpr.h>
#include <suez/turing/expression/syntax/AtomicSyntaxExpr.h>
#include <suez/turing/expression/syntax/BinarySyntaxExpr.h>
#include <suez/turing/expression/syntax/FuncSyntaxExpr.h>
#include <suez/turing/expression/syntax/MultiSyntaxExpr.h>
#include <suez/turing/expression/util/TableInfo.h>

namespace suez {
namespace turing {

class SyntaxExprParser
{
public:
    SyntaxExprParser();
    ~SyntaxExprParser();
public:
    AndSyntaxExpr* createAndExpr(SyntaxExpr *exprA, SyntaxExpr *exprB);
    OrSyntaxExpr* createOrExpr(SyntaxExpr *exprA, SyntaxExpr *exprB);

    EqualSyntaxExpr* createEqualExpr(SyntaxExpr *exprA, SyntaxExpr *exprB);
    NotEqualSyntaxExpr* createNotEqualExpr(SyntaxExpr *exprA, SyntaxExpr *exprB);

    LessSyntaxExpr* createLessExpr(SyntaxExpr *exprA, SyntaxExpr *exprB);
    LessEqualSyntaxExpr* createLessEqualExpr(SyntaxExpr *exprA, SyntaxExpr *exprB);

    GreaterSyntaxExpr* createGreaterExpr(SyntaxExpr *exprA, SyntaxExpr *exprB);
    GreaterEqualSyntaxExpr* createGreaterEqualExpr(SyntaxExpr *exprA, SyntaxExpr *exprB);

    AtomicSyntaxExpr* createAtomicExpr(std::string* expr, AtomicSyntaxExprType valueType, bool isPositive = true);

    AddSyntaxExpr* createAddExpr(SyntaxExpr *exprA, SyntaxExpr *exprB);
    MinusSyntaxExpr* createMinusExpr(SyntaxExpr *exprA, SyntaxExpr *exprB);
    MulSyntaxExpr* createMulExpr(SyntaxExpr *exprA, SyntaxExpr *exprB);
    DivSyntaxExpr* createDivExpr(SyntaxExpr *exprA, SyntaxExpr *exprB);

    BitOrSyntaxExpr* createBitOrExpr(SyntaxExpr *exprA, SyntaxExpr *exprB);
    BitXorSyntaxExpr* createBitXorExpr(SyntaxExpr *exprA, SyntaxExpr *exprB);
    BitAndSyntaxExpr* createBitAndExpr(SyntaxExpr *exprA, SyntaxExpr *exprB);
    FuncSyntaxExpr* createFuncExpr(std::string *funcName,
                                   const std::vector<SyntaxExpr*> *param = NULL);
    MultiSyntaxExpr *createMultiSyntax();
private:
    bool isBooleanExpr(SyntaxExpr *expr) const;
private:
    HA3_LOG_DECLARE();
};

}
}

#endif //ISEARCH_SYNTAXEXPRPARSER_H
