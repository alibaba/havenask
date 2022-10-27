#include <ha3/queryparser/SyntaxExprParser.h>
#include <sstream>
#include <suez/turing/expression/util/TypeTransformer.h>

using namespace std;

namespace suez {
namespace turing {

HA3_LOG_SETUP(queryparser, SyntaxExprParser);

SyntaxExprParser::SyntaxExprParser() {
}

SyntaxExprParser::~SyntaxExprParser() { 
}

AndSyntaxExpr* SyntaxExprParser::createAndExpr(SyntaxExpr *exprA, SyntaxExpr *exprB) {
    if (!isBooleanExpr(exprA) || !isBooleanExpr(exprB)) {
        HA3_LOG(TRACE3, "createAndExpr: leftExpr or rightExpr is not boolean");
        return NULL;
    }

    AndSyntaxExpr *ret = new AndSyntaxExpr(exprA, exprB);
    HA3_LOG(TRACE2, "createAndExpr(%p, %p), ret(%p)", exprA, exprB, ret);
    return ret;
}

OrSyntaxExpr* SyntaxExprParser::createOrExpr(SyntaxExpr *exprA, SyntaxExpr *exprB) {
    if (!isBooleanExpr(exprA) || !isBooleanExpr(exprB)) {
        HA3_LOG(TRACE3, "createAndExpr: leftExpr or rightExpr is not boolean");
        return NULL;
    }

    OrSyntaxExpr *ret = new OrSyntaxExpr(exprA, exprB);
    HA3_LOG(TRACE2, "createOrExpr(%p, %p), ret(%p)", exprA, exprB, ret);
    return ret;
}

EqualSyntaxExpr* SyntaxExprParser::createEqualExpr(SyntaxExpr *exprA, SyntaxExpr *exprB) {
    EqualSyntaxExpr *ret =  new EqualSyntaxExpr(exprA, exprB);
    HA3_LOG(TRACE2, "createEqualExpr(%p, %p), ret(%p)", exprA, exprB, ret);
    return ret;
}

LessSyntaxExpr* SyntaxExprParser::createLessExpr(SyntaxExpr *exprA, SyntaxExpr *exprB) {
    LessSyntaxExpr *ret =  new LessSyntaxExpr(exprA, exprB);
    HA3_LOG(TRACE2, "createLessExpr(%p, %p), ret(%p)", exprA, exprB, ret);
    return ret;
}

GreaterSyntaxExpr* SyntaxExprParser::createGreaterExpr(SyntaxExpr *exprA, SyntaxExpr *exprB) {
    GreaterSyntaxExpr *ret =  new GreaterSyntaxExpr(exprA, exprB);
    HA3_LOG(TRACE2, "createGreaterExpr(%p, %p), ret(%p)", exprA, exprB, ret);
    return ret;
}

LessEqualSyntaxExpr* SyntaxExprParser::createLessEqualExpr(SyntaxExpr *exprA, SyntaxExpr *exprB) {
    LessEqualSyntaxExpr *ret =  new LessEqualSyntaxExpr(exprA, exprB);
    HA3_LOG(TRACE2, "createLessEqualExpr(%p, %p), ret(%p)", exprA, exprB, ret);
    return ret;
}

GreaterEqualSyntaxExpr* SyntaxExprParser::createGreaterEqualExpr(SyntaxExpr *exprA, SyntaxExpr *exprB) {
    GreaterEqualSyntaxExpr *ret =  new GreaterEqualSyntaxExpr(exprA, exprB);
    HA3_LOG(TRACE2, "createGreaterEqualExpr(%p, %p), ret(%p)", exprA, exprB, ret);
    return ret;
}

NotEqualSyntaxExpr* SyntaxExprParser::createNotEqualExpr(SyntaxExpr *exprA, SyntaxExpr *exprB) {
    NotEqualSyntaxExpr *ret =  new NotEqualSyntaxExpr(exprA, exprB);
    HA3_LOG(TRACE2, "createNotEqualExpr(%p, %p), ret(%p)", exprA, exprB, ret);
    return ret;
}

AtomicSyntaxExpr* SyntaxExprParser::createAtomicExpr(string* expr, 
        AtomicSyntaxExprType atomicSyntaxExprType,
        bool isPositive) 
{
    assert(expr);
    const string &exprStr = isPositive ? *expr : ("-" + *expr);
    AtomicSyntaxExpr *ret =  new AtomicSyntaxExpr(exprStr, vt_unknown, atomicSyntaxExprType);
    HA3_LOG(TRACE2, "createAtomicExpr: %s, ret(%p)", exprStr.c_str(), ret);
    delete expr;
    return ret;
}

AddSyntaxExpr* SyntaxExprParser::createAddExpr(SyntaxExpr *exprA, SyntaxExpr *exprB) {
    AddSyntaxExpr *ret =  new AddSyntaxExpr(exprA, exprB);
    ret->setExprResultType(exprA->getExprResultType());
    HA3_LOG(TRACE2, "createAddExpr(%p, %p), ret(%p)", exprA, exprB, ret);
    return ret;
}

MinusSyntaxExpr* SyntaxExprParser::createMinusExpr(SyntaxExpr *exprA, SyntaxExpr *exprB) {
    MinusSyntaxExpr *ret =  new MinusSyntaxExpr(exprA, exprB);
    ret->setExprResultType(exprA->getExprResultType());
    HA3_LOG(TRACE2, "createMinusExpr(%p, %p), ret(%p)", exprA, exprB, ret);
    return ret;
}

MulSyntaxExpr* SyntaxExprParser::createMulExpr(SyntaxExpr *exprA, SyntaxExpr *exprB) {
    MulSyntaxExpr *ret =  new MulSyntaxExpr(exprA, exprB);
    ret->setExprResultType(exprA->getExprResultType());
    HA3_LOG(TRACE2, "createMulExpr(%p, %p), ret(%p)", exprA, exprB, ret);
    return ret;
}
DivSyntaxExpr* SyntaxExprParser::createDivExpr(SyntaxExpr *exprA, SyntaxExpr *exprB) {
    DivSyntaxExpr *ret =  new DivSyntaxExpr(exprA, exprB);
    ret->setExprResultType(exprA->getExprResultType());
    HA3_LOG(TRACE2, "createDivExpr(%p, %p), ret(%p)", exprA, exprB, ret);
    return ret;
}

BitOrSyntaxExpr* SyntaxExprParser::createBitOrExpr(SyntaxExpr *exprA, SyntaxExpr *exprB) {
    BitOrSyntaxExpr *ret =  new BitOrSyntaxExpr(exprA, exprB);
    ret->setExprResultType(exprA->getExprResultType());
    HA3_LOG(TRACE2, "createBitOrExpr(%p, %p), ret(%p)", exprA, exprB, ret);
    return ret;
}

BitXorSyntaxExpr* SyntaxExprParser::createBitXorExpr(SyntaxExpr *exprA, SyntaxExpr *exprB) {
    BitXorSyntaxExpr *ret =  new BitXorSyntaxExpr(exprA, exprB);
    ret->setExprResultType(exprA->getExprResultType());
    HA3_LOG(TRACE2, "createBitXorExpr(%p, %p), ret(%p)", exprA, exprB, ret);
    return ret;
}

BitAndSyntaxExpr* SyntaxExprParser::createBitAndExpr(SyntaxExpr *exprA, SyntaxExpr *exprB) {
    BitAndSyntaxExpr *ret =  new BitAndSyntaxExpr(exprA, exprB);
    ret->setExprResultType(exprA->getExprResultType());
    HA3_LOG(TRACE2, "createBitAndExpr(%p, %p), ret(%p)", exprA, exprB, ret);
    return ret;
}

FuncSyntaxExpr* SyntaxExprParser::createFuncExpr(std::string *funcName,
        const std::vector<SyntaxExpr*> *param)
{
    assert(funcName);
    FuncSyntaxExpr *ret = NULL;
    if (!param) {
        ret = new FuncSyntaxExpr(*funcName, vector<SyntaxExpr*>());
    } else {
        ret = new FuncSyntaxExpr(*funcName, *param);
    }
    HA3_LOG(TRACE2, "createFuncExpr(%s), ret(%p)", funcName->c_str(), ret);
    delete funcName;
    delete param;
    return ret;
}

MultiSyntaxExpr *SyntaxExprParser::createMultiSyntax() {
    return new suez::turing::MultiSyntaxExpr();
}

bool SyntaxExprParser::isBooleanExpr(SyntaxExpr *expr) const {
    return expr->getExprResultType() == vt_bool;
}

}
}

