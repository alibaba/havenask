#include <ha3/queryparser/BinaryQueryExpr.h>

BEGIN_HA3_NAMESPACE(queryparser);
HA3_LOG_SETUP(queryparser, BinaryQueryExpr);

BinaryQueryExpr::BinaryQueryExpr(QueryExpr *a, QueryExpr *b) { 
    _exprA = a;
    _exprB = b;
}

BinaryQueryExpr::~BinaryQueryExpr() { 
    delete _exprA;
    delete _exprB;
}



END_HA3_NAMESPACE(queryparser);

