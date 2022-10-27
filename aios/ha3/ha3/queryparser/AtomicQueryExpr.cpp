#include <ha3/queryparser/AtomicQueryExpr.h>

BEGIN_HA3_NAMESPACE(queryparser);
HA3_LOG_SETUP(queryparser, AtomicQueryExpr);

AtomicQueryExpr::AtomicQueryExpr() { 
    _boost = DEFAULT_BOOST_VALUE;
    _secondaryChain = "";
    _text = "";
}

AtomicQueryExpr::~AtomicQueryExpr() { 
}
END_HA3_NAMESPACE(queryparser);

