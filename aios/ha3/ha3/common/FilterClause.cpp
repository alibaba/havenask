#include <ha3/common/FilterClause.h>

using namespace suez::turing;

BEGIN_HA3_NAMESPACE(common);
HA3_LOG_SETUP(common, FilterClause);

FilterClause::FilterClause() { 
    _filterExpr = NULL;
}

FilterClause::FilterClause(SyntaxExpr *filterExpr) { 
    _filterExpr = filterExpr;
}

FilterClause::~FilterClause() { 
    DELETE_AND_SET_NULL(_filterExpr);
}

void FilterClause::setRootSyntaxExpr(SyntaxExpr *filterExpr) {
    if (_filterExpr != NULL) {
        delete _filterExpr;
        _filterExpr = NULL;
    }
    _filterExpr = filterExpr;
}

const SyntaxExpr *FilterClause::getRootSyntaxExpr() const {
    return _filterExpr;
}

void FilterClause::serialize(autil::DataBuffer &dataBuffer) const
{
    dataBuffer.write(_originalString);
    dataBuffer.write(_filterExpr);
}

void FilterClause::deserialize(autil::DataBuffer &dataBuffer)
{
    dataBuffer.read(_originalString);
    dataBuffer.read(_filterExpr);
}

std::string FilterClause::toString() const {
    assert(_filterExpr);
    return _filterExpr->getExprString();
}

END_HA3_NAMESPACE(common);

