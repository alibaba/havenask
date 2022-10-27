#include <ha3/common/SearcherCacheClause.h>

using namespace suez::turing;

BEGIN_HA3_NAMESPACE(common);
HA3_LOG_SETUP(common, SearcherCacheClause);

SearcherCacheClause::SearcherCacheClause() { 
    _use = false;
    _currentTime = INVALID_TIME;
    _key = 0L;
    _filterClause = NULL;
    _expireExpr = NULL;
    _cacheDocNumVec.push_back(200); // upward compatible with old cache doc num config
    _cacheDocNumVec.push_back(3000);
}

SearcherCacheClause::~SearcherCacheClause() { 
    delete _filterClause;
    delete _expireExpr;
}

void SearcherCacheClause::setFilterClause(FilterClause *filterClause) {
    if (_filterClause != NULL) {
        delete _filterClause;
    }
    _filterClause = filterClause;
}

void SearcherCacheClause::setExpireExpr(SyntaxExpr *syntaxExpr) {
    if (_expireExpr != NULL) {
        delete _expireExpr;
    }
    _expireExpr = syntaxExpr;
}

void SearcherCacheClause::serialize(autil::DataBuffer &dataBuffer) const {
    dataBuffer.write(_originalString);
    dataBuffer.write(_use);
    dataBuffer.write(_currentTime);
    dataBuffer.write(_key);
    dataBuffer.write(_cacheDocNumVec);
    dataBuffer.write(_filterClause);
    dataBuffer.write(_expireExpr);
    dataBuffer.write(_refreshAttributes);
}

void SearcherCacheClause::deserialize(autil::DataBuffer &dataBuffer) {
    dataBuffer.read(_originalString);
    dataBuffer.read(_use);
    dataBuffer.read(_currentTime);
    dataBuffer.read(_key);
    dataBuffer.read(_cacheDocNumVec);
    dataBuffer.read(_filterClause);
    dataBuffer.read(_expireExpr);
    dataBuffer.read(_refreshAttributes);
}

std::string SearcherCacheClause::toString() const {
    return _originalString;
}

END_HA3_NAMESPACE(common);

