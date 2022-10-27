#ifndef ISEARCH_SEARCHERCACHECLAUSE_H
#define ISEARCH_SEARCHERCACHECLAUSE_H

#include <vector>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <autil/DataBuffer.h>
#include <ha3/common/FilterClause.h>
#include <ha3/common/ClauseBase.h>
#include <suez/turing/expression/syntax/SyntaxExpr.h>

BEGIN_HA3_NAMESPACE(common);

class SearcherCacheClause : public ClauseBase
{
public:
    static const uint32_t INVALID_TIME = uint32_t(-1);
public:
    SearcherCacheClause();
    ~SearcherCacheClause();
private:
    SearcherCacheClause(const SearcherCacheClause &);
    SearcherCacheClause& operator=(const SearcherCacheClause &);
public:
    void serialize(autil::DataBuffer &dataBuffer) const override;
    void deserialize(autil::DataBuffer &dataBuffer) override;
    std::string toString() const override;
public:
    void setKey(uint64_t key) { _key = key; }
    uint64_t getKey() const { return _key; }
    void setUse(bool use) { _use = use; }
    bool getUse() const { return _use; }
    void setCurrentTime(uint32_t curTime) { _currentTime = curTime; }
    uint32_t getCurrentTime() const { return _currentTime; }
    void setFilterClause(FilterClause *filterClause);
    FilterClause* getFilterClause() const { return _filterClause;}
    void setExpireExpr(suez::turing::SyntaxExpr *syntaxExpr);
    suez::turing::SyntaxExpr* getExpireExpr() const { return _expireExpr; }
    const std::vector<uint32_t>& getCacheDocNumVec() const{ return _cacheDocNumVec; }
    void setCacheDocNumVec(const std::vector<uint32_t>& cacheDocNumVec) {
        _cacheDocNumVec = cacheDocNumVec;
    }
    void setRefreshAttributes(const std::set<std::string> &refreshAttribute) {
        _refreshAttributes = refreshAttribute;
    }
    const std::set<std::string>& getRefreshAttributes() const {
        return _refreshAttributes;
    }
private:
    bool _use;
    uint32_t _currentTime;//s
    uint64_t _key;
    FilterClause *_filterClause;
    suez::turing::SyntaxExpr *_expireExpr;
    std::vector<uint32_t> _cacheDocNumVec;
    std::set<std::string> _refreshAttributes;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(SearcherCacheClause);

END_HA3_NAMESPACE(common);

#endif //ISEARCH_SEARCHERCACHECLAUSE_H
