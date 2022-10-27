#ifndef ISEARCH_SEARCHERCACHEPARSER_H
#define ISEARCH_SEARCHERCACHEPARSER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/SearcherCacheClause.h>
#include <ha3/common/ErrorResult.h>
#include <autil/StringTokenizer.h>

BEGIN_HA3_NAMESPACE(queryparser);

class SearcherCacheParser
{
public:
    SearcherCacheParser(common::ErrorResult *errorResult);
    ~SearcherCacheParser();
private:
    SearcherCacheParser(const SearcherCacheParser &);
    SearcherCacheParser& operator=(const SearcherCacheParser &);
public:
    common::SearcherCacheClause *createSearcherCacheClause();
    common::FilterClause* createFilterClause(suez::turing::SyntaxExpr *expr);
    void setSearcherCacheUse(common::SearcherCacheClause *searcherCacheClause,
                             const std::string &useStr);
    void setSearcherCacheKey(common::SearcherCacheClause *searcherCacheClause,
                             const std::string &keyStr);
    void setSearcherCacheCurTime(
            common::SearcherCacheClause *searcherCacheClause,
            const std::string &curTimeStr);
    void setSearcherCacheDocNum(
            common::SearcherCacheClause *searcherCacheClause,
            const std::string &cacheDocNumStr);
    void setSearcherCacheExpireExpr(
            common::SearcherCacheClause *searcherCacheClause,
            suez::turing::SyntaxExpr *expr);
    void setRefreshAttributes(common::SearcherCacheClause *searcherCacheClause, 
                              const std::string &refreshAttributes);
private:
    bool isOldCacheDocConfig(const autil::StringTokenizer& tokens);
    bool parseOldCacheDocConfig(const autil::StringTokenizer& tokens,         
                                std::vector<uint32_t>& cacheDocNumVec);
    bool parseOldCacheDocNum(const std::string &str, uint32_t &docNum);
    friend class SearcherCacheParserTest;
private:
    common::ErrorResult *_errorResult;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(SearcherCacheParser);

END_HA3_NAMESPACE(queryparser);

#endif //ISEARCH_SEARCHERCACHEPARSER_H
