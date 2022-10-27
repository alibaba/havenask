#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/queryparser/ClauseParserContext.h>

using namespace std;
using namespace suez::turing;
USE_HA3_NAMESPACE(common);
BEGIN_HA3_NAMESPACE(queryparser);

class SearcherCacheBisonParserTest : public TESTBASE {
public:
    SearcherCacheBisonParserTest();
    ~SearcherCacheBisonParserTest();
public:
    void setUp();
    void tearDown();
protected:
    ClauseParserContext *_ctx;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(queryparser, SearcherCacheBisonParserTest);


SearcherCacheBisonParserTest::SearcherCacheBisonParserTest() { 
}

SearcherCacheBisonParserTest::~SearcherCacheBisonParserTest() { 
}

void SearcherCacheBisonParserTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
    _ctx = new ClauseParserContext;
}

void SearcherCacheBisonParserTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
    delete _ctx;
}

TEST_F(SearcherCacheBisonParserTest, testSimpleProcess) { 
    HA3_LOG(DEBUG, "Begin Test!");
    
    string searcherCacheClauseStr = 
        "use:yes,"
        "key:123456,"
        "expire_time:func(end_time, 15, 10),"
        "cur_time:123456,"
        "cache_filter:olu=yes,"
        "cache_doc_num_limit:200;700,"
        "refresh_attributes:price;post;price";
    
    bool ret = _ctx->parseSearcherCacheClause(searcherCacheClauseStr.c_str());
    ASSERT_TRUE(ret);
    ASSERT_EQ(ERROR_NONE, _ctx->getErrorResult().getErrorCode());
    
    SearcherCacheClause *cacheClause = _ctx->stealSearcherCacheClause();
    ASSERT_TRUE(cacheClause);
    unique_ptr<SearcherCacheClause> cacheClause_ptr(cacheClause);

    ASSERT_TRUE(cacheClause->getUse());
    ASSERT_EQ((uint64_t)123456, cacheClause->getKey());
    ASSERT_EQ((uint32_t)123456, cacheClause->getCurrentTime());

    SyntaxExpr *expireTimeExpr = cacheClause->getExpireExpr();
    ASSERT_TRUE(expireTimeExpr);
    ASSERT_EQ(string("func(end_time , 15 , 10)"), 
                         expireTimeExpr->getExprString());
    
    FilterClause *filterClause = cacheClause->getFilterClause();
    ASSERT_TRUE(filterClause);
    ASSERT_TRUE(filterClause->getRootSyntaxExpr());
    ASSERT_EQ(string("(olu=yes)"), 
                         filterClause->getRootSyntaxExpr()->getExprString());
    ASSERT_EQ((size_t)2, cacheClause->getCacheDocNumVec().size());
    ASSERT_EQ((uint32_t)200, cacheClause->getCacheDocNumVec()[0]);
    ASSERT_EQ((uint32_t)700, cacheClause->getCacheDocNumVec()[1]);

    const set<string> &refreshAttributes = cacheClause->getRefreshAttributes();
    ASSERT_EQ(size_t(2), refreshAttributes.size());
    ASSERT_TRUE(refreshAttributes.find("price") != refreshAttributes.end());
    ASSERT_TRUE(refreshAttributes.find("post") != refreshAttributes.end());

}

TEST_F(SearcherCacheBisonParserTest, testParseSearcherCacheClauseSeperate) { 
    HA3_LOG(DEBUG, "Begin Test!");

    const char* clauseStrs[] = {
        "use:yes",
        "key:123456",
        "expire_time:func(end_time, 15, 10)",
        "cur_time:123456",
        "cache_filter:olu=yes",
        "cache_doc_num_limit:100;3000#200;100000#3000",
        "cache_doc_num_limit:100;200;300"
    };
    
    for (int i = 0; i < (int)(sizeof(clauseStrs) / sizeof(char*)); i++) {
        const char *clause = clauseStrs[i];
        ClauseParserContext ctx;
        ASSERT_TRUE(ctx.parseSearcherCacheClause(clause))<<string(clause);
        ASSERT_EQ(ERROR_NONE, ctx.getErrorResult().getErrorCode())<<string(clause);
    }

}

TEST_F(SearcherCacheBisonParserTest, testParseEmptyClauseStr) { 
    HA3_LOG(DEBUG, "Begin Test!");
    
    string searcherCacheClauseStr = "";
    bool ret = _ctx->parseSearcherCacheClause(searcherCacheClauseStr.c_str());
    ASSERT_TRUE(!ret);
    ASSERT_EQ(ERROR_CACHE_CLAUSE, _ctx->getErrorResult().getErrorCode());
}

TEST_F(SearcherCacheBisonParserTest, testInvalidSearcherCacheClause) {
    HA3_LOG(DEBUG, "Begin Test!");
    
    const char* clauseStrs[] = {
        "use:yes,", // incomplete clause
        "use_wrong:yes",
        "use:none",
        "key_wrong:123456",
        "key:aa",
        "expire_time_wrong:func(end_time, 15, 10)",
        "expire_time:func(end_time, 15, 10",
        "cur_time_wrong:123456",
        "cur_time:123456aa",
        "cache_filter_wrong:olu=yes",
        "cache_filter:",
        "cache_doc_num_limit_wrong:100;3000#200;100000#3000",
        "cache_doc_num_limit:",
        "refresh_attributes:",
    };
    
    for (int i = 0; i < (int)(sizeof(clauseStrs) / sizeof(char*)); i++) {
        const char *clause = clauseStrs[i];
        ClauseParserContext ctx;
        ASSERT_TRUE(!ctx.parseSearcherCacheClause(clause))<<clause;
        ASSERT_EQ(ERROR_CACHE_CLAUSE, ctx.getErrorResult().getErrorCode())<<clause;
    }
}

END_HA3_NAMESPACE(queryparser);

