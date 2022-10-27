#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/queryparser/SearcherCacheParser.h>
#include <ha3/common/ErrorDefine.h>

using namespace std;
USE_HA3_NAMESPACE(common);

BEGIN_HA3_NAMESPACE(queryparser);

class SearcherCacheParserTest : public TESTBASE {
public:
    SearcherCacheParserTest();
    ~SearcherCacheParserTest();
public:
    void setUp();
    void tearDown();
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(queryparser, SearcherCacheParserTest);


SearcherCacheParserTest::SearcherCacheParserTest() { 
}

SearcherCacheParserTest::~SearcherCacheParserTest() { 
}

void SearcherCacheParserTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void SearcherCacheParserTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(SearcherCacheParserTest, testSetSearcherCacheUse) { 
    HA3_LOG(DEBUG, "Begin Test!");
    ErrorResult errResult;
    SearcherCacheParser cacheParser(&errResult);
    SearcherCacheClause cacheClause;
    cacheParser.setSearcherCacheUse(&cacheClause, "yes");
    ASSERT_EQ(ERROR_NONE, errResult.getErrorCode());    
    ASSERT_TRUE(cacheClause.getUse());

    cacheParser.setSearcherCacheUse(&cacheClause, "no");
    ASSERT_EQ(ERROR_NONE,errResult.getErrorCode());    
    ASSERT_TRUE(!cacheClause.getUse());

    cacheParser.setSearcherCacheUse(&cacheClause, "none");
    ASSERT_EQ(ERROR_CACHE_CLAUSE, errResult.getErrorCode());    
}

TEST_F(SearcherCacheParserTest, testSetSearcherCacheKey) { 
    HA3_LOG(DEBUG, "Begin Test!");
    ErrorResult errResult;
    SearcherCacheParser cacheParser(&errResult);
    SearcherCacheClause cacheClause;
    cacheParser.setSearcherCacheKey(&cacheClause, "123");
    ASSERT_EQ(ERROR_NONE, errResult.getErrorCode());    
    ASSERT_EQ((uint64_t)123, cacheClause.getKey());

    cacheParser.setSearcherCacheKey(&cacheClause, "123aaa");
    ASSERT_EQ(ERROR_CACHE_CLAUSE, errResult.getErrorCode()); 

    cacheParser.setSearcherCacheKey(&cacheClause, "-123");
    ASSERT_EQ(ERROR_CACHE_CLAUSE, errResult.getErrorCode()); 

    cacheParser.setSearcherCacheKey(&cacheClause, "");
    ASSERT_EQ(ERROR_CACHE_CLAUSE,errResult.getErrorCode());    

    cacheParser.setSearcherCacheKey(&cacheClause, "23.3");
    ASSERT_EQ(ERROR_CACHE_CLAUSE, errResult.getErrorCode());    

    cacheParser.setSearcherCacheKey(&cacheClause, "seing");
    ASSERT_EQ(ERROR_CACHE_CLAUSE, errResult.getErrorCode());    
}

TEST_F(SearcherCacheParserTest, testSetSearcherCacheCurTime) { 
    HA3_LOG(DEBUG, "Begin Test!");
    ErrorResult errResult;
    SearcherCacheParser cacheParser(&errResult);
    SearcherCacheClause cacheClause;
    cacheParser.setSearcherCacheCurTime(&cacheClause, "123");
    ASSERT_EQ(ERROR_NONE, errResult.getErrorCode());    
    ASSERT_EQ((uint32_t)123, cacheClause.getCurrentTime());

    cacheParser.setSearcherCacheCurTime(&cacheClause, "123aa");
    ASSERT_EQ(ERROR_CACHE_CLAUSE, errResult.getErrorCode()); 

    cacheParser.setSearcherCacheCurTime(&cacheClause, "-123");
    ASSERT_EQ(ERROR_CACHE_CLAUSE, errResult.getErrorCode()); 

    cacheParser.setSearcherCacheCurTime(&cacheClause, "");
    ASSERT_EQ(ERROR_CACHE_CLAUSE,errResult.getErrorCode());    

    cacheParser.setSearcherCacheCurTime(&cacheClause, "23.3");
    ASSERT_EQ(ERROR_CACHE_CLAUSE, errResult.getErrorCode());    

    cacheParser.setSearcherCacheCurTime(&cacheClause, "seing");
    ASSERT_EQ(ERROR_CACHE_CLAUSE, errResult.getErrorCode());    
}

TEST_F(SearcherCacheParserTest, testSetSearcherCacheDocNum) {
    ErrorResult errResult;
    SearcherCacheParser cacheParser(&errResult);
    SearcherCacheClause cacheClause;
    cacheParser.setSearcherCacheDocNum(&cacheClause, "123");
    ASSERT_EQ(ERROR_NONE, errResult.getErrorCode());    
    ASSERT_EQ((size_t)1, cacheClause.getCacheDocNumVec().size());
    ASSERT_EQ((uint32_t)123, cacheClause.getCacheDocNumVec()[0]);

    cacheParser.setSearcherCacheDocNum(&cacheClause, "123;");
    ASSERT_EQ(ERROR_NONE, errResult.getErrorCode());    
    ASSERT_EQ((size_t)1, cacheClause.getCacheDocNumVec().size());
    ASSERT_EQ((uint32_t)123, cacheClause.getCacheDocNumVec()[0]);

    cacheParser.setSearcherCacheDocNum(&cacheClause, ";123;;");
    ASSERT_EQ(ERROR_NONE, errResult.getErrorCode());    
    ASSERT_EQ((size_t)1, cacheClause.getCacheDocNumVec().size());
    ASSERT_EQ((uint32_t)123, cacheClause.getCacheDocNumVec()[0]);

    cacheParser.setSearcherCacheDocNum(&cacheClause, 
            "3000;1000;2000;");
    ASSERT_EQ(ERROR_NONE, errResult.getErrorCode());    
    ASSERT_EQ((size_t)3, cacheClause.getCacheDocNumVec().size());
    ASSERT_EQ((uint32_t)1000, cacheClause.getCacheDocNumVec()[0]);
    ASSERT_EQ((uint32_t)2000, cacheClause.getCacheDocNumVec()[1]);
    ASSERT_EQ((uint32_t)3000, cacheClause.getCacheDocNumVec()[2]);

    cacheParser.setSearcherCacheDocNum(&cacheClause, "");
    ASSERT_EQ(ERROR_CACHE_CLAUSE, errResult.getErrorCode()); 
}

TEST_F(SearcherCacheParserTest, testOldSetSearcherCacheDocNum) {
    HA3_LOG(DEBUG, "Begin Test!");
    ErrorResult errResult;
    SearcherCacheParser cacheParser(&errResult);
    SearcherCacheClause cacheClause;
    cacheParser.setSearcherCacheDocNum(&cacheClause, 
            "123;3000#200;100000#3000;");
    ASSERT_EQ(ERROR_NONE, errResult.getErrorCode());    
    ASSERT_EQ((size_t)2, cacheClause.getCacheDocNumVec().size());
    ASSERT_EQ((uint32_t)200, cacheClause.getCacheDocNumVec()[0]);
    ASSERT_EQ((uint32_t)3000, cacheClause.getCacheDocNumVec()[1]);

    cacheParser.setSearcherCacheDocNum(&cacheClause, 
            "123;3000aa#200;10000#3000;");
    ASSERT_EQ(ERROR_CACHE_CLAUSE, errResult.getErrorCode());

    cacheParser.setSearcherCacheDocNum(&cacheClause, 
            "123;3000#200aa;10000#3000;");
    ASSERT_EQ(ERROR_CACHE_CLAUSE, errResult.getErrorCode());

    cacheParser.setSearcherCacheDocNum(&cacheClause, 
            "123;3000#200;10000aa#3000;");
    ASSERT_EQ(ERROR_CACHE_CLAUSE, errResult.getErrorCode());
 
    cacheParser.setSearcherCacheDocNum(&cacheClause, 
            "123;3000#200;10000#3000aa;");
    ASSERT_EQ(ERROR_CACHE_CLAUSE, errResult.getErrorCode());
}

END_HA3_NAMESPACE(queryparser);

