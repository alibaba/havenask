#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/SearcherCacheClause.h>
#include <suez/turing/expression/syntax/AtomicSyntaxExpr.h>
#include <suez/turing/expression/syntax/BinarySyntaxExpr.h>

using namespace std;
using namespace autil;
using namespace suez::turing;

BEGIN_HA3_NAMESPACE(common);

class SearcherCacheClauseTest : public TESTBASE {
public:
    SearcherCacheClauseTest();
    ~SearcherCacheClauseTest();
public:
    void setUp();
    void tearDown();
protected:
    void prepareCacheClause(SearcherCacheClause &clause);
    void checkResult(const SearcherCacheClause &cacheClause, 
                     const SearcherCacheClause &cacheClause2);
    
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(common, SearcherCacheClauseTest);


SearcherCacheClauseTest::SearcherCacheClauseTest() { 
}

SearcherCacheClauseTest::~SearcherCacheClauseTest() { 
}

void SearcherCacheClauseTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void SearcherCacheClauseTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(SearcherCacheClauseTest, testSerializeAndDeserialize) { 
    HA3_LOG(DEBUG, "Begin Test!");
    
    SearcherCacheClause cacheClause;
    prepareCacheClause(cacheClause);

    autil::DataBuffer buffer;
    cacheClause.serialize(buffer);

    SearcherCacheClause cacheClause2;
    cacheClause2.deserialize(buffer);
    
    checkResult(cacheClause, cacheClause2);
}

void SearcherCacheClauseTest::checkResult(
        const SearcherCacheClause &cacheClause, 
        const SearcherCacheClause &cacheClause2)
{
    ASSERT_EQ(cacheClause.getOriginalString(), 
                         cacheClause2.getOriginalString());
    ASSERT_EQ(cacheClause.getKey(),
                         cacheClause2.getKey());
    ASSERT_EQ(cacheClause.getUse(),
                         cacheClause2.getUse());
    ASSERT_EQ(cacheClause.getCurrentTime(),
                         cacheClause2.getCurrentTime());

    FilterClause *filterClause1 = cacheClause.getFilterClause();
    FilterClause *filterClause2 = cacheClause2.getFilterClause();
    ASSERT_TRUE(filterClause2);
    ASSERT_EQ(filterClause1->getOriginalString(),
                         filterClause2->getOriginalString());

    bool ret = *(filterClause1->getRootSyntaxExpr()) == 
               filterClause2->getRootSyntaxExpr();
    ASSERT_TRUE(ret);
    ret = *(cacheClause.getExpireExpr()) == cacheClause2.getExpireExpr();
    ASSERT_TRUE(ret);
    ASSERT_EQ(cacheClause.getCacheDocNumVec().size(),
                         cacheClause2.getCacheDocNumVec().size());

    for(uint32_t i = 0; i < cacheClause.getCacheDocNumVec().size(); i++){
        ASSERT_EQ(cacheClause.getCacheDocNumVec()[i],
                             cacheClause2.getCacheDocNumVec()[i]);
    }
    ASSERT_TRUE(cacheClause.getRefreshAttributes() == 
                   cacheClause2.getRefreshAttributes());
}

void SearcherCacheClauseTest::prepareCacheClause(
        SearcherCacheClause &cacheClause) 
{
    cacheClause.setOriginalString("use:yes,key:1234,expire_time:func(end_time, 15, 10),cur_time:123456,cache_filter:expr,cache_doc_num_limit:100;3000#200;100000#3000,refresh_attributes:aa;bb");
    cacheClause.setKey(1234);
    cacheClause.setUse(true);
    cacheClause.setCurrentTime(123456);
    AtomicSyntaxExpr *leftExpr = new AtomicSyntaxExpr("type", 
            vt_unknown, ATTRIBUTE_NAME);
    AtomicSyntaxExpr *rightExpr = new AtomicSyntaxExpr("2", 
            vt_unknown, INTEGER_VALUE);
    LessSyntaxExpr *checkFilterSyntaxExpr = 
        new LessSyntaxExpr(leftExpr, rightExpr);
    FilterClause *filterClause = new FilterClause(checkFilterSyntaxExpr);
    filterClause->setOriginalString("type<2");
    cacheClause.setFilterClause(filterClause);

    SyntaxExpr *expireExpr = new AtomicSyntaxExpr("type", 
            vt_int32, ATTRIBUTE_NAME);
    cacheClause.setExpireExpr(expireExpr);
    vector<uint32_t> cacheDocNumVec;
    cacheDocNumVec.push_back(200);
    cacheDocNumVec.push_back(3000);
    cacheClause.setCacheDocNumVec(cacheDocNumVec);

    set<string> refreshAttributes;
    refreshAttributes.insert("aa");
    refreshAttributes.insert("bb");
}

END_HA3_NAMESPACE(common);

