#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/config/SearcherCacheConfig.h>
#include <autil/legacy/jsonizable.h>
BEGIN_HA3_NAMESPACE(config);

class SearcherCacheConfigTest : public TESTBASE {
public:
    SearcherCacheConfigTest();
    ~SearcherCacheConfigTest();
public:
    void setUp();
    void tearDown();
protected:
    HA3_LOG_DECLARE();
};

using namespace std;
HA3_LOG_SETUP(config, SearcherCacheConfigTest);


SearcherCacheConfigTest::SearcherCacheConfigTest() { 
}

SearcherCacheConfigTest::~SearcherCacheConfigTest() { 
}

void SearcherCacheConfigTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void SearcherCacheConfigTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(SearcherCacheConfigTest, testSimpleProcess) { 
    HA3_LOG(DEBUG, "Begin Test!");
    string defaultCacheConfig ="{ \
        \"max_size\" : 1024,                    \
        \"max_item_num\" : 200000,              \
        \"inc_doc_limit\" : 1000,               \
        \"inc_deletion_percent\" : 100,         \
        \"latency_limit\" : 0,                   \
        \"min_allowed_cache_doc_num\" : 10,                   \
        \"max_allowed_cache_doc_num\" : 100                   \
    }";
    SearcherCacheConfig searchCacheConfig;
    FromJsonString(searchCacheConfig, defaultCacheConfig);
    ASSERT_EQ((uint32_t)1024, searchCacheConfig.maxSize);
    ASSERT_EQ((uint32_t)200000, searchCacheConfig.maxItemNum);
    ASSERT_EQ((uint32_t)1000, searchCacheConfig.incDocLimit);
    ASSERT_EQ((uint32_t)100, searchCacheConfig.incDeletionPercent);
    ASSERT_EQ((float)0.0, searchCacheConfig.latencyLimitInMs);
    ASSERT_EQ((uint32_t)10, searchCacheConfig.minAllowedCacheDocNum);
    ASSERT_EQ((uint32_t)100, searchCacheConfig.maxAllowedCacheDocNum);
}

END_HA3_NAMESPACE(config);

