#include <unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/SubSimpleMatchDataFetcher.h>
#include <ha3/common/Ha3MatchDocAllocator.h>

using namespace std;
USE_HA3_NAMESPACE(common);
BEGIN_HA3_NAMESPACE(search);

class SubSimpleMatchDataFetcherTest : public TESTBASE {
public:
    SubSimpleMatchDataFetcherTest() {};
    ~SubSimpleMatchDataFetcherTest() {};
public:
    void setUp() {};
    void tearDown() {};
protected:
    autil::mem_pool::Pool _pool;
protected:
    HA3_LOG_DECLARE();
};

TEST_F(SubSimpleMatchDataFetcherTest, testRequire) {
    Ha3MatchDocAllocator allocator(&_pool);
    SubSimpleMatchDataFetcher fetcher;
    ASSERT_TRUE(fetcher.require(&allocator, SUB_SIMPLE_MATCH_DATA_REF, 10)
                != NULL);
    auto matchdoc = allocator.allocate(0);
    allocator.allocateSub(matchdoc, 1);
    auto fieldGroupPair = allocator._subAllocator->findGroup(
            HA3_SUBMATCHDATA_GROUP);
    ASSERT_TRUE(fieldGroupPair.first != NULL);
    ASSERT_TRUE(fieldGroupPair.second);
    auto matchDataRef = fieldGroupPair.first->findReferenceWithoutType(
            SUB_SIMPLE_MATCH_DATA_REF);
    ASSERT_TRUE(matchDataRef != NULL);
}

END_HA3_NAMESPACE(search);
