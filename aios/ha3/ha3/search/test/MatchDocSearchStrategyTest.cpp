#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/MatchDocSearchStrategy.h>
#include <autil/mem_pool/Pool.h>

using namespace std;
USE_HA3_NAMESPACE(common);

BEGIN_HA3_NAMESPACE(search);

class MatchDocSearchStrategyTest : public TESTBASE {
public:
    MatchDocSearchStrategyTest();
    ~MatchDocSearchStrategyTest();
public:
    void setUp();
    void tearDown();
protected:
    autil::mem_pool::Pool _pool;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(search, MatchDocSearchStrategyTest);


MatchDocSearchStrategyTest::MatchDocSearchStrategyTest() {
}

MatchDocSearchStrategyTest::~MatchDocSearchStrategyTest() {
}

void MatchDocSearchStrategyTest::setUp() {
    HA3_LOG(DEBUG, "setUp!");
}

void MatchDocSearchStrategyTest::tearDown() {
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(MatchDocSearchStrategyTest, testTruncateResult) {
    HA3_LOG(DEBUG, "Begin Test!");

    Result* result = new Result;
    unique_ptr<Result> auto_result(result);
    MatchDocs *matchDocs = new MatchDocs;
    result->setMatchDocs(matchDocs);
    common::Ha3MatchDocAllocator *matchDocAllocator = new common::Ha3MatchDocAllocator(&_pool);

    for (uint32_t i = 0; i < 5; i ++) {
        matchDocs->addMatchDoc(matchDocAllocator->allocate(i));
    }

    common::Ha3MatchDocAllocatorPtr matchDocAllocatorPtr(matchDocAllocator);
    matchDocs->setMatchDocAllocator(matchDocAllocatorPtr);

    uint32_t requireTopK = 6;

    MatchDocSearchStrategy::truncateResult(requireTopK, result);
    ASSERT_EQ((uint32_t)5, result->getMatchDocs()->size());
    requireTopK = 5;
    MatchDocSearchStrategy::truncateResult(requireTopK, result);
    ASSERT_EQ((uint32_t)5, result->getMatchDocs()->size());

    requireTopK = 4;
    MatchDocSearchStrategy::truncateResult(requireTopK, result);
    ASSERT_EQ((uint32_t)4, result->getMatchDocs()->size());
    ASSERT_EQ((docid_t)3,
                         result->getMatchDocs()->getMatchDoc(3).getDocId());
}

END_HA3_NAMESPACE(search);

