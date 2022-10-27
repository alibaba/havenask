#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/Ha3MatchDocAllocator.h>
#include <ha3/search/MatchDataFetcher.h>
#include <matchdoc/Trait.h>

using namespace std;

USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(rank);

BEGIN_HA3_NAMESPACE(search);

class MatchDataFetcherTest : public TESTBASE {
public:
    MatchDataFetcherTest() {};
    ~MatchDataFetcherTest() {};
public:
    void setUp() {};
    void tearDown() {};
protected:
    autil::mem_pool::Pool _pool;
protected:
    HA3_LOG_DECLARE();
};

class MockMatchDataFetcher : public MatchDataFetcher {
public:
    matchdoc::ReferenceBase *require(common::Ha3MatchDocAllocator *allocator,
            const std::string &refName, uint32_t termCount) override {
        return NULL;
    }

    IE_NAMESPACE(common)::ErrorCode fillMatchData(
            const SingleLayerExecutors &singleLayerExecutors,
            matchdoc::MatchDoc matchDoc,
            matchdoc::MatchDoc subDoc) const override
    {
        return IE_NAMESPACE(common)::ErrorCode::OK;
    }
};

class MockMatchData
{
public:
    static uint32_t sizeOf(uint32_t termCount) {
        return 4;
    }
};

TEST_F(MatchDataFetcherTest, testCreateReference) {
    {
        MockMatchDataFetcher mockFetcher;
        common::Ha3MatchDocAllocator allocator(&_pool);
        mockFetcher.createReference<MockMatchData>(&allocator, "mockRef", 10);
        allocator.allocate(1);
        auto fieldGroupPair = allocator.findGroup(common::HA3_MATCHDATA_GROUP);
        ASSERT_TRUE(fieldGroupPair.first != NULL);
        ASSERT_TRUE(fieldGroupPair.second);
        auto matchDataRef = fieldGroupPair.first->findReferenceWithoutType(
                "mockRef");
        ASSERT_TRUE(matchDataRef != NULL);
    }
    {
        MockMatchDataFetcher mockFetcher;
        common::Ha3MatchDocAllocator allocator(&_pool);
        mockFetcher.createSubReference<MockMatchData>(&allocator, "mockRef", 10);
        auto matchdoc = allocator.allocate(0);
        allocator.allocateSub(matchdoc, 1);
        auto fieldGroupPair = allocator._subAllocator->findGroup(
                common::HA3_SUBMATCHDATA_GROUP);
        ASSERT_TRUE(fieldGroupPair.first != NULL);
        ASSERT_TRUE(fieldGroupPair.second);
        auto matchDataRef = fieldGroupPair.first->findReferenceWithoutType(
                "mockRef");
        ASSERT_TRUE(matchDataRef != NULL);
    }
}

END_HA3_NAMESPACE(search);

NOT_SUPPORT_CLONE_TYPE(isearch::search::MockMatchData);
NOT_SUPPORT_SERIZLIE_TYPE(isearch::search::MockMatchData);
DECLARE_NO_NEED_DESTRUCT_TYPE(isearch::search::MockMatchData);
