#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/CacheMissSearchStrategy.h>
#include <ha3_sdk/testlib/index/FakeIndexPartitionReader.h>
#include <ha3/common/Result.h>
#include <ha3/common/MatchDocs.h>
#include <ha3_sdk/testlib/index/FakeIndexPartitionReaderCreator.h>
#include <ha3/search/CacheResult.h>

using namespace std;
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(index_base);
USE_HA3_NAMESPACE(common);

BEGIN_HA3_NAMESPACE(search);

class CacheMissSearchStrategyTest : public TESTBASE {
public:
    CacheMissSearchStrategyTest();
    ~CacheMissSearchStrategyTest();
public:
    void setUp();
    void tearDown();
protected:
    static IndexPartitionReaderWrapperPtr makeFakeIndexPartReader();
protected:
    autil::mem_pool::Pool _pool;
    common::Ha3MatchDocAllocator *_allocator;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(search, CacheMissSearchStrategyTest);


CacheMissSearchStrategyTest::CacheMissSearchStrategyTest() {
}

CacheMissSearchStrategyTest::~CacheMissSearchStrategyTest() {
}

void CacheMissSearchStrategyTest::setUp() {
    HA3_LOG(DEBUG, "setUp!");
    _allocator = new common::Ha3MatchDocAllocator(&_pool);
}

void CacheMissSearchStrategyTest::tearDown() {
    HA3_LOG(DEBUG, "tearDown!");
    delete _allocator;
}

TEST_F(CacheMissSearchStrategyTest, testFillGids) {
    MatchDocs * matchDocs = new MatchDocs;
    Result * result = new Result;
    unique_ptr<Result> result_ptr(result);
    PartitionInfoPtr partInfoPtr;
    vector<globalid_t> gids;
    CacheMissSearchStrategy::fillGids(result, partInfoPtr, gids);
    ASSERT_EQ((size_t)0, gids.size());

    result->setMatchDocs(matchDocs);
    partInfoPtr.reset(new PartitionInfo);
    CacheMissSearchStrategy::fillGids(result, partInfoPtr, gids);
    ASSERT_EQ((size_t)0, gids.size());

    vector<matchdoc::MatchDoc> matchDocVect;
    size_t docCount = 10;
    for (size_t i = 0; i < docCount; i++) {
        matchdoc::MatchDoc matchDoc = _allocator->allocate(i);
        matchDocVect.push_back(matchDoc);
        matchDocs->addMatchDoc(matchDoc);
    }
    PartitionInfoHint infoHint;
    infoHint.lastIncSegmentId = 0;
    partInfoPtr->SetPartitionInfoHint(infoHint);
    vector<docid_t> baseDocIds;
    baseDocIds.push_back(0);
    partInfoPtr->SetBaseDocIds(baseDocIds);
    Version version;
    version.AddSegment(0);
    partInfoPtr->SetVersion(version);

    CacheMissSearchStrategy::fillGids(result, partInfoPtr, gids);
    ASSERT_EQ(docCount, gids.size());
}

TEST_F(CacheMissSearchStrategyTest, testConstructCacheResult) {
    HA3_LOG(DEBUG, "Begin Test!");

    Result* result = new Result;
    MatchDocs *matchDocs = new MatchDocs;
    result->setMatchDocs(matchDocs);
    result->setTotalMatchDocs(100);
    result->setActualMatchDocs(100);
    common::Ha3MatchDocAllocator *matchDocAllocator = new common::Ha3MatchDocAllocator(&_pool);

    for (uint32_t i = 0; i < 5; i ++) {
        matchDocs->addMatchDoc(matchDocAllocator->allocate(i));
    }

    common::Ha3MatchDocAllocatorPtr matchDocAllocatorPtr(matchDocAllocator);
    matchDocs->setMatchDocAllocator(matchDocAllocatorPtr);

    CacheMinScoreFilter minScoreFilter;
    minScoreFilter.addMinScore(1.0);
    CacheResult *cacheResult = CacheMissSearchStrategy::constructCacheResult(
            result, minScoreFilter,
            makeFakeIndexPartReader());
    unique_ptr<CacheResult> auto_cacheResult(cacheResult);
    ASSERT_DOUBLE_EQ((score_t)1.0,
                     cacheResult->getHeader()->minScoreFilter.getMinScore(0));
    ASSERT_TRUE(cacheResult->getResult());

    vector<globalid_t> gids = cacheResult->getGids();
    ASSERT_EQ((size_t)5, gids.size());
    ASSERT_EQ((globalid_t)0, gids[0]);
    ASSERT_EQ((globalid_t)1, gids[1]);
    ASSERT_EQ((globalid_t)2, gids[2]);
    ASSERT_EQ((globalid_t)3, gids[3]);
    ASSERT_EQ((globalid_t)4, gids[4]);
}

IndexPartitionReaderWrapperPtr
CacheMissSearchStrategyTest::makeFakeIndexPartReader()
{
    FakeIndex fakeIndex;
    fakeIndex.indexes["phrase"] =
        "with:0[1];1[1];2[2];3[3];4[4];5[5];6[6];7[1];8[8];9[9];\n";
    fakeIndex.attributes =
        "price:uint64_t:1, 2, 3, 4, 5, 6, 7, 8, 9, 10;"
        "attr_expire_time:uint32_t:3, 10, 11, 4, 5, 6, 7, 8, 9, 12;"
        "attr_sort:uint32_t:110, 111, 113, 114, 115, 116, 117, 118, 119, 112;"
        "attr_sort2:uint32_t:1, 2, 3, 4, 5, 6, 7, 8, 9, 12;";

    return FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
}

END_HA3_NAMESPACE(search);
