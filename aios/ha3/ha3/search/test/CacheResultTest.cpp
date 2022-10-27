#include<unittest/unittest.h>
#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/CacheResult.h>

using namespace std;
USE_HA3_NAMESPACE(util);
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(index);

BEGIN_HA3_NAMESPACE(search);

class CacheResultTest : public TESTBASE {
public:
    CacheResultTest();
    ~CacheResultTest();
public:
    void setUp();
    void tearDown();
protected:
    autil::mem_pool::Pool _pool;
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(search, CacheResultTest);


CacheResultTest::CacheResultTest() { 
}

CacheResultTest::~CacheResultTest() { 
}

void CacheResultTest::setUp() { 
    HA3_LOG(DEBUG, "setUp!");
}

void CacheResultTest::tearDown() { 
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(CacheResultTest, testSerializeAndDeserialize) { 
    HA3_LOG(DEBUG, "Begin Test!");

    CacheResult cacheResult;
    CacheHeader *header = cacheResult.getHeader();
    header->expireTime = 3;
    header->minScoreFilter.addMinScore(1.1);

    PartitionInfoHint partInfoHint;
    partInfoHint.lastIncSegmentId = 1;
    partInfoHint.lastRtSegmentId = 2;
    partInfoHint.lastRtSegmentDocCount = 3;
    cacheResult.setPartInfoHint(partInfoHint);

    vector<globalid_t> gids;
    gids.push_back(1);
    gids.push_back(2);
    cacheResult.setGids(gids);
    
    cacheResult.setTruncated(true);
    cacheResult.setUseTruncateOptimizer(true);
    
    autil::DataBuffer dataBuffer;
    cacheResult.serialize(dataBuffer);

    CacheResult cacheResult2;
    cacheResult2.deserialize(dataBuffer, &_pool);
    
    ASSERT_TRUE(!cacheResult2.getResult());
    header = cacheResult2.getHeader();
    ASSERT_EQ((uint32_t)3, header->expireTime);
    ASSERT_DOUBLE_EQ((score_t)1.1, header->minScoreFilter.getMinScore(0));

    ASSERT_EQ((segmentid_t)1, 
                         cacheResult2.getPartInfoHint().lastIncSegmentId);
    ASSERT_EQ((segmentid_t)2, 
                         cacheResult2.getPartInfoHint().lastRtSegmentId);
    ASSERT_EQ((uint32_t)3, 
                         cacheResult2.getPartInfoHint().lastRtSegmentDocCount);
    const vector<globalid_t> &gids2 = cacheResult2.getGids();
    ASSERT_EQ((size_t)2, gids2.size());
    ASSERT_EQ((globalid_t)1, gids2[0]);
    ASSERT_EQ((globalid_t)2, gids2[1]);
    ASSERT_TRUE(cacheResult2.isTruncated());
    ASSERT_TRUE(cacheResult2.useTruncateOptimizer());
}

TEST_F(CacheResultTest, testSerializeAndDeserializeWithResult) {
    HA3_LOG(DEBUG, "Begin Test!");

    CacheResult cacheResult;
    CacheHeader *header = cacheResult.getHeader();
    header->expireTime = 3;
    header->minScoreFilter.addMinScore(1.1);

    Result *result = new Result(new MatchDocs);
    MatchDocs *matchDocs = result->getMatchDocs();
    common::Ha3MatchDocAllocatorPtr matchDocAllocatorPtr(new common::Ha3MatchDocAllocator(&_pool));
    matchdoc::Reference<int32_t> *intRef =
        matchDocAllocatorPtr->declare<int32_t>("int32", false, true);
    matchdoc::Reference<int64_t> *longRef =
        matchDocAllocatorPtr->declare<int64_t>("int64", false, true);
    matchDocs->setMatchDocAllocator(matchDocAllocatorPtr);

    matchdoc::MatchDoc matchDoc = matchDocAllocatorPtr->allocate(1);
    intRef->set(matchDoc, 1);
    longRef->set(matchDoc, 2);
    matchDocs->addMatchDoc(matchDoc);

    matchDoc = matchDocAllocatorPtr->allocate(2);
    intRef->set(matchDoc, 3);
    longRef->set(matchDoc, 4);
    matchDocs->addMatchDoc(matchDoc);

    vector<globalid_t> gids;
    gids.push_back(1);
    gids.push_back(2);
    cacheResult.setGids(gids);

    result->setTotalMatchDocs(100);
    result->setActualMatchDocs(90);
    cacheResult.setResult(result);

    autil::DataBuffer dataBuffer;
    cacheResult.serialize(dataBuffer);

    CacheResult cacheResult2;
    cacheResult2.deserialize(dataBuffer, &_pool);

    header = cacheResult2.getHeader();
    ASSERT_EQ((uint32_t)3, header->expireTime);
    ASSERT_DOUBLE_EQ((score_t)1.1, header->minScoreFilter.getMinScore(0));

    Result *result2 = cacheResult2.getResult();
    ASSERT_TRUE(result2);
    ASSERT_EQ((uint32_t)100, result2->getTotalMatchDocs());
    ASSERT_EQ((uint32_t)90, result2->getActualMatchDocs());

    MatchDocs *matchDocs2 = result2->getMatchDocs();
    ASSERT_TRUE(matchDocs2);
    ASSERT_EQ((uint32_t)2, matchDocs2->size());

    const auto &allocatorPtr = matchDocs->getMatchDocAllocator();
    ASSERT_TRUE(allocatorPtr != NULL);
    matchdoc::Reference<int32_t> *intRef2 =
        allocatorPtr->findReference<int32_t>("int32");
    ASSERT_TRUE(intRef2);
    matchdoc::Reference<int64_t> *longRef2 =
        allocatorPtr->findReference<int64_t>("int64");
    ASSERT_TRUE(longRef2);

    matchDoc = matchDocs->getMatchDoc(0);
    ASSERT_TRUE(matchdoc::INVALID_MATCHDOC != matchDoc);
    ASSERT_EQ((docid_t)1, matchDoc.getDocId());
    ASSERT_EQ((int32_t)1,
                         *intRef2->getPointer(matchDoc));
    ASSERT_EQ((int64_t)2,
                         *longRef2->getPointer(matchDoc));

    matchDoc = matchDocs->getMatchDoc(1);
    ASSERT_TRUE(matchdoc::INVALID_MATCHDOC != matchDoc);
    ASSERT_EQ((docid_t)2, matchDoc.getDocId());
    ASSERT_EQ((int32_t)3,
                         *intRef2->getPointer(matchDoc));
    ASSERT_EQ((int64_t)4,
                         *longRef2->getPointer(matchDoc));
}

END_HA3_NAMESPACE(search);

