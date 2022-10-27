#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include <ha3/sql/ops/scan/RangeScanIteratorWithoutFilter.h>
#include <ha3/search/LayerMetas.h>
#include <autil/mem_pool/Pool.h>
#include <matchdoc/MatchDocAllocator.h>
using namespace std;
using namespace testing;
using namespace matchdoc;
USE_HA3_NAMESPACE(search);

BEGIN_HA3_NAMESPACE(sql);

class RangeScanIteratorWithoutFilterTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
};

void RangeScanIteratorWithoutFilterTest::setUp() {
}

void RangeScanIteratorWithoutFilterTest::tearDown() {
}

TEST_F(RangeScanIteratorWithoutFilterTest, testBatchSeek1) {
    int32_t begin = 10;
    int32_t end = 100;
    autil::mem_pool::Pool pool;
    LayerMetaPtr layerMeta(new LayerMeta(&pool));
    layerMeta->push_back(DocIdRangeMeta(begin, end, end - begin + 1));
    MatchDocAllocatorPtr allocator(new MatchDocAllocator(&pool));
    IE_NAMESPACE(index)::DeletionMapReaderPtr delMapReader;
    RangeScanIteratorWithoutFilter scanIter(allocator, delMapReader, layerMeta);
    vector<MatchDoc> matchDocVec;
    int batchSize = 1;
    for (int32_t i = begin; i <= end; i++ ) {
        bool ret = scanIter.batchSeek(batchSize, matchDocVec);
        ASSERT_TRUE(!ret);
        ASSERT_EQ(1, matchDocVec.size());
        ASSERT_EQ(i, matchDocVec[0].getDocId());
        matchDocVec.clear();
    }
    bool ret = scanIter.batchSeek(batchSize, matchDocVec);
    ASSERT_TRUE(ret);
    ASSERT_EQ(0, matchDocVec.size());
}

TEST_F(RangeScanIteratorWithoutFilterTest, testBatchSeek1WithMultiRange) {
    int32_t begin = 10;
    int32_t end = 100;
    autil::mem_pool::Pool pool;
    LayerMetaPtr layerMeta(new LayerMeta(&pool));
    for (int i = begin; i <= end; i++) {
        layerMeta->push_back(DocIdRangeMeta(i, i, 1));
    }
    MatchDocAllocatorPtr allocator(new MatchDocAllocator(&pool));
    IE_NAMESPACE(index)::DeletionMapReaderPtr delMapReader;
    RangeScanIteratorWithoutFilter scanIter(allocator, delMapReader, layerMeta);
    vector<MatchDoc> matchDocVec;
    int batchSize = 1;
    for (int32_t i = begin; i <= end; i++ ) {
        bool ret = scanIter.batchSeek(batchSize, matchDocVec);
        ASSERT_TRUE(!ret);
        ASSERT_EQ(1, matchDocVec.size());
        ASSERT_EQ(i, matchDocVec[0].getDocId());
        matchDocVec.clear();
    }
    bool ret = scanIter.batchSeek(batchSize, matchDocVec);
    ASSERT_TRUE(ret);
    ASSERT_EQ(0, matchDocVec.size());
    ASSERT_FALSE(scanIter.isTimeout());
}


TEST_F(RangeScanIteratorWithoutFilterTest, testBatchSeek100) {
    int32_t begin = 10;
    int32_t end = 100;
    autil::mem_pool::Pool pool;
    LayerMetaPtr layerMeta(new LayerMeta(&pool));
    layerMeta->push_back(DocIdRangeMeta(begin, end, end - begin + 1));
    MatchDocAllocatorPtr allocator(new MatchDocAllocator(&pool));
    IE_NAMESPACE(index)::DeletionMapReaderPtr delMapReader;
    RangeScanIteratorWithoutFilter scanIter(allocator, delMapReader, layerMeta);
    vector<MatchDoc> matchDocVec;
    int batchSize = 100;
    bool ret = scanIter.batchSeek(batchSize, matchDocVec);
    ASSERT_TRUE(ret);
    ASSERT_EQ(end - begin + 1, matchDocVec.size());
    for (int32_t i = begin; i <= end; i++ ) {
        ASSERT_EQ(i, matchDocVec[i - begin].getDocId());
    }
}

TEST_F(RangeScanIteratorWithoutFilterTest, testBatchSeek100WithMultRange) {
    int32_t begin = 10;
    int32_t end = 100;
    autil::mem_pool::Pool pool;
    LayerMetaPtr layerMeta(new LayerMeta(&pool));
    for (int i = begin; i <= end; i++) {
        layerMeta->push_back(DocIdRangeMeta(i, i, 1));
    }
    MatchDocAllocatorPtr allocator(new MatchDocAllocator(&pool));
    IE_NAMESPACE(index)::DeletionMapReaderPtr delMapReader;
    RangeScanIteratorWithoutFilter scanIter(allocator, delMapReader, layerMeta);
    vector<MatchDoc> matchDocVec;
    int batchSize = 100;
    bool ret = scanIter.batchSeek(batchSize, matchDocVec);
    ASSERT_TRUE(ret);
    ASSERT_EQ(end - begin + 1, matchDocVec.size());
    for (int32_t i = begin; i <= end; i++ ) {
        ASSERT_EQ(i, matchDocVec[i - begin].getDocId());
    }
}

TEST_F(RangeScanIteratorWithoutFilterTest, testSeekTimeOut) {
    int32_t begin = 10;
    int32_t end = 100;
    autil::mem_pool::Pool pool;
    LayerMetaPtr layerMeta(new LayerMeta(&pool));
    layerMeta->push_back(DocIdRangeMeta(begin, end, end - begin + 1));
    MatchDocAllocatorPtr allocator(new MatchDocAllocator(&pool));
    IE_NAMESPACE(index)::DeletionMapReaderPtr delMapReader;
    int64_t startTime = autil::TimeUtility::currentTime();
    common::TimeoutTerminator timeoutTerminator(1, startTime);
    usleep(1000);
    RangeScanIteratorWithoutFilter scanIter(allocator, delMapReader, layerMeta, &timeoutTerminator);
    vector<MatchDoc> matchDocVec;
    int batchSize = 1;
    bool ret = scanIter.batchSeek(batchSize, matchDocVec);
    ASSERT_TRUE(ret);
    ASSERT_EQ(0, matchDocVec.size());
    ASSERT_TRUE(scanIter.isTimeout());
}

END_HA3_NAMESPACE();

