#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include <ha3/sql/ops/scan/Ha3ScanIterator.h>
#include <ha3/search/LayerMetas.h>
#include <autil/mem_pool/Pool.h>
#include <ha3/sql/ops/scan/RangeQueryExecutor.h>
#include <matchdoc/MatchDocAllocator.h>
using namespace std;
using namespace testing;
using namespace matchdoc;
USE_HA3_NAMESPACE(search);

BEGIN_HA3_NAMESPACE(sql);

class Ha3ScanIteratorTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
};

void Ha3ScanIteratorTest::setUp() {
}

void Ha3ScanIteratorTest::tearDown() {
}

TEST_F(Ha3ScanIteratorTest, testBatchSeek1) {
    int32_t begin = 10;
    int32_t end = 100;
    autil::mem_pool::Pool pool;
    LayerMetaPtr layerMeta(new LayerMeta(&pool));
    layerMeta->push_back(DocIdRangeMeta(begin, end, end - begin + 1));
    QueryExecutorPtr queryExecutor(new RangeQueryExecutor(layerMeta.get()));
    MatchDocAllocatorPtr allocator(new MatchDocAllocator(&pool));
    Ha3ScanIteratorParam param;
    param.queryExecutor = queryExecutor;
    param.matchDocAllocator = allocator;
    param.layerMeta = layerMeta;
    Ha3ScanIterator scanIter(param);
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

TEST_F(Ha3ScanIteratorTest, testBatchSeek100) {
    int32_t begin = 10;
    int32_t end = 100;
    autil::mem_pool::Pool pool;
    LayerMetaPtr layerMeta(new LayerMeta(&pool));
    layerMeta->push_back(DocIdRangeMeta(begin, end, end - begin + 1));
    QueryExecutorPtr queryExecutor(new RangeQueryExecutor(layerMeta.get()));
    MatchDocAllocatorPtr allocator(new MatchDocAllocator(&pool));
    Ha3ScanIteratorParam param;
    param.queryExecutor = queryExecutor;
    param.matchDocAllocator = allocator;
    param.layerMeta = layerMeta;
    Ha3ScanIterator scanIter(param);
    vector<MatchDoc> matchDocVec;
    int batchSize = 100;
    bool ret = scanIter.batchSeek(batchSize, matchDocVec);
    ASSERT_TRUE(ret);
    ASSERT_EQ(end - begin + 1 , matchDocVec.size());
    for (int32_t i = begin; i <= end; i++ ) {
        ASSERT_EQ(i, matchDocVec[i - begin].getDocId());
    }

}

END_HA3_NAMESPACE();

