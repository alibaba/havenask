#include "sql/ops/scan/Ha3ScanIterator.h"

#include <iosfwd>
#include <memory>
#include <stddef.h>
#include <stdint.h>
#include <vector>

#include "autil/result/Result.h"
#include "ha3/search/Filter.h"
#include "ha3/search/LayerMetas.h"
#include "ha3/search/QueryExecutor.h"
#include "ha3/search/RangeQueryExecutor.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/Trait.h"
#include "navi/util/NaviTestPool.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace matchdoc;
using namespace isearch::search;

namespace sql {

class Ha3ScanIteratorTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
};

void Ha3ScanIteratorTest::setUp() {}

void Ha3ScanIteratorTest::tearDown() {}

TEST_F(Ha3ScanIteratorTest, testBatchSeek0) {
    int32_t begin = 10;
    int32_t end = 100;
    autil::mem_pool::PoolAsan pool;
    LayerMetaPtr layerMeta(new LayerMeta(&pool));
    layerMeta->push_back(DocIdRangeMeta(begin, end, DocIdRangeMeta::OT_UNKNOWN, end - begin + 1));
    QueryExecutorPtr queryExecutor(new RangeQueryExecutor(layerMeta.get()));
    MatchDocAllocatorPtr allocator(new MatchDocAllocator(&pool));
    Ha3ScanIteratorParam param;
    param.queryExecutors = {queryExecutor};
    param.matchDocAllocator = allocator;
    param.layerMetas = {layerMeta};
    Ha3ScanIterator scanIter(param);
    vector<MatchDoc> matchDocVec;
    size_t batchSize = 0;
    bool ret = scanIter.batchSeek(batchSize, matchDocVec).unwrap();
    ASSERT_TRUE(ret);
    ASSERT_EQ(end - begin + 1, matchDocVec.size());
    for (int32_t i = begin; i <= end; i++) {
        ASSERT_EQ(i, matchDocVec[i - begin].getDocId());
    }
}

TEST_F(Ha3ScanIteratorTest, testBatchSeek1) {
    int32_t begin = 10;
    int32_t end = 100;
    autil::mem_pool::PoolAsan pool;
    LayerMetaPtr layerMeta(new LayerMeta(&pool));
    layerMeta->push_back(DocIdRangeMeta(begin, end, DocIdRangeMeta::OT_UNKNOWN, end - begin + 1));
    QueryExecutorPtr queryExecutor(new RangeQueryExecutor(layerMeta.get()));
    MatchDocAllocatorPtr allocator(new MatchDocAllocator(&pool));
    Ha3ScanIteratorParam param;
    param.queryExecutors = {queryExecutor};
    param.matchDocAllocator = allocator;
    param.layerMetas = {layerMeta};
    Ha3ScanIterator scanIter(param);
    vector<MatchDoc> matchDocVec;
    int batchSize = 1;
    for (int32_t i = begin; i <= end; i++) {
        bool ret = scanIter.batchSeek(batchSize, matchDocVec).unwrap();
        ASSERT_TRUE(!ret);
        ASSERT_EQ(1, matchDocVec.size());
        ASSERT_EQ(i, matchDocVec[0].getDocId());
        matchDocVec.clear();
    }
    bool ret = scanIter.batchSeek(batchSize, matchDocVec).unwrap();
    ASSERT_TRUE(ret);
    ASSERT_EQ(0, matchDocVec.size());
}

TEST_F(Ha3ScanIteratorTest, testBatchSeek100) {
    int32_t begin = 10;
    int32_t end = 100;
    autil::mem_pool::PoolAsan pool;
    LayerMetaPtr layerMeta(new LayerMeta(&pool));
    layerMeta->push_back(DocIdRangeMeta(begin, end, DocIdRangeMeta::OT_UNKNOWN, end - begin + 1));
    QueryExecutorPtr queryExecutor(new RangeQueryExecutor(layerMeta.get()));
    MatchDocAllocatorPtr allocator(new MatchDocAllocator(&pool));
    Ha3ScanIteratorParam param;
    param.queryExecutors = {queryExecutor};
    param.matchDocAllocator = allocator;
    param.layerMetas = {layerMeta};
    Ha3ScanIterator scanIter(param);
    vector<MatchDoc> matchDocVec;
    int batchSize = 100;
    bool ret = scanIter.batchSeek(batchSize, matchDocVec).unwrap();
    ASSERT_TRUE(ret);
    ASSERT_EQ(end - begin + 1, matchDocVec.size());
    for (int32_t i = begin; i <= end; i++) {
        ASSERT_EQ(i, matchDocVec[i - begin].getDocId());
    }
}

} // namespace sql
