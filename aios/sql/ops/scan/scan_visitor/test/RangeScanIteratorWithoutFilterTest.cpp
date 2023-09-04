#include "sql/ops/scan/RangeScanIteratorWithoutFilter.h"

#include <iosfwd>
#include <memory>
#include <stdint.h>
#include <unistd.h>
#include <vector>

#include "autil/TimeUtility.h"
#include "autil/TimeoutTerminator.h"
#include "autil/result/Result.h"
#include "ha3/common/TimeoutTerminator.h"
#include "ha3/search/LayerMetas.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader_adaptor.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/ValueType.h"
#include "navi/util/NaviTestPool.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace matchdoc;
using namespace isearch::search;

namespace sql {

class RangeScanIteratorWithoutFilterTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
};

void RangeScanIteratorWithoutFilterTest::setUp() {}

void RangeScanIteratorWithoutFilterTest::tearDown() {}

TEST_F(RangeScanIteratorWithoutFilterTest, testBatchSeek1) {
    int32_t begin = 10;
    int32_t end = 100;
    autil::mem_pool::PoolAsan pool;
    LayerMetaPtr layerMeta(new LayerMeta(&pool));
    layerMeta->push_back(DocIdRangeMeta(begin, end, DocIdRangeMeta::OT_UNKNOWN, end - begin + 1));
    MatchDocAllocatorPtr allocator(new MatchDocAllocator(&pool));
    indexlib::index::DeletionMapReaderPtr delMapReader;
    auto delMapReaderAdaptor = make_shared<indexlib::index::DeletionMapReaderAdaptor>(delMapReader);
    RangeScanIteratorWithoutFilter scanIter(allocator, delMapReaderAdaptor, layerMeta);
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

TEST_F(RangeScanIteratorWithoutFilterTest, testBatchSeek1WithMultiRange) {
    int32_t begin = 10;
    int32_t end = 100;
    autil::mem_pool::PoolAsan pool;
    LayerMetaPtr layerMeta(new LayerMeta(&pool));
    for (int i = begin; i <= end; i++) {
        layerMeta->push_back(DocIdRangeMeta(i, i, DocIdRangeMeta::OT_UNKNOWN, 1));
    }
    MatchDocAllocatorPtr allocator(new MatchDocAllocator(&pool));
    indexlib::index::DeletionMapReaderPtr delMapReader;
    auto delMapReaderAdaptor = make_shared<indexlib::index::DeletionMapReaderAdaptor>(delMapReader);
    RangeScanIteratorWithoutFilter scanIter(allocator, delMapReaderAdaptor, layerMeta);
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
    ASSERT_FALSE(scanIter.isTimeout());
}

TEST_F(RangeScanIteratorWithoutFilterTest, testBatchSeek100) {
    int32_t begin = 10;
    int32_t end = 100;
    autil::mem_pool::PoolAsan pool;
    LayerMetaPtr layerMeta(new LayerMeta(&pool));
    layerMeta->push_back(DocIdRangeMeta(begin, end, DocIdRangeMeta::OT_UNKNOWN, end - begin + 1));
    MatchDocAllocatorPtr allocator(new MatchDocAllocator(&pool));
    indexlib::index::DeletionMapReaderPtr delMapReader;
    auto delMapReaderAdaptor = make_shared<indexlib::index::DeletionMapReaderAdaptor>(delMapReader);
    RangeScanIteratorWithoutFilter scanIter(allocator, delMapReaderAdaptor, layerMeta);
    vector<MatchDoc> matchDocVec;
    int batchSize = 100;
    bool ret = scanIter.batchSeek(batchSize, matchDocVec).unwrap();
    ASSERT_TRUE(ret);
    ASSERT_EQ(end - begin + 1, matchDocVec.size());
    for (int32_t i = begin; i <= end; i++) {
        ASSERT_EQ(i, matchDocVec[i - begin].getDocId());
    }
}

TEST_F(RangeScanIteratorWithoutFilterTest, testBatchSeek100WithMultRange) {
    int32_t begin = 10;
    int32_t end = 100;
    autil::mem_pool::PoolAsan pool;
    LayerMetaPtr layerMeta(new LayerMeta(&pool));
    for (int i = begin; i <= end; i++) {
        layerMeta->push_back(DocIdRangeMeta(i, i, DocIdRangeMeta::OT_UNKNOWN, 1));
    }
    MatchDocAllocatorPtr allocator(new MatchDocAllocator(&pool));
    indexlib::index::DeletionMapReaderPtr delMapReader;
    auto delMapReaderAdaptor = make_shared<indexlib::index::DeletionMapReaderAdaptor>(delMapReader);
    RangeScanIteratorWithoutFilter scanIter(allocator, delMapReaderAdaptor, layerMeta);
    vector<MatchDoc> matchDocVec;
    int batchSize = 100;
    bool ret = scanIter.batchSeek(batchSize, matchDocVec).unwrap();
    ASSERT_TRUE(ret);
    ASSERT_EQ(end - begin + 1, matchDocVec.size());
    for (int32_t i = begin; i <= end; i++) {
        ASSERT_EQ(i, matchDocVec[i - begin].getDocId());
    }
}

TEST_F(RangeScanIteratorWithoutFilterTest, testSeekTimeOut) {
    int32_t begin = 10;
    int32_t end = 100;
    autil::mem_pool::PoolAsan pool;
    LayerMetaPtr layerMeta(new LayerMeta(&pool));
    layerMeta->push_back(DocIdRangeMeta(begin, end, DocIdRangeMeta::OT_UNKNOWN, end - begin + 1));
    MatchDocAllocatorPtr allocator(new MatchDocAllocator(&pool));
    indexlib::index::DeletionMapReaderPtr delMapReader;
    auto delMapReaderAdaptor = make_shared<indexlib::index::DeletionMapReaderAdaptor>(delMapReader);
    int64_t startTime = autil::TimeUtility::currentTime();
    autil::TimeoutTerminator timeoutTerminator(1, startTime);
    usleep(1000);
    RangeScanIteratorWithoutFilter scanIter(
        allocator, delMapReaderAdaptor, layerMeta, &timeoutTerminator);
    vector<MatchDoc> matchDocVec;
    int batchSize = 1;
    auto ret = scanIter.batchSeek(batchSize, matchDocVec);
    ASSERT_TRUE(ret.as<isearch::common::TimeoutError>());
    ASSERT_EQ(0, matchDocVec.size());
    ASSERT_TRUE(scanIter.isTimeout());
}

} // namespace sql
