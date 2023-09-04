#include "sql/ops/scan/RangeScanIterator.h"

#include <cstdint>
#include <iosfwd>
#include <memory>
#include <set>
#include <unistd.h>
#include <vector>

#include "autil/TimeUtility.h"
#include "autil/mem_pool/PoolBase.h"
#include "autil/result/Result.h"
#include "ha3/common/TimeoutTerminator.h"
#include "ha3/search/Filter.h"
#include "ha3/search/FilterWrapper.h"
#include "ha3/search/LayerMetas.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader_adaptor.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/Trait.h"
#include "navi/util/NaviTestPool.h"
#include "suez/turing/expression/framework/AttributeExpression.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace matchdoc;
using namespace isearch;
using namespace isearch::search;

namespace sql {

class RangeScanIteratorTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
};

void RangeScanIteratorTest::setUp() {}

void RangeScanIteratorTest::tearDown() {}

class MyAttributeExpressionTyped : public suez::turing::AttributeExpressionTyped<bool> {
public:
    MyAttributeExpressionTyped(set<int32_t> docIds) {
        _docIds = docIds;
    }

public:
    virtual bool evaluateAndReturn(matchdoc::MatchDoc matchDoc) {
        if (_docIds.count(matchDoc.getDocId()) > 0) {
            return true;
        } else {
            return false;
        }
    }

private:
    set<int32_t> _docIds;
};

TEST_F(RangeScanIteratorTest, testBatchSeek1) {
    int32_t begin = 10;
    int32_t end = 100;
    autil::mem_pool::PoolAsan pool;
    LayerMetaPtr layerMeta(new LayerMeta(&pool));
    layerMeta->push_back(DocIdRangeMeta(begin, end, DocIdRangeMeta::OT_UNKNOWN, end - begin + 1));
    MatchDocAllocatorPtr allocator(new MatchDocAllocator(&pool));
    indexlib::index::DeletionMapReaderPtr delMapReader;
    auto delMapReaderAdaptor = make_shared<indexlib::index::DeletionMapReaderAdaptor>(delMapReader);
    set<int32_t> expect {10, 20, 30, 40, 50, 55, 60, 80};
    search::FilterWrapperPtr filterWrapper(new FilterWrapper());
    MyAttributeExpressionTyped myExpression(expect);
    search::Filter *filter = POOL_NEW_CLASS((&pool), search::Filter, (&myExpression));
    filterWrapper->setFilter(filter);
    RangeScanIterator scanIter(filterWrapper, allocator, delMapReaderAdaptor, layerMeta);
    vector<MatchDoc> matchDocVec;
    int batchSize = 1;
    for (auto docid : expect) {
        bool ret = scanIter.batchSeek(batchSize, matchDocVec).unwrap();
        ASSERT_TRUE(!ret);
        ASSERT_EQ(1, matchDocVec.size());
        ASSERT_EQ(docid, matchDocVec[0].getDocId());
        matchDocVec.clear();
    }
    bool ret = scanIter.batchSeek(batchSize, matchDocVec).unwrap();
    ASSERT_TRUE(ret);
    ASSERT_EQ(0, matchDocVec.size());
}

TEST_F(RangeScanIteratorTest, testBatchSeek1WithMultiRange) {
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
    set<int32_t> expect {10, 20, 30, 40, 50, 55, 60, 80};
    search::FilterWrapperPtr filterWrapper(new FilterWrapper());
    MyAttributeExpressionTyped myExpression(expect);
    search::Filter *filter = POOL_NEW_CLASS((&pool), search::Filter, (&myExpression));
    filterWrapper->setFilter(filter);
    RangeScanIterator scanIter(filterWrapper, allocator, delMapReaderAdaptor, layerMeta);
    vector<MatchDoc> matchDocVec;
    int batchSize = 1;
    for (auto docid : expect) {
        bool ret = scanIter.batchSeek(batchSize, matchDocVec).unwrap();
        ASSERT_TRUE(!ret);
        ASSERT_EQ(1, matchDocVec.size());
        ASSERT_EQ(docid, matchDocVec[0].getDocId());
        matchDocVec.clear();
    }
    bool ret = scanIter.batchSeek(batchSize, matchDocVec).unwrap();
    ASSERT_TRUE(ret);
    ASSERT_EQ(0, matchDocVec.size());
}

TEST_F(RangeScanIteratorTest, testBatchSeek1WithMultiRange2) {
    int32_t begin = 10;
    int32_t end = 101;
    autil::mem_pool::PoolAsan pool;
    LayerMetaPtr layerMeta(new LayerMeta(&pool));
    for (int i = begin; i <= end; i++) {
        layerMeta->push_back(DocIdRangeMeta(i, i + 1, DocIdRangeMeta::OT_UNKNOWN, 2));
    }
    MatchDocAllocatorPtr allocator(new MatchDocAllocator(&pool));
    indexlib::index::DeletionMapReaderPtr delMapReader;
    auto delMapReaderAdaptor = make_shared<indexlib::index::DeletionMapReaderAdaptor>(delMapReader);
    set<int32_t> expect {10, 20, 30, 40, 50, 55, 60, 80};
    search::FilterWrapperPtr filterWrapper(new FilterWrapper());
    MyAttributeExpressionTyped myExpression(expect);
    search::Filter *filter = POOL_NEW_CLASS((&pool), search::Filter, (&myExpression));
    filterWrapper->setFilter(filter);
    RangeScanIterator scanIter(filterWrapper, allocator, delMapReaderAdaptor, layerMeta);
    vector<MatchDoc> matchDocVec;
    int batchSize = 1;
    for (auto docid : expect) {
        bool ret = scanIter.batchSeek(batchSize, matchDocVec).unwrap();
        ASSERT_TRUE(!ret);
        ASSERT_EQ(1, matchDocVec.size());
        ASSERT_EQ(docid, matchDocVec[0].getDocId());
        matchDocVec.clear();
    }
    bool ret = scanIter.batchSeek(batchSize, matchDocVec).unwrap();
    ASSERT_TRUE(ret);
    ASSERT_EQ(0, matchDocVec.size());
}

TEST_F(RangeScanIteratorTest, testBatchSeek100) {
    int32_t begin = 10;
    int32_t end = 100;
    autil::mem_pool::PoolAsan pool;
    LayerMetaPtr layerMeta(new LayerMeta(&pool));
    layerMeta->push_back(DocIdRangeMeta(begin, end, DocIdRangeMeta::OT_UNKNOWN, end - begin + 1));
    MatchDocAllocatorPtr allocator(new MatchDocAllocator(&pool));
    indexlib::index::DeletionMapReaderPtr delMapReader;
    auto delMapReaderAdaptor = make_shared<indexlib::index::DeletionMapReaderAdaptor>(delMapReader);
    set<int32_t> expect {10, 20, 30, 40, 50, 55, 60, 80};
    search::FilterWrapperPtr filterWrapper(new FilterWrapper());
    MyAttributeExpressionTyped myExpression(expect);
    search::Filter *filter = POOL_NEW_CLASS((&pool), search::Filter, (&myExpression));
    filterWrapper->setFilter(filter);
    RangeScanIterator scanIter(filterWrapper, allocator, delMapReaderAdaptor, layerMeta);
    vector<MatchDoc> matchDocVec;
    int batchSize = 100;
    bool ret = scanIter.batchSeek(batchSize, matchDocVec).unwrap();
    ASSERT_TRUE(ret);
    ASSERT_EQ(expect.size(), matchDocVec.size());
    for (auto docid : matchDocVec) {
        ASSERT_TRUE(expect.count(docid.getDocId()) > 0);
    }
}

TEST_F(RangeScanIteratorTest, testBatchSeek100WithMultRange) {
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
    set<int32_t> expect {10, 20, 30, 40, 50, 55, 60, 80};
    search::FilterWrapperPtr filterWrapper(new FilterWrapper());
    MyAttributeExpressionTyped myExpression(expect);
    search::Filter *filter = POOL_NEW_CLASS((&pool), search::Filter, (&myExpression));
    filterWrapper->setFilter(filter);
    RangeScanIterator scanIter(filterWrapper, allocator, delMapReaderAdaptor, layerMeta);
    vector<MatchDoc> matchDocVec;
    int batchSize = 100;
    bool ret = scanIter.batchSeek(batchSize, matchDocVec).unwrap();
    ASSERT_TRUE(ret);
    ASSERT_EQ(expect.size(), matchDocVec.size());
    for (auto docid : matchDocVec) {
        ASSERT_TRUE(expect.count(docid.getDocId()) > 0);
    }
}

TEST_F(RangeScanIteratorTest, testBatchSeek100WithMultRange2) {
    int32_t begin = 10;
    int32_t end = 101;
    autil::mem_pool::PoolAsan pool;
    LayerMetaPtr layerMeta(new LayerMeta(&pool));
    for (int i = begin; i <= end; i += 2) {
        layerMeta->push_back(DocIdRangeMeta(i, i + 1, DocIdRangeMeta::OT_UNKNOWN, 2));
    }
    MatchDocAllocatorPtr allocator(new MatchDocAllocator(&pool));
    indexlib::index::DeletionMapReaderPtr delMapReader;
    auto delMapReaderAdaptor = make_shared<indexlib::index::DeletionMapReaderAdaptor>(delMapReader);
    set<int32_t> expect {10, 20, 30, 40, 50, 55, 60, 80};
    search::FilterWrapperPtr filterWrapper(new FilterWrapper());
    MyAttributeExpressionTyped myExpression(expect);
    search::Filter *filter = POOL_NEW_CLASS((&pool), search::Filter, (&myExpression));
    filterWrapper->setFilter(filter);
    RangeScanIterator scanIter(filterWrapper, allocator, delMapReaderAdaptor, layerMeta);
    vector<MatchDoc> matchDocVec;
    int batchSize = 100;
    bool ret = scanIter.batchSeek(batchSize, matchDocVec).unwrap();
    ASSERT_TRUE(ret);
    ASSERT_EQ(expect.size(), matchDocVec.size());
    for (auto docid : matchDocVec) {
        ASSERT_TRUE(expect.count(docid.getDocId()) > 0);
    }
}

TEST_F(RangeScanIteratorTest, testBatchSeek1WithoutFilter) {
    int32_t begin = 10;
    int32_t end = 100;
    autil::mem_pool::PoolAsan pool;
    LayerMetaPtr layerMeta(new LayerMeta(&pool));
    layerMeta->push_back(DocIdRangeMeta(begin, end, DocIdRangeMeta::OT_UNKNOWN, end - begin + 1));
    MatchDocAllocatorPtr allocator(new MatchDocAllocator(&pool));
    indexlib::index::DeletionMapReaderPtr delMapReader;
    auto delMapReaderAdaptor = make_shared<indexlib::index::DeletionMapReaderAdaptor>(delMapReader);
    search::FilterWrapperPtr filterWrapper;
    RangeScanIterator scanIter(filterWrapper, allocator, delMapReaderAdaptor, layerMeta);
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

TEST_F(RangeScanIteratorTest, testBatchSeek1WithMultiRangeWithoutFilter) {
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
    search::FilterWrapperPtr filterWrapper;
    RangeScanIterator scanIter(filterWrapper, allocator, delMapReaderAdaptor, layerMeta);
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

TEST_F(RangeScanIteratorTest, testBatchSeek100WithoutFilter) {
    int32_t begin = 10;
    int32_t end = 100;
    autil::mem_pool::PoolAsan pool;
    LayerMetaPtr layerMeta(new LayerMeta(&pool));
    layerMeta->push_back(DocIdRangeMeta(begin, end, DocIdRangeMeta::OT_UNKNOWN, end - begin + 1));
    MatchDocAllocatorPtr allocator(new MatchDocAllocator(&pool));
    indexlib::index::DeletionMapReaderPtr delMapReader;
    auto delMapReaderAdaptor = make_shared<indexlib::index::DeletionMapReaderAdaptor>(delMapReader);
    search::FilterWrapperPtr filterWrapper;
    RangeScanIterator scanIter(filterWrapper, allocator, delMapReaderAdaptor, layerMeta);
    vector<MatchDoc> matchDocVec;
    int batchSize = 100;
    bool ret = scanIter.batchSeek(batchSize, matchDocVec).unwrap();
    ASSERT_TRUE(ret);
    ASSERT_EQ(end - begin + 1, matchDocVec.size());
    for (int32_t i = begin; i <= end; i++) {
        ASSERT_EQ(i, matchDocVec[i - begin].getDocId());
    }
}

TEST_F(RangeScanIteratorTest, testBatchSeek100WithMultRangeWithoutFilter) {
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
    search::FilterWrapperPtr filterWrapper;
    RangeScanIterator scanIter(filterWrapper, allocator, delMapReaderAdaptor, layerMeta);
    vector<MatchDoc> matchDocVec;
    int batchSize = 100;
    bool ret = scanIter.batchSeek(batchSize, matchDocVec).unwrap();
    ASSERT_TRUE(ret);
    ASSERT_EQ(end - begin + 1, matchDocVec.size());
    for (int32_t i = begin; i <= end; i++) {
        ASSERT_EQ(i, matchDocVec[i - begin].getDocId());
    }
}

TEST_F(RangeScanIteratorTest, testSeekTimeout) {
    int32_t begin = 10;
    int32_t end = 100;
    autil::mem_pool::PoolAsan pool;
    LayerMetaPtr layerMeta(new LayerMeta(&pool));
    layerMeta->push_back(DocIdRangeMeta(begin, end, DocIdRangeMeta::OT_UNKNOWN, end - begin + 1));
    MatchDocAllocatorPtr allocator(new MatchDocAllocator(&pool));
    indexlib::index::DeletionMapReaderPtr delMapReader;
    auto delMapReaderAdaptor = make_shared<indexlib::index::DeletionMapReaderAdaptor>(delMapReader);
    set<int32_t> expect {10, 20, 30, 40, 50, 55, 60, 80};
    search::FilterWrapperPtr filterWrapper(new FilterWrapper());
    MyAttributeExpressionTyped myExpression(expect);
    search::Filter *filter = POOL_NEW_CLASS((&pool), search::Filter, (&myExpression));
    filterWrapper->setFilter(filter);
    int64_t startTime = autil::TimeUtility::currentTime();
    common::TimeoutTerminator timeoutTerminator(1, startTime);
    usleep(1000);
    RangeScanIterator scanIter(
        filterWrapper, allocator, delMapReaderAdaptor, layerMeta, &timeoutTerminator);
    vector<MatchDoc> matchDocVec;
    int batchSize = 1;
    auto ret = scanIter.batchSeek(batchSize, matchDocVec);
    ASSERT_TRUE(ret.as<common::TimeoutError>());
    ASSERT_EQ(0, matchDocVec.size());
    ASSERT_TRUE(scanIter.isTimeout());
}

} // namespace sql
