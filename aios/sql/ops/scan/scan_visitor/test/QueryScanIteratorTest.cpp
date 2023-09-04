#include "sql/ops/scan/QueryScanIterator.h"

#include <assert.h>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <unistd.h>
#include <vector>

#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/mem_pool/PoolBase.h"
#include "autil/result/Result.h"
#include "ha3/common/TimeoutTerminator.h"
#include "ha3/search/Filter.h"
#include "ha3/search/FilterWrapper.h"
#include "ha3/search/IndexPartitionReaderUtil.h"
#include "ha3/search/IndexPartitionReaderWrapper.h"
#include "ha3/search/LayerMetas.h"
#include "ha3/search/QueryExecutor.h"
#include "ha3/search/TermQueryExecutor.h"
#include "ha3/search/test/QueryExecutorConstructor.h"
#include "ha3_sdk/testlib/index/FakeIndexPartitionReaderCreator.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/online_config.h"
#include "indexlib/index/attribute/Constant.h"
#include "indexlib/index/normal/deletionmap/deletion_map_reader_adaptor.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/partition/index_partition.h"
#include "indexlib/testlib/indexlib_partition_creator.h"
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
using namespace indexlib::index;

namespace sql {

class QueryScanIteratorTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
    indexlib::partition::IndexPartitionPtr makeIndexPartition(const std::string &rootPath,
                                                              const std::string &tableName);

private:
    IndexPartitionReaderWrapperPtr _indexReaderWrapper;
    QueryExecutorPtr _queryExecutor;
    autil::mem_pool::PoolAsan _pool;
};

void QueryScanIteratorTest::setUp() {
    FakeIndex fakeIndex;
    string indexStr = "ALIBABA:";
    for (size_t i = 0; i < 1000; i++) {
        indexStr += autil::StringUtil::toString(i) + ";";
    }
    indexStr += "\n";
    fakeIndex.indexes["phrase"] = indexStr;
    _indexReaderWrapper = FakeIndexPartitionReaderCreator::createIndexPartitionReader(fakeIndex);
    _indexReaderWrapper->setTopK(1000);
    auto queryExecutor = QueryExecutorConstructor::prepareTermQueryExecutor(
        &_pool, "ALIBABA", "phrase", _indexReaderWrapper.get());
    _queryExecutor.reset(queryExecutor, [](QueryExecutor *p) { POOL_DELETE_CLASS(p); });
}

void QueryScanIteratorTest::tearDown() {
    _indexReaderWrapper.reset();
    _queryExecutor.reset();
}

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

TEST_F(QueryScanIteratorTest, testBatchSeek1) {
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
    QueryScanIterator scanIter(
        _queryExecutor, filterWrapper, allocator, delMapReaderAdaptor, layerMeta);
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

TEST_F(QueryScanIteratorTest, testBatchSeek1WithMultiRange) {
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
    QueryScanIterator scanIter(
        _queryExecutor, filterWrapper, allocator, delMapReaderAdaptor, layerMeta);
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

TEST_F(QueryScanIteratorTest, testBatchSeek1WithMultiRange2) {
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
    QueryScanIterator scanIter(
        _queryExecutor, filterWrapper, allocator, delMapReaderAdaptor, layerMeta);
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

TEST_F(QueryScanIteratorTest, testBatchSeek100) {
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
    QueryScanIterator scanIter(
        _queryExecutor, filterWrapper, allocator, delMapReaderAdaptor, layerMeta);
    vector<MatchDoc> matchDocVec;
    int batchSize = 100;
    bool ret = scanIter.batchSeek(batchSize, matchDocVec).unwrap();
    ASSERT_TRUE(ret);
    ASSERT_EQ(expect.size(), matchDocVec.size());
    for (auto docid : matchDocVec) {
        ASSERT_TRUE(expect.count(docid.getDocId()) > 0);
    }
}

TEST_F(QueryScanIteratorTest, testBatchSeek100WithMultRange) {
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
    QueryScanIterator scanIter(
        _queryExecutor, filterWrapper, allocator, delMapReaderAdaptor, layerMeta);
    vector<MatchDoc> matchDocVec;
    int batchSize = 100;
    bool ret = scanIter.batchSeek(batchSize, matchDocVec).unwrap();
    ASSERT_TRUE(ret);
    ASSERT_EQ(expect.size(), matchDocVec.size());
    for (auto docid : matchDocVec) {
        ASSERT_TRUE(expect.count(docid.getDocId()) > 0);
    }
}

TEST_F(QueryScanIteratorTest, testBatchSeek100WithMultRange2) {
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
    QueryScanIterator scanIter(
        _queryExecutor, filterWrapper, allocator, delMapReaderAdaptor, layerMeta);
    vector<MatchDoc> matchDocVec;
    int batchSize = 100;
    bool ret = scanIter.batchSeek(batchSize, matchDocVec).unwrap();
    ASSERT_TRUE(ret);
    ASSERT_EQ(expect.size(), matchDocVec.size());
    for (auto docid : matchDocVec) {
        ASSERT_TRUE(expect.count(docid.getDocId()) > 0);
    }
}

TEST_F(QueryScanIteratorTest, testBatchSeek1WithoutFilter) {
    int32_t begin = 10;
    int32_t end = 100;
    LayerMetaPtr layerMeta(new LayerMeta(&_pool));
    layerMeta->push_back(DocIdRangeMeta(begin, end, DocIdRangeMeta::OT_UNKNOWN, end - begin + 1));
    MatchDocAllocatorPtr allocator(new MatchDocAllocator(&_pool));
    QueryScanIterator scanIter(_queryExecutor, {}, allocator, {}, layerMeta);
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

TEST_F(QueryScanIteratorTest, testBatchSeek100WithoutFilter) {
    int32_t begin = 10;
    int32_t end = 100;
    LayerMetaPtr layerMeta(new LayerMeta(&_pool));
    layerMeta->push_back(DocIdRangeMeta(begin, end, DocIdRangeMeta::OT_UNKNOWN, end - begin + 1));
    MatchDocAllocatorPtr allocator(new MatchDocAllocator(&_pool));
    QueryScanIterator scanIter(_queryExecutor, {}, allocator, {}, layerMeta);
    vector<MatchDoc> matchDocVec;
    int batchSize = 100;
    bool ret = scanIter.batchSeek(batchSize, matchDocVec).unwrap();
    ASSERT_TRUE(ret);
    ASSERT_EQ(end - begin + 1, matchDocVec.size());
    for (int32_t i = begin; i <= end; i++) {
        ASSERT_EQ(i, matchDocVec[i - begin].getDocId());
    }
}
TEST_F(QueryScanIteratorTest, testSeekTimeout) {
    int32_t begin = 10;
    int32_t end = 100;
    LayerMetaPtr layerMeta(new LayerMeta(&_pool));
    layerMeta->push_back(DocIdRangeMeta(begin, end, DocIdRangeMeta::OT_UNKNOWN, end - begin + 1));
    MatchDocAllocatorPtr allocator(new MatchDocAllocator(&_pool));
    int64_t startTime = autil::TimeUtility::currentTime();
    common::TimeoutTerminator timeoutTerminator(1, startTime);
    usleep(1000);
    QueryScanIterator scanIter(_queryExecutor, {}, allocator, {}, layerMeta, &timeoutTerminator);
    vector<MatchDoc> matchDocVec;
    int batchSize = 1;
    auto ret = scanIter.batchSeek(batchSize, matchDocVec);
    ASSERT_TRUE(ret.as<common::TimeoutError>());
    ASSERT_EQ(0, matchDocVec.size());
    ASSERT_TRUE(scanIter.isTimeout());
}

TEST_F(QueryScanIteratorTest, testSeekWithSubTerm) {
    string testPath = GET_TEMP_DATA_PATH() + "/index_path/";
    auto indexPartition = makeIndexPartition(testPath, "test");
    int32_t begin = 0;
    int32_t end = 10;
    LayerMetaPtr layerMeta(new LayerMeta(&_pool));
    layerMeta->push_back(DocIdRangeMeta(begin, end, DocIdRangeMeta::OT_UNKNOWN, end - begin + 1));
    MatchDocAllocatorPtr allocator(new MatchDocAllocator(&_pool));

    auto indexPartitionReader = indexPartition->GetReader();
    auto indexReaderWrapper
        = IndexPartitionReaderUtil::createIndexPartitionReaderWrapper(indexPartitionReader);
    auto queryExecutor = QueryExecutorConstructor::prepareTermQueryExecutor(
        &_pool, "ab", "sub_index_2", indexReaderWrapper.get());
    ASSERT_TRUE(queryExecutor);
    _queryExecutor.reset(queryExecutor, [](QueryExecutor *p) { POOL_DELETE_CLASS(p); });
    QueryScanIterator scanIter(_queryExecutor, {}, allocator, {}, layerMeta);
    vector<MatchDoc> matchDocVec;
    int batchSize = 4;
    bool ret = scanIter.batchSeek(batchSize, matchDocVec).unwrap();
    ASSERT_TRUE(ret);
    ASSERT_EQ(3, matchDocVec.size());
    ASSERT_EQ(0, matchDocVec[0].getDocId());
    ASSERT_EQ(2, matchDocVec[1].getDocId());
    ASSERT_EQ(3, matchDocVec[2].getDocId());
}

indexlib::partition::IndexPartitionPtr
QueryScanIteratorTest::makeIndexPartition(const std::string &rootPath,
                                          const std::string &tableName) {
    int64_t ttl = std::numeric_limits<int64_t>::max();
    auto mainSchema = indexlib::testlib::IndexlibPartitionCreator::CreateSchema(
        tableName,
        "attr1:uint32;attr2:int32:true;id:int64;name:string;index2:text", // fields
        "id:primarykey64:id;index_2:text:index2;name:string:name",        // fields
        "attr1;attr2;id",                                                 // attributes
        "name",                                                           // summary
        "");                                                              // truncateProfile

    auto subSchema = indexlib::testlib::IndexlibPartitionCreator::CreateSchema(
        "sub_" + tableName,
        "sub_id:int64;sub_attr1:int32;sub_index2:string",           // fields
        "sub_id:primarykey64:sub_id;sub_index_2:string:sub_index2", // indexs
        "sub_attr1;sub_id;sub_index2",                              // attributes
        "",                                                         // summarys
        "");                                                        // truncateProfile
    string docsStr
        = "cmd=add,attr1=0,attr2=0 10,id=1,name=aa,index2=a a "
          "a,sub_id=1^2^3,sub_attr1=1^2^2,sub_index2=aa^ab^ac;"
          "cmd=add,attr1=1,attr2=1 11,id=2,name=bb,index2=a b c,sub_id=4,sub_attr1=1,sub_index2=aa;"
          "cmd=add,attr1=2,attr2=2 22,id=3,name=cc,index2=a c d "
          "stop,sub_id=5,sub_attr1=1,sub_index2=ab;"
          "cmd=add,attr1=3,attr2=3 33,id=4,name=dd,index2=b c "
          "d,sub_id=6^7,sub_attr1=2^1,sub_index2=ab^ab";

    indexlib::config::IndexPartitionOptions options;
    options.GetOnlineConfig().ttl = ttl;
    auto schema
        = indexlib::testlib::IndexlibPartitionCreator::CreateSchemaWithSub(mainSchema, subSchema);

    indexlib::partition::IndexPartitionPtr indexPartition
        = indexlib::testlib::IndexlibPartitionCreator::CreateIndexPartition(
            schema, rootPath, docsStr, options, "", true);
    assert(indexPartition.get() != nullptr);
    return indexPartition;
}

} // namespace sql
