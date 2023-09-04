#include "sql/ops/scan/OrderedHa3ScanIterator.h"

#include <cstdint>
#include <iosfwd>
#include <limits>
#include <memory>
#include <ostream>
#include <stddef.h>
#include <string>
#include <vector>

#include "autil/MultiValueCreator.h"
#include "autil/MultiValueType.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/mem_pool/Pool.h"
#include "autil/result/Result.h"
#include "ha3/search/DocIdsQueryExecutor.h"
#include "ha3/search/Filter.h"
#include "ha3/search/LayerMetas.h"
#include "ha3/search/QueryExecutor.h"
#include "indexlib/base/Types.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/index_application.h"
#include "indexlib/partition/partition_reader_snapshot.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/Trait.h"
#include "navi/tester/NaviResourceHelper.h"
#include "sql/ops/scan/Ha3ScanIterator.h"
#include "sql/ops/sort/SortInitParam.h"
#include "sql/ops/test/OpTestBase.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/framework/AttributeExpressionCreator.h"
#include "suez/turing/expression/util/TableInfo.h"
#include "suez/turing/navi/TableInfoR.h"
#include "unittest/unittest.h"

namespace matchdoc {
template <typename T>
class Reference;
} // namespace matchdoc
namespace suez {
namespace turing {
class VirtualAttribute;
} // namespace turing
} // namespace suez

using namespace std;
using namespace testing;
using namespace matchdoc;
using namespace autil;
using namespace suez::turing;
using namespace isearch::search;

namespace sql {

class OrderedHa3ScanIteratorTest : public OpTestBase {
public:
    void SetUp() override {
        _needBuildIndex = true;
        _needExprResource = true;
        OpTestBase::SetUp();
    }
    void prepareInvertedTableData(std::string &tableName,
                                  std::string &fields,
                                  std::string &indexes,
                                  std::string &attributes,
                                  std::string &summarys,
                                  std::string &truncateProfileStr,
                                  std::string &docs,
                                  int64_t &ttl) override {
        tableName = _tableName;
        fields = "attr1:int32;attr2:uint32:true;attr3:double;id:int64;name:string;index2:text";
        indexes = "id:primarykey64:id;index_2:text:index2;name:string:name";
        attributes = "attr1;attr2;id;attr3";
        summarys = "name";
        truncateProfileStr = "";
        docs = "cmd=add,attr1=3,attr2=0 10,id=1,attr3=1.1,name=aa,index2=a a a;"
               "cmd=add,attr1=5,attr2=1 11,id=2,attr3=2.2,name=bb,index2=a b c;"
               "cmd=add,attr1=7,attr2=2 22,id=3,attr3=3.3,name=cc,index2=a c d stop;"
               "cmd=add,attr1=4,attr2=2 22,id=4,attr3=3.3,name=cc,index2=a c d stop;"
               "cmd=add,attr1=5,attr2=2 22,id=0,attr3=3.3,name=cc,index2=a c d stop;"
               "cmd=add,attr1=6,attr2=2 22,id=6,attr3=4.4,name=cc,index2=a c d stop;"
               "cmd=add,attr1=7,attr2=2 22,id=7,attr3=3.3,name=cc,index2=a c d stop;"
               "cmd=add,attr1=8,attr2=2 22,id=8,attr3=3.3,name=cc,index2=a c d stop;"
               "cmd=add,attr1=5,attr2=2 20,id=9,attr3=4.4,name=cc,index2=a c d stop;"
               "cmd=add,attr1=2,attr2=2 22,id=10,attr3=3.3,name=cc,index2=a c d stop;";
        ttl = std::numeric_limits<int64_t>::max();
    }

    void prepareResource() {
        _matchDocAllocator.reset(new matchdoc::MatchDocAllocator(_poolPtr.get(), false));
        _indexAppSnapshot = _indexApp->CreateSnapshot();
        std::vector<suez::turing::VirtualAttribute *> virtualAttributes;
        auto *naviRHelper = getNaviRHelper();
        auto *tableInfoR = naviRHelper->getOrCreateRes<TableInfoR>();
        _tableInfo = tableInfoR->getTableInfo(_tableName);
        _attributeExpressionCreator.reset(new AttributeExpressionCreator(_poolPtr.get(),
                                                                         _matchDocAllocator.get(),
                                                                         _tableName,
                                                                         _indexAppSnapshot.get(),
                                                                         _tableInfo,
                                                                         virtualAttributes,
                                                                         nullptr,
                                                                         nullptr,
                                                                         nullptr));
    }
    void checkDocIds(const std::vector<MatchDoc> &matchDocs, const std::vector<int32_t> &docIds) {
        ASSERT_EQ(docIds.size(), matchDocs.size());
        vector<int32_t> outputs;
        for (size_t i = 0; i < matchDocs.size(); i++) {
            outputs.emplace_back(matchDocs[i].getDocId());
        }
        for (size_t i = 0; i < docIds.size(); i++) {
            ASSERT_EQ(docIds[i], outputs[i]) << autil::StringUtil::toString(docIds) << endl
                                             << autil::StringUtil::toString(outputs);
        }
    }
    template <typename T>
    void checkReference(const std::vector<MatchDoc> &matchDocs,
                        const std::string &name,
                        const std::vector<T> &values) {
        ASSERT_EQ(values.size(), matchDocs.size());
        matchdoc::Reference<T> *ref = _matchDocAllocator->findReference<T>(name);
        ASSERT_TRUE(ref != nullptr);
        vector<T> outputs;
        for (size_t i = 0; i < values.size(); i++) {
            outputs.emplace_back(ref->get(matchDocs[i]));
        }
        for (size_t i = 0; i < values.size(); i++) {
            ASSERT_EQ(values[i], outputs[i]) << autil::StringUtil::toString(values) << endl
                                             << autil::StringUtil::toString(outputs);
        }
    }

    template <typename T>
    void checkReference(const std::vector<MatchDoc> &matchDocs,
                        const std::string &name,
                        const std::vector<std::vector<T>> &values) {
        ASSERT_EQ(values.size(), matchDocs.size());
        matchdoc::Reference<MultiValueType<T>> *ref
            = _matchDocAllocator->findReference<MultiValueType<T>>(name);
        ASSERT_TRUE(ref != nullptr);

        autil::mem_pool::Pool pool;
        for (size_t i = 0; i < values.size(); i++) {
            autil::MultiValueType<T> p(
                autil::MultiValueCreator::createMultiValueBuffer(values[i], &pool));
            ASSERT_EQ(p, ref->get(matchDocs[i])) << p << "|" << ref->get(matchDocs[i]);
        }
    }

    void createLayerMeta(int start,
                         int end,
                         DocIdRangeMeta::OrderedType type,
                         std::vector<LayerMetaPtr> &layerMetas);

    void createQueryExecutor(const vector<docid_t> &docIds,
                             std::vector<QueryExecutorPtr> &queryExecutors);

private:
    matchdoc::MatchDocAllocatorPtr _matchDocAllocator;
    indexlib::partition::PartitionReaderSnapshotPtr _indexAppSnapshot;
    suez::turing::TableInfoPtr _tableInfo;
    suez::turing::AttributeExpressionCreatorPtr _attributeExpressionCreator;
};

void OrderedHa3ScanIteratorTest::createLayerMeta(int start,
                                                 int end,
                                                 DocIdRangeMeta::OrderedType type,
                                                 std::vector<LayerMetaPtr> &layerMetas) {
    LayerMetaPtr meta(new LayerMeta(_poolPtr.get()));
    meta->push_back(DocIdRangeMeta(start, end, type, end - start + 1));
    layerMetas.emplace_back(meta);
}

void OrderedHa3ScanIteratorTest::createQueryExecutor(
    const vector<docid_t> &docIds, std::vector<QueryExecutorPtr> &queryExecutors) {
    QueryExecutorPtr queryExecutor(new DocIdsQueryExecutor(docIds));
    queryExecutors.emplace_back(queryExecutor);
}

TEST_F(OrderedHa3ScanIteratorTest, testInitFailed) {
    prepareResource();
    Ha3ScanIteratorParam param;
    param.matchDocAllocator = _matchDocAllocator;

    SortInitParam sortDesc;
    sortDesc.topk = 5;
    sortDesc.keys = {"not_exist"};
    sortDesc.orders = {false};

    OrderedHa3ScanIterator orderedHa3ScanIterator(
        param, _poolPtr.get(), sortDesc, _attributeExpressionCreator.get());

    ASSERT_FALSE(orderedHa3ScanIterator.init());
}

TEST_F(OrderedHa3ScanIteratorTest, testInitSuccess) {
    prepareResource();
    Ha3ScanIteratorParam param;
    param.matchDocAllocator = _matchDocAllocator;

    SortInitParam sortDesc;
    sortDesc.topk = 5;
    sortDesc.keys = {"attr1"};
    sortDesc.orders = {false};

    OrderedHa3ScanIterator orderedHa3ScanIterator(
        param, _poolPtr.get(), sortDesc, _attributeExpressionCreator.get());

    ASSERT_TRUE(orderedHa3ScanIterator.init());
}

TEST_F(OrderedHa3ScanIteratorTest, testEmptyQuery) {
    prepareResource();

    Ha3ScanIteratorParam param;
    param.matchDocAllocator = _matchDocAllocator;

    SortInitParam sortDesc;
    sortDesc.topk = 5;
    sortDesc.keys = {"attr1"};
    sortDesc.orders = {false};

    OrderedHa3ScanIterator orderedHa3ScanIterator(
        param, _poolPtr.get(), sortDesc, _attributeExpressionCreator.get());

    ASSERT_TRUE(orderedHa3ScanIterator.init());

    vector<MatchDoc> output;
    ASSERT_TRUE(orderedHa3ScanIterator.batchSeek(10, output).unwrap());
    ASSERT_TRUE(output.empty());
}

TEST_F(OrderedHa3ScanIteratorTest, testEmpty) {
    prepareResource();
    std::vector<LayerMetaPtr> layerMetas;
    std::vector<QueryExecutorPtr> queryExecutors;
    createLayerMeta(0, 2, DocIdRangeMeta::OT_ORDERED, layerMetas);
    createLayerMeta(3, 6, DocIdRangeMeta::OT_ORDERED, layerMetas);
    createLayerMeta(7, 7, DocIdRangeMeta::OT_UNORDERED, layerMetas);
    createLayerMeta(8, 9, DocIdRangeMeta::OT_UNORDERED, layerMetas);

    createQueryExecutor({}, queryExecutors);
    createQueryExecutor({}, queryExecutors);
    createQueryExecutor({}, queryExecutors);
    createQueryExecutor({}, queryExecutors);

    Ha3ScanIteratorParam param;
    param.queryExecutors = queryExecutors;
    param.matchDocAllocator = _matchDocAllocator;
    param.layerMetas = layerMetas;

    SortInitParam sortDesc;
    sortDesc.topk = 5;
    sortDesc.keys = {"attr1"};
    sortDesc.orders = {false};
    // sortDesc.keys = {"attr1", "id"};
    // sortDesc.orders = {false, false};

    OrderedHa3ScanIterator orderedHa3ScanIterator(
        param, _poolPtr.get(), sortDesc, _attributeExpressionCreator.get());

    ASSERT_TRUE(orderedHa3ScanIterator.init());

    vector<MatchDoc> output;
    ASSERT_TRUE(orderedHa3ScanIterator.batchSeek(10, output).unwrap());
    ASSERT_TRUE(output.empty());
}

TEST_F(OrderedHa3ScanIteratorTest, testSimple) {
    prepareResource();
    std::vector<LayerMetaPtr> layerMetas;
    std::vector<QueryExecutorPtr> queryExecutors;
    createLayerMeta(0, 2, DocIdRangeMeta::OT_ORDERED, layerMetas);
    createLayerMeta(3, 6, DocIdRangeMeta::OT_ORDERED, layerMetas);
    createLayerMeta(7, 7, DocIdRangeMeta::OT_UNORDERED, layerMetas);
    createLayerMeta(8, 9, DocIdRangeMeta::OT_UNORDERED, layerMetas);

    createQueryExecutor({0, 1, 2}, queryExecutors);
    createQueryExecutor({3, 4, 5, 6}, queryExecutors);
    createQueryExecutor({7}, queryExecutors);
    createQueryExecutor({8, 9}, queryExecutors);

    Ha3ScanIteratorParam param;
    param.queryExecutors = queryExecutors;
    param.matchDocAllocator = _matchDocAllocator;
    param.layerMetas = layerMetas;

    SortInitParam sortDesc;
    sortDesc.topk = 5;
    sortDesc.keys = {"attr1"};
    sortDesc.orders = {false};
    // sortDesc.keys = {"attr1", "id"};
    // sortDesc.orders = {false, false};

    OrderedHa3ScanIterator orderedHa3ScanIterator(
        param, _poolPtr.get(), sortDesc, _attributeExpressionCreator.get());

    ASSERT_TRUE(orderedHa3ScanIterator.init());

    vector<MatchDoc> output;
    ASSERT_TRUE(orderedHa3ScanIterator.batchSeek(10, output).unwrap());
    ASSERT_NO_FATAL_FAILURE(checkDocIds(output, {9, 0, 3, 1, 4}));
    ASSERT_NO_FATAL_FAILURE(checkReference<int32_t>(output, "attr1", {2, 3, 4, 5, 5}));
}

TEST_F(OrderedHa3ScanIteratorTest, testTwoDims) {
    prepareResource();
    std::vector<LayerMetaPtr> layerMetas;
    std::vector<QueryExecutorPtr> queryExecutors;
    createLayerMeta(0, 2, DocIdRangeMeta::OT_ORDERED, layerMetas);
    createLayerMeta(3, 6, DocIdRangeMeta::OT_ORDERED, layerMetas);
    createLayerMeta(7, 7, DocIdRangeMeta::OT_UNORDERED, layerMetas);
    createLayerMeta(8, 9, DocIdRangeMeta::OT_UNORDERED, layerMetas);

    createQueryExecutor({0, 1, 2}, queryExecutors);
    createQueryExecutor({3, 4, 5, 6}, queryExecutors);
    createQueryExecutor({7}, queryExecutors);
    createQueryExecutor({8, 9}, queryExecutors);

    Ha3ScanIteratorParam param;
    param.queryExecutors = queryExecutors;
    param.matchDocAllocator = _matchDocAllocator;
    param.layerMetas = layerMetas;

    SortInitParam sortDesc;
    sortDesc.topk = 5;
    sortDesc.keys = {"attr1", "id"};
    sortDesc.orders = {false, true};

    OrderedHa3ScanIterator orderedHa3ScanIterator(
        param, _poolPtr.get(), sortDesc, _attributeExpressionCreator.get());

    ASSERT_TRUE(orderedHa3ScanIterator.init());

    vector<MatchDoc> output;
    ASSERT_TRUE(orderedHa3ScanIterator.batchSeek(10, output).unwrap());
    ASSERT_NO_FATAL_FAILURE(checkDocIds(output, {9, 0, 3, 8, 1}));
    ASSERT_NO_FATAL_FAILURE(checkReference<int32_t>(output, "attr1", {2, 3, 4, 5, 5}));
    ASSERT_NO_FATAL_FAILURE(checkReference<int64_t>(output, "id", {10, 1, 4, 9, 2}));
}

TEST_F(OrderedHa3ScanIteratorTest, testTwoDims2) {
    prepareResource();
    std::vector<LayerMetaPtr> layerMetas;
    std::vector<QueryExecutorPtr> queryExecutors;
    createLayerMeta(0, 2, DocIdRangeMeta::OT_ORDERED, layerMetas);
    createLayerMeta(3, 6, DocIdRangeMeta::OT_ORDERED, layerMetas);
    createLayerMeta(7, 7, DocIdRangeMeta::OT_UNORDERED, layerMetas);
    createLayerMeta(8, 9, DocIdRangeMeta::OT_UNORDERED, layerMetas);

    createQueryExecutor({0, 1, 2}, queryExecutors);
    createQueryExecutor({3, 4, 5, 6}, queryExecutors);
    createQueryExecutor({7}, queryExecutors);
    createQueryExecutor({8, 9}, queryExecutors);

    Ha3ScanIteratorParam param;
    param.queryExecutors = queryExecutors;
    param.matchDocAllocator = _matchDocAllocator;
    param.layerMetas = layerMetas;

    SortInitParam sortDesc;
    sortDesc.topk = 5;
    sortDesc.keys = {"attr1", "attr2"};
    sortDesc.orders = {false, true};

    OrderedHa3ScanIterator orderedHa3ScanIterator(
        param, _poolPtr.get(), sortDesc, _attributeExpressionCreator.get());

    ASSERT_TRUE(orderedHa3ScanIterator.init());

    vector<MatchDoc> output;
    ASSERT_TRUE(orderedHa3ScanIterator.batchSeek(10, output).unwrap());
    ASSERT_NO_FATAL_FAILURE(checkDocIds(output, {9, 0, 3, 4, 8}));
    ASSERT_NO_FATAL_FAILURE(checkReference<int32_t>(output, "attr1", {2, 3, 4, 5, 5}));
    ASSERT_NO_FATAL_FAILURE(
        checkReference<uint32_t>(output, "attr2", {{2, 22}, {0, 10}, {2, 22}, {2, 22}, {2, 20}}));
}

TEST_F(OrderedHa3ScanIteratorTest, testThreeDims) {
    prepareResource();
    std::vector<LayerMetaPtr> layerMetas;
    std::vector<QueryExecutorPtr> queryExecutors;
    createLayerMeta(0, 2, DocIdRangeMeta::OT_ORDERED, layerMetas);
    createLayerMeta(3, 6, DocIdRangeMeta::OT_ORDERED, layerMetas);
    createLayerMeta(7, 7, DocIdRangeMeta::OT_UNORDERED, layerMetas);
    createLayerMeta(8, 9, DocIdRangeMeta::OT_UNORDERED, layerMetas);

    createQueryExecutor({0, 1, 2}, queryExecutors);
    createQueryExecutor({3, 4, 5, 6}, queryExecutors);
    createQueryExecutor({7}, queryExecutors);
    createQueryExecutor({8, 9}, queryExecutors);

    Ha3ScanIteratorParam param;
    param.queryExecutors = queryExecutors;
    param.matchDocAllocator = _matchDocAllocator;
    param.layerMetas = layerMetas;

    SortInitParam sortDesc;
    sortDesc.topk = 5;
    sortDesc.keys = {"attr1", "attr3", "id"};
    sortDesc.orders = {false, true, false};

    OrderedHa3ScanIterator orderedHa3ScanIterator(
        param, _poolPtr.get(), sortDesc, _attributeExpressionCreator.get());

    ASSERT_TRUE(orderedHa3ScanIterator.init());

    vector<MatchDoc> output;
    ASSERT_TRUE(orderedHa3ScanIterator.batchSeek(10, output).unwrap());
    ASSERT_NO_FATAL_FAILURE(checkDocIds(output, {9, 0, 3, 8, 4}));
    ASSERT_NO_FATAL_FAILURE(checkReference<int32_t>(output, "attr1", {2, 3, 4, 5, 5}));
    ASSERT_NO_FATAL_FAILURE(checkReference<double>(output, "attr3", {3.3, 1.1, 3.3, 4.4, 3.3}));
    ASSERT_NO_FATAL_FAILURE(checkReference<int64_t>(output, "id", {10, 1, 4, 9, 0}));
}

TEST_F(OrderedHa3ScanIteratorTest, testTop9) {
    prepareResource();
    std::vector<LayerMetaPtr> layerMetas;
    std::vector<QueryExecutorPtr> queryExecutors;
    createLayerMeta(0, 2, DocIdRangeMeta::OT_ORDERED, layerMetas);
    createLayerMeta(3, 6, DocIdRangeMeta::OT_ORDERED, layerMetas);
    createLayerMeta(7, 7, DocIdRangeMeta::OT_UNORDERED, layerMetas);
    createLayerMeta(8, 9, DocIdRangeMeta::OT_UNORDERED, layerMetas);

    createQueryExecutor({0, 1, 2}, queryExecutors);
    createQueryExecutor({3, 4, 5, 6}, queryExecutors);
    createQueryExecutor({7}, queryExecutors);
    createQueryExecutor({8, 9}, queryExecutors);

    Ha3ScanIteratorParam param;
    param.queryExecutors = queryExecutors;
    param.matchDocAllocator = _matchDocAllocator;
    param.layerMetas = layerMetas;

    SortInitParam sortDesc;
    sortDesc.topk = 9;
    sortDesc.keys = {"attr1", "id"};
    sortDesc.orders = {false, false};

    OrderedHa3ScanIterator orderedHa3ScanIterator(
        param, _poolPtr.get(), sortDesc, _attributeExpressionCreator.get());

    ASSERT_TRUE(orderedHa3ScanIterator.init());

    vector<MatchDoc> output;
    ASSERT_TRUE(orderedHa3ScanIterator.batchSeek(10, output).unwrap());
    ASSERT_NO_FATAL_FAILURE(checkDocIds(output, {9, 0, 3, 4, 1, 8, 5, 2, 6}));
    ASSERT_NO_FATAL_FAILURE(checkReference<int32_t>(output, "attr1", {2, 3, 4, 5, 5, 5, 6, 7, 7}));
    ASSERT_NO_FATAL_FAILURE(checkReference<int64_t>(output, "id", {10, 1, 4, 0, 2, 9, 6, 3, 7}));
}

} // namespace sql
