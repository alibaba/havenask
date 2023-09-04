#include "sql/ops/scan/QueryExecutorExpressionWrapper.h"

#include <algorithm>
#include <iosfwd>
#include <memory>
#include <stddef.h>
#include <type_traits>
#include <vector>

#include "autil/mem_pool/PoolBase.h"
#include "ha3/common/Query.h"
#include "ha3/common/Term.h"
#include "ha3/common/TermQuery.h"
#include "ha3/search/IndexPartitionReaderUtil.h"
#include "ha3/search/IndexPartitionReaderWrapper.h"
#include "ha3/search/LayerMetas.h"
#include "ha3/search/PartitionInfoWrapper.h"
#include "ha3/search/QueryExecutor.h"
#include "ha3/search/RangeQueryExecutor.h"
#include "indexlib/partition/index_application.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/ValueType.h"
#include "navi/log/NaviLoggerProvider.h"
#include "navi/tester/NaviResourceHelper.h"
#include "navi/util/NaviTestPool.h"
#include "sql/ops/scan/ScanR.h"
#include "sql/ops/test/OpTestBase.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace isearch::search;
using namespace isearch::common;
using namespace matchdoc;

namespace sql {

class QueryExecutorExpressionWrapperTest : public OpTestBase {
public:
    void SetUp() override {
        _needBuildIndex = true;
        OpTestBase::SetUp();
        auto *naviRHelper = getNaviRHelper();
        auto *scanR = naviRHelper->getOrCreateRes<ScanR>();
        _indexPartitionReader = IndexPartitionReaderUtil::createIndexPartitionReaderWrapper(
            scanR->partitionReaderSnapshot.get(), _tableName);
    }

private:
    IndexPartitionReaderWrapperPtr _indexPartitionReader;
};

TEST_F(QueryExecutorExpressionWrapperTest, testInitError_createQueryNotExist) {
    navi::NaviLoggerProvider provider("ERROR");
    Query *query(new TermQuery("a", "not_exist", {}, ""));
    QueryExecutorExpressionWrapper wrapper(query);
    LayerMetaPtr layerMeta1(new LayerMeta(_poolPtr.get()));
    layerMeta1->push_back(DocIdRangeMeta(0, 1));
    LayerMetaPtr layerMeta2(new LayerMeta(_poolPtr.get()));
    layerMeta2->push_back(DocIdRangeMeta(2, 3));
    std::vector<LayerMetaPtr> layerMetas = {layerMeta1, layerMeta2};
    ASSERT_TRUE(
        wrapper.init(_indexPartitionReader, _tableName, _poolPtr.get(), nullptr, layerMetas));
    ASSERT_EQ(2, wrapper._queryExecutors.size());
    ASSERT_TRUE(wrapper._queryExecutors[0]->isEmpty());
}

TEST_F(QueryExecutorExpressionWrapperTest, testInitError_emptyLayerMeta) {
    navi::NaviLoggerProvider provider("ERROR");
    Query *query(new TermQuery("a", "index_2", {}, ""));
    QueryExecutorExpressionWrapper wrapper(query);
    LayerMetaPtr layerMeta1(new LayerMeta(_poolPtr.get()));
    LayerMetaPtr layerMeta2(new LayerMeta(_poolPtr.get()));
    std::vector<LayerMetaPtr> layerMetas = {layerMeta1, layerMeta2};
    ASSERT_FALSE(
        wrapper.init(_indexPartitionReader, _tableName, _poolPtr.get(), nullptr, layerMetas));
    auto traces = provider.getTrace("");
    CHECK_TRACE_COUNT(
        1, "unexpected layer meta is empty, query [TermQuery$1:[Term:[index_2||a|100|]]]", traces);
}

TEST_F(QueryExecutorExpressionWrapperTest, testInitError_emptyLayerMetas) {
    navi::NaviLoggerProvider provider("ERROR");
    Query *query(new TermQuery("a", "index_2", {}, ""));
    QueryExecutorExpressionWrapper wrapper(query);
    std::vector<LayerMetaPtr> layerMetas;
    ASSERT_FALSE(
        wrapper.init(_indexPartitionReader, _tableName, _poolPtr.get(), nullptr, layerMetas));
    auto traces = provider.getTrace("");
    CHECK_TRACE_COUNT(1, "unexpected query expression is empty", traces);
}

TEST_F(QueryExecutorExpressionWrapperTest, testInit) {
    Query *query(new TermQuery("a", "index_2", {}, ""));
    QueryExecutorExpressionWrapper wrapper(query);
    LayerMetaPtr layerMeta1(new LayerMeta(_poolPtr.get()));
    layerMeta1->push_back(DocIdRangeMeta(0, 1));
    LayerMetaPtr layerMeta2(new LayerMeta(_poolPtr.get()));
    layerMeta2->push_back(DocIdRangeMeta(2, 3));
    std::vector<LayerMetaPtr> layerMetas = {layerMeta1, layerMeta2};
    ASSERT_TRUE(
        wrapper.init(_indexPartitionReader, _tableName, _poolPtr.get(), nullptr, layerMetas));
    ASSERT_EQ(2, wrapper._queryExecutors.size());
}

TEST_F(QueryExecutorExpressionWrapperTest, testEvaluateAndReturn) {
    autil::mem_pool::PoolAsan pool;
    LayerMeta layerMeta(&pool);
    layerMeta.push_back(DocIdRangeMeta(5, 7));
    RangeQueryExecutor *executor = POOL_NEW_CLASS(_poolPtr.get(), RangeQueryExecutor, &layerMeta);
    QueryExecutorExpressionWrapper wrapper(nullptr);
    wrapper._queryExecutors.push_back(executor);
    ASSERT_FALSE(wrapper.evaluateAndReturn(MatchDoc(0, 1)));
    ASSERT_FALSE(wrapper.evaluateAndReturn(MatchDoc(0, 4)));
    ASSERT_TRUE(wrapper.evaluateAndReturn(MatchDoc(0, 5)));
    ASSERT_TRUE(wrapper.evaluateAndReturn(MatchDoc(0, 6)));
    ASSERT_TRUE(wrapper.evaluateAndReturn(MatchDoc(0, 7)));
    ASSERT_FALSE(wrapper.evaluateAndReturn(MatchDoc(0, 8)));
}

TEST_F(QueryExecutorExpressionWrapperTest, testMultiLayer) {
    Query *query(new TermQuery("a", "index_2", {}, ""));
    QueryExecutorExpressionWrapper wrapper(query);
    LayerMetaPtr layerMeta1(new LayerMeta(_poolPtr.get()));
    layerMeta1->push_back(DocIdRangeMeta(0, 1));
    LayerMetaPtr layerMeta2(new LayerMeta(_poolPtr.get()));
    layerMeta2->push_back(DocIdRangeMeta(2, 3));
    std::vector<LayerMetaPtr> layerMetas = {layerMeta1, layerMeta2};
    ASSERT_TRUE(
        wrapper.init(_indexPartitionReader, _tableName, _poolPtr.get(), nullptr, layerMetas));
    ASSERT_EQ(2, wrapper._queryExecutors.size());

    ASSERT_TRUE(wrapper.evaluateAndReturn(MatchDoc(0, 0)));
    ASSERT_TRUE(wrapper.evaluateAndReturn(MatchDoc(0, 2)));
    ASSERT_TRUE(wrapper.evaluateAndReturn(MatchDoc(0, 1)));
    ASSERT_FALSE(wrapper.evaluateAndReturn(MatchDoc(0, 3)));
    ASSERT_FALSE(wrapper.evaluateAndReturn(MatchDoc(0, 4)));
}

TEST_F(QueryExecutorExpressionWrapperTest, testSingleLayer) {
    Query *query(new TermQuery("a", "index_2", {}, ""));
    QueryExecutorExpressionWrapper wrapper(query);
    LayerMetaPtr layerMeta1(new LayerMeta(_poolPtr.get()));
    layerMeta1->push_back(DocIdRangeMeta(0, 1));
    layerMeta1->push_back(DocIdRangeMeta(2, 3));
    std::vector<LayerMetaPtr> layerMetas = {layerMeta1};
    ASSERT_TRUE(
        wrapper.init(_indexPartitionReader, _tableName, _poolPtr.get(), nullptr, layerMetas));
    ASSERT_EQ(1, wrapper._queryExecutors.size());

    ASSERT_TRUE(wrapper.evaluateAndReturn(MatchDoc(0, 0)));
    ASSERT_TRUE(wrapper.evaluateAndReturn(MatchDoc(0, 1)));
    ASSERT_TRUE(wrapper.evaluateAndReturn(MatchDoc(0, 2)));
    ASSERT_FALSE(wrapper.evaluateAndReturn(MatchDoc(0, 3)));
    ASSERT_FALSE(wrapper.evaluateAndReturn(MatchDoc(0, 4)));
}

TEST_F(QueryExecutorExpressionWrapperTest, testRepeatInit) {
    Query *query(new TermQuery("a", "index_2", {}, ""));
    QueryExecutorExpressionWrapper wrapper(query);
    LayerMetaPtr layerMeta1(new LayerMeta(_poolPtr.get()));
    layerMeta1->push_back(DocIdRangeMeta(0, 1));
    layerMeta1->push_back(DocIdRangeMeta(2, 3));
    std::vector<LayerMetaPtr> layerMetas = {layerMeta1};

    {
        ASSERT_TRUE(
            wrapper.init(_indexPartitionReader, _tableName, _poolPtr.get(), nullptr, layerMetas));
        ASSERT_EQ(1, wrapper._queryExecutors.size());

        ASSERT_TRUE(wrapper.evaluateAndReturn(MatchDoc(0, 0)));
        ASSERT_TRUE(wrapper.evaluateAndReturn(MatchDoc(0, 1)));
        ASSERT_TRUE(wrapper.evaluateAndReturn(MatchDoc(0, 2)));
        ASSERT_FALSE(wrapper.evaluateAndReturn(MatchDoc(0, 3)));
        ASSERT_FALSE(wrapper.evaluateAndReturn(MatchDoc(0, 4)));
    }

    {
        ASSERT_TRUE(
            wrapper.init(_indexPartitionReader, _tableName, _poolPtr.get(), nullptr, layerMetas));
        ASSERT_EQ(1, wrapper._queryExecutors.size());

        ASSERT_TRUE(wrapper.evaluateAndReturn(MatchDoc(0, 0)));
        ASSERT_TRUE(wrapper.evaluateAndReturn(MatchDoc(0, 1)));
        ASSERT_TRUE(wrapper.evaluateAndReturn(MatchDoc(0, 2)));
        ASSERT_FALSE(wrapper.evaluateAndReturn(MatchDoc(0, 3)));
        ASSERT_FALSE(wrapper.evaluateAndReturn(MatchDoc(0, 4)));
    }
}

TEST_F(QueryExecutorExpressionWrapperTest, testCreateQueryExecutor) {
    LayerMetaPtr layerMeta(new LayerMeta(_poolPtr.get()));
    layerMeta->push_back(DocIdRangeMeta(0, 1));
    { // query is empty
        QueryExecutor *executor = QueryExecutorExpressionWrapper::createQueryExecutor(
            NULL, _indexPartitionReader, _tableName, _poolPtr.get(), NULL, layerMeta.get());
        ASSERT_TRUE(executor == NULL);
    }
    { // term not exist
        Term term;
        term.setIndexName("not_exist");
        term.setWord("not_exist");
        QueryPtr query(new TermQuery(term, ""));
        QueryExecutor *executor = QueryExecutorExpressionWrapper::createQueryExecutor(
            query.get(), _indexPartitionReader, _tableName, _poolPtr.get(), NULL, layerMeta.get());
        QueryExecutorPtr executorPtr(executor, [](QueryExecutor *p) { POOL_DELETE_CLASS(p); });
        ASSERT_TRUE(executorPtr);
        ASSERT_EQ("TermQueryExecutor", executorPtr->getName());
    }
    {
        Term term;
        term.setIndexName("name");
        term.setWord("aa");
        QueryPtr query(new TermQuery(term, ""));
        QueryExecutor *executor = QueryExecutorExpressionWrapper::createQueryExecutor(
            query.get(), _indexPartitionReader, _tableName, _poolPtr.get(), NULL, layerMeta.get());
        QueryExecutorPtr executorPtr(executor, [](QueryExecutor *p) { POOL_DELETE_CLASS(p); });
        ASSERT_TRUE(executorPtr);
        ASSERT_EQ("BufferedTermQueryExecutor", executorPtr->getName());
    }
}

} // namespace sql
