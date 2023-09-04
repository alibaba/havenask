#include "sql/ops/scan/ScanIteratorCreatorR.h"

#include <algorithm>
#include <assert.h>
#include <cstddef>
#include <cstdint>
#include <engine/ResourceInitContext.h>
#include <exception>
#include <ext/alloc_traits.h>
#include <fstream>
#include <limits>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "autil/CommonMacros.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/json.h"
#include "autil/legacy/jsonizable.h"
#include "autil/mem_pool/PoolBase.h"
#include "autil/mem_pool/PoolVector.h"
#include "autil/result/Result.h"
#include "ha3/common/AndQuery.h"
#include "ha3/common/DocIdsQuery.h"
#include "ha3/common/OrQuery.h"
#include "ha3/common/Query.h"
#include "ha3/common/Term.h"
#include "ha3/common/TermQuery.h"
#include "ha3/isearch.h"
#include "ha3/search/AuxiliaryChainDefine.h"
#include "ha3/search/FilterWrapper.h"
#include "ha3/search/IndexPartitionReaderWrapper.h"
#include "ha3/search/LayerMetas.h"
#include "ha3/search/MatchDataManager.h"
#include "ha3/search/QueryExecutor.h"
#include "indexlib/base/Types.h"
#include "indexlib/config/FileCompressSchema.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/legacy_schema_adapter.h"
#include "indexlib/config/online_config.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/index_application.h"
#include "indexlib/partition/index_partition.h"
#include "indexlib/testlib/indexlib_partition_creator.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/ValueType.h"
#include "navi/common.h"
#include "navi/log/NaviLoggerProvider.h"
#include "navi/tester/NaviResourceHelper.h"
#include "navi/util/NaviTestPool.h"
#include "sql/common/IndexInfo.h"
#include "sql/ops/calc/CalcInitParamR.h"
#include "sql/ops/condition/NotExpressionWrapper.h"
#include "sql/ops/scan/AttributeExpressionCreatorR.h"
#include "sql/ops/scan/DocIdsScanIterator.h"
#include "sql/ops/scan/QueryExecutorExpressionWrapper.h"
#include "sql/ops/scan/QueryScanIterator.h"
#include "sql/ops/scan/RangeScanIterator.h"
#include "sql/ops/scan/RangeScanIteratorWithoutFilter.h"
#include "sql/ops/scan/ScanInitParamR.h"
#include "sql/ops/scan/ScanIterator.h"
#include "sql/ops/scan/UseSubR.h"
#include "sql/ops/scan/udf_to_query/UdfToQueryManagerR.h"
#include "sql/ops/sort/SortInitParam.h"
#include "sql/ops/test/OpTestBase.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/framework/AttributeExpression.h"
#include "suez/turing/expression/framework/AttributeExpressionCreator.h"
#include "suez/turing/expression/provider/common.h"
#include "unittest/unittest.h"

namespace autil {
namespace mem_pool {
class Pool;
} // namespace mem_pool
} // namespace autil

using namespace std;
using namespace testing;
using namespace autil::legacy;
using namespace autil;
using namespace autil::legacy::json;
using namespace matchdoc;
using namespace indexlib::config;
using namespace suez::turing;

using namespace isearch::search;
using namespace isearch::common;

namespace sql {

class ScanIteratorCreatorRTest : public OpTestBase {
public:
    void setUp() override;
    IndexPartitionReaderWrapperPtr getIndexPartitionReaderWrapper();
    void innerTestProportionalLayerQuota(const string &layerMetaStr,
                                         uint32_t quota,
                                         const string &expectedQuotaStr);
    LayerMeta createLayerMeta(autil::mem_pool::Pool *pool, const string &layerMetaStr);
    void prepareInvertedTable() override {
        string tableName = _tableName;
        std::string testPath = _testPath + tableName;
        auto indexPartition = makeIndexPartition(testPath, tableName);
        std::string schemaName = indexPartition->GetSchema()->GetSchemaName();
        _indexPartitionMap.insert(make_pair(schemaName, indexPartition));
    }
    indexlib::partition::IndexPartitionPtr makeIndexPartition(const std::string &rootPath,
                                                              const std::string &tableName);
    void prepareResource(ScanIteratorCreatorR &scanIterCreatorR, bool useSubFlag = false);
    IndexPartitionSchemaPtr prepareSchema() {
        string schemaFile
            = GET_TEST_DATA_PATH() + "/test_schema/term_match/mainse_excellent_search_schema.json";
        return readSchema(schemaFile);
    }
    IndexPartitionSchemaPtr readSchema(const string &fileName) {
        // read config file
        ifstream in(fileName.c_str());
        string line;
        string jsonString;
        while (getline(in, line)) {
            jsonString += line;
        }
        IndexPartitionSchemaPtr schema(new IndexPartitionSchema("noname"));
        try {
            Any any = ParseJson(jsonString);
            FromJson(*schema, any);
        } catch (const std::exception &e) { return IndexPartitionSchemaPtr(); }
        return schema;
    }

private:
    autil::mem_pool::PoolAsan _pool;
};

void ScanIteratorCreatorRTest::setUp() {
    _needBuildIndex = true;
    _needExprResource = true;
}

void ScanIteratorCreatorRTest::prepareResource(ScanIteratorCreatorR &scanIterCreatorR,
                                               bool useSub) {
    auto *naviRHelper = getNaviRHelper();
    auto calcInitParamR = std::make_shared<CalcInitParamR>();
    ASSERT_TRUE(naviRHelper->addExternalRes(calcInitParamR));
    auto scanInitParamR = std::make_shared<ScanInitParamR>();
    scanInitParamR->parallelIndex = 0;
    scanInitParamR->parallelNum = 1;
    scanInitParamR->limit = std::numeric_limits<uint32_t>::max();
    scanInitParamR->tableName = _tableName;
    scanInitParamR->calcInitParamR = calcInitParamR.get();
    ASSERT_TRUE(naviRHelper->addExternalRes(scanInitParamR));
    scanIterCreatorR._scanInitParamR = scanInitParamR.get();
    auto useSubR = std::make_shared<UseSubR>();
    useSubR->_useSub = false;
    ASSERT_TRUE(naviRHelper->addExternalRes(useSubR));
    scanIterCreatorR._useSubR = useSubR.get();
    auto udfR = std::make_shared<UdfToQueryManagerR>();
    ASSERT_TRUE(naviRHelper->addExternalRes(udfR));
    scanIterCreatorR._udfToQueryManagerR = udfR.get();
    ASSERT_TRUE(naviRHelper->getOrCreateRes(scanIterCreatorR._timeoutTerminatorR));
    ASSERT_TRUE(naviRHelper->getOrCreateRes(scanIterCreatorR._queryMemPoolR));
    ASSERT_TRUE(naviRHelper->getOrCreateRes(scanIterCreatorR._attributeExpressionCreatorR));
    ASSERT_TRUE(naviRHelper->getOrCreateRes(scanIterCreatorR._analyzerFactoryR));
    ASSERT_TRUE(naviRHelper->getOrCreateRes(scanIterCreatorR._ha3TableInfoR));
    auto *attrCreatorR = scanIterCreatorR._attributeExpressionCreatorR;
    ASSERT_TRUE(attrCreatorR);
    ASSERT_TRUE(attrCreatorR->_attributeExpressionCreator);
    ASSERT_TRUE(attrCreatorR->_indexPartitionReaderWrapper);
    ASSERT_TRUE(attrCreatorR->_matchDocAllocator);
    scanIterCreatorR._attributeExpressionCreator = attrCreatorR->_attributeExpressionCreator;
    scanIterCreatorR._indexPartitionReaderWrapper = attrCreatorR->_indexPartitionReaderWrapper;
    scanIterCreatorR._matchDocAllocator = attrCreatorR->_matchDocAllocator;
    scanIterCreatorR._matchDataManager = std::make_shared<isearch::search::MatchDataManager>();
    useSubR->_useSub = useSub;
}

indexlib::partition::IndexPartitionPtr
ScanIteratorCreatorRTest::makeIndexPartition(const std::string &rootPath,
                                             const std::string &tableName) {
    int64_t ttl = std::numeric_limits<int64_t>::max();
    auto mainSchema = indexlib::testlib::IndexlibPartitionCreator::CreateSchema(
        tableName,
        "attr1:uint32;attr2:int32:true;id:int64;name:string;index2:string", // fields
        "id:primarykey64:id;index_2:string:index2;name:string:name",        // indexs
        "attr1;attr2;id",                                                   // attributes
        "name",                                                             // summary
        "");                                                                // truncateProfile

    auto subSchema = indexlib::testlib::IndexlibPartitionCreator::CreateSchema(
        "sub_" + tableName,
        "sub_id:int64;sub_attr1:int32;sub_index2:string",           // fields
        "sub_id:primarykey64:sub_id;sub_index_2:string:sub_index2", // indexs
        "sub_attr1;sub_id;sub_index2",                              // attributes
        "",                                                         // summarys
        "");                                                        // truncateProfile
    string docsStr = "cmd=add,attr1=0,attr2=0 10,id=1,name=aa,index2=b,"
                     "sub_id=1^2^3,sub_attr1=1^2^2,sub_index2=aa^ab^ac;"
                     "cmd=add,attr1=1,attr2=1 11,id=2,name=bb,index2=a,"
                     "sub_id=4,sub_attr1=1,sub_index2=aa;"
                     "cmd=add,attr1=2,attr2=2 22,id=3,name=cc,index2=b,"
                     "sub_id=5,sub_attr1=1,sub_index2=ab;"
                     "cmd=add,attr1=3,attr2=3 33,id=4,name=dd,index2=c,"
                     "sub_id=6^7,sub_attr1=2^1,sub_index2=abc^ab";

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

TEST_F(ScanIteratorCreatorRTest, testCreateScanIteratorWithDocIds) {
    ScanIteratorCreatorR scanIterCreatorR;
    ASSERT_NO_FATAL_FAILURE(prepareResource(scanIterCreatorR, true));
    LayerMetaPtr layerMeta = scanIterCreatorR.createLayerMeta();
    std::vector<docid_t> docIds = {10, 5, 8};
    QueryPtr query(new DocIdsQuery(docIds));
    scanIterCreatorR._createScanIteratorInfo.query = query;
    scanIterCreatorR._createScanIteratorInfo.filterWrapper = {};
    scanIterCreatorR._createScanIteratorInfo.queryExprs = {};
    scanIterCreatorR._createScanIteratorInfo.layerMeta = layerMeta;
    bool emptyScan = false;
    auto iter = scanIterCreatorR.createScanIterator(emptyScan);
    ASSERT_TRUE(dynamic_cast<DocIdsScanIterator *>(iter.get()) != nullptr);
    ASSERT_FALSE(emptyScan);
}

TEST_F(ScanIteratorCreatorRTest, testCreateScanIteratorWithQueryFilterFailed) {
    ScanIteratorCreatorR scanIterCreatorR;
    ASSERT_NO_FATAL_FAILURE(prepareResource(scanIterCreatorR, true));
    LayerMetaPtr layerMeta = scanIterCreatorR.createLayerMeta();
    AttributeExpressionTyped<bool> *queryExpr
        = POOL_NEW_CLASS((&_pool), QueryExecutorExpressionWrapper, nullptr);
    ASSERT_TRUE(queryExpr != nullptr);
    AttributeExpression *boolAttrExpr = POOL_NEW_CLASS((&_pool), NotExpressionWrapper, queryExpr);
    ASSERT_TRUE(boolAttrExpr != nullptr);
    FilterWrapperPtr filterWrapper;
    ASSERT_TRUE(scanIterCreatorR.createFilterWrapper(boolAttrExpr, {}, NULL, filterWrapper));
    QueryPtr query(new TermQuery("aa", "name", RequiredFields(), ""));
    scanIterCreatorR._createScanIteratorInfo.query = query;
    scanIterCreatorR._createScanIteratorInfo.filterWrapper = filterWrapper;
    scanIterCreatorR._createScanIteratorInfo.queryExprs = {queryExpr};
    scanIterCreatorR._createScanIteratorInfo.layerMeta = layerMeta;
    bool emptyScan = false;
    navi::NaviLoggerProvider provider("WARN");
    auto iter = scanIterCreatorR.createScanIterator(emptyScan);
    ASSERT_TRUE(iter.get() == nullptr);
    auto traces = provider.getTrace("");
    CHECK_TRACE_COUNT(1, "query is null", traces);
    POOL_DELETE_CLASS(boolAttrExpr);
    POOL_DELETE_CLASS(queryExpr);
}

TEST_F(ScanIteratorCreatorRTest, testCreateScanIteratorWithQueryFilter) {
    ScanIteratorCreatorR scanIterCreatorR;
    ASSERT_NO_FATAL_FAILURE(prepareResource(scanIterCreatorR, true));
    LayerMetaPtr layerMeta = scanIterCreatorR.createLayerMeta();
    Query *query2(new TermQuery("bb", "name", RequiredFields(), ""));
    AttributeExpressionTyped<bool> *queryExpr
        = POOL_NEW_CLASS((&_pool), QueryExecutorExpressionWrapper, query2);
    ASSERT_TRUE(queryExpr != nullptr);
    AttributeExpression *boolAttrExpr = POOL_NEW_CLASS((&_pool), NotExpressionWrapper, queryExpr);
    ASSERT_TRUE(boolAttrExpr != nullptr);
    FilterWrapperPtr filterWrapper;
    ASSERT_TRUE(scanIterCreatorR.createFilterWrapper(boolAttrExpr, {}, NULL, filterWrapper));
    QueryPtr query(new TermQuery("aa", "name", RequiredFields(), ""));
    scanIterCreatorR._createScanIteratorInfo.query = query;
    scanIterCreatorR._createScanIteratorInfo.filterWrapper = filterWrapper;
    scanIterCreatorR._createScanIteratorInfo.queryExprs = {queryExpr};
    scanIterCreatorR._createScanIteratorInfo.layerMeta = layerMeta;
    bool emptyScan = false;
    auto iter = scanIterCreatorR.createScanIterator(emptyScan);
    // QueryScanIterator
    ASSERT_TRUE(dynamic_cast<QueryScanIterator *>(iter.get()) != nullptr);
    ASSERT_FALSE(emptyScan);
    ASSERT_TRUE(iter != nullptr);
    vector<MatchDoc> matchDocs;
    bool ret = iter->batchSeek(10, matchDocs).unwrap();
    ASSERT_TRUE(ret);
    ASSERT_EQ(1, matchDocs.size());
    POOL_DELETE_CLASS(boolAttrExpr);
    POOL_DELETE_CLASS(queryExpr);
}

TEST_F(ScanIteratorCreatorRTest, testCreateScanIteratorWithQuery) {
    ScanIteratorCreatorR scanIterCreatorR;
    ASSERT_NO_FATAL_FAILURE(prepareResource(scanIterCreatorR, true));
    bool emptyScan = false;
    LayerMetaPtr layerMeta = scanIterCreatorR.createLayerMeta();
    AttributeExpression *boolAttrExpr
        = scanIterCreatorR._attributeExpressionCreator->createAttributeExpression("id=1");
    ASSERT_TRUE(boolAttrExpr != nullptr);
    FilterWrapperPtr filterWrapper;
    ASSERT_TRUE(scanIterCreatorR.createFilterWrapper(boolAttrExpr, {}, NULL, filterWrapper));
    QueryPtr query(new TermQuery("aa", "name", RequiredFields(), ""));
    { // QueryScanIterator
        scanIterCreatorR._createScanIteratorInfo.query = query;
        scanIterCreatorR._createScanIteratorInfo.filterWrapper = filterWrapper;
        scanIterCreatorR._createScanIteratorInfo.queryExprs = {};
        scanIterCreatorR._createScanIteratorInfo.layerMeta = layerMeta;
        auto iter = scanIterCreatorR.createScanIterator(emptyScan);
        ASSERT_TRUE(dynamic_cast<QueryScanIterator *>(iter.get()) != nullptr);
        ASSERT_FALSE(emptyScan);
        ASSERT_TRUE(iter != nullptr);
        vector<MatchDoc> matchDocs;
        bool ret = iter->batchSeek(10, matchDocs).unwrap();
        ASSERT_TRUE(ret);
        ASSERT_EQ(1, matchDocs.size());
    }
    { // RangeScanIterator
        scanIterCreatorR._createScanIteratorInfo.query = QueryPtr();
        scanIterCreatorR._createScanIteratorInfo.filterWrapper = filterWrapper;
        scanIterCreatorR._createScanIteratorInfo.queryExprs = {};
        scanIterCreatorR._createScanIteratorInfo.layerMeta = layerMeta;
        auto iter = scanIterCreatorR.createScanIterator(emptyScan);
        ASSERT_TRUE(dynamic_cast<RangeScanIterator *>(iter.get()) != nullptr);
        ASSERT_FALSE(emptyScan);
        ASSERT_TRUE(iter != nullptr);
        vector<MatchDoc> matchDocs;
        bool ret = iter->batchSeek(10, matchDocs).unwrap();
        ASSERT_TRUE(ret);
        ASSERT_EQ(1, matchDocs.size());
    }
    { // RangeScanIteratorWihtoutFilter
        scanIterCreatorR._createScanIteratorInfo.query = QueryPtr();
        scanIterCreatorR._createScanIteratorInfo.filterWrapper = FilterWrapperPtr();
        scanIterCreatorR._createScanIteratorInfo.queryExprs = {};
        scanIterCreatorR._createScanIteratorInfo.layerMeta = layerMeta;
        auto iter = scanIterCreatorR.createScanIterator(emptyScan);
        ASSERT_TRUE(dynamic_cast<RangeScanIteratorWithoutFilter *>(iter.get()) != nullptr);
        ASSERT_FALSE(emptyScan);
        ASSERT_TRUE(iter != nullptr);
        vector<MatchDoc> matchDocs;
        bool ret = iter->batchSeek(10, matchDocs).unwrap();
        ASSERT_TRUE(ret);
        ASSERT_EQ(4, matchDocs.size());
    }
}

TEST_F(ScanIteratorCreatorRTest, testCreateHa3ScanIterator) {
    ScanIteratorCreatorR scanIterCreatorR;
    ASSERT_NO_FATAL_FAILURE(prepareResource(scanIterCreatorR, true));
    LayerMetaPtr layerMeta = scanIterCreatorR.createLayerMeta();
    ScanIteratorCreatorR::proportionalLayerQuota(*layerMeta.get());
    AttributeExpression *boolAttrExpr
        = scanIterCreatorR._attributeExpressionCreator->createAttributeExpression("id=1");
    ASSERT_TRUE(boolAttrExpr != nullptr);
    FilterWrapperPtr filterWrapper;
    ASSERT_TRUE(scanIterCreatorR.createFilterWrapper(boolAttrExpr, {}, NULL, filterWrapper));
    {
        QueryPtr query(new TermQuery("aa", "name", RequiredFields(), ""));
        bool emptyScan = false;
        auto iter
            = scanIterCreatorR.createHa3ScanIterator(query, filterWrapper, layerMeta, emptyScan);
        ASSERT_FALSE(emptyScan);
        ASSERT_TRUE(iter != nullptr);
        vector<MatchDoc> matchDocs;
        bool ret = iter->batchSeek(10, matchDocs).unwrap();
        ASSERT_TRUE(ret);
        ASSERT_EQ(1, matchDocs.size());
        DELETE_AND_SET_NULL(iter);
    }
    {
        QueryPtr emptyQuery(new TermQuery("aaaa", "name", RequiredFields(), ""));
        bool emptyScan = false;
        auto iter = scanIterCreatorR.createHa3ScanIterator(
            emptyQuery, filterWrapper, layerMeta, emptyScan);
        ASSERT_TRUE(emptyScan);
        ASSERT_TRUE(iter == nullptr);
    }
}

TEST_F(ScanIteratorCreatorRTest, testCreateOrderedHa3ScanIterator) {
    ScanIteratorCreatorR scanIterCreatorR;
    ASSERT_NO_FATAL_FAILURE(prepareResource(scanIterCreatorR, false));
    AttributeExpression *boolAttrExpr
        = scanIterCreatorR._attributeExpressionCreator->createAttributeExpression("id>1");
    ASSERT_TRUE(boolAttrExpr != nullptr);
    FilterWrapperPtr filterWrapper;
    ASSERT_TRUE(scanIterCreatorR.createFilterWrapper(boolAttrExpr, {}, NULL, filterWrapper));
    QueryPtr q1(new TermQuery("aa", "name", RequiredFields(), ""));
    QueryPtr q2(new TermQuery("bb", "name", RequiredFields(), ""));
    QueryPtr q3(new TermQuery("cc", "name", RequiredFields(), ""));
    QueryPtr q4(new TermQuery("dd", "name", RequiredFields(), ""));
    QueryPtr query(new OrQuery(""));
    query->addQuery(q1);
    query->addQuery(q2);
    query->addQuery(q3);
    query->addQuery(q4);
    bool emptyScan = false;
    {
        navi::NaviLoggerProvider provider("ERROR");
        LayerMetaPtr layerMeta(new LayerMeta(_poolPtr.get()));
        scanIterCreatorR._useSubR->_useSub = true;
        layerMeta->push_back(DocIdRangeMeta(0, 1, DocIdRangeMeta::OT_ORDERED, 2));
        auto iter = scanIterCreatorR.createOrderedHa3ScanIterator(
            query, filterWrapper, {layerMeta}, emptyScan);
        ASSERT_FALSE(emptyScan);
        ASSERT_FALSE(iter != nullptr);
        auto traces = provider.getTrace("");
        CHECK_TRACE_COUNT(1, "unexpected ordered scan iter has UNNEST table", traces);
        scanIterCreatorR._useSubR->_useSub = false;
    }
    {
        navi::NaviLoggerProvider provider("WARN");
        LayerMetaPtr layerMeta(new LayerMeta(_poolPtr.get()));
        layerMeta->push_back(DocIdRangeMeta(0, 1, DocIdRangeMeta::OT_ORDERED, 2));
        scanIterCreatorR._scanInitParamR->sortDesc.keys = {"not_exist"};
        auto iter = scanIterCreatorR.createOrderedHa3ScanIterator(
            query, filterWrapper, {layerMeta}, emptyScan);
        ASSERT_FALSE(emptyScan);
        ASSERT_FALSE(iter != nullptr);
        auto traces = provider.getTrace("");
        CHECK_TRACE_COUNT(1, "create orderedHa3ScanIterator failed", traces);
        scanIterCreatorR._scanInitParamR->sortDesc.keys = {};
        DELETE_AND_SET_NULL(iter);
    }
    {
        LayerMetaPtr layerMeta(new LayerMeta(_poolPtr.get()));
        layerMeta->push_back(DocIdRangeMeta(0, 1, DocIdRangeMeta::OT_ORDERED, 2));
        auto iter = scanIterCreatorR.createOrderedHa3ScanIterator(
            query, filterWrapper, {layerMeta}, emptyScan);
        ASSERT_FALSE(emptyScan);
        ASSERT_TRUE(iter != nullptr);
        DELETE_AND_SET_NULL(iter);
    }
    {
        LayerMetaPtr layerMeta(new LayerMeta(_poolPtr.get()));
        layerMeta->push_back(DocIdRangeMeta(0, 1, DocIdRangeMeta::OT_ORDERED, 2));
        layerMeta->push_back(DocIdRangeMeta(2, 3, DocIdRangeMeta::OT_UNORDERED, 2));
        scanIterCreatorR._scanInitParamR->sortDesc.keys = {"attr1"};
        scanIterCreatorR._scanInitParamR->sortDesc.orders = {false};
        scanIterCreatorR._scanInitParamR->sortDesc.topk = 2;
        auto iter = scanIterCreatorR.createOrderedHa3ScanIterator(
            query, filterWrapper, {layerMeta}, emptyScan);
        ASSERT_FALSE(emptyScan);
        ASSERT_TRUE(iter != nullptr);
        vector<MatchDoc> matchDocs;
        bool ret = iter->batchSeek(10, matchDocs).unwrap();
        ASSERT_TRUE(ret);
        ASSERT_EQ(2, matchDocs.size());
        ASSERT_EQ(1, matchDocs[0].getDocId());
        ASSERT_EQ(2, matchDocs[1].getDocId());
        DELETE_AND_SET_NULL(iter);
        scanIterCreatorR._scanInitParamR->sortDesc = {};
        DELETE_AND_SET_NULL(iter);
    }
}

TEST_F(ScanIteratorCreatorRTest, testCreateHa3ScanIterator_withSubQuery) {
    ScanIteratorCreatorR scanIterCreatorR;
    ASSERT_NO_FATAL_FAILURE(prepareResource(scanIterCreatorR, false));
    LayerMetaPtr layerMeta = scanIterCreatorR.createLayerMeta();
    AttributeExpression *boolAttrExpr
        = scanIterCreatorR._attributeExpressionCreator->createAttributeExpression("id=1");
    ASSERT_TRUE(boolAttrExpr != nullptr);
    FilterWrapperPtr filterWrapper;
    ASSERT_TRUE(scanIterCreatorR.createFilterWrapper(boolAttrExpr, {}, NULL, filterWrapper));
    QueryPtr query(new TermQuery("aa", "sub_index_2", RequiredFields(), ""));
    bool emptyScan = false;
    navi::NaviLoggerProvider provider("TRACE1");
    auto iter = scanIterCreatorR.createHa3ScanIterator(query, filterWrapper, layerMeta, emptyScan);
    ASSERT_FALSE(iter);
    ASSERT_FALSE(emptyScan);
    auto traces = provider.getTrace("");
    CHECK_TRACE_COUNT(1, "use sub query [sub_term:aa] without unnest sub table", traces);
}

TEST_F(ScanIteratorCreatorRTest, testCreateHa3ScanIterator_withSubQuerySubdoc) {
    ScanIteratorCreatorR scanIterCreatorR;
    ASSERT_NO_FATAL_FAILURE(prepareResource(scanIterCreatorR, true));
    LayerMetaPtr layerMeta = scanIterCreatorR.createLayerMeta();
    AttributeExpression *boolAttrExpr
        = scanIterCreatorR._attributeExpressionCreator->createAttributeExpression("id=1");
    ASSERT_TRUE(boolAttrExpr != nullptr);
    FilterWrapperPtr filterWrapper;
    ASSERT_TRUE(scanIterCreatorR.createFilterWrapper(boolAttrExpr, {}, NULL, filterWrapper));
    QueryPtr query(new TermQuery("aa", "sub_index_2", RequiredFields(), ""));
    bool emptyScan = false;
    navi::NaviLoggerProvider provider("TRACE1");
    auto iter = scanIterCreatorR.createHa3ScanIterator(query, filterWrapper, layerMeta, emptyScan);
    ASSERT_TRUE(iter);
    DELETE_AND_SET_NULL(iter);
}

TEST_F(ScanIteratorCreatorRTest, testCreateScanIteratorWithCond) {
    ScanIteratorCreatorR scanIterCreatorR;
    ASSERT_NO_FATAL_FAILURE(prepareResource(scanIterCreatorR, true));
    bool emptyScan = false;
    {
        scanIterCreatorR._scanInitParamR->calcInitParamR->conditionJson
            = R"json({"op":"LIKE", "params":["$name", "aa"]})json";
        scanIterCreatorR._scanInitParamR->indexInfos = {{"name", IndexInfo("name", "TEXT")}};
        navi::ResourceInitContext ctx;
        ASSERT_EQ(navi::EC_NONE, scanIterCreatorR.init(ctx));
        auto iter = scanIterCreatorR.createScanIterator(emptyScan);
        ASSERT_FALSE(emptyScan);
        ASSERT_TRUE(iter != nullptr);
        vector<MatchDoc> matchDocs;
        bool ret = iter->batchSeek(10, matchDocs).unwrap();
        ASSERT_TRUE(ret);
        ASSERT_EQ(1, matchDocs.size());
    }
    { // condition failed
        scanIterCreatorR._scanInitParamR->calcInitParamR->conditionJson
            = R"json({"op":"LIKE", "xxx":["$name", "aa"]})json";
        scanIterCreatorR._scanInitParamR->indexInfos = {{"name", IndexInfo("name", "TEXT")}};
        navi::ResourceInitContext ctx;
        ASSERT_EQ(navi::EC_ABORT, scanIterCreatorR.init(ctx));
    }
    { // query executor is empty
        scanIterCreatorR._scanInitParamR->calcInitParamR->conditionJson
            = R"json({"op":"LIKE", "params":["$name", "not_exist"]})json";
        scanIterCreatorR._scanInitParamR->indexInfos = {{"name", IndexInfo("name", "TEXT")}};
        navi::ResourceInitContext ctx;
        ASSERT_EQ(navi::EC_NONE, scanIterCreatorR.init(ctx));
        emptyScan = false;
        auto iter = scanIterCreatorR.createScanIterator(emptyScan);
        ASSERT_TRUE(iter == nullptr);
        ASSERT_TRUE(emptyScan);
    }
}

TEST_F(ScanIteratorCreatorRTest, testGenCreateScanIteratorInfo1) {
    ScanIteratorCreatorR scanIterCreatorR;
    ASSERT_NO_FATAL_FAILURE(prepareResource(scanIterCreatorR, true));
    scanIterCreatorR._scanInitParamR->calcInitParamR->conditionJson
        = R"json({"op":"=", "params":["$name", "aa"]})json";
    scanIterCreatorR._scanInitParamR->indexInfos = {{"name", IndexInfo("name", "TEXT")}};
    navi::ResourceInitContext ctx;
    ASSERT_EQ(navi::EC_NONE, scanIterCreatorR.init(ctx));
    auto &info = scanIterCreatorR._createScanIteratorInfo;
    ASSERT_TRUE(info.query != nullptr);
    ASSERT_TRUE(info.filterWrapper == nullptr);
    ASSERT_TRUE(info.layerMeta != nullptr);
}

TEST_F(ScanIteratorCreatorRTest, testGenCreateScanIteratorInfo2) {
    // layer meta is null
    ScanIteratorCreatorR scanIterCreatorR;
    ASSERT_NO_FATAL_FAILURE(prepareResource(scanIterCreatorR, true));
    scanIterCreatorR._scanInitParamR->parallelIndex = 2;
    scanIterCreatorR._scanInitParamR->parallelNum = 2;
    scanIterCreatorR._scanInitParamR->calcInitParamR->conditionJson
        = R"json({"op":"=", "params":["$name", "aa"]})json";
    scanIterCreatorR._scanInitParamR->indexInfos = {{"name", IndexInfo("name", "TEXT")}};
    navi::ResourceInitContext ctx;
    ASSERT_EQ(navi::EC_ABORT, scanIterCreatorR.init(ctx));
}

TEST_F(ScanIteratorCreatorRTest, testGenCreateScanIteratorInfo3) {
    // condition failed
    ScanIteratorCreatorR scanIterCreatorR;
    ASSERT_NO_FATAL_FAILURE(prepareResource(scanIterCreatorR, true));
    scanIterCreatorR._scanInitParamR->calcInitParamR->conditionJson
        = R"json({"op":"LIKE", "xxx":["$name", "aa"]})json";
    scanIterCreatorR._scanInitParamR->indexInfos = {{"name", IndexInfo("name", "TEXT")}};
    navi::ResourceInitContext ctx;
    ASSERT_EQ(navi::EC_ABORT, scanIterCreatorR.init(ctx));
}

TEST_F(ScanIteratorCreatorRTest, testGenCreateScanIteratorInfo4) {
    // index failed
    ScanIteratorCreatorR scanIterCreatorR;
    ASSERT_NO_FATAL_FAILURE(prepareResource(scanIterCreatorR, true));
    scanIterCreatorR._scanInitParamR->calcInitParamR->conditionJson
        = R"json({"op":"LIKE", "params":["$name", "aa"]})json";
    scanIterCreatorR._scanInitParamR->indexInfos = {{"name1", IndexInfo("name", "TEXT")}};
    navi::ResourceInitContext ctx;
    ASSERT_EQ(navi::EC_ABORT, scanIterCreatorR.init(ctx));
}

TEST_F(ScanIteratorCreatorRTest, testGenCreateScanIteratorInfo5) {
    // query term not exist
    ScanIteratorCreatorR scanIterCreatorR;
    ASSERT_NO_FATAL_FAILURE(prepareResource(scanIterCreatorR, true));
    scanIterCreatorR._scanInitParamR->calcInitParamR->conditionJson
        = R"json({"op":"LIKE", "params":["$name", "not_exist"]})json";
    scanIterCreatorR._scanInitParamR->indexInfos = {{"name", IndexInfo("name", "TEXT")}};
    navi::ResourceInitContext ctx;
    ASSERT_EQ(navi::EC_NONE, scanIterCreatorR.init(ctx));
}

TEST_F(ScanIteratorCreatorRTest, testGenCreateScanIteratorInfo6) {
    // filter failed
    ScanIteratorCreatorR scanIterCreatorR;
    ASSERT_NO_FATAL_FAILURE(prepareResource(scanIterCreatorR, true));
    scanIterCreatorR._scanInitParamR->calcInitParamR->conditionJson
        = R"json({"op":"=", "params":["namea", "not_exist"]})json";
    navi::ResourceInitContext ctx;
    ASSERT_EQ(navi::EC_NONE, scanIterCreatorR.init(ctx));
}

TEST_F(ScanIteratorCreatorRTest, testCreateQueryExecutor) {
    ScanIteratorCreatorR scanIterCreatorR;
    ASSERT_NO_FATAL_FAILURE(prepareResource(scanIterCreatorR, true));
    auto layerMeta = scanIterCreatorR.createLayerMeta();
    { // query is empty
        bool emptyScan = false;
        QueryExecutor *executor = scanIterCreatorR.createQueryExecutor(
            QueryPtr(), _tableName, layerMeta.get(), emptyScan);
        QueryExecutorPtr executorPtr(executor, [](QueryExecutor *p) { POOL_DELETE_CLASS(p); });
        ASSERT_TRUE(executorPtr);
        ASSERT_EQ("RangeQueryExecutor", executorPtr->getName());
        ASSERT_FALSE(emptyScan);
    }
    { // term not exist
        bool emptyScan = false;
        Term term;
        term.setIndexName("not_exist");
        term.setWord("not_exist");
        QueryPtr query(new TermQuery(term, ""));
        QueryExecutor *executor
            = scanIterCreatorR.createQueryExecutor(query, _tableName, layerMeta.get(), emptyScan);
        ASSERT_TRUE(executor == nullptr);
        ASSERT_TRUE(emptyScan);
    }
    {
        bool emptyScan = false;
        Term term;
        term.setIndexName("name");
        term.setWord("aa");
        QueryPtr query(new TermQuery(term, ""));
        QueryExecutor *executor
            = scanIterCreatorR.createQueryExecutor(query, _tableName, layerMeta.get(), emptyScan);
        QueryExecutorPtr executorPtr(executor, [](QueryExecutor *p) { POOL_DELETE_CLASS(p); });
        ASSERT_TRUE(executorPtr);
        ASSERT_EQ("BufferedTermQueryExecutor", executorPtr->getName());
        ASSERT_FALSE(emptyScan);
    }
}

TEST_F(ScanIteratorCreatorRTest, testCreateFilterWrapper) {
    ScanIteratorCreatorR scanIterCreatorR;
    ASSERT_NO_FATAL_FAILURE(prepareResource(scanIterCreatorR, true));
    { // attribute expression is null
        FilterWrapperPtr filterWrapper;
        ASSERT_TRUE(scanIterCreatorR.createFilterWrapper(nullptr, {}, nullptr, filterWrapper));
    }
    { // attribute expression not bool
        FilterWrapperPtr filterWrapper;
        AttributeExpression *intAttrExpr
            = scanIterCreatorR._attributeExpressionCreator->createAtomicExpr("id");
        ASSERT_TRUE(intAttrExpr != nullptr);
        ASSERT_FALSE(scanIterCreatorR.createFilterWrapper(intAttrExpr, {}, nullptr, filterWrapper));
    }
    {
        FilterWrapperPtr filterWrapper;
        AttributeExpression *boolAttrExpr
            = scanIterCreatorR._attributeExpressionCreator->createAttributeExpression("id=1");
        ASSERT_TRUE(boolAttrExpr != nullptr);
        ASSERT_TRUE(scanIterCreatorR.createFilterWrapper(boolAttrExpr, {}, nullptr, filterWrapper));
        ASSERT_TRUE(filterWrapper != nullptr);
        ASSERT_TRUE(filterWrapper->getFilter() != nullptr);
    }
}

TEST_F(ScanIteratorCreatorRTest, testCreateLayerMeta) {
    ScanIteratorCreatorR scanIterCreatorR;
    ASSERT_NO_FATAL_FAILURE(prepareResource(scanIterCreatorR, true));
    auto layerMeta = scanIterCreatorR.createLayerMeta();
    ASSERT_EQ(1, layerMeta->size());
    ASSERT_EQ(0, (*layerMeta)[0].begin);
    ASSERT_EQ(3, (*layerMeta)[0].end);
    ASSERT_EQ(0, (*layerMeta)[0].quota);
    ASSERT_EQ(0, (*layerMeta)[0].nextBegin);
    ASSERT_EQ(4294967295, layerMeta->quota);
    ASSERT_EQ(std::numeric_limits<uint32_t>::max(), layerMeta->maxQuota);
    ASSERT_EQ(QM_PER_DOC, layerMeta->quotaMode);
    ASSERT_TRUE(layerMeta->needAggregate);
}

void ScanIteratorCreatorRTest::innerTestProportionalLayerQuota(const string &layerMetaStr,
                                                               uint32_t quota,
                                                               const string &expectedQuotaStr) {
    LayerMeta layerMeta = createLayerMeta(&_pool, layerMetaStr);
    layerMeta.quota = quota;
    ScanIteratorCreatorR::proportionalLayerQuota(layerMeta);
    vector<uint32_t> expectedQuotas;
    autil::StringUtil::fromString<uint32_t>(expectedQuotaStr, expectedQuotas, ",");
    ASSERT_EQ(expectedQuotas.size(), layerMeta.size());
    for (size_t i = 0; i < expectedQuotas.size(); i++) {
        ASSERT_EQ(expectedQuotas[i], layerMeta[i].quota);
    }
    ASSERT_EQ(0u, layerMeta.quota);
}

LayerMeta ScanIteratorCreatorRTest::createLayerMeta(autil::mem_pool::Pool *pool,
                                                    const string &layerMetaStr) {
    LayerMeta meta(pool);
    for (const auto &rangeStr : autil::StringUtil::split(layerMetaStr, ";")) {
        const auto &rangeVec = autil::StringUtil::split(rangeStr, ",");
        EXPECT_EQ(3, rangeVec.size());
        docid_t begin = autil::StringUtil::fromString<docid_t>(rangeVec[0]);
        docid_t end = autil::StringUtil::fromString<docid_t>(rangeVec[1]);
        uint32_t quota = numeric_limits<uint32_t>::max();
        if (rangeVec[2] != "UNLIMITED") {
            quota = autil::StringUtil::fromString<uint32_t>(rangeVec[2]);
        }
        DocIdRangeMeta range(begin, end, DocIdRangeMeta::OT_UNKNOWN, quota);
        meta.push_back(range);
    }
    return meta;
}

TEST_F(ScanIteratorCreatorRTest, testProportionalLayerQuota) {
    ASSERT_NO_FATAL_FAILURE(innerTestProportionalLayerQuota("1,4,4;5,7,3", 0, "4,3"));
    ASSERT_NO_FATAL_FAILURE(innerTestProportionalLayerQuota("1,4,4;5,7,3", 1, "1,0"));
    ASSERT_NO_FATAL_FAILURE(innerTestProportionalLayerQuota("1,4,4;5,7,3", 11, "4,3"));
    ASSERT_NO_FATAL_FAILURE(innerTestProportionalLayerQuota("1,4,4", 11, "4"));
    ASSERT_NO_FATAL_FAILURE(innerTestProportionalLayerQuota("7,10,4;12,14,3", 11, "4,3"));
    ASSERT_NO_FATAL_FAILURE(
        innerTestProportionalLayerQuota("1,4,4;5,7,3;7,10,4;12,14,3", 11, "4,2,3,2"));
    ASSERT_NO_FATAL_FAILURE(
        innerTestProportionalLayerQuota("0,13006392,0;13006393,13111882,0", 83334, "82664,670"));
}

TEST_F(ScanIteratorCreatorRTest, testSplitLayerMeta) {
    {
        // 0 range -> 1 spit
        LayerMetaPtr layerMeta(new LayerMeta(&_pool));
        LayerMetaPtr newLayerMeta = ScanIteratorCreatorR::splitLayerMeta(&_pool, layerMeta, 0, 1);
        ASSERT_TRUE(newLayerMeta);
        ASSERT_EQ(0, newLayerMeta->size());
    }

    {
        // 1 range -> 1 split
        LayerMetaPtr layerMeta(new LayerMeta(&_pool));
        layerMeta->push_back(DocIdRangeMeta(10, 20));
        LayerMetaPtr splitLayerMeta0
            = ScanIteratorCreatorR::splitLayerMeta(&_pool, layerMeta, 0, 1);
        ASSERT_TRUE(splitLayerMeta0);
        ASSERT_EQ(1, splitLayerMeta0->size());
        const auto &rangeMeta0 = (*splitLayerMeta0)[0];
        ASSERT_EQ(10, rangeMeta0.begin);
        ASSERT_EQ(20, rangeMeta0.end);
        ASSERT_EQ(11, rangeMeta0.quota);
    }

    {
        // 1 range -> 2 split
        LayerMetaPtr layerMeta(new LayerMeta(&_pool));
        layerMeta->push_back(DocIdRangeMeta(10, 20));
        LayerMetaPtr splitLayerMeta0
            = ScanIteratorCreatorR::splitLayerMeta(&_pool, layerMeta, 0, 2);
        ASSERT_TRUE(splitLayerMeta0);
        ASSERT_EQ(1, splitLayerMeta0->size());
        const auto &rangeMeta0 = (*splitLayerMeta0)[0];
        ASSERT_EQ(10, rangeMeta0.begin);
        ASSERT_EQ(14, rangeMeta0.end);
        ASSERT_EQ(5, rangeMeta0.quota);

        LayerMetaPtr splitLayerMeta1
            = ScanIteratorCreatorR::splitLayerMeta(&_pool, layerMeta, 1, 2);
        ASSERT_TRUE(splitLayerMeta1);
        ASSERT_EQ(1, splitLayerMeta1->size());
        const auto &rangeMeta1 = (*splitLayerMeta1)[0];
        ASSERT_EQ(15, rangeMeta1.begin);
        ASSERT_EQ(20, rangeMeta1.end);
        ASSERT_EQ(6, rangeMeta1.quota);
    }

    {
        // 1 range(1 document) -> 2 split
        LayerMetaPtr layerMeta(new LayerMeta(&_pool));
        layerMeta->push_back(DocIdRangeMeta(10, 10));
        LayerMetaPtr splitLayerMeta0
            = ScanIteratorCreatorR::splitLayerMeta(&_pool, layerMeta, 0, 2);
        ASSERT_TRUE(splitLayerMeta0);
        ASSERT_EQ(0, splitLayerMeta0->size());

        LayerMetaPtr splitLayerMeta1
            = ScanIteratorCreatorR::splitLayerMeta(&_pool, layerMeta, 1, 2);
        ASSERT_TRUE(splitLayerMeta1);
        ASSERT_EQ(1, splitLayerMeta1->size());
        const auto &rangeMeta1 = (*splitLayerMeta1)[0];
        ASSERT_EQ(10, rangeMeta1.begin);
        ASSERT_EQ(10, rangeMeta1.end);
        ASSERT_EQ(1, rangeMeta1.quota);
    }

    {
        // 2 range -> 1 split
        LayerMetaPtr layerMeta(new LayerMeta(&_pool));
        layerMeta->push_back(DocIdRangeMeta(10, 20));
        layerMeta->push_back(DocIdRangeMeta(21, 30));
        LayerMetaPtr splitLayerMeta0
            = ScanIteratorCreatorR::splitLayerMeta(&_pool, layerMeta, 0, 1);
        ASSERT_TRUE(splitLayerMeta0);
        ASSERT_EQ(2, splitLayerMeta0->size());
        const auto &rangeMeta0 = (*splitLayerMeta0)[0];
        ASSERT_EQ(10, rangeMeta0.begin);
        ASSERT_EQ(20, rangeMeta0.end);
        ASSERT_EQ(11, rangeMeta0.quota);
        const auto &rangeMeta1 = (*splitLayerMeta0)[1];
        ASSERT_EQ(21, rangeMeta1.begin);
        ASSERT_EQ(30, rangeMeta1.end);
        ASSERT_EQ(10, rangeMeta1.quota);
    }

    {
        // 2 range -> 3 split
        LayerMetaPtr layerMeta(new LayerMeta(&_pool));
        layerMeta->push_back(DocIdRangeMeta(10, 20));
        layerMeta->push_back(DocIdRangeMeta(30, 40));

        LayerMetaPtr splitLayerMeta0
            = ScanIteratorCreatorR::splitLayerMeta(&_pool, layerMeta, 0, 3);
        ASSERT_TRUE(splitLayerMeta0);
        ASSERT_EQ(1, splitLayerMeta0->size());
        const auto &rangeMeta00 = (*splitLayerMeta0)[0];
        ASSERT_EQ(10, rangeMeta00.begin);
        ASSERT_EQ(16, rangeMeta00.end);
        ASSERT_EQ(7, rangeMeta00.quota);

        LayerMetaPtr splitLayerMeta1
            = ScanIteratorCreatorR::splitLayerMeta(&_pool, layerMeta, 1, 3);
        ASSERT_TRUE(splitLayerMeta1);
        ASSERT_EQ(2, splitLayerMeta1->size());
        const auto &rangeMeta10 = (*splitLayerMeta1)[0];
        ASSERT_EQ(17, rangeMeta10.begin);
        ASSERT_EQ(20, rangeMeta10.end);
        ASSERT_EQ(4, rangeMeta10.quota);
        const auto &rangeMeta11 = (*splitLayerMeta1)[1];
        ASSERT_EQ(30, rangeMeta11.begin);
        ASSERT_EQ(32, rangeMeta11.end);
        ASSERT_EQ(3, rangeMeta11.quota);

        LayerMetaPtr splitLayerMeta2
            = ScanIteratorCreatorR::splitLayerMeta(&_pool, layerMeta, 2, 3);
        ASSERT_TRUE(splitLayerMeta2);
        ASSERT_EQ(1, splitLayerMeta2->size());
        const auto &rangeMeta20 = (*splitLayerMeta2)[0];
        ASSERT_EQ(33, rangeMeta20.begin);
        ASSERT_EQ(40, rangeMeta20.end);
        ASSERT_EQ(8, rangeMeta20.quota);
    }

    {
        // 3 range -> 2 plit
        LayerMetaPtr layerMeta(new LayerMeta(&_pool));
        layerMeta->push_back(DocIdRangeMeta(10, 11));
        layerMeta->push_back(DocIdRangeMeta(13, 14));
        layerMeta->push_back(DocIdRangeMeta(16, 17));
        LayerMetaPtr splitLayerMeta0
            = ScanIteratorCreatorR::splitLayerMeta(&_pool, layerMeta, 0, 2);
        ASSERT_TRUE(splitLayerMeta0);
        ASSERT_EQ(2, splitLayerMeta0->size());
        const auto &rangeMeta00 = (*splitLayerMeta0)[0];
        ASSERT_EQ(10, rangeMeta00.begin);
        ASSERT_EQ(10, rangeMeta00.nextBegin);
        ASSERT_EQ(11, rangeMeta00.end);
        ASSERT_EQ(2, rangeMeta00.quota);
        const auto &rangeMeta01 = (*splitLayerMeta0)[1];
        ASSERT_EQ(13, rangeMeta01.begin);
        ASSERT_EQ(13, rangeMeta01.nextBegin);
        ASSERT_EQ(13, rangeMeta01.end);
        ASSERT_EQ(1, rangeMeta01.quota);

        LayerMetaPtr splitLayerMeta1
            = ScanIteratorCreatorR::splitLayerMeta(&_pool, layerMeta, 1, 2);
        ASSERT_TRUE(splitLayerMeta1);
        ASSERT_EQ(2, splitLayerMeta1->size());
        const auto &rangeMeta10 = (*splitLayerMeta1)[0];
        ASSERT_EQ(14, rangeMeta10.begin);
        ASSERT_EQ(14, rangeMeta10.end);
        ASSERT_EQ(1, rangeMeta10.quota);
        const auto &rangeMeta11 = (*splitLayerMeta1)[1];
        ASSERT_EQ(16, rangeMeta11.begin);
        ASSERT_EQ(17, rangeMeta11.end);
        ASSERT_EQ(2, rangeMeta11.quota);
    }
}

TEST_F(ScanIteratorCreatorRTest, testParseTruncateDesc_InvalidOption) {
    navi::NaviLoggerProvider provider("WARN");
    string desc("a|b");
    vector<string> names;
    vector<SelectAuxChainType> types;
    ASSERT_FALSE(ScanIteratorCreatorR::parseTruncateDesc(desc, names, types));
    auto traces = provider.getTrace("");
    CHECK_TRACE_COUNT(1, "Invalid truncate query option[a], skip optimize", traces);
}

TEST_F(ScanIteratorCreatorRTest, testParseTruncateDesc_InvalidType) {
    navi::NaviLoggerProvider provider("WARN");
    string desc("select#not_exist");
    vector<string> names;
    vector<SelectAuxChainType> types;
    ASSERT_FALSE(ScanIteratorCreatorR::parseTruncateDesc(desc, names, types));
    auto traces = provider.getTrace("");
    CHECK_TRACE_COUNT(1, "Invalid truncate query select type [not_exist], skip optimize", traces);
}

TEST_F(ScanIteratorCreatorRTest, testParseTruncateDesc_NoTruncateName) {
    navi::NaviLoggerProvider provider("WARN");
    string desc("select#all");
    vector<string> names;
    vector<SelectAuxChainType> types;
    ASSERT_FALSE(ScanIteratorCreatorR::parseTruncateDesc(desc, names, types));
    auto traces = provider.getTrace("");
    CHECK_TRACE_COUNT(1, "Invalid truncate query, truncate name not found", traces);
}

TEST_F(ScanIteratorCreatorRTest, testParseTruncateDesc_bigger) {
    navi::NaviLoggerProvider provider("WARN");
    string desc("select#bigger|aux_name#uvsum");
    vector<string> names;
    vector<SelectAuxChainType> types;
    ASSERT_TRUE(ScanIteratorCreatorR::parseTruncateDesc(desc, names, types));
    ASSERT_EQ(1, names.size());
    ASSERT_EQ("uvsum", names[0]);
    ASSERT_EQ(1, types.size());
    ASSERT_EQ(SelectAuxChainType::SAC_DF_BIGGER, types[0]);
}

TEST_F(ScanIteratorCreatorRTest, testParseTruncateDesc_smaller) {
    navi::NaviLoggerProvider provider("WARN");
    string desc("select#smaller|aux_name#uvsum");
    vector<string> names;
    vector<SelectAuxChainType> types;
    ASSERT_TRUE(ScanIteratorCreatorR::parseTruncateDesc(desc, names, types));
    ASSERT_EQ(1, names.size());
    ASSERT_EQ("uvsum", names[0]);
    ASSERT_EQ(1, types.size());
    ASSERT_EQ(SelectAuxChainType::SAC_DF_SMALLER, types[0]);
}

TEST_F(ScanIteratorCreatorRTest, testParseTruncateDesc_all) {
    navi::NaviLoggerProvider provider("WARN");
    string desc("aux_name#uvsum|select#all");
    vector<string> names;
    vector<SelectAuxChainType> types;
    ASSERT_TRUE(ScanIteratorCreatorR::parseTruncateDesc(desc, names, types));
    ASSERT_EQ(1, names.size());
    ASSERT_EQ("uvsum", names[0]);
    ASSERT_EQ(1, types.size());
    ASSERT_EQ(SelectAuxChainType::SAC_ALL, types[0]);
}

TEST_F(ScanIteratorCreatorRTest, testParseTruncateDesc_NoType) {
    navi::NaviLoggerProvider provider("WARN");
    string desc("aux_name#uvsum");
    vector<string> names;
    vector<SelectAuxChainType> types;
    ASSERT_TRUE(ScanIteratorCreatorR::parseTruncateDesc(desc, names, types));
    ASSERT_EQ(1, names.size());
    ASSERT_EQ("uvsum", names[0]);
}

TEST_F(ScanIteratorCreatorRTest, testTruncateQuery) {
    ScanIteratorCreatorR scanIterCreatorR;
    ASSERT_NO_FATAL_FAILURE(prepareResource(scanIterCreatorR, true));
    QueryPtr query(new AndQuery("and"));
    TermQueryPtr q1(new TermQuery("a", "index2", {}, "a"));
    TermQueryPtr q2(new TermQuery("b", "index2", {}, "b"));
    query->addQuery(q1);
    query->addQuery(q2);
    std::string truncateDesc = "aux_name#bitmap|select#all;aux_name#xxx";
    scanIterCreatorR.truncateQuery(query, truncateDesc);
    ASSERT_EQ("xxx", q1->getTerm().getTruncateName());
    ASSERT_EQ("bitmap", q2->getTerm().getTruncateName());
}

TEST_F(ScanIteratorCreatorRTest, testSubMatchQuery) {
    QueryPtr query(new AndQuery("and"));
    TermQueryPtr q1(new TermQuery("a", "index2", {}, ""));
    TermQueryPtr q2(new TermQuery("b", "index2", {}, ""));
    query->addQuery(q1);
    query->addQuery(q2);
    ASSERT_EQ(MDL_SUB_QUERY, query->getMatchDataLevel());
    ASSERT_EQ(MDL_TERM, q1->getMatchDataLevel());
    ASSERT_EQ(MDL_TERM, q2->getMatchDataLevel());
    ScanIteratorCreatorR::subMatchQuery(query, nullptr, "");
    ASSERT_EQ(MDL_SUB_QUERY, query->getMatchDataLevel());
    ASSERT_EQ(MDL_NONE, q1->getMatchDataLevel());
    ASSERT_EQ(MDL_NONE, q2->getMatchDataLevel());
}

TEST_F(ScanIteratorCreatorRTest, testSubMatchQuery_skip) {
    QueryPtr query(new AndQuery(""));
    TermQueryPtr q1(new TermQuery("a", "index2", {}, ""));
    TermQueryPtr q2(new TermQuery("b", "index2", {}, ""));
    query->addQuery(q1);
    query->addQuery(q2);
    ASSERT_EQ(MDL_NONE, query->getMatchDataLevel());
    ASSERT_EQ(MDL_TERM, q1->getMatchDataLevel());
    ASSERT_EQ(MDL_TERM, q2->getMatchDataLevel());
    ScanIteratorCreatorR::subMatchQuery(query, nullptr, "");
    ASSERT_EQ(MDL_NONE, query->getMatchDataLevel());
    ASSERT_EQ(MDL_TERM, q1->getMatchDataLevel());
    ASSERT_EQ(MDL_TERM, q2->getMatchDataLevel());
}

TEST_F(ScanIteratorCreatorRTest, testSubMatchQuery_full_term) {
    auto schema = prepareSchema();
    ASSERT_TRUE(schema);
    auto tabletSchema = std::make_shared<indexlib::config::LegacySchemaAdapter>(schema);

    {
        QueryPtr query(new AndQuery("q1"));
        TermQueryPtr q1(new TermQuery("a", "phrase_bts", {}, ""));
        TermQueryPtr q2(new TermQuery("b", "phrase_bts_pay", {}, ""));
        query->addQuery(q1);
        query->addQuery(q2);
        QueryPtr query2(new OrQuery(""));
        TermQueryPtr q3(new TermQuery("c", "phrase_bts", {}, "abc"));
        query2->addQuery(query);
        query2->addQuery(q3);

        ASSERT_EQ(MDL_SUB_QUERY, query->getMatchDataLevel());
        ASSERT_EQ(MDL_NONE, query2->getMatchDataLevel());
        ASSERT_EQ(MDL_TERM, q1->getMatchDataLevel());
        ASSERT_EQ(MDL_TERM, q2->getMatchDataLevel());
        ASSERT_EQ(MDL_TERM, q3->getMatchDataLevel());

        ScanIteratorCreatorR::subMatchQuery(query2, tabletSchema, "full_term");
        ASSERT_EQ(MDL_SUB_QUERY, query->getMatchDataLevel());
        ASSERT_EQ(MDL_NONE, query2->getMatchDataLevel());
        ASSERT_EQ(MDL_NONE, q1->getMatchDataLevel());
        ASSERT_EQ(MDL_TERM, q2->getMatchDataLevel());
        ASSERT_EQ(MDL_TERM, q3->getMatchDataLevel());
    }
    {
        QueryPtr query(new AndQuery("q1"));
        TermQueryPtr q1(new TermQuery("a", "phrase_bts", {}, ""));
        TermQueryPtr q2(new TermQuery("b", "phrase_bts_pay", {}, ""));
        query->addQuery(q1);
        query->addQuery(q2);
        QueryPtr query2(new OrQuery(""));
        TermQueryPtr q3(new TermQuery("c", "phrase_bts", {}, "abc"));
        query2->addQuery(query);
        query2->addQuery(q3);

        ASSERT_EQ(MDL_SUB_QUERY, query->getMatchDataLevel());
        ASSERT_EQ(MDL_NONE, query2->getMatchDataLevel());
        ASSERT_EQ(MDL_TERM, q1->getMatchDataLevel());
        ASSERT_EQ(MDL_TERM, q2->getMatchDataLevel());
        ASSERT_EQ(MDL_TERM, q3->getMatchDataLevel());

        ScanIteratorCreatorR::subMatchQuery(query2, tabletSchema, "sub");
        ASSERT_EQ(MDL_SUB_QUERY, query->getMatchDataLevel());
        ASSERT_EQ(MDL_NONE, query2->getMatchDataLevel());
        ASSERT_EQ(MDL_NONE, q1->getMatchDataLevel());
        ASSERT_EQ(MDL_NONE, q2->getMatchDataLevel());
        ASSERT_EQ(MDL_TERM, q3->getMatchDataLevel());
    }
}

} // namespace sql
