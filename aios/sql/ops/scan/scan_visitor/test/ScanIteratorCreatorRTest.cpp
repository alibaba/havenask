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

TEST_F(ScanIteratorCreatorRTest, testSplitLayerMetaByStep) {
    uint32_t parallelBlockCount = 10;
    {
        // 0 range -> 1 spit
        LayerMetaPtr layerMeta(new LayerMeta(&_pool));
        LayerMetaPtr newLayerMeta = ScanIteratorCreatorR::splitLayerMetaByStep(
            &_pool, layerMeta, 0, 1, parallelBlockCount);
        ASSERT_TRUE(newLayerMeta);
        ASSERT_EQ(0, newLayerMeta->size());
    }
    {
        // 1 range -> 1 split
        LayerMetaPtr layerMeta(new LayerMeta(&_pool));
        layerMeta->push_back(DocIdRangeMeta(0, 1000000));
        LayerMetaPtr splitLayerMeta0 = ScanIteratorCreatorR::splitLayerMetaByStep(
            &_pool, layerMeta, 0, 1, parallelBlockCount);
        ASSERT_TRUE(splitLayerMeta0);
        EXPECT_EQ("(quota: 0 maxQuota: 4294967295 quotaMode: 0 needAggregate: 1 quotaType: 0) "
                  "begin: 0 end: 1000000 nextBegin: 0 quota: 0;",
                  splitLayerMeta0->toString());
    }
    {
        // 1 range -> 2 split
        LayerMetaPtr layerMeta(new LayerMeta(&_pool));
        layerMeta->push_back(DocIdRangeMeta(0, 1000000));
        LayerMetaPtr splitLayerMeta0 = ScanIteratorCreatorR::splitLayerMetaByStep(
            &_pool, layerMeta, 0, 2, parallelBlockCount);
        ASSERT_TRUE(splitLayerMeta0);
        EXPECT_EQ(
            "(quota: 0 maxQuota: 4294967295 quotaMode: 0 needAggregate: 1 quotaType: 0) begin: 0 "
            "end: 50000 nextBegin: 0 quota: 50001;begin: 100002 end: 150002 nextBegin: 100002 "
            "quota: 50001;begin: 200004 end: 250004 nextBegin: 200004 quota: 50001;begin: 300006 "
            "end: 350006 nextBegin: 300006 quota: 50001;begin: 400008 end: 450008 nextBegin: "
            "400008 quota: 50001;begin: 500010 end: 550010 nextBegin: 500010 quota: 50001;begin: "
            "600012 end: 650012 nextBegin: 600012 quota: 50001;begin: 700014 end: 750014 "
            "nextBegin: 700014 quota: 50001;begin: 800016 end: 850016 nextBegin: 800016 quota: "
            "50001;begin: 900018 end: 950018 nextBegin: 900018 quota: 50001;",
            splitLayerMeta0->toString());
        LayerMetaPtr splitLayerMeta1 = ScanIteratorCreatorR::splitLayerMetaByStep(
            &_pool, layerMeta, 1, 2, parallelBlockCount);
        ASSERT_TRUE(splitLayerMeta1);
        EXPECT_EQ(
            "(quota: 0 maxQuota: 4294967295 quotaMode: 0 needAggregate: 1 quotaType: 0) begin: "
            "50001 end: 100001 nextBegin: 50001 quota: 50001;begin: 150003 end: 200003 nextBegin: "
            "150003 quota: 50001;begin: 250005 end: 300005 nextBegin: 250005 quota: 50001;begin: "
            "350007 end: 400007 nextBegin: 350007 quota: 50001;begin: 450009 end: 500009 "
            "nextBegin: 450009 quota: 50001;begin: 550011 end: 600011 nextBegin: 550011 quota: "
            "50001;begin: 650013 end: 700013 nextBegin: 650013 quota: 50001;begin: 750015 end: "
            "800015 nextBegin: 750015 quota: 50001;begin: 850017 end: 900017 nextBegin: 850017 "
            "quota: 50001;begin: 950019 end: 1000000 nextBegin: 950019 quota: 49982;",
            splitLayerMeta1->toString());
    }
    {
        // 1 range(1 document) -> 2 split
        LayerMetaPtr layerMeta(new LayerMeta(&_pool));
        layerMeta->push_back(DocIdRangeMeta(1000000, 1000000));
        LayerMetaPtr splitLayerMeta0 = ScanIteratorCreatorR::splitLayerMetaByStep(
            &_pool, layerMeta, 0, 2, parallelBlockCount);
        ASSERT_TRUE(splitLayerMeta0);
        EXPECT_EQ("(quota: 0 maxQuota: 4294967295 quotaMode: 0 needAggregate: 1 quotaType: 0) "
                  "begin: 1000000 end: 1000000 nextBegin: 1000000 quota: 1;",
                  splitLayerMeta0->toString());
        LayerMetaPtr splitLayerMeta1 = ScanIteratorCreatorR::splitLayerMetaByStep(
            &_pool, layerMeta, 1, 2, parallelBlockCount);
        ASSERT_TRUE(splitLayerMeta1);
        EXPECT_EQ("(quota: 0 maxQuota: 4294967295 quotaMode: 0 needAggregate: 1 quotaType: 0) ",
                  splitLayerMeta1->toString());
    }
    {
        // 2 range -> 1 split
        LayerMetaPtr layerMeta(new LayerMeta(&_pool));
        layerMeta->push_back(DocIdRangeMeta(0, 500000));
        layerMeta->push_back(DocIdRangeMeta(500001, 1000000));
        LayerMetaPtr splitLayerMeta0 = ScanIteratorCreatorR::splitLayerMetaByStep(
            &_pool, layerMeta, 0, 1, parallelBlockCount);
        ASSERT_TRUE(splitLayerMeta0);
        EXPECT_EQ("(quota: 0 maxQuota: 4294967295 quotaMode: 0 needAggregate: 1 quotaType: 0) "
                  "begin: 0 end: 500000 nextBegin: 0 quota: 0;begin: 500001 end: 1000000 "
                  "nextBegin: 500001 quota: 0;",
                  splitLayerMeta0->toString());
    }
    {
        // 2 range -> 3 split
        LayerMetaPtr layerMeta(new LayerMeta(&_pool));
        layerMeta->push_back(DocIdRangeMeta(0, 498729));
        layerMeta->push_back(DocIdRangeMeta(679320, 1000000));
        LayerMetaPtr splitLayerMeta0 = ScanIteratorCreatorR::splitLayerMetaByStep(
            &_pool, layerMeta, 0, 3, parallelBlockCount);
        ASSERT_TRUE(splitLayerMeta0);
        EXPECT_EQ(
            "(quota: 0 maxQuota: 4294967295 quotaMode: 0 needAggregate: 1 quotaType: 0) begin: 0 "
            "end: 27313 nextBegin: 0 quota: 27314;begin: 81942 end: 109255 nextBegin: 81942 quota: "
            "27314;begin: 163884 end: 191197 nextBegin: 163884 quota: 27314;begin: 245826 end: "
            "273139 nextBegin: 245826 quota: 27314;begin: 327768 end: 355081 nextBegin: 327768 "
            "quota: 27314;begin: 409710 end: 437023 nextBegin: 409710 quota: 27314;begin: 491652 "
            "end: 498729 nextBegin: 491652 quota: 7078;begin: 679320 end: 699555 nextBegin: 679320 "
            "quota: 20236;begin: 754184 end: 781497 nextBegin: 754184 quota: 27314;begin: 836126 "
            "end: 863439 nextBegin: 836126 quota: 27314;begin: 918068 end: 945381 nextBegin: "
            "918068 quota: 27314;",
            splitLayerMeta0->toString());
        LayerMetaPtr splitLayerMeta1 = ScanIteratorCreatorR::splitLayerMetaByStep(
            &_pool, layerMeta, 1, 3, parallelBlockCount);
        EXPECT_EQ(
            "(quota: 0 maxQuota: 4294967295 quotaMode: 0 needAggregate: 1 quotaType: 0) begin: "
            "27314 end: 54627 nextBegin: 27314 quota: 27314;begin: 109256 end: 136569 nextBegin: "
            "109256 quota: 27314;begin: 191198 end: 218511 nextBegin: 191198 quota: 27314;begin: "
            "273140 end: 300453 nextBegin: 273140 quota: 27314;begin: 355082 end: 382395 "
            "nextBegin: 355082 quota: 27314;begin: 437024 end: 464337 nextBegin: 437024 quota: "
            "27314;begin: 699556 end: 726869 nextBegin: 699556 quota: 27314;begin: 781498 end: "
            "808811 nextBegin: 781498 quota: 27314;begin: 863440 end: 890753 nextBegin: 863440 "
            "quota: 27314;begin: 945382 end: 972695 nextBegin: 945382 quota: 27314;",
            splitLayerMeta1->toString());
        LayerMetaPtr splitLayerMeta2 = ScanIteratorCreatorR::splitLayerMetaByStep(
            &_pool, layerMeta, 2, 3, parallelBlockCount);
        ASSERT_TRUE(splitLayerMeta2);
        EXPECT_EQ(
            "(quota: 0 maxQuota: 4294967295 quotaMode: 0 needAggregate: 1 quotaType: 0) begin: "
            "54628 end: 81941 nextBegin: 54628 quota: 27314;begin: 136570 end: 163883 nextBegin: "
            "136570 quota: 27314;begin: 218512 end: 245825 nextBegin: 218512 quota: 27314;begin: "
            "300454 end: 327767 nextBegin: 300454 quota: 27314;begin: 382396 end: 409709 "
            "nextBegin: 382396 quota: 27314;begin: 464338 end: 491651 nextBegin: 464338 quota: "
            "27314;begin: 726870 end: 754183 nextBegin: 726870 quota: 27314;begin: 808812 end: "
            "836125 nextBegin: 808812 quota: 27314;begin: 890754 end: 918067 nextBegin: 890754 "
            "quota: 27314;begin: 972696 end: 1000000 nextBegin: 972696 quota: 27305;",
            splitLayerMeta2->toString());
    }
    {
        // 3 range -> 2 split
        LayerMetaPtr layerMeta(new LayerMeta(&_pool));
        layerMeta->push_back(DocIdRangeMeta(10, 180000));
        layerMeta->push_back(DocIdRangeMeta(192000, 540300));
        layerMeta->push_back(DocIdRangeMeta(563000, 1000000));
        LayerMetaPtr splitLayerMeta0 = ScanIteratorCreatorR::splitLayerMetaByStep(
            &_pool, layerMeta, 0, 2, parallelBlockCount);
        ASSERT_TRUE(splitLayerMeta0);
        EXPECT_EQ(
            "(quota: 0 maxQuota: 4294967295 quotaMode: 0 needAggregate: 1 quotaType: 0) begin: 10 "
            "end: 48274 nextBegin: 10 quota: 48265;begin: 96540 end: 144804 nextBegin: 96540 "
            "quota: 48265;begin: 205069 end: 253333 nextBegin: 205069 quota: 48265;begin: 301599 "
            "end: 349863 nextBegin: 301599 quota: 48265;begin: 398129 end: 446393 nextBegin: "
            "398129 quota: 48265;begin: 494659 end: 540300 nextBegin: 494659 quota: 45642;begin: "
            "563000 end: 565622 nextBegin: 563000 quota: 2623;begin: 613888 end: 662152 nextBegin: "
            "613888 quota: 48265;begin: 710418 end: 758682 nextBegin: 710418 quota: 48265;begin: "
            "806948 end: 855212 nextBegin: 806948 quota: 48265;begin: 903478 end: 951742 "
            "nextBegin: 903478 quota: 48265;",
            splitLayerMeta0->toString());
        LayerMetaPtr splitLayerMeta1 = ScanIteratorCreatorR::splitLayerMetaByStep(
            &_pool, layerMeta, 1, 2, parallelBlockCount);
        ASSERT_TRUE(splitLayerMeta1);
        EXPECT_EQ(
            "(quota: 0 maxQuota: 4294967295 quotaMode: 0 needAggregate: 1 quotaType: 0) begin: "
            "48275 end: 96539 nextBegin: 48275 quota: 48265;begin: 144805 end: 180000 nextBegin: "
            "144805 quota: 35196;begin: 192000 end: 205068 nextBegin: 192000 quota: 13069;begin: "
            "253334 end: 301598 nextBegin: 253334 quota: 48265;begin: 349864 end: 398128 "
            "nextBegin: 349864 quota: 48265;begin: 446394 end: 494658 nextBegin: 446394 quota: "
            "48265;begin: 565623 end: 613887 nextBegin: 565623 quota: 48265;begin: 662153 end: "
            "710417 nextBegin: 662153 quota: 48265;begin: 758683 end: 806947 nextBegin: 758683 "
            "quota: 48265;begin: 855213 end: 903477 nextBegin: 855213 quota: 48265;begin: 951743 "
            "end: 1000000 nextBegin: 951743 quota: 48258;",
            splitLayerMeta1->toString());
    }
    // // invalid parallel index
    {
        LayerMetaPtr layerMeta(new LayerMeta(&_pool));
        layerMeta->push_back(DocIdRangeMeta(1, 1000000));
        LayerMetaPtr splitLayerMeta0 = ScanIteratorCreatorR::splitLayerMetaByStep(
            &_pool, layerMeta, 3, 3, parallelBlockCount);
        EXPECT_FALSE(splitLayerMeta0);
    }
    // blockSize too small, ceil to PARALLEL_MIN_BLOCK_SIZE
    {
        LayerMetaPtr layerMeta(new LayerMeta(&_pool));
        layerMeta->push_back(DocIdRangeMeta(1, 100000));
        LayerMetaPtr splitLayerMeta0 = ScanIteratorCreatorR::splitLayerMetaByStep(
            &_pool, layerMeta, 0, 2, parallelBlockCount);
        ASSERT_TRUE(splitLayerMeta0);
        EXPECT_EQ("(quota: 0 maxQuota: 4294967295 quotaMode: 0 needAggregate: 1 quotaType: 0) "
                  "begin: 1 end: 16384 nextBegin: 1 quota: 16384;begin: 32769 end: 49152 "
                  "nextBegin: 32769 quota: 16384;begin: 65537 end: 81920 nextBegin: 65537 quota: "
                  "16384;begin: 98305 end: 100000 nextBegin: 98305 quota: 1696;",
                  splitLayerMeta0->toString());
        LayerMetaPtr splitLayerMeta1 = ScanIteratorCreatorR::splitLayerMetaByStep(
            &_pool, layerMeta, 1, 2, parallelBlockCount);
        ASSERT_TRUE(splitLayerMeta1);
        EXPECT_EQ(
            "(quota: 0 maxQuota: 4294967295 quotaMode: 0 needAggregate: 1 quotaType: 0) begin: "
            "16385 end: 32768 nextBegin: 16385 quota: 16384;begin: 49153 end: 65536 nextBegin: "
            "49153 quota: 16384;begin: 81921 end: 98304 nextBegin: 81921 quota: 16384;",
            splitLayerMeta1->toString());
    }
    // parallel block count too large, floor to PARALLEL_MAX_BLOCK_COUNT
    {
        uint32_t parallelBlockCount = PARALLEL_MAX_BLOCK_COUNT + 10;
        LayerMetaPtr layerMeta(new LayerMeta(&_pool));
        layerMeta->push_back(DocIdRangeMeta(1, 60000000));
        LayerMetaPtr splitLayerMeta0 = ScanIteratorCreatorR::splitLayerMetaByStep(
            &_pool, layerMeta, 0, 2, parallelBlockCount);
        ASSERT_TRUE(splitLayerMeta0);
        ASSERT_EQ(PARALLEL_MAX_BLOCK_COUNT, splitLayerMeta0->size());
        LayerMetaPtr splitLayerMeta1 = ScanIteratorCreatorR::splitLayerMetaByStep(
            &_pool, layerMeta, 1, 2, parallelBlockCount);
        ASSERT_TRUE(splitLayerMeta1);
        ASSERT_EQ(PARALLEL_MAX_BLOCK_COUNT, splitLayerMeta1->size());
    }

    // each meta size is small, but compress to split
    {
        uint32_t parallelBlockCount = 2;
        LayerMetaPtr layerMeta(new LayerMeta(&_pool));
        layerMeta->push_back(DocIdRangeMeta(0, 20000));
        layerMeta->push_back(DocIdRangeMeta(30000, 50000));
        layerMeta->push_back(DocIdRangeMeta(60000, 80000));
        layerMeta->push_back(DocIdRangeMeta(90000, 110000));
        layerMeta->push_back(DocIdRangeMeta(120000, 140000));
        LayerMetaPtr splitLayerMeta0 = ScanIteratorCreatorR::splitLayerMetaByStep(
            &_pool, layerMeta, 0, 2, parallelBlockCount);
        ASSERT_TRUE(splitLayerMeta0);
        EXPECT_EQ("(quota: 0 maxQuota: 4294967295 quotaMode: 0 needAggregate: 1 quotaType: 0) "
                  "begin: 0 end: 20000 nextBegin: 0 quota: 20001;begin: 30000 end: 35000 "
                  "nextBegin: 30000 quota: 5001;begin: 70002 end: 80000 nextBegin: 70002 quota: "
                  "9999;begin: 90000 end: 105002 nextBegin: 90000 quota: 15003;",
                  splitLayerMeta0->toString());
        LayerMetaPtr splitLayerMeta1 = ScanIteratorCreatorR::splitLayerMetaByStep(
            &_pool, layerMeta, 1, 2, parallelBlockCount);
        ASSERT_TRUE(splitLayerMeta1);
        EXPECT_EQ("(quota: 0 maxQuota: 4294967295 quotaMode: 0 needAggregate: 1 quotaType: 0) "
                  "begin: 35001 end: 50000 nextBegin: 35001 quota: 15000;begin: 60000 end: 70001 "
                  "nextBegin: 60000 quota: 10002;begin: 105003 end: 110000 nextBegin: 105003 "
                  "quota: 4998;begin: 120000 end: 140000 nextBegin: 120000 quota: 20001;",
                  splitLayerMeta1->toString());
    }
    // block size exceed the limit of uint32_t
    {
        uint32_t parallelBlockCount = 1;
        LayerMetaPtr layerMeta(new LayerMeta(&_pool));
        const int32_t INF = std::numeric_limits<int32_t>::max();
        layerMeta->push_back(DocIdRangeMeta(1, INF / 2));
        layerMeta->push_back(DocIdRangeMeta(INF / 2 + 1, INF));
        LayerMetaPtr splitLayerMeta0 = ScanIteratorCreatorR::splitLayerMetaByStep(
            &_pool, layerMeta, 0, 2, parallelBlockCount);
        ASSERT_TRUE(splitLayerMeta0);
        EXPECT_EQ("(quota: 0 maxQuota: 4294967295 quotaMode: 0 needAggregate: 1 quotaType: 0) "
                  "begin: 1 end: 1073741823 nextBegin: 1 quota: 1073741823;begin: 1073741824 end: "
                  "1073741824 nextBegin: 1073741824 quota: 1;",
                  splitLayerMeta0->toString());
        LayerMetaPtr splitLayerMeta1 = ScanIteratorCreatorR::splitLayerMetaByStep(
            &_pool, layerMeta, 1, 2, parallelBlockCount);
        ASSERT_TRUE(splitLayerMeta1);
        EXPECT_EQ("(quota: 0 maxQuota: 4294967295 quotaMode: 0 needAggregate: 1 quotaType: 0) "
                  "begin: 1073741825 end: 2147483647 nextBegin: 1073741825 quota: 1073741823;",
                  splitLayerMeta1->toString());
    }
}
} // namespace sql
