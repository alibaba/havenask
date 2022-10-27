#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include <ha3/sql/ops/scan/ScanIteratorCreator.h>
#include <indexlib/testlib/indexlib_partition_creator.h>
#include <suez/turing/expression/util/TableInfoConfigurator.h>
#include <indexlib/storage/file_system_wrapper.h>
#include <indexlib/index_base/schema_adapter.h>
#include <ha3/search/IndexPartitionReaderUtil.h>
#include <ha3/common/TermQuery.h>
#include <indexlib/partition/index_application.h>
#include <ha3/sql/ops/scan/Ha3ScanIterator.h>
#include <ha3/sql/ops/scan/RangeScanIterator.h>
#include <ha3/sql/ops/scan/RangeScanIteratorWithoutFilter.h>
#include <ha3/sql/ops/scan/QueryScanIterator.h>
using namespace std;
using namespace testing;
using namespace matchdoc;
using namespace suez::turing;

USE_HA3_NAMESPACE(search);
USE_HA3_NAMESPACE(common);

BEGIN_HA3_NAMESPACE(sql);

class ScanIteratorCreatorTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
    void initIndexPartition();
    search::IndexPartitionReaderWrapperPtr getIndexPartitionReaderWrapper();

private:
    string _tableName;
    string _testPath;
    IE_NAMESPACE(partition)::IndexApplication::IndexPartitionMap _indexPartitionMap;
    IE_NAMESPACE(partition)::IndexApplicationPtr _indexApp;
    autil::mem_pool::Pool _pool;
    suez::turing::TableInfoPtr _tableInfo;
    matchdoc::MatchDocAllocatorPtr _matchDocAllocator;
    suez::turing::AttributeExpressionCreatorPtr _attributeExpressionCreator;
    IE_NAMESPACE(partition)::PartitionReaderSnapshotPtr _indexAppSnapshot;
    };

void ScanIteratorCreatorTest::setUp() {
    _testPath = GET_TEMPLATE_DATA_PATH() + "sql_op_index_path/";
    _tableName = "invertedTable";
    initIndexPartition();
    IE_NAMESPACE(partition)::JoinRelationMap joinMap;
    _indexApp.reset(new IE_NAMESPACE(partition)::IndexApplication);
    _indexApp->Init(_indexPartitionMap, joinMap);
    _tableInfo = TableInfoConfigurator::createFromIndexApp(_tableName, _indexApp);
    bool useSubFlag = _tableInfo->hasSubSchema();
    _matchDocAllocator.reset(new matchdoc::MatchDocAllocator(&_pool, useSubFlag));
    _indexAppSnapshot = _indexApp->CreateSnapshot();
    std::vector<suez::turing::VirtualAttribute*> virtualAttributes;
    _attributeExpressionCreator.reset(new AttributeExpressionCreator(
                    &_pool, _matchDocAllocator.get(),
                    _tableName, _indexAppSnapshot.get(),
                    _tableInfo, virtualAttributes,
                    NULL,
                    suez::turing::CavaPluginManagerPtr(),
                    NULL));
}

void ScanIteratorCreatorTest::tearDown() {
    _matchDocAllocator.reset();
    _indexPartitionMap.clear();
    _indexApp.reset();
}

void ScanIteratorCreatorTest::initIndexPartition() {
    string tableName, fields, indexes, attributes, summarys, truncateProfileStr, docs;
    int64_t ttl;
    tableName = _tableName;
    fields = "attr1:uint32;attr2:int32:true;id:int64;name:string;index2:text";
    indexes = "id:primarykey64:id;index_2:text:index2;name:string:name";
    attributes = "attr1;attr2;id";
    summarys = "name";
    truncateProfileStr = "";
    docs = "cmd=add,attr1=0,attr2=0 10,id=1,name=aa,index2=a a a;"
           "cmd=add,attr1=1,attr2=1 11,id=2,name=bb,index2=a b c;"
           "cmd=add,attr1=2,attr2=2 22,id=3,name=cc,index2=a c d stop;"
           "cmd=add,attr1=3,attr2=3 33,id=4,name=dd,index2=b c d";
    ttl = numeric_limits<int64_t>::max();
    string testPath = _testPath + tableName;

    IE_NAMESPACE(config)::IndexPartitionSchemaPtr schema =
        IE_NAMESPACE(testlib)::IndexlibPartitionCreator::CreateSchema(
                tableName, fields, indexes, attributes, summarys, truncateProfileStr);
    assert(schema.get() != nullptr);
    IE_NAMESPACE(config)::IndexPartitionOptions options;
    options.TEST_mQuickExit = true;
    options.GetOnlineConfig().ttl = ttl;

    IE_NAMESPACE(partition)::IndexPartitionPtr indexPartition =
        IE_NAMESPACE(testlib)::IndexlibPartitionCreator::CreateIndexPartition(
                schema, testPath, docs, options, "", true);
    assert(indexPartition.get() != nullptr);
    string schemaName = indexPartition->GetSchema()->GetSchemaName();
    _indexPartitionMap.insert(make_pair(schemaName, indexPartition));
}

search::IndexPartitionReaderWrapperPtr ScanIteratorCreatorTest::getIndexPartitionReaderWrapper() {
    auto indexReaderWrapper =
        IndexPartitionReaderUtil::createIndexPartitionReaderWrapper(_indexAppSnapshot.get(), _tableName);
    if (indexReaderWrapper == NULL) {
        return IndexPartitionReaderWrapperPtr();
     }
    indexReaderWrapper->setSessionPool(&_pool);
    return indexReaderWrapper;
}

TEST_F(ScanIteratorCreatorTest, testCreateScanIteratorWithQuery) {
    auto indexReaderWrapper = getIndexPartitionReaderWrapper();
    ASSERT_TRUE(indexReaderWrapper != NULL);
    ScanIteratorCreatorParam param;
    param.tableName = _tableName;
    param.indexPartitionReaderWrapper = indexReaderWrapper;
    param.attributeExpressionCreator = _attributeExpressionCreator;
    param.matchDocAllocator = _matchDocAllocator;
    param.pool = &_pool;
    param.tableInfo = _tableInfo;
    bool emptyScan = false;
    LayerMetaPtr layerMeta = ScanIteratorCreator::createLayerMeta(indexReaderWrapper, &_pool);
    FilterWrapperPtr filterWrapper;
    AttributeExpression *boolAttrExpr = _attributeExpressionCreator->createAttributeExpression("id=1");
    ASSERT_TRUE(boolAttrExpr!= NULL);
    ASSERT_TRUE(ScanIteratorCreator::createFilterWrapper(boolAttrExpr,
                    _matchDocAllocator.get(), &_pool, filterWrapper));
    QueryPtr query(new TermQuery("aa", "name", RequiredFields(), ""));
    { // QueryScanIterator
        ScanIteratorCreator iterCreator(param);
        auto iter = iterCreator.createScanIterator(query, filterWrapper, layerMeta,  emptyScan);
        ASSERT_TRUE(dynamic_cast<QueryScanIterator*>(iter) != nullptr);
        ASSERT_FALSE(emptyScan);
        ASSERT_TRUE(iter != NULL);
        vector<MatchDoc> matchDocs;
        bool ret = iter->batchSeek(10, matchDocs);
        ASSERT_TRUE(ret);
        ASSERT_EQ(1, matchDocs.size());
        DELETE_AND_SET_NULL(iter);
    }
    { // RangeScanIterator
        ScanIteratorCreator iterCreator(param);
        auto iter = iterCreator.createScanIterator(QueryPtr(), filterWrapper, layerMeta,  emptyScan);
        ASSERT_TRUE(dynamic_cast<RangeScanIterator*>(iter) != nullptr);
        ASSERT_FALSE(emptyScan);
        ASSERT_TRUE(iter != NULL);
        vector<MatchDoc> matchDocs;
        bool ret = iter->batchSeek(10, matchDocs);
        ASSERT_TRUE(ret);
        ASSERT_EQ(1, matchDocs.size());
        DELETE_AND_SET_NULL(iter);
    }
    { // RangeScanIteratorWihtoutFilter
        ScanIteratorCreator iterCreator(param);
        auto iter = iterCreator.createScanIterator(QueryPtr(), FilterWrapperPtr(), layerMeta,  emptyScan);
        ASSERT_TRUE(dynamic_cast<RangeScanIteratorWithoutFilter*>(iter) != nullptr);
        ASSERT_FALSE(emptyScan);
        ASSERT_TRUE(iter != NULL);
        vector<MatchDoc> matchDocs;
        bool ret = iter->batchSeek(10, matchDocs);
        ASSERT_TRUE(ret);
        ASSERT_EQ(4, matchDocs.size());
        DELETE_AND_SET_NULL(iter);
    }
    { // RangeScanIteratorWihtoutFilter
        ScanIteratorCreator iterCreator(param);
        auto iter = iterCreator.createScanIterator(QueryPtr(), FilterWrapperPtr(), layerMeta,  emptyScan);
        ASSERT_TRUE(dynamic_cast<RangeScanIteratorWithoutFilter*>(iter) != nullptr);
        ASSERT_FALSE(emptyScan);
        ASSERT_TRUE(iter != NULL);
        vector<MatchDoc> matchDocs;
        bool ret = iter->batchSeek(10, matchDocs);
        ASSERT_TRUE(ret);
        ASSERT_EQ(4, matchDocs.size());
        DELETE_AND_SET_NULL(iter);
    }
}

TEST_F(ScanIteratorCreatorTest, testCreateHa3ScanIterator) {
    auto indexReaderWrapper = getIndexPartitionReaderWrapper();
    ASSERT_TRUE(indexReaderWrapper != NULL);
    ScanIteratorCreatorParam param;
    param.tableName = _tableName;
    param.indexPartitionReaderWrapper = indexReaderWrapper;
    param.attributeExpressionCreator = _attributeExpressionCreator;
    param.matchDocAllocator = _matchDocAllocator;
    param.pool = &_pool;
    param.tableInfo = _tableInfo;
    bool emptyScan = false;
    LayerMetaPtr layerMeta = ScanIteratorCreator::createLayerMeta(indexReaderWrapper, &_pool);
    FilterWrapperPtr filterWrapper;
    AttributeExpression *boolAttrExpr = _attributeExpressionCreator->createAttributeExpression("id=1");
    ASSERT_TRUE(boolAttrExpr!= NULL);
    ASSERT_TRUE(ScanIteratorCreator::createFilterWrapper(boolAttrExpr,
                    _matchDocAllocator.get(), &_pool, filterWrapper));
    QueryPtr query(new TermQuery("aa", "name", RequiredFields(), ""));
    {
        ScanIteratorCreator iterCreator(param);
        auto iter = iterCreator.createHa3ScanIterator(query, filterWrapper, indexReaderWrapper,
                _matchDocAllocator, NULL, layerMeta, emptyScan);
        ASSERT_FALSE(emptyScan);
        ASSERT_TRUE(iter != NULL);
        vector<MatchDoc> matchDocs;
        bool ret = iter->batchSeek(10, matchDocs);
        ASSERT_TRUE(ret);
        ASSERT_EQ(1, matchDocs.size());
        DELETE_AND_SET_NULL(iter);
    }
    {
        QueryPtr emptyQuery(new TermQuery("aaaa", "name", RequiredFields(), ""));
        ScanIteratorCreator iterCreator(param);
        auto iter = iterCreator.createHa3ScanIterator(emptyQuery, filterWrapper, indexReaderWrapper,
                _matchDocAllocator, NULL, layerMeta, emptyScan);
        ASSERT_TRUE(emptyScan);
        ASSERT_TRUE(iter == NULL);
    }
}

TEST_F(ScanIteratorCreatorTest, testCreateScanIteratorWithCond) {
    auto indexReaderWrapper = getIndexPartitionReaderWrapper();
    ASSERT_TRUE(indexReaderWrapper != NULL);
    ScanIteratorCreatorParam param;
    param.tableName = _tableName;
    param.indexPartitionReaderWrapper = indexReaderWrapper;
    param.attributeExpressionCreator = _attributeExpressionCreator;
    param.matchDocAllocator = _matchDocAllocator;
    param.pool = &_pool;
    param.tableInfo = _tableInfo;
    bool emptyScan = false;
    {
        ScanIteratorCreator iterCreator(param);
        string condStr  = R"json({"op":"LIKE", "params":["$name", "aa"]})json";
        map<string, IndexInfo> indexInfos = { { "name",
                                                IndexInfo("name", "TEXT") } };
        auto iter = iterCreator.createScanIterator(condStr, indexInfos, emptyScan);
        ASSERT_FALSE(emptyScan);
        ASSERT_TRUE(iter != NULL);
        vector<MatchDoc> matchDocs;
        bool ret = iter->batchSeek(10, matchDocs);
        ASSERT_TRUE(ret);
        ASSERT_EQ(1, matchDocs.size());
        DELETE_AND_SET_NULL(iter);
    }
    { // condition failed
        ScanIteratorCreator iterCreator(param);
        string condStr  = R"json({"op":"LIKE", "xxx":["$name", "aa"]})json";
        map<string, IndexInfo> indexInfos = { { "name",
                                                IndexInfo("name", "TEXT") } };
        emptyScan = false;
        auto iter = iterCreator.createScanIterator(condStr, indexInfos, emptyScan);
        ASSERT_FALSE(emptyScan);
        ASSERT_TRUE(iter == NULL);
    }
    { //query executor is empty
        ScanIteratorCreator iterCreator(param);
        string condStr  = R"json({"op":"LIKE", "params":["$name", "not_exist"]})json";
        map<string, IndexInfo> indexInfos = { { "name",
                                                IndexInfo("name", "TEXT") } };
        emptyScan = false;
        auto iter = iterCreator.createScanIterator(condStr, indexInfos, emptyScan);
        ASSERT_TRUE(iter == NULL);
        ASSERT_TRUE(emptyScan);
    }
}

TEST_F(ScanIteratorCreatorTest, testGenCreateScanIteratorInfo) {
    auto indexReaderWrapper = getIndexPartitionReaderWrapper();
    ASSERT_TRUE(indexReaderWrapper != NULL);
    ScanIteratorCreatorParam param;
    param.tableName = _tableName;
    param.indexPartitionReaderWrapper = indexReaderWrapper;
    param.attributeExpressionCreator = _attributeExpressionCreator;
    param.matchDocAllocator = _matchDocAllocator;
    param.pool = &_pool;
    param.tableInfo = _tableInfo;
    {
        ScanIteratorCreator iterCreator(param);
        string condStr  = R"json({"op":"=", "params":["$name", "aa"]})json";
        map<string, IndexInfo> indexInfos = { { "name",
                                                IndexInfo("name", "TEXT") } };
        CreateScanIteratorInfo info;
        bool ret = iterCreator.genCreateScanIteratorInfo(condStr, indexInfos, info);
        ASSERT_TRUE(ret);
        ASSERT_TRUE(info.query != nullptr);
        ASSERT_TRUE(info.filterWrapper == nullptr);
        ASSERT_TRUE(info.layerMeta != nullptr);
    }
    { //layer meta is null
        auto newParam = param;
        newParam.parallelIndex = 2;
        newParam.parallelNum = 2;
        ScanIteratorCreator iterCreator(newParam);
        string condStr  = R"json({"op":"=", "params":["$name", "aa"]})json";
        map<string, IndexInfo> indexInfos = { { "name",
                                                IndexInfo("name", "TEXT") } };
        CreateScanIteratorInfo info;
        bool ret = iterCreator.genCreateScanIteratorInfo(condStr, indexInfos, info);
        ASSERT_FALSE(ret);
    }
    { // condition failed
        ScanIteratorCreator iterCreator(param);
        string condStr  = R"json({"op":"LIKE", "xxx":["$name", "aa"]})json";
        map<string, IndexInfo> indexInfos = { { "name",
                                                IndexInfo("name", "TEXT") } };
        CreateScanIteratorInfo info;
        bool ret = iterCreator.genCreateScanIteratorInfo(condStr, indexInfos, info);
        ASSERT_FALSE(ret);
    }
    { //index failed
        ScanIteratorCreator iterCreator(param);
        string condStr  = R"json({"op":"LIKE", "params":["$name", "aa"]})json";
        map<string, IndexInfo> indexInfos = { { "name1",
                                                IndexInfo("name", "TEXT") } };
        CreateScanIteratorInfo info;
        bool ret = iterCreator.genCreateScanIteratorInfo(condStr, indexInfos, info);
        ASSERT_FALSE(ret);
    }
    { //query term not exist
        ScanIteratorCreator iterCreator(param);
        string condStr  = R"json({"op":"LIKE", "params":["$name", "not_exist"]})json";
        map<string, IndexInfo> indexInfos = { { "name",
                                                IndexInfo("name", "TEXT") } };
        CreateScanIteratorInfo info;
        bool ret = iterCreator.genCreateScanIteratorInfo(condStr, indexInfos, info);
        ASSERT_TRUE(ret);
    }
    { //filter failed
        ScanIteratorCreator iterCreator(param);
        string condStr  = R"json({"op":"=", "params":["namea", "not_exist"]})json";
        map<string, IndexInfo> indexInfos = {};
        CreateScanIteratorInfo info;
        bool ret = iterCreator.genCreateScanIteratorInfo(condStr, indexInfos, info);
        ASSERT_FALSE(ret);
    }
}

TEST_F(ScanIteratorCreatorTest, testCreateQueryExecutor) {
    auto indexReaderWrapper = getIndexPartitionReaderWrapper();
    auto layerMeta = ScanIteratorCreator::createLayerMeta(indexReaderWrapper, &_pool);
    { // query is empty
        bool emptyScan = false;
        QueryExecutor *executor = ScanIteratorCreator::createQueryExecutor(QueryPtr(),
                indexReaderWrapper, _tableName, &_pool, NULL, layerMeta.get(), emptyScan);
        QueryExecutorPtr executorPtr(executor,
                [](QueryExecutor *p) {
                    POOL_DELETE_CLASS(p);
                });
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
        QueryExecutor *executor = ScanIteratorCreator::createQueryExecutor(query,
                indexReaderWrapper, _tableName, &_pool, NULL, layerMeta.get(), emptyScan);
        ASSERT_TRUE(executor == NULL);
        ASSERT_TRUE(emptyScan);
    }
    {
        bool emptyScan = false;
        Term term;
        term.setIndexName("name");
        term.setWord("aa");
        QueryPtr query(new TermQuery(term, ""));
        QueryExecutor *executor = ScanIteratorCreator::createQueryExecutor(query,
                indexReaderWrapper, _tableName, &_pool, NULL, layerMeta.get(), emptyScan);
        QueryExecutorPtr executorPtr(executor,
                [](QueryExecutor *p) {
                    POOL_DELETE_CLASS(p);
                });
        ASSERT_TRUE(executorPtr);
        ASSERT_EQ("BufferedTermQueryExecutor", executorPtr->getName());
        ASSERT_FALSE(emptyScan);
    }
}

TEST_F(ScanIteratorCreatorTest, testCreateFilterWrapper) {
    { // attribute expression is null
        FilterWrapperPtr filterWrapper;
        ASSERT_FALSE(ScanIteratorCreator::createFilterWrapper(NULL, _matchDocAllocator.get(), &_pool, filterWrapper));
    }
    { // attribute expression not bool
        FilterWrapperPtr filterWrapper;
        AttributeExpression *intAttrExpr = _attributeExpressionCreator->createAtomicExpr("id");
        ASSERT_TRUE(intAttrExpr!= NULL);
        ASSERT_FALSE(ScanIteratorCreator::createFilterWrapper(intAttrExpr,
                        _matchDocAllocator.get(), &_pool, filterWrapper));
    }
    {
        FilterWrapperPtr filterWrapper;
        AttributeExpression *boolAttrExpr = _attributeExpressionCreator->createAttributeExpression("id=1");
        ASSERT_TRUE(boolAttrExpr!= NULL);
        ASSERT_TRUE(ScanIteratorCreator::createFilterWrapper(boolAttrExpr,
                        _matchDocAllocator.get(), &_pool, filterWrapper));
        ASSERT_TRUE(filterWrapper != NULL);
        ASSERT_TRUE(filterWrapper->getFilter() != NULL);
    }
}

TEST_F(ScanIteratorCreatorTest, testCreateLayerMeta) {
    auto indexReaderWrapper = getIndexPartitionReaderWrapper();
    ASSERT_TRUE(indexReaderWrapper != NULL);
    auto layerMeta = ScanIteratorCreator::createLayerMeta(indexReaderWrapper, &_pool);
    ASSERT_EQ(1, layerMeta->size());
    ASSERT_EQ(0, (*layerMeta)[0].begin);
    ASSERT_EQ(3, (*layerMeta)[0].end);
    ASSERT_EQ(4, (*layerMeta)[0].quota);
    ASSERT_EQ(0, (*layerMeta)[0].nextBegin);
    ASSERT_EQ(std::numeric_limits<uint32_t>::max(), layerMeta->quota);
    ASSERT_EQ(std::numeric_limits<uint32_t>::max(), layerMeta->maxQuota);
    ASSERT_EQ(QM_PER_LAYER, layerMeta->quotaMode);
    ASSERT_TRUE(layerMeta->needAggregate);
}

TEST_F(ScanIteratorCreatorTest, testSplitLayerMeta) {
    {
        // 0 range -> 1 spit
        LayerMetaPtr layerMeta(new LayerMeta(&_pool));
        LayerMetaPtr newLayerMeta = ScanIteratorCreator::splitLayerMeta(&_pool, layerMeta, 0, 1);
        ASSERT_TRUE(newLayerMeta);
        ASSERT_EQ(0, newLayerMeta->size());
    }

    {
        // 1 range -> 1 split
        LayerMetaPtr layerMeta(new LayerMeta(&_pool));
        layerMeta->push_back(DocIdRangeMeta(10, 20));
        LayerMetaPtr splitLayerMeta0 = ScanIteratorCreator::splitLayerMeta(&_pool, layerMeta, 0, 1);
        ASSERT_TRUE(splitLayerMeta0);
        ASSERT_EQ(1, splitLayerMeta0->size());
        const auto& rangeMeta0 = (*splitLayerMeta0)[0];
        ASSERT_EQ(10, rangeMeta0.begin);
        ASSERT_EQ(20, rangeMeta0.end);
        ASSERT_EQ(11, rangeMeta0.quota);
    }

    {
        // 1 range -> 2 split
        LayerMetaPtr layerMeta(new LayerMeta(&_pool));
        layerMeta->push_back(DocIdRangeMeta(10, 20));
        LayerMetaPtr splitLayerMeta0 = ScanIteratorCreator::splitLayerMeta(&_pool, layerMeta, 0, 2);
        ASSERT_TRUE(splitLayerMeta0);
        ASSERT_EQ(1, splitLayerMeta0->size());
        const auto& rangeMeta0 = (*splitLayerMeta0)[0];
        ASSERT_EQ(10, rangeMeta0.begin);
        ASSERT_EQ(14, rangeMeta0.end);
        ASSERT_EQ(5, rangeMeta0.quota);

        LayerMetaPtr splitLayerMeta1 = ScanIteratorCreator::splitLayerMeta(&_pool, layerMeta, 1, 2);
        ASSERT_TRUE(splitLayerMeta1);
        ASSERT_EQ(1, splitLayerMeta1->size());
        const auto& rangeMeta1 = (*splitLayerMeta1)[0];
        ASSERT_EQ(15, rangeMeta1.begin);
        ASSERT_EQ(20, rangeMeta1.end);
        ASSERT_EQ(6, rangeMeta1.quota);
    }

    {
        // 1 range(1 document) -> 2 split
        LayerMetaPtr layerMeta(new LayerMeta(&_pool));
        layerMeta->push_back(DocIdRangeMeta(10, 10));
        LayerMetaPtr splitLayerMeta0 = ScanIteratorCreator::splitLayerMeta(&_pool, layerMeta, 0, 2);
        ASSERT_TRUE(splitLayerMeta0);
        ASSERT_EQ(0, splitLayerMeta0->size());

        LayerMetaPtr splitLayerMeta1 = ScanIteratorCreator::splitLayerMeta(&_pool, layerMeta, 1, 2);
        ASSERT_TRUE(splitLayerMeta1);
        ASSERT_EQ(1, splitLayerMeta1->size());
        const auto& rangeMeta1 = (*splitLayerMeta1)[0];
        ASSERT_EQ(10, rangeMeta1.begin);
        ASSERT_EQ(10, rangeMeta1.end);
        ASSERT_EQ(1, rangeMeta1.quota);
    }

    {
        // 2 range -> 1 split
        LayerMetaPtr layerMeta(new LayerMeta(&_pool));
        layerMeta->push_back(DocIdRangeMeta(10, 20));
        layerMeta->push_back(DocIdRangeMeta(21, 30));
        LayerMetaPtr splitLayerMeta0 = ScanIteratorCreator::splitLayerMeta(&_pool, layerMeta, 0, 1);
        ASSERT_TRUE(splitLayerMeta0);
        ASSERT_EQ(2, splitLayerMeta0->size());
        const auto& rangeMeta0 = (*splitLayerMeta0)[0];
        ASSERT_EQ(10, rangeMeta0.begin);
        ASSERT_EQ(20, rangeMeta0.end);
        ASSERT_EQ(11, rangeMeta0.quota);
        const auto& rangeMeta1 = (*splitLayerMeta0)[1];
        ASSERT_EQ(21, rangeMeta1.begin);
        ASSERT_EQ(30, rangeMeta1.end);
        ASSERT_EQ(10, rangeMeta1.quota);
    }

    {
        // 2 range -> 3 split
        LayerMetaPtr layerMeta(new LayerMeta(&_pool));
        layerMeta->push_back(DocIdRangeMeta(10, 20));
        layerMeta->push_back(DocIdRangeMeta(30, 40));

        LayerMetaPtr splitLayerMeta0 = ScanIteratorCreator::splitLayerMeta(&_pool, layerMeta, 0, 3);
        ASSERT_TRUE(splitLayerMeta0);
        ASSERT_EQ(1, splitLayerMeta0->size());
        const auto& rangeMeta00 = (*splitLayerMeta0)[0];
        ASSERT_EQ(10, rangeMeta00.begin);
        ASSERT_EQ(16, rangeMeta00.end);
        ASSERT_EQ(7, rangeMeta00.quota);

        LayerMetaPtr splitLayerMeta1 = ScanIteratorCreator::splitLayerMeta(&_pool, layerMeta, 1, 3);
        ASSERT_TRUE(splitLayerMeta1);
        ASSERT_EQ(2, splitLayerMeta1->size());
        const auto& rangeMeta10 = (*splitLayerMeta1)[0];
        ASSERT_EQ(17, rangeMeta10.begin);
        ASSERT_EQ(20, rangeMeta10.end);
        ASSERT_EQ(4, rangeMeta10.quota);
        const auto& rangeMeta11 = (*splitLayerMeta1)[1];
        ASSERT_EQ(30, rangeMeta11.begin);
        ASSERT_EQ(32, rangeMeta11.end);
        ASSERT_EQ(3, rangeMeta11.quota);

        LayerMetaPtr splitLayerMeta2 = ScanIteratorCreator::splitLayerMeta(&_pool, layerMeta, 2, 3);
        ASSERT_TRUE(splitLayerMeta2);
        ASSERT_EQ(1, splitLayerMeta2->size());
        const auto& rangeMeta20 = (*splitLayerMeta2)[0];
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
        LayerMetaPtr splitLayerMeta0 = ScanIteratorCreator::splitLayerMeta(&_pool, layerMeta, 0, 2);
        ASSERT_TRUE(splitLayerMeta0);
        ASSERT_EQ(2, splitLayerMeta0->size());
        const auto& rangeMeta00 = (*splitLayerMeta0)[0];
        ASSERT_EQ(10, rangeMeta00.begin);
        ASSERT_EQ(10, rangeMeta00.nextBegin);
        ASSERT_EQ(11, rangeMeta00.end);
        ASSERT_EQ(2, rangeMeta00.quota);
        const auto& rangeMeta01 = (*splitLayerMeta0)[1];
        ASSERT_EQ(13, rangeMeta01.begin);
        ASSERT_EQ(13, rangeMeta01.nextBegin);
        ASSERT_EQ(13, rangeMeta01.end);
        ASSERT_EQ(1, rangeMeta01.quota);

        LayerMetaPtr splitLayerMeta1 = ScanIteratorCreator::splitLayerMeta(&_pool, layerMeta, 1, 2);
        ASSERT_TRUE(splitLayerMeta1);
        ASSERT_EQ(2, splitLayerMeta1->size());
        const auto& rangeMeta10 = (*splitLayerMeta1)[0];
        ASSERT_EQ(14, rangeMeta10.begin);
        ASSERT_EQ(14, rangeMeta10.end);
        ASSERT_EQ(1, rangeMeta10.quota);
        const auto& rangeMeta11 = (*splitLayerMeta1)[1];
        ASSERT_EQ(16, rangeMeta11.begin);
        ASSERT_EQ(17, rangeMeta11.end);
        ASSERT_EQ(2, rangeMeta11.quota);
    }
}

END_HA3_NAMESPACE();
