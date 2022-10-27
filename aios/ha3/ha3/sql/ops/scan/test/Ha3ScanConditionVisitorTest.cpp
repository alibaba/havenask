#include <ha3/test/test.h>
#include <ha3/sql/ops/test/OpTestBase.h>
#include <ha3/sql/ops/scan/Ha3ScanConditionVisitor.h>
#include <ha3/sql/ops/scan/ScanIteratorCreator.h>
#include <indexlib/testlib/indexlib_partition_creator.h>
#include <suez/turing/expression/util/TableInfoConfigurator.h>
#include <indexlib/storage/file_system_wrapper.h>
#include <indexlib/index_base/schema_adapter.h>
#include <ha3/search/IndexPartitionReaderUtil.h>
#include <ha3/common/TermQuery.h>
#include <ha3/common/MultiTermQuery.h>
#include <indexlib/partition/index_application.h>
#include <ha3/config/QueryInfo.h>
#include <build_service/analyzer/AnalyzerFactory.h>
#include <build_service/analyzer/AnalyzerInfos.h>
#include <build_service/analyzer/TokenizerManager.h>
#include <ha3/sql/ops/condition/ConditionParser.h>
#include <ha3/sql/ops/condition/CaseExpression.h>
#include <ha3/sql/ops/util/SqlJsonUtil.h>
#include <ha3/sql/ops/scan/test/FakeTokenizer.h>


using namespace suez::turing;
using namespace build_service::analyzer;
using namespace build_service::config;
using namespace std;
using namespace testing;
using namespace matchdoc;
using namespace suez::turing;

USE_HA3_NAMESPACE(search);
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(config);

BEGIN_HA3_NAMESPACE(sql);

class Ha3ScanConditionVisitorTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
    void initIndexPartition();
    search::IndexPartitionReaderWrapperPtr getIndexPartitionReaderWrapper();
    AnalyzerFactoryPtr initAnalyzerFactory();
    Ha3ScanConditionVisitorPtr createVisitor();

private:
    string _tableName;
    string _testPath;
    IE_NAMESPACE(partition)::IndexApplication::IndexPartitionMap _indexPartitionMap;
    IE_NAMESPACE(partition)::IndexApplicationPtr _indexApp;
    autil::mem_pool::Pool _pool;
    suez::turing::TableInfoPtr _tableInfo;
    matchdoc::MatchDocAllocatorPtr _matchDocAllocator;
    suez::turing::AttributeExpressionCreatorPtr _attributeExpressionCreator;
    AnalyzerFactoryPtr _analyzerFactory;
    QueryInfo _queryInfo;
    IE_NAMESPACE(partition)::PartitionReaderSnapshotPtr _indexAppSnapshot;
    search::IndexPartitionReaderWrapperPtr _indexPartitionReaderWrapper;
    search::LayerMetaPtr _layerMeta;
    map<string, IndexInfo> _indexInfoMap;
};

void Ha3ScanConditionVisitorTest::setUp() {
    _testPath = GET_TEMPLATE_DATA_PATH() + "sql_op_index_path/";
    _tableName = "invertedTable";
    initIndexPartition();
    IE_NAMESPACE(partition)::JoinRelationMap joinMap;
    _indexApp.reset(new IE_NAMESPACE(partition)::IndexApplication);
    _indexApp->Init(_indexPartitionMap, joinMap);
    _analyzerFactory = initAnalyzerFactory();
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
    _queryInfo.setDefaultIndexName("index_2");
}

void Ha3ScanConditionVisitorTest::tearDown() {
    _matchDocAllocator.reset();
    _indexPartitionMap.clear();
    _indexApp.reset();
}

void Ha3ScanConditionVisitorTest::initIndexPartition() {
    int64_t ttl;
    string tableName = _tableName;

    auto mainSchema = IE_NAMESPACE(testlib)::IndexlibPartitionCreator::CreateSchema(
            tableName,
            "attr1:uint32;attr2:int32:true;id:int64;name:string;index2:text", // fields
            "id:primarykey64:id;index_2:text:index2;name:string:name;attr2:number:attr2", //indexs
            "attr1;attr2;id", // attributes
            "name", // summarys
            ""); // truncateProfile
    auto subSchema = IE_NAMESPACE(testlib)::IndexlibPartitionCreator::CreateSchema(
            tableName,
            "sub_id:int64", // fields
            "sub_id:primarykey64:sub_id", //indexs
            "sub_id", //attributes
            "", // summarys
            ""); // truncateProfile
    string docsStr = "cmd=add,attr1=0,attr2=0 10,id=1,name=aa,index2=a a a,sub_id=1^2;"
           "cmd=add,attr1=1,attr2=1 11,id=2,name=bb,index2=a b c,sub_id=3;"
           "cmd=add,attr1=2,attr2=2 22,id=3,name=cc,index2=a c d stop,sub_id=4;"
           "cmd=add,attr1=3,attr2=3 33,id=4,name=dd,index2=b c d,sub_id=5";
    ttl = numeric_limits<int64_t>::max();
    string testPath = _testPath + tableName;
    IE_NAMESPACE(config)::IndexPartitionSchemaPtr schema =
        IE_NAMESPACE(testlib)::IndexlibPartitionCreator::CreateSchemaWithSub(
                mainSchema, subSchema);
    assert(schema.get() != nullptr);
    IE_NAMESPACE(config)::IndexPartitionOptions options;
    options.GetOnlineConfig().ttl = ttl;
    options.TEST_mQuickExit = true;

    IE_NAMESPACE(partition)::IndexPartitionPtr indexPartition =
        IE_NAMESPACE(testlib)::IndexlibPartitionCreator::CreateIndexPartition(
                schema, testPath, docsStr, options, "", true);
    assert(indexPartition.get() != nullptr);
    string schemaName = indexPartition->GetSchema()->GetSchemaName();
    _indexPartitionMap.insert(make_pair(schemaName, indexPartition));
}

search::IndexPartitionReaderWrapperPtr Ha3ScanConditionVisitorTest::getIndexPartitionReaderWrapper() {
    auto indexReaderWrapper =
        IndexPartitionReaderUtil::createIndexPartitionReaderWrapper(_indexAppSnapshot.get(), _tableName);
    if (indexReaderWrapper == NULL) {
        return IndexPartitionReaderWrapperPtr();
     }
    indexReaderWrapper->setSessionPool(&_pool);
    return indexReaderWrapper;
}

AnalyzerFactoryPtr Ha3ScanConditionVisitorTest::initAnalyzerFactory() {
    const std::string fakeAnalyzerName = "fake_analyzer";
    AnalyzerInfosPtr infosPtr(new AnalyzerInfos());
    unique_ptr<AnalyzerInfo> taobaoInfo(new AnalyzerInfo());
    taobaoInfo->setTokenizerConfig(TOKENIZER_TYPE, SIMPLE_ANALYZER);
    taobaoInfo->setTokenizerConfig(DELIMITER, " ");
    taobaoInfo->addStopWord(string("stop"));
    infosPtr->addAnalyzerInfo("taobao_analyzer", *taobaoInfo);

    string basePath = ALIWSLIB_DATA_PATH;
    string aliwsConfig = basePath + "/conf/";

    infosPtr->makeCompatibleWithOldConfig();
    TokenizerManagerPtr tokenizerManagerPtr(
            new TokenizerManager(ResourceReaderPtr(new ResourceReader(aliwsConfig))));
    EXPECT_TRUE(tokenizerManagerPtr->init(infosPtr->getTokenizerConfig()));

    unique_ptr<AnalyzerInfo> fakeAnalyzerInfo(new AnalyzerInfo());
    fakeAnalyzerInfo->setTokenizerConfig(TOKENIZER_TYPE, SIMPLE_ANALYZER);
    fakeAnalyzerInfo->setTokenizerConfig(DELIMITER, " ");
    fakeAnalyzerInfo->addStopWord(string("stop"));
    fakeAnalyzerInfo->setTokenizerName(fakeAnalyzerName);
    infosPtr->addAnalyzerInfo(fakeAnalyzerName, *fakeAnalyzerInfo);
    EXPECT_EQ(2, infosPtr->getAnalyzerCount());

    tokenizerManagerPtr->addTokenizer(fakeAnalyzerName, new FakeTokenizer());
    auto *tokenizer = tokenizerManagerPtr->getTokenizerByName(fakeAnalyzerName);
    EXPECT_NE(nullptr, tokenizer);
    DELETE_AND_SET_NULL(tokenizer);

    AnalyzerFactory *analyzerFactory = new AnalyzerFactory();
    EXPECT_TRUE(analyzerFactory->init(infosPtr, tokenizerManagerPtr));
    AnalyzerFactoryPtr ptr(analyzerFactory);
    EXPECT_TRUE(ptr->hasAnalyzer(fakeAnalyzerName));
    EXPECT_FALSE(ptr->hasAnalyzer(fakeAnalyzerName + "1111"));
    return ptr;
}

Ha3ScanConditionVisitorPtr Ha3ScanConditionVisitorTest::createVisitor() {
    _indexPartitionReaderWrapper = getIndexPartitionReaderWrapper();
    _layerMeta = ScanIteratorCreator::createLayerMeta(_indexPartitionReaderWrapper, &_pool);
    Ha3ScanConditionVisitorParam param;
    _indexInfoMap["name"] = IndexInfo("name", "string");
    _indexInfoMap["index_2"] = IndexInfo("index_2", "string");
    _indexInfoMap["attr2"] = IndexInfo("attr2", "number");

    param.attrExprCreator = _attributeExpressionCreator;
    param.indexPartitionReaderWrapper = _indexPartitionReaderWrapper;
    param.mainTableName = _tableName;
    param.layerMeta = _layerMeta.get();
    param.pool = &_pool;
    param.analyzerFactory = _analyzerFactory.get();
    param.indexInfos = _tableInfo->getIndexInfos();
    param.indexInfo = &_indexInfoMap;
    param.queryInfo = &_queryInfo;
    Ha3ScanConditionVisitorPtr vistor(new Ha3ScanConditionVisitor(param));
    return vistor;
}

TEST_F(Ha3ScanConditionVisitorTest, testVisitOrCondition) {
    {
        ConditionParser parser;
        string condStr1 ="{\"op\":\"QUERY\", \"params\":[\"aa bb stop\"],\"type\":\"UDF\"}";
        string condStr2 ="{\"op\":\"!=\", \"params\":[\"$attr1\", 1]}";

        string jsonStr = "{\"op\" : \"OR\" , \"params\" : [" + condStr1 +","+condStr2 + "]}";
        ConditionPtr cond;
        bool success = parser.parseCondition(jsonStr, cond);
        ASSERT_TRUE(success);
        auto visitor = createVisitor();
        visitor->visitOrCondition((OrCondition*)(cond.get()));
        ASSERT_TRUE(visitor->getQuery() == NULL);
        ASSERT_TRUE(visitor->getAttributeExpression() != NULL);
        ASSERT_TRUE(!visitor->isError());
    }
    {
        ConditionParser parser;
        string condStr1 ="{\"op\":\"QUERY\", \"params\":[\"aa bb stop\"],\"type\":\"UDF\"}";
        string condStr2 ="{\"op\":\"<>\", \"params\":[\"$attr1\", 1]}";
        string jsonStr = "{\"op\" : \"OR\" , \"params\" : [" + condStr1 +","+condStr2 + "]}";
        ConditionPtr cond;
        bool success = parser.parseCondition(jsonStr, cond);
        ASSERT_TRUE(success);
        auto visitor = createVisitor();
        visitor->visitOrCondition((OrCondition*)(cond.get()));
        ASSERT_TRUE(visitor->getQuery() == NULL);
        ASSERT_TRUE(visitor->getAttributeExpression() != NULL);
        ASSERT_TRUE(!visitor->isError());
    }
    {
        ConditionParser parser;
        string condStr1 ="{\"op\":\"QUERY\", \"params\":[\"aa bb stop\"],\"type\":\"UDF\"}";
        string condStr2 ="{\"op\":\"QUERY\", \"params\":[\"aa bb stop\"],\"type\":\"UDF\"}";
        string jsonStr = "{\"op\" : \"OR\" , \"params\" : [" + condStr1 +","+condStr2 + "]}";
        ConditionPtr cond;
        bool success = parser.parseCondition(jsonStr, cond);
        ASSERT_TRUE(success);
        auto visitor = createVisitor();
        visitor->visitOrCondition((OrCondition*)(cond.get()));
        ASSERT_TRUE(visitor->getQuery() != NULL);
        ASSERT_TRUE(visitor->getAttributeExpression() == NULL);
        ASSERT_TRUE(!visitor->isError());
    }
    {
        ConditionParser parser;
        string condStr1 ="{\"op\":\"!=\", \"params\":[\"$attr1\", 1]}";
        string condStr2 ="{\"op\":\"<>\", \"params\":[\"$attr1\", 1]}";

        string jsonStr = "{\"op\" : \"OR\" , \"params\" : [" + condStr1 +","+condStr2 + "]}";
        ConditionPtr cond;
        bool success = parser.parseCondition(jsonStr, cond);
        ASSERT_TRUE(success);
        auto visitor = createVisitor();
        visitor->visitOrCondition((OrCondition*)(cond.get()));
        ASSERT_TRUE(visitor->getQuery() == NULL);
        ASSERT_TRUE(visitor->getAttributeExpression() != NULL);
        ASSERT_TRUE(!visitor->isError());
    }
    {
        ConditionParser parser;
        string condStr1 ="{\"op\":\"QUERY\", \"params\":[\"aa bb stop\"],\"type\":\"UDF\"}";
        string condStr2 ="{\"op\":\"!=\", \"params\":[\"$attr1\", 1]}";
        string condStr3 = "{\"op\" : \"AND\" , \"params\" : [" + condStr1 +","+condStr2 + "]}";
        string condStr4 ="{\"op\":\"QUERY\", \"params\":[\"dd\"],\"type\":\"UDF\"}";
        string jsonStr = "{\"op\" : \"OR\" , \"params\" : [" + condStr3 +","+condStr4 + "]}";
        ConditionPtr cond;
        bool success = parser.parseCondition(jsonStr, cond);
        ASSERT_TRUE(success);
        auto visitor = createVisitor();
        visitor->visitOrCondition((OrCondition*)(cond.get()));
        ASSERT_TRUE(visitor->getQuery() == NULL);
        ASSERT_TRUE(visitor->getAttributeExpression() != NULL);
        ASSERT_EQ("((AndQuery:[TermQuery:[Term:[index_2||aa|100|]], TermQuery:[Term:[index_2||bb|100|]], ] AND (attr1!=1)) OR TermQuery:[Term:[index_2||dd|100|]])", visitor->getAttributeExpression()->getOriginalString());
        ASSERT_TRUE(!visitor->isError());
    }

    { //leaf has error, name1 not exist
        ConditionParser parser;
        string condStr1 ="{\"op\":\"QUERY\", \"params\":[\"name1\", \"aa bb stop\"],\"type\":\"UDF\"}";
        string condStr2 ="{\"op\":\"!=\", \"params\":[\"$attr1\", 1]}";

        string jsonStr = "{\"op\" : \"OR\" , \"params\" : [" + condStr1 +","+condStr2 + "]}";
        ConditionPtr cond;
        bool success = parser.parseCondition(jsonStr, cond);
        ASSERT_TRUE(success);
        auto visitor = createVisitor();
        visitor->visitOrCondition((OrCondition*)(cond.get()));
        ASSERT_TRUE(visitor->getQuery() == NULL);
        ASSERT_TRUE(visitor->getAttributeExpression() == NULL);
        ASSERT_TRUE(visitor->isError());
    }
    { //leaf has error, attr not cloumn
        ConditionParser parser;
        string condStr1 ="{\"op\":\"QUERY\", \"params\":[\"aa bb stop\"],\"type\":\"UDF\"}";
        string condStr2 ="{\"op\":\"!=\", \"params\":[\"attr1\", 1]}";

        string jsonStr = "{\"op\" : \"OR\" , \"params\" : [" + condStr1 +","+condStr2 + "]}";
        ConditionPtr cond;
        bool success = parser.parseCondition(jsonStr, cond);
        ASSERT_TRUE(success);
        auto visitor = createVisitor();
        visitor->visitOrCondition((OrCondition*)(cond.get()));
        ASSERT_TRUE(visitor->getQuery() == NULL);
        ASSERT_TRUE(visitor->getAttributeExpression() == NULL);
        ASSERT_TRUE(visitor->isError());
    }
}


TEST_F(Ha3ScanConditionVisitorTest, testVisitAndCondition) {
    {
        ConditionParser parser;
        string condStr1 ="{\"op\":\"QUERY\", \"params\":[\"aa bb stop\"],\"type\":\"UDF\"}";
        string condStr2 ="{\"op\":\"!=\", \"params\":[\"$attr1\", 1]}";

        string jsonStr = "{\"op\" : \"AND\" , \"params\" : [" + condStr1 +","+condStr2 + "]}";
        ConditionPtr cond;
        bool success = parser.parseCondition(jsonStr, cond);
        ASSERT_TRUE(success);
        auto visitor = createVisitor();
        visitor->visitAndCondition((AndCondition*)(cond.get()));
        ASSERT_TRUE(visitor->getQuery() != NULL);
        ASSERT_TRUE(visitor->getAttributeExpression() != NULL);
        ASSERT_TRUE(!visitor->isError());
    }
    {
        ConditionParser parser;
        string condStr1 ="{\"op\":\"QUERY\", \"params\":[\"aa bb stop\"],\"type\":\"UDF\"}";
        string condStr2 ="{\"op\":\"QUERY\", \"params\":[\"aa bb stop\"],\"type\":\"UDF\"}";

        string jsonStr = "{\"op\" : \"AND\" , \"params\" : [" + condStr1 +","+condStr2 + "]}";
        ConditionPtr cond;
        bool success = parser.parseCondition(jsonStr, cond);
        ASSERT_TRUE(success);
        auto visitor = createVisitor();
        visitor->visitAndCondition((AndCondition*)(cond.get()));
        ASSERT_TRUE(visitor->getQuery() != NULL);
        ASSERT_TRUE(visitor->getAttributeExpression() == NULL);
        ASSERT_TRUE(!visitor->isError());
    }
    {
        ConditionParser parser;
        string condStr1 ="{\"op\":\"!=\", \"params\":[\"$attr1\", 1]}";
        string condStr2 ="{\"op\":\"!=\", \"params\":[\"$attr1\", 1]}";

        string jsonStr = "{\"op\" : \"AND\" , \"params\" : [" + condStr1 +","+condStr2 + "]}";
        ConditionPtr cond;
        bool success = parser.parseCondition(jsonStr, cond);
        ASSERT_TRUE(success);
        auto visitor = createVisitor();
        visitor->visitAndCondition((AndCondition*)(cond.get()));
        ASSERT_TRUE(visitor->getQuery() == NULL);
        ASSERT_TRUE(visitor->getAttributeExpression() != NULL);
        ASSERT_TRUE(!visitor->isError());
    }
    {
        ConditionParser parser;
        string condStr1 ="{\"op\":\"QUERY\", \"params\":[\"aa bb stop\"],\"type\":\"UDF\"}";
        string condStr2 ="{\"op\":\"!=\", \"params\":[\"$attr1\", 1]}";
        string condStr3 = "{\"op\" : \"AND\" , \"params\" : [" + condStr1 +","+condStr2 + "]}";
        string condStr4 ="{\"op\":\"QUERY\", \"params\":[\"dd\"],\"type\":\"UDF\"}";
        string jsonStr = "{\"op\" : \"AND\" , \"params\" : [" + condStr3 +","+condStr4 + "]}";
        ConditionPtr cond;
        bool success = parser.parseCondition(jsonStr, cond);
        ASSERT_TRUE(success);
        auto visitor = createVisitor();
        visitor->visitAndCondition((AndCondition*)(cond.get()));
        ASSERT_TRUE(visitor->getQuery() != NULL);
        ASSERT_TRUE(visitor->getAttributeExpression() != NULL);
        ASSERT_EQ("(attr1!=1)", visitor->getAttributeExpression()->getOriginalString());
        ASSERT_EQ("AndQuery:[AndQuery:[TermQuery:[Term:[index_2||aa|100|]], TermQuery:[Term:[index_2||bb|100|]], ], TermQuery:[Term:[index_2||dd|100|]], ]", visitor->getQuery()->toString());
        ASSERT_TRUE(!visitor->isError());
    }
    { //leaf has error, name1 not exist
        ConditionParser parser;
        string condStr1 ="{\"op\":\"QUERY\", \"params\":[\"name1\", \"aa bb stop\"],\"type\":\"UDF\"}";
        string condStr2 ="{\"op\":\"!=\", \"params\":[\"$attr1\", 1]}";

        string jsonStr = "{\"op\" : \"AND\" , \"params\" : [" + condStr1 +","+condStr2 + "]}";
        ConditionPtr cond;
        bool success = parser.parseCondition(jsonStr, cond);
        ASSERT_TRUE(success);
        auto visitor = createVisitor();
        visitor->visitAndCondition((AndCondition*)(cond.get()));
        ASSERT_TRUE(visitor->getQuery() == NULL);
        ASSERT_TRUE(visitor->getAttributeExpression() == NULL);
        ASSERT_TRUE(visitor->isError());
    }
    { //leaf has error, attr not cloumn
        ConditionParser parser;
        string condStr1 ="{\"op\":\"QUERY\", \"params\":[\"aa bb stop\"],\"type\":\"UDF\"}";
        string condStr2 ="{\"op\":\"!=\", \"params\":[\"attr1\", 1]}";

        string jsonStr = "{\"op\" : \"AND\" , \"params\" : [" + condStr1 +","+condStr2 + "]}";
        ConditionPtr cond;
        bool success = parser.parseCondition(jsonStr, cond);
        ASSERT_TRUE(success);
        auto visitor = createVisitor();
        visitor->visitAndCondition((AndCondition*)(cond.get()));
        ASSERT_TRUE(visitor->getQuery() == NULL);
        ASSERT_TRUE(visitor->getAttributeExpression() == NULL);
        ASSERT_TRUE(visitor->isError());
    }
    {
        ConditionParser parser;
        auto visitor = createVisitor();
        autil_rapidjson::SimpleDocument simpleDoc;
        string condStr = R"({"op":"AND","params":[{"op":"=","params":["$attr1",0],"type":"OTHER"},{"op":"CASE","params":[{"op":"=","params":[{"op":"*","params":[{"op":"+","params":["$attr1",1],"type":"OTHER"},2],"type":"OTHER"},{"op":"CAST","cast_type":"BIGINT","params":[48],"type":"UDF"}],"type":"OTHER"},{"op":">","params":["$attr1",10],"type":"OTHER"},{"op":"=","params":[{"op":"*","params":[{"op":"+","params":["$attr1",1],"type":"OTHER"},2],"type":"OTHER"},{"op":"CAST","cast_type":"BIGINT","params":[24],"type":"UDF"}],"type":"OTHER"},{"op":">","params":["$id",10],"type":"OTHER"},{"op":"CAST","cast_type":"BOOLEAN","params":[false],"type":"UDF"}],"type":"OTHER"}], "type":"OTHER"})";
        ConditionPtr cond;
        bool success = parser.parseCondition(condStr, cond);
        ASSERT_TRUE(success);
        visitor->visitAndCondition((AndCondition*)(cond.get()));
        ASSERT_TRUE(visitor->getQuery() == NULL);
        ASSERT_TRUE(visitor->getAttributeExpression() != NULL);
        ASSERT_TRUE(!visitor->isError());
    }

}

TEST_F(Ha3ScanConditionVisitorTest, testVisitNotCondition) {
    { // not query
        ConditionParser parser;
        string condStr ="{\"op\":\"QUERY\", \"params\":[\"aa\"],\"type\":\"UDF\"}";
        string jsonStr = "{\"op\" : \"NOT\" , \"params\" : [" + condStr + "]}";
        ConditionPtr cond;
        bool success = parser.parseCondition(jsonStr, cond);
        ASSERT_TRUE(success);
        auto visitor = createVisitor();
        visitor->visitNotCondition((NotCondition *)(cond.get()));
        ASSERT_TRUE(visitor->getAttributeExpression() != NULL);
        EXPECT_EQ("not (TermQuery:[Term:[index_2||aa|100|]])",
                  visitor->getAttributeExpression()->getOriginalString());
        ASSERT_TRUE(!visitor->isError());
    }
    {// not expr
        ConditionParser parser;
        string condStr ="{\"op\":\"=\", \"params\":[\"$attr1\", 1]}";
        string jsonStr = "{\"op\" : \"NOT\" , \"params\" : [" + condStr + "]}";
        ConditionPtr cond;
        bool success = parser.parseCondition(jsonStr, cond);
        ASSERT_TRUE(success);
        auto visitor = createVisitor();
        visitor->visitNotCondition((NotCondition *)(cond.get()));
        ASSERT_TRUE(visitor->getAttributeExpression() != NULL);
        EXPECT_EQ("not ((attr1=1))",
                  visitor->getAttributeExpression()->getOriginalString());
        ASSERT_TRUE(!visitor->isError());
    }
    {// not query
        ConditionParser parser;
        string condStr ="{\"op\":\"=\", \"params\":[\"$attr2\", 1]}";
        string jsonStr = "{\"op\" : \"NOT\" , \"params\" : [" + condStr + "]}";
        ConditionPtr cond;
        bool success = parser.parseCondition(jsonStr, cond);
        ASSERT_TRUE(success);
        auto visitor = createVisitor();
        visitor->visitNotCondition((NotCondition *)(cond.get()));
        ASSERT_TRUE(visitor->getAttributeExpression() != NULL);
        EXPECT_EQ("not (NumberQuery:[NumberTerm: [attr2,[1, 1||1|100|]])",
                  visitor->getAttributeExpression()->getOriginalString());
        ASSERT_TRUE(!visitor->isError());
    }
    {//not expr
        ConditionParser parser;
        string condStr ="{\"op\":\"!=\", \"params\":[\"$attr2\", 1]}";
        string jsonStr = "{\"op\" : \"NOT\" , \"params\" : [" + condStr + "]}";
        ConditionPtr cond;
        bool success = parser.parseCondition(jsonStr, cond);
        ASSERT_TRUE(success);
        auto visitor = createVisitor();
        visitor->visitNotCondition((NotCondition *)(cond.get()));
        ASSERT_TRUE(visitor->getAttributeExpression() != NULL);
        EXPECT_EQ("not ((attr2!=1))",
                  visitor->getAttributeExpression()->getOriginalString());
        ASSERT_TRUE(!visitor->isError());
    }
    {// not expr
        ConditionParser parser;
        string condStr1 ="{\"op\":\"QUERY\", \"params\":[\"aa\"],\"type\":\"UDF\"}";
        string condStr2 ="{\"op\":\"!=\", \"params\":[\"$attr1\", 1]}";
        string condStr = "{\"op\" : \"OR\" , \"params\" : [" + condStr1 +","+condStr2 + "]}";
        string jsonStr = "{\"op\" : \"NOT\" , \"params\" : [" + condStr + "]}";
        ConditionPtr cond;
        bool success = parser.parseCondition(jsonStr, cond);
        ASSERT_TRUE(success);
        auto visitor = createVisitor();
        visitor->visitNotCondition((NotCondition *)(cond.get()));
        ASSERT_TRUE(visitor->getAttributeExpression() != NULL);
        EXPECT_EQ("not (((attr1!=1) OR TermQuery:[Term:[index_2||aa|100|]]))",
                  visitor->getAttributeExpression()->getOriginalString());
        ASSERT_TRUE(!visitor->isError());
    }
    {//not query and not expr
        ConditionParser parser;
        string condStr1 ="{\"op\":\"QUERY\", \"params\":[\"aa\"],\"type\":\"UDF\"}";
        string condStr2 ="{\"op\":\"!=\", \"params\":[\"$attr1\", 1]}";
        string condStr = "{\"op\" : \"AND\" , \"params\" : [" + condStr1 +","+condStr2 + "]}";
        string jsonStr = "{\"op\" : \"NOT\" , \"params\" : [" + condStr + "]}";
        ConditionPtr cond;
        bool success = parser.parseCondition(jsonStr, cond);
        ASSERT_TRUE(success);
        auto visitor = createVisitor();
        visitor->visitNotCondition((NotCondition *)(cond.get()));
        ASSERT_TRUE(visitor->getAttributeExpression() != NULL);
        EXPECT_EQ("not (TermQuery:[Term:[index_2||aa|100|]] AND (attr1!=1))",
                  visitor->getAttributeExpression()->getOriginalString());
        ASSERT_TRUE(!visitor->isError());
    }
    {
        //not of case expr
        ConditionParser parser;
        string condStr = R"({"op":"CASE","params":[{"op":"=","params":[{"op":"*","params":[{"op":"+","params":["$attr1",1],"type":"OTHER"},2],"type":"OTHER"},{"op":"CAST","cast_type":"BIGINT","params":[48],"type":"UDF"}],"type":"OTHER"},{"op":">","params":["$attr1",10],"type":"OTHER"},{"op":"=","params":[{"op":"*","params":[{"op":"+","params":["$attr1",1],"type":"OTHER"},2],"type":"OTHER"},{"op":"CAST","cast_type":"BIGINT","params":[24],"type":"UDF"}],"type":"OTHER"},{"op":">","params":["$id",10],"type":"OTHER"},{"op":"CAST","cast_type":"BOOLEAN","params":[false],"type":"UDF"}],"type":"OTHER"})";
        string jsonStr = "{\"op\" : \"NOT\" , \"params\" : [" + condStr + "]}";
        ConditionPtr cond;
        bool success = parser.parseCondition(jsonStr, cond);
        ASSERT_TRUE(success);
        auto visitor = createVisitor();
        visitor->visitNotCondition((NotCondition *)(cond.get()));
        ASSERT_TRUE(visitor->getAttributeExpression() != NULL);
        ASSERT_TRUE(!visitor->isError());
    }

}

TEST_F(Ha3ScanConditionVisitorTest, testVisitLeafCondition) {
    {
        auto visitor = createVisitor();
        autil_rapidjson::SimpleDocument simpleDoc;
        string condStr ="{\"op\":\"QUERY\", \"params\":[\"aa bb stop\"],\"type\":\"UDF\"}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        LeafCondition cond(simpleDoc);
        visitor->visitLeafCondition(&cond);
        ASSERT_TRUE(visitor->getQuery() != NULL);
        ASSERT_TRUE(visitor->getAttributeExpression() == NULL);
        ASSERT_TRUE(!visitor->isError());
    }
    {
        auto visitor = createVisitor();
        autil_rapidjson::SimpleDocument simpleDoc;
        string condStr ="{\"op\":\"!=\", \"params\":[\"$attr1\", 1]}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        LeafCondition cond(simpleDoc);
        visitor->visitLeafCondition(&cond);
        ASSERT_TRUE(visitor->getQuery() == NULL);
        ASSERT_TRUE(visitor->getAttributeExpression() != NULL);
        ASSERT_TRUE(!visitor->isError());
    }
    { // toquery error
        auto visitor = createVisitor();
        autil_rapidjson::SimpleDocument simpleDoc;
        string condStr ="{\"op\":\"QUERY\", \"params1\":[\"aa bb stop\"],\"type\":\"UDF\"}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        LeafCondition cond(simpleDoc);
        visitor->visitLeafCondition(&cond);
        ASSERT_TRUE(visitor->getQuery() == NULL);
        ASSERT_TRUE(visitor->getAttributeExpression() == NULL);
        ASSERT_TRUE(visitor->isError());
    }
    { // to expression error
        auto visitor = createVisitor();
        autil_rapidjson::SimpleDocument simpleDoc;
        string condStr ="{\"op\":\"!=\", \"params\":[\"attr\", 1]}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        LeafCondition cond(simpleDoc);
        visitor->visitLeafCondition(&cond);
        ASSERT_TRUE(visitor->getQuery() == NULL);
        ASSERT_TRUE(visitor->getAttributeExpression() == NULL);
        ASSERT_TRUE(visitor->isError());
    }
    {
        auto visitor = createVisitor();
        autil_rapidjson::SimpleDocument simpleDoc;
        string condStr = R"({"op":"CASE","params":[{"op":"=","params":[{"op":"*","params":[{"op":"+","params":["$attr1",1],"type":"OTHER"},2],"type":"OTHER"},{"op":"CAST","cast_type":"BIGINT","params":[48],"type":"UDF"}],"type":"OTHER"},{"op":">","params":["$attr1",10],"type":"OTHER"},{"op":"=","params":[{"op":"*","params":[{"op":"+","params":["$attr1",1],"type":"OTHER"},2],"type":"OTHER"},{"op":"CAST","cast_type":"BIGINT","params":[24],"type":"UDF"}],"type":"OTHER"},{"op":">","params":["$id",10],"type":"OTHER"},{"op":"CAST","cast_type":"BOOLEAN","params":[false],"type":"UDF"}],"type":"OTHER"})";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        LeafCondition cond(simpleDoc);
        visitor->visitLeafCondition(&cond);
        ASSERT_TRUE(visitor->getQuery() == NULL);
        ASSERT_TRUE(visitor->getAttributeExpression() != NULL);
        ASSERT_TRUE(!visitor->isError());
    }
    { // leaf condition for case op: bool, current supported
        ConditionParser parser;
        auto visitor = createVisitor();
        autil_rapidjson::SimpleDocument simpleDoc;
        string condStr = R"({"op":"CASE","params":[{"op":"=","params":[{"op":"*","params":[{"op":"+","params":["$attr1",1],"type":"OTHER"},2],"type":"OTHER"},{"op":"CAST","cast_type":"BIGINT","params":[48],"type":"UDF"}],"type":"OTHER"},{"op":">","params":["$attr1",10],"type":"OTHER"},{"op":"=","params":[{"op":"*","params":[{"op":"+","params":["$attr1",1],"type":"OTHER"},2],"type":"OTHER"},{"op":"CAST","cast_type":"BIGINT","params":[24],"type":"UDF"}],"type":"OTHER"},{"op":">","params":["$id",10],"type":"OTHER"},{"op":"CAST","cast_type":"BOOLEAN","params":[false],"type":"UDF"}],"type":"OTHER"})";
        ConditionPtr cond;
        bool success = parser.parseCondition(condStr, cond);
        ASSERT_TRUE(success);
        visitor->visitLeafCondition((LeafCondition*)(cond.get()));
        ASSERT_TRUE(visitor->getQuery() == NULL);
        const AttributeExpression* expr = visitor->getAttributeExpression();
        ASSERT_TRUE(expr != NULL);
        ASSERT_TRUE(expr->getType() == vt_bool);
        using ExprType = suez::turing::VariableTypeTraits<vt_bool, false>::AttrExprType;
        const CaseExpression<ExprType>* caseExpr = dynamic_cast<const CaseExpression<ExprType>* >(expr);
        ASSERT_TRUE(caseExpr != NULL);
        ASSERT_TRUE(!visitor->isError());
    }
    { // leaf condition for case op: not bool, current not supported
        ConditionParser parser;
        auto visitor = createVisitor();
        autil_rapidjson::SimpleDocument simpleDoc;
        string condStr = R"({"op":"CASE","params":[{"op":"=","params":[{"op":"*","params":[{"op":"+","params":["$attr1",1],"type":"OTHER"},2],"type":"OTHER"},{"op":"CAST","cast_type":"BIGINT","params":[48],"type":"UDF"}],"type":"OTHER"},"abc",{"op":"=","params":[{"op":"*","params":[{"op":"+","params":["$attr1",1],"type":"OTHER"},2],"type":"OTHER"},{"op":"CAST","cast_type":"BIGINT","params":[24],"type":"UDF"}],"type":"OTHER"},"def","ghi"],"type":"OTHER"})";
        ConditionPtr cond;
        bool success = parser.parseCondition(condStr, cond);
        ASSERT_TRUE(success);
        visitor->visitLeafCondition((LeafCondition*)(cond.get()));
        ASSERT_TRUE(visitor->getQuery() == NULL);
        const AttributeExpression* expr = visitor->getAttributeExpression();
        ASSERT_TRUE(expr == NULL);
    }

}

TEST_F(Ha3ScanConditionVisitorTest, testNullQuery) {
    {
        navi::NaviLoggerProvider provider("WARN");
        auto visitor = createVisitor();
        autil_rapidjson::SimpleDocument simpleDoc;
        string condStr = R"json({"op":"QUERY", "params":["index_2", "\"\"\"\"", "global_analyzer:fake_analyzer"], "type":"UDF"})json";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        QueryPtr query(visitor->toQuery(simpleDoc));
        ASSERT_TRUE(query == nullptr);
        auto traces = provider.getTrace("");
        CHECK_TRACE_COUNT(1, "validate query failed, error msg [Query is null]", traces);
        CHECK_TRACE_COUNT(1, "validate query failed: [\"\"\"\"]", traces);
    }
    {
        navi::NaviLoggerProvider provider("WARN");
        auto visitor = createVisitor();
        autil_rapidjson::SimpleDocument simpleDoc;
        string condStr = R"json({"op":"MATCHINDEX", "params":["index_2", "\"\"\"\"", "global_analyzer:fake_analyzer"], "type":"UDF"})json";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        QueryPtr query(visitor->toMatchQuery(simpleDoc));
        ASSERT_TRUE(query == nullptr);
        auto traces = provider.getTrace("");
        CHECK_TRACE_COUNT(1, "validate query failed: [\"\"\"\"]", traces);
    }
}

TEST_F(Ha3ScanConditionVisitorTest, testToQuery) {
    {// 1 param , use default index_2
        auto visitor = createVisitor();
        autil_rapidjson::SimpleDocument simpleDoc;
        string condStr ="{\"op\":\"QUERY\", \"params\":[\"aa bb stop\"],\"type\":\"UDF\"}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        QueryPtr query(visitor->toQuery(simpleDoc));
        ASSERT_TRUE(query != NULL);
        ASSERT_EQ("AndQuery", query->getQueryName());
        ASSERT_EQ(2, query->getChildQuery()->size());
        ASSERT_EQ("index_2", ((TermQuery*)((*query->getChildQuery())[0].get()))->getTerm().getIndexName());
    }
    {// 2 param, index:name needn't token
        auto visitor = createVisitor();
        autil_rapidjson::SimpleDocument simpleDoc;
        string condStr ="{\"op\":\"QUERY\", \"params\":[\"name\", \"aa bb\"],\"type\":\"UDF\"}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        QueryPtr query(visitor->toQuery(simpleDoc));
        ASSERT_TRUE(query != NULL);
        ASSERT_EQ("AndQuery", query->getQueryName());
        ASSERT_EQ(2, query->getChildQuery()->size());
        ASSERT_EQ("name", ((TermQuery*)((*query->getChildQuery())[0].get()))->getTerm().getIndexName());
    }
    {// 2 param, use or
        auto visitor = createVisitor();
        autil_rapidjson::SimpleDocument simpleDoc;
        string condStr ="{\"op\":\"QUERY\", \"params\":[\"name\", \"aa bb\", \"default_op:or\"],\"type\":\"UDF\"}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        QueryPtr query(visitor->toQuery(simpleDoc));
        ASSERT_TRUE(query != NULL);
        ASSERT_EQ("OrQuery", query->getQueryName());
        ASSERT_EQ(2, query->getChildQuery()->size());
        ASSERT_EQ("name", ((TermQuery*)((*query->getChildQuery())[0].get()))->getTerm().getIndexName());
    }
    {// 3 param, no token
        auto visitor = createVisitor();
        autil_rapidjson::SimpleDocument simpleDoc;
        string condStr ="{\"op\":\"QUERY\", \"params\":[ \"index_2\", \"aa bb stop\", \"tokenize_query:false\"],\"type\":\"UDF\"}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        QueryPtr query(visitor->toQuery(simpleDoc));
        ASSERT_TRUE(query != NULL);
        ASSERT_EQ("AndQuery", query->getQueryName());
        ASSERT_EQ(2, query->getChildQuery()->size());
        ASSERT_EQ("index_2", ((TermQuery*)((*query->getChildQuery())[0].get()))->getTerm().getIndexName());
    }
    {// 3 param, no token
        auto visitor = createVisitor();
        autil_rapidjson::SimpleDocument simpleDoc;
        string condStr ="{\"op\":\"QUERY\", \"params\":[\"index_2\", \"aa bb stop\", \"remove_stopwords:false\"],\"type\":\"UDF\"}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        QueryPtr query(visitor->toQuery(simpleDoc));
        ASSERT_TRUE(query != NULL);
        ASSERT_EQ(2, query->getChildQuery()->size());
    }
    {// 4 have column prefix
        auto visitor = createVisitor();
        autil_rapidjson::SimpleDocument simpleDoc;
        string condStr ="{\"op\":\"QUERY\", \"params\":[\"$index_2\", \"aa bb stop\", \"remove_stopwords:false\"],\"type\":\"UDF\"}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        QueryPtr query(visitor->toQuery(simpleDoc));
        ASSERT_TRUE(query != NULL);
        ASSERT_EQ(2, query->getChildQuery()->size());
    }
    { //condition is not object
        auto visitor = createVisitor();
        autil_rapidjson::SimpleDocument simpleDoc;
        string condStr ="[]";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        QueryPtr query(visitor->toQuery(simpleDoc));
        ASSERT_TRUE(query == NULL);
    }
    { //condition op name error
        auto visitor = createVisitor();
        autil_rapidjson::SimpleDocument simpleDoc;
        string condStr ="{\"op1\":\"QUERY\", \"params\":[\"$name\", 1],\"type\":\"UDF\"}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        QueryPtr query(visitor->toQuery(simpleDoc));
        ASSERT_TRUE(query == NULL);
    }
    { //condition param name error
        auto visitor = createVisitor();
        autil_rapidjson::SimpleDocument simpleDoc;
        string condStr ="{\"op\":\"QUERY\", \"params11\":[\"$name\", 1],\"type\":\"UDF\"}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        QueryPtr query(visitor->toQuery(simpleDoc));
        ASSERT_TRUE(query == NULL);
    }
    { //operator value error
        auto visitor = createVisitor();
        autil_rapidjson::SimpleDocument simpleDoc;
        string condStr ="{\"op\":\"!=\", \"params\":[\"$name\", 1]}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        QueryPtr query(visitor->toTermQuery(simpleDoc));
        ASSERT_TRUE(query == NULL);
    }
    { //params size not equal 0
        auto visitor = createVisitor();
        autil_rapidjson::SimpleDocument simpleDoc;
        string condStr ="{\"op\":\"QUERY\", \"params\":[]}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        QueryPtr query(visitor->toTermQuery(simpleDoc));
        ASSERT_TRUE(query == NULL);
    }
}

TEST_F(Ha3ScanConditionVisitorTest, testToMatchQuery) {
    {// 1 param
        auto visitor = createVisitor();
        autil_rapidjson::SimpleDocument simpleDoc;
        string condStr ="{\"op\":\"MATCHINDEX\", \"params\":[\"aa bb stop\"],\"type\":\"UDF\"}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        QueryPtr query(visitor->toMatchQuery(simpleDoc));
        ASSERT_TRUE(query != NULL);
        ASSERT_EQ("AndQuery", query->getQueryName());
        ASSERT_EQ(2, query->getChildQuery()->size());
        ASSERT_EQ("index_2", ((TermQuery*)((*query->getChildQuery())[0].get()))->getTerm().getIndexName());
    }
    {// 2 param, index:name needn't token
        auto visitor = createVisitor();
        autil_rapidjson::SimpleDocument simpleDoc;
        string condStr ="{\"op\":\"MATCHINDEX\", \"params\":[\"name\", \"aa bb stop\"],\"type\":\"UDF\"}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        QueryPtr query(visitor->toMatchQuery(simpleDoc));
        ASSERT_TRUE(query != NULL);
        ASSERT_EQ("TermQuery", query->getQueryName());
        ASSERT_EQ("name", ((TermQuery*)(query.get()))->getTerm().getIndexName());
    }
    {// 3 param, no token
        auto visitor = createVisitor();
        autil_rapidjson::SimpleDocument simpleDoc;
        string condStr ="{\"op\":\"MATCHINDEX\", \"params\":[\"index_2\", \"aa bb stop\", \"tokenize_query:false\"],\"type\":\"UDF\"}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        QueryPtr query(visitor->toMatchQuery(simpleDoc));
        ASSERT_TRUE(query != NULL);
        ASSERT_EQ("TermQuery", query->getQueryName());
        ASSERT_EQ("index_2", ((TermQuery*)(query.get()))->getTerm().getIndexName());
    }
    {// 3 param, no token
        auto visitor = createVisitor();
        autil_rapidjson::SimpleDocument simpleDoc;
        string condStr ="{\"op\":\"MATCHINDEX\", \"params\":[\"index_2\", \"aa bb stop\", \"remove_stopwords:false\"],\"type\":\"UDF\"}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        QueryPtr query(visitor->toMatchQuery(simpleDoc));
        ASSERT_TRUE(query != NULL);
        ASSERT_EQ(3, query->getChildQuery()->size());
    }
    { //condition is not object
        auto visitor = createVisitor();
        autil_rapidjson::SimpleDocument simpleDoc;
        string condStr ="[]";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        QueryPtr query(visitor->toMatchQuery(simpleDoc));
        ASSERT_TRUE(query == NULL);
    }
    { //condition op name error
        auto visitor = createVisitor();
        autil_rapidjson::SimpleDocument simpleDoc;
        string condStr ="{\"op1\":\"MATCHINDEX\", \"params\":[\"name\", \"1\"],\"type\":\"UDF\"}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        QueryPtr query(visitor->toMatchQuery(simpleDoc));
        ASSERT_TRUE(query == NULL);
    }
    { //condition param name error
        auto visitor = createVisitor();
        autil_rapidjson::SimpleDocument simpleDoc;
        string condStr ="{\"op\":\"MATCHINDEX\", \"params11\":[\"name\", \"1\"],\"type\":\"UDF\"}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        QueryPtr query(visitor->toMatchQuery(simpleDoc));
        ASSERT_TRUE(query == NULL);
    }
    { //operator value error
        auto visitor = createVisitor();
        autil_rapidjson::SimpleDocument simpleDoc;
        string condStr ="{\"op\":\"!=\", \"params\":[\"$name\", \"1\"]}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        QueryPtr query(visitor->toTermQuery(simpleDoc));
        ASSERT_TRUE(query == NULL);
    }
    { //params size not equal 0
        auto visitor = createVisitor();
        autil_rapidjson::SimpleDocument simpleDoc;
        string condStr ="{\"op\":\"MATCHINDEX\", \"params\":[]}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        QueryPtr query(visitor->toTermQuery(simpleDoc));
        ASSERT_TRUE(query == NULL);
    }
    {// have column prefix
        auto visitor = createVisitor();
        autil_rapidjson::SimpleDocument simpleDoc;
        string condStr ="{\"op\":\"MATCHINDEX\", \"params\":[\"$index_2\", \"aa bb stop\", \"remove_stopwords:false\"],\"type\":\"UDF\"}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        QueryPtr query(visitor->toMatchQuery(simpleDoc));
        ASSERT_TRUE(query != NULL);
        ASSERT_EQ(3, query->getChildQuery()->size());
    }

}

TEST_F(Ha3ScanConditionVisitorTest, testudfToQuery) {
    {// name index
        auto visitor = createVisitor();
        autil_rapidjson::SimpleDocument simpleDoc;
        string condStr ="{\"op\":\"contain\", \"params\":[\"$name\", \"aa|bb\"],\"type\":\"UDF\"}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        QueryPtr query(visitor->udfToQuery(simpleDoc));
        ASSERT_TRUE(query != NULL);
        ASSERT_EQ("MultiTermQuery", query->getQueryName());
        MultiTermQuery *multiQuery =  dynamic_cast<MultiTermQuery*>(query.get());
        ASSERT_TRUE(multiQuery != NULL);
        ASSERT_EQ(2, multiQuery->getTermArray().size());
    }
    {// name index with sep str
        auto visitor = createVisitor();
        autil_rapidjson::SimpleDocument simpleDoc;
        string condStr ="{\"op\":\"contain\", \"params\":[\"$name\", \"aa#bb\", \"#\"],\"type\":\"UDF\"}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        QueryPtr query(visitor->udfToQuery(simpleDoc));
        ASSERT_TRUE(query != NULL);
        ASSERT_EQ("MultiTermQuery", query->getQueryName());
        MultiTermQuery *multiQuery =  dynamic_cast<MultiTermQuery*>(query.get());
        ASSERT_TRUE(multiQuery != NULL);
        ASSERT_EQ(2, multiQuery->getTermArray().size());
    }
    {// number index
        auto visitor = createVisitor();
        autil_rapidjson::SimpleDocument simpleDoc;
        string condStr ="{\"op\":\"contain\", \"params\":[\"$attr2\", \"1|2\"],\"type\":\"UDF\"}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        QueryPtr query(visitor->udfToQuery(simpleDoc));
        ASSERT_TRUE(query != NULL);
        ASSERT_EQ("MultiTermQuery", query->getQueryName());
        MultiTermQuery *multiQuery =  dynamic_cast<MultiTermQuery*>(query.get());
        ASSERT_TRUE(multiQuery != NULL);
        ASSERT_EQ(2, multiQuery->getTermArray().size());
    }
    {// number index has error
        auto visitor = createVisitor();
        autil_rapidjson::SimpleDocument simpleDoc;
        string condStr ="{\"op\":\"contain\", \"params\":[\"$attr2\", \"1|a2\"],\"type\":\"UDF\"}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        QueryPtr query(visitor->udfToQuery(simpleDoc));
        ASSERT_TRUE(query == NULL);
    }
    {// no index
        auto visitor = createVisitor();
        autil_rapidjson::SimpleDocument simpleDoc;
        string condStr ="{\"op\":\"contain\", \"params\":[\"$attr1\", \"1|2\"],\"type\":\"UDF\"}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        QueryPtr query(visitor->udfToQuery(simpleDoc));
        ASSERT_TRUE(query == NULL);
    }
    {// name index
        auto visitor = createVisitor();
        autil_rapidjson::SimpleDocument simpleDoc;
        string condStr ="{\"op\":\"ha_in\", \"params\":[\"$name\", \"aa|bb\"],\"type\":\"UDF\"}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        QueryPtr query(visitor->udfToQuery(simpleDoc));
        ASSERT_TRUE(query != NULL);
        ASSERT_EQ("MultiTermQuery", query->getQueryName());
        MultiTermQuery *multiQuery =  dynamic_cast<MultiTermQuery*>(query.get());
        ASSERT_TRUE(multiQuery != NULL);
        ASSERT_EQ(2, multiQuery->getTermArray().size());
    }
    {// ha_in name index with sep
        auto visitor = createVisitor();
        autil_rapidjson::SimpleDocument simpleDoc;
        string condStr ="{\"op\":\"ha_in\", \"params\":[\"$name\", \"aa#bb\", \"#\"],\"type\":\"UDF\"}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        QueryPtr query(visitor->udfToQuery(simpleDoc));
        ASSERT_TRUE(query != NULL);
        ASSERT_EQ("MultiTermQuery", query->getQueryName());
        MultiTermQuery *multiQuery =  dynamic_cast<MultiTermQuery*>(query.get());
        ASSERT_TRUE(multiQuery != NULL);
        ASSERT_EQ(2, multiQuery->getTermArray().size());
    }

    {// number index
        auto visitor = createVisitor();
        autil_rapidjson::SimpleDocument simpleDoc;
        string condStr ="{\"op\":\"ha_in\", \"params\":[\"$attr2\", \"1|2\"],\"type\":\"UDF\"}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        QueryPtr query(visitor->udfToQuery(simpleDoc));
        ASSERT_TRUE(query != NULL);
        ASSERT_EQ("MultiTermQuery", query->getQueryName());
        MultiTermQuery *multiQuery =  dynamic_cast<MultiTermQuery*>(query.get());
        ASSERT_TRUE(multiQuery != NULL);
        ASSERT_EQ(2, multiQuery->getTermArray().size());
    }
    {// number index has error
        auto visitor = createVisitor();
        autil_rapidjson::SimpleDocument simpleDoc;
        string condStr ="{\"op\":\"ha_in\", \"params\":[\"$attr2\", \"1|a2\"],\"type\":\"UDF\"}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        QueryPtr query(visitor->udfToQuery(simpleDoc));
        ASSERT_TRUE(query == NULL);
    }
    {// no index
        auto visitor = createVisitor();
        autil_rapidjson::SimpleDocument simpleDoc;
        string condStr ="{\"op\":\"ha_in\", \"params\":[\"$attr1\", \"1|2\"],\"type\":\"UDF\"}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        QueryPtr query(visitor->udfToQuery(simpleDoc));
        ASSERT_TRUE(query == NULL);
    }

}

TEST_F(Ha3ScanConditionVisitorTest, testSpToQuery) {
    {//valid sp query
        auto visitor = createVisitor();
        autil_rapidjson::SimpleDocument simpleDoc;
        string condStr ="{\"op\":\"sp_query_match\", \"params\":[\"(+(attr2:1|name:test)+index_2:index)\"],\"type\":\"UDF\"}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        QueryPtr query(visitor->spToQuery(simpleDoc));
        ASSERT_TRUE(query != NULL);
    }
    {//invalid sp query
        auto visitor = createVisitor();
        autil_rapidjson::SimpleDocument simpleDoc;
        string condStr ="{\"op\":\"sp_query_match\", \"params\":[\"123456\"],\"type\":\"UDF\"}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        QueryPtr query(visitor->spToQuery(simpleDoc));
        ASSERT_TRUE(query == NULL);
    }
}

TEST_F(Ha3ScanConditionVisitorTest, testToTermQuery) {
    { //index not exist
        auto visitor = createVisitor();
        autil_rapidjson::SimpleDocument simpleDoc;
        string condStr ="{\"op\":\"=\", \"params\":[\"$index_no_exist\", 1]}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        QueryPtr query(visitor->toTermQuery(simpleDoc));
        ASSERT_TRUE(query == NULL);
    }
    { //number query, string index
        auto visitor = createVisitor();
        autil_rapidjson::SimpleDocument simpleDoc;
        string condStr ="{\"op\":\"=\", \"params\":[\"$name\", 1]}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        QueryPtr query(visitor->toTermQuery(simpleDoc));
        ASSERT_TRUE(query != NULL);
        ASSERT_EQ("TermQuery", query->getQueryName());
    }
    { //number query
        auto visitor = createVisitor();
        autil_rapidjson::SimpleDocument simpleDoc;
        string condStr ="{\"op\":\"=\", \"params\":[\"$attr2\", 1]}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        QueryPtr query(visitor->toTermQuery(simpleDoc));
        ASSERT_TRUE(query != NULL);
        ASSERT_EQ("NumberQuery", query->getQueryName());
    }

    {//term query
        auto visitor = createVisitor();
        autil_rapidjson::SimpleDocument simpleDoc;
        string condStr ="{\"op\":\"LIKE\", \"params\":[\"$name\", \"name\"]}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        QueryPtr query(visitor->toTermQuery(simpleDoc));
        ASSERT_TRUE(query != NULL);
        ASSERT_EQ("TermQuery", query->getQueryName());
    }
    { //condition is not object
        auto visitor = createVisitor();
        autil_rapidjson::SimpleDocument simpleDoc;
        string condStr ="[]";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        QueryPtr query(visitor->toTermQuery(simpleDoc));
        ASSERT_TRUE(query == NULL);
    }
    { //condition op name error
        auto visitor = createVisitor();
        autil_rapidjson::SimpleDocument simpleDoc;
        string condStr ="{\"op1\":\"=\", \"params\":[\"$name\", 1]}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        QueryPtr query(visitor->toTermQuery(simpleDoc));
        ASSERT_TRUE(query == NULL);
    }
    { //condition param name error
        auto visitor = createVisitor();
        autil_rapidjson::SimpleDocument simpleDoc;
        string condStr ="{\"op\":\"=\", \"params11\":[\"$name\", 1]}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        QueryPtr query(visitor->toTermQuery(simpleDoc));
        ASSERT_TRUE(query == NULL);
    }
    { //operator value error
        auto visitor = createVisitor();
        autil_rapidjson::SimpleDocument simpleDoc;
        string condStr ="{\"op\":\"!=\", \"params\":[\"$name\", 1]}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        QueryPtr query(visitor->toTermQuery(simpleDoc));
        ASSERT_TRUE(query == NULL);
    }
    { //params size not equal 2
        auto visitor = createVisitor();
        autil_rapidjson::SimpleDocument simpleDoc;
        string condStr ="{\"op\":\"=\", \"params\":[\"$name\", 1, 1]}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        QueryPtr query(visitor->toTermQuery(simpleDoc));
        ASSERT_TRUE(query == NULL);
    }
    { //params no index name
        auto visitor = createVisitor();
        autil_rapidjson::SimpleDocument simpleDoc;
        string condStr ="{\"op\":\"=\", \"params\":[\"name\", 1]}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        QueryPtr query(visitor->toTermQuery(simpleDoc));
        ASSERT_TRUE(query == NULL);
    }
    { //params both index name
        auto visitor = createVisitor();
        autil_rapidjson::SimpleDocument simpleDoc;
        string condStr ="{\"op\":\"=\", \"params\":[\"$name\", \"$name\"]}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        QueryPtr query(visitor->toTermQuery(simpleDoc));
        ASSERT_TRUE(query == NULL);
    }
    { //has cast udf
        auto visitor = createVisitor();
        autil_rapidjson::SimpleDocument simpleDoc;
        string castStr = "{\"op\":\"CAST\",\"cast_type\":\"integer\",\"type\":\"UDF\",\"params\":[\"$name\"]}";
        string condStr ="{\"op\":\"=\", \"params\":["+ castStr +", 1]}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        QueryPtr query(visitor->toTermQuery(simpleDoc));
        ASSERT_TRUE(query != NULL);
        ASSERT_EQ("TermQuery", query->getQueryName());
    }
    { //has cast udf
        auto visitor = createVisitor();
        autil_rapidjson::SimpleDocument simpleDoc;
        string castStr = "{\"op\":\"CAST\",\"cast_type\":\"integer\",\"type\":\"UDF\",\"params\":[\"$attr2\"]}";
        string condStr ="{\"op\":\"=\", \"params\":["+ castStr +", 1]}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        QueryPtr query(visitor->toTermQuery(simpleDoc));
        ASSERT_TRUE(query != NULL);
        ASSERT_EQ("NumberQuery", query->getQueryName());
    }

    { //has cast udf
        auto visitor = createVisitor();
        autil_rapidjson::SimpleDocument simpleDoc;
        string castStr = "{\"op\":\"CAST\",\"cast_type\":\"integer\",\"type\":\"UDF\",\"params\":[\"$name\"]}";
        string condStr ="{\"op\":\"=\", \"params\":[1," + castStr +"]}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        QueryPtr query(visitor->toTermQuery(simpleDoc));
        ASSERT_TRUE(query != NULL);
        ASSERT_EQ("TermQuery", query->getQueryName());
    }
    { //param has outher udf
        auto visitor = createVisitor();
        autil_rapidjson::SimpleDocument simpleDoc;
        string castStr = "{\"op\":\"CAST1\",\"cast_type\":\"integer\",\"type\":\"UDF\",\"params\":[\"$name\"]}";
        string condStr ="{\"op\":\"=\", \"params\":[1," + castStr +"]}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        QueryPtr query(visitor->toTermQuery(simpleDoc));
        ASSERT_TRUE(query == NULL);
    }

}

TEST_F(Ha3ScanConditionVisitorTest, testTokenizeQuery) {
    auto visitor = createVisitor();
    Term term;
    term.setIndexName("index_2");
    term.setWord("a stop");
    QueryPtr query(new TermQuery(term, ""));
    set<string> noTokenIndexs;
    string analyzerName;
    map<string, string> indexAnalyzerName;
    Query *newQuery = visitor->tokenizeQuery(query.get(), _queryInfo.getDefaultOperator(), noTokenIndexs,
            analyzerName, indexAnalyzerName);
    ASSERT_TRUE(query != NULL);
    ASSERT_TRUE(newQuery != NULL);
    ASSERT_EQ("AndQuery", newQuery->getQueryName());
    DELETE_AND_SET_NULL(newQuery);
}

TEST_F(Ha3ScanConditionVisitorTest, testCleanStopWords) {
    { // is stopword
        build_service::analyzer::Token token("aa", 0);
        token.setIsStopWord(true);
        Term term(token, "name", RequiredFields());
        QueryPtr query(new TermQuery(term, ""));
        Query *newQuery = Ha3ScanConditionVisitor::cleanStopWords(query.get());
        ASSERT_TRUE(query != NULL);
        ASSERT_TRUE(newQuery == NULL);
    }
    {
        build_service::analyzer::Token token("aa", 0);
        Term term(token, "name", RequiredFields());
        QueryPtr query(new TermQuery(term, ""));
        Query *newQuery = Ha3ScanConditionVisitor::cleanStopWords(query.get());
        ASSERT_TRUE(query != NULL);
        ASSERT_TRUE(newQuery != NULL);
        DELETE_AND_SET_NULL(newQuery);
    }
}

TEST_F(Ha3ScanConditionVisitorTest, testCreateQueryExecutor) {
    auto indexReaderWrapper = getIndexPartitionReaderWrapper();
    auto layerMeta = ScanIteratorCreator::createLayerMeta(indexReaderWrapper, &_pool);
    { // query is empty
        QueryExecutor *executor = Ha3ScanConditionVisitor::createQueryExecutor(NULL,
                indexReaderWrapper, _tableName, &_pool, NULL, layerMeta.get());
        ASSERT_TRUE(executor == NULL);
    }
    { // term not exist
        Term term;
        term.setIndexName("not_exist");
        term.setWord("not_exist");
        QueryPtr query(new TermQuery(term, ""));
        QueryExecutor *executor = Ha3ScanConditionVisitor::createQueryExecutor(query.get(),
                indexReaderWrapper, _tableName, &_pool, NULL, layerMeta.get());
        ASSERT_TRUE(executor == NULL);
    }
    {
        Term term;
        term.setIndexName("name");
        term.setWord("aa");
        QueryPtr query(new TermQuery(term, ""));
        QueryExecutor *executor = Ha3ScanConditionVisitor::createQueryExecutor(query.get(),
                indexReaderWrapper, _tableName, &_pool, NULL, layerMeta.get());
        QueryExecutorPtr executorPtr(executor,
                [](QueryExecutor *p) {
                    POOL_DELETE_CLASS(p);
                });
        ASSERT_TRUE(executorPtr);
        ASSERT_EQ("BufferedTermQueryExecutor", executorPtr->getName());
    }
}

TEST_F(Ha3ScanConditionVisitorTest, testIsQueryOrMatchUdf) {
    {
        autil_rapidjson::SimpleDocument simpleDoc;
        string condStr ="{\"op\":\"QUERY\", \"params\":[\"name\", \"aa OR bb\"], \"type\":\"UDF\"}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        ASSERT_TRUE(Ha3ScanConditionVisitor::isQueryUdf(simpleDoc));
        ASSERT_FALSE(Ha3ScanConditionVisitor::isMatchUdf(simpleDoc));
    }
    {
        autil_rapidjson::SimpleDocument simpleDoc;
        string condStr ="{\"op\":\"MATCHINDEX\", \"params\":[\"name\", \"aa OR bb\"], \"type\":\"UDF\"}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        ASSERT_FALSE(Ha3ScanConditionVisitor::isQueryUdf(simpleDoc));
        ASSERT_TRUE(Ha3ScanConditionVisitor::isMatchUdf(simpleDoc));
    }
    {
        autil_rapidjson::SimpleDocument simpleDoc;
        string condStr ="{\"op\":\"AAA\", \"params\":[\"aa OR bb\", \"name\"], \"type\":\"UDF\"}";
        simpleDoc.Parse(condStr.c_str());
        ASSERT_FALSE(simpleDoc.HasParseError());
        ASSERT_FALSE(Ha3ScanConditionVisitor::isQueryUdf(simpleDoc));
        ASSERT_FALSE(Ha3ScanConditionVisitor::isMatchUdf(simpleDoc));
    }
}

TEST_F(Ha3ScanConditionVisitorTest, testParseKvPairInfo) {
    {
        string parseStr = "remove_stopwords:false,tokenize_query:false,default_op:and,no_token_indexes:a;b;c,global_analyzer:xxx,other:other1,specific_index_analyzer:default#extend_analyzer;index2#aliws";
        string globalAnalyzer;
        string defaultOpStr;
        set<string> noTokenIndex;
        map<string, string> indexAnalyzerMap;
        bool tokenQuery = true;
        bool removeWords = true;
        Ha3ScanConditionVisitor::parseKvPairInfo(parseStr, indexAnalyzerMap, globalAnalyzer,
                defaultOpStr, noTokenIndex, tokenQuery, removeWords);
        ASSERT_EQ("xxx", globalAnalyzer);
        ASSERT_EQ("and", defaultOpStr);
        ASSERT_FALSE(tokenQuery);
        ASSERT_FALSE(removeWords);
        ASSERT_THAT(noTokenIndex, ElementsAre("a", "b", "c"));
        ASSERT_EQ(2, indexAnalyzerMap.size());
        ASSERT_EQ("extend_analyzer", indexAnalyzerMap["default"]);
        ASSERT_EQ("aliws", indexAnalyzerMap["index2"]);
    }
}

TEST_F(Ha3ScanConditionVisitorTest, testConditionWithSubAttr) {
    // check isSubExpression flag

    {
        ConditionParser parser;
        string subCondition = R"json({"op":">", "params":["$sub_id", 2]})json"; //query
        string jsonStr = "{\"op\":\"AND\", \"params\":[" + subCondition + ", " + subCondition + "]}";

        ConditionPtr cond;
        bool success = parser.parseCondition(jsonStr, cond);
        ASSERT_TRUE(success);
        auto visitor = createVisitor();
        ASSERT_NE(nullptr, cond);
        visitor->visitAndCondition((AndCondition *)cond.get());
        ASSERT_FALSE(visitor->isError());
        ASSERT_NE(nullptr, visitor->_attrExpr);
        ASSERT_TRUE(visitor->_attrExpr->isSubExpression());
    }
    {
        ConditionParser parser;
        string subCondition = R"json({"op":">", "params":["$sub_id", 2]})json"; //query
        string jsonStr = "{\"op\":\"OR\", \"params\":[" + subCondition + ", " + subCondition + "]}";

        ConditionPtr cond;
        bool success = parser.parseCondition(jsonStr, cond);
        ASSERT_TRUE(success);
        auto visitor = createVisitor();
        ASSERT_NE(nullptr, cond);
        visitor->visitOrCondition((OrCondition *)cond.get());
        ASSERT_FALSE(visitor->isError());
        ASSERT_NE(nullptr, visitor->_attrExpr);
        ASSERT_TRUE(visitor->_attrExpr->isSubExpression());
    }
    {
        ConditionParser parser;
        string subCondition = R"json({"op":">", "params":["$sub_id", 2]})json"; //query
        string jsonStr = "{\"op\":\"NOT\", \"params\":[" + subCondition + "]}";

        ConditionPtr cond;
        bool success = parser.parseCondition(jsonStr, cond);
        ASSERT_TRUE(success);
        auto visitor = createVisitor();
        ASSERT_NE(nullptr, cond);
        visitor->visitNotCondition((NotCondition *)cond.get());
        ASSERT_FALSE(visitor->isError());
        ASSERT_NE(nullptr, visitor->_attrExpr);
        ASSERT_TRUE(visitor->_attrExpr->isSubExpression());
    }
}

END_HA3_NAMESPACE();
