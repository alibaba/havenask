#include <ha3/sql/ops/test/OpTestBase.h>
#include <ha3/test/test.h>
#include <ha3/sql/ops/scan/ScanKernel.h>
#include <ha3/turing/ops/SqlSessionResource.h>
#include <build_service/analyzer/AnalyzerFactory.h>
#include <build_service/analyzer/AnalyzerInfos.h>
#include <build_service/analyzer/TokenizerManager.h>
#include <ha3/sql/ops/scan/test/FakeTokenizer.h>

using namespace std;
using namespace suez::turing;
using namespace suez;
using namespace build_service::analyzer;
using namespace navi;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;

BEGIN_HA3_NAMESPACE(sql);

class ScanKernelTest : public OpTestBase {
public:
    ScanKernelTest();
    ~ScanKernelTest();
public:
    void setUp() override {
        _needBuildIndex = true;
        _needExprResource = true;
    }
    void tearDown() override {
    }
private:
    void prepareAttributeMap() {
        _attributeMap.clear();
        _attributeMap["table_type"] = string("normal");
        _attributeMap["table_name"] = _tableName;
        _attributeMap["db_name"] = string("default");
        _attributeMap["catalog_name"] = string("default");
        _attributeMap["hash_fields"] = ParseJson(string(R"json(["id"])json"));
        _attributeMap["output_fields"] = ParseJson(string(R"json(["$attr1", "$attr2", "$id"])json"));
        _attributeMap["use_nest_table"] = Any(false);
        _attributeMap["limit"] = Any(1000);
        _attributeMap["hash_fields"] = ParseJson(R"json(["id"])json");
    }

    void setResource(KernelTesterBuilder &testerBuilder) {
        _sqlResource->analyzerFactory = initAnalyzerFactory();
        _sqlResource->queryInfo.setDefaultIndexName("index_2");
        for (auto &resource : _resourceMap) {
            testerBuilder.resource(resource.first, resource.second);
        }
    }

    KernelTesterPtr buildTester(KernelTesterBuilder &builder) {

        setResource(builder);
        builder.kernel("ScanKernel");
        builder.output("output0");
        builder.attrs(autil::legacy::ToJsonString(_attributeMap));
        return KernelTesterPtr(builder.build());
    }

    void getOutputTable(KernelTester &tester, TablePtr &outputTable) {
        DataPtr outputData;
        bool eof = false;
        ASSERT_TRUE(tester.getOutput("output0", outputData, eof));
        ASSERT_TRUE(outputData != nullptr);
        outputTable = getTable(outputData);
        ASSERT_TRUE(outputTable != nullptr);
    }

    void runKernel(TablePtr &outputTable) {
        KernelTesterBuilder builder;
        auto testerPtr = buildTester(builder);
        ASSERT_TRUE(testerPtr.get());
        auto &tester = *testerPtr;
        ASSERT_TRUE(tester.compute()); // kernel compute success
        getOutputTable(tester, outputTable);
    }
    void prepareUserIndex() override {
        prepareInvertedTable();
    }
    void prepareInvertedTable() override {
        { // table 1
            string tableName = _tableName;
            std::string testPath = _testPath + tableName;
            auto indexPartition = makeIndexPartition(testPath, tableName);
            std::string schemaName = indexPartition->GetSchema()->GetSchemaName();
            _bizInfo->_itemTableName = schemaName;
            _indexPartitionMap.insert(make_pair(schemaName, indexPartition));
        }
        { // table 2
            string tableName = _tableName + "_2";
            std::string testPath = _testPath + tableName;
            auto indexPartition = makeIndexPartition(testPath, tableName);
            std::string schemaName = indexPartition->GetSchema()->GetSchemaName();
            _indexPartitionMap.insert(make_pair(schemaName, indexPartition));
        }
    }

private:
    AnalyzerFactoryPtr initAnalyzerFactory();
    IE_NAMESPACE(partition)::IndexPartitionPtr makeIndexPartition(const std::string &rootPath,
            const std::string &tableName);
    static std::string fakeAnalyzerName;

private:
    autil::legacy::json::JsonMap _attributeMap;
};

std::string ScanKernelTest::fakeAnalyzerName = "fake_analyzer";

ScanKernelTest::ScanKernelTest() {
}

ScanKernelTest::~ScanKernelTest() {
}

AnalyzerFactoryPtr ScanKernelTest::initAnalyzerFactory() {
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

IE_NAMESPACE(partition)::IndexPartitionPtr ScanKernelTest::makeIndexPartition(const std::string &rootPath,
            const std::string &tableName)
    {
        int64_t ttl = std::numeric_limits<int64_t>::max();
        auto mainSchema = IE_NAMESPACE(testlib)::IndexlibPartitionCreator::CreateSchema(
                tableName,
                "attr1:uint32;attr2:int32:true;id:int64;name:string;index2:text", //fields
                "id:primarykey64:id;index_2:text:index2;name:string:name", //fields
                "attr1;attr2;id", //attributes
                "name", // summary
                "");// truncateProfile

        auto subSchema = IE_NAMESPACE(testlib)::IndexlibPartitionCreator::CreateSchema(
                "sub_" + tableName,
                "sub_id:int64;sub_attr1:int32;sub_index2:string", // fields
                "sub_id:primarykey64:sub_id;sub_index_2:string:sub_index2", //indexs
                "sub_attr1;sub_id;sub_index2", //attributes
                "", // summarys
                ""); // truncateProfile
        string docsStr = "cmd=add,attr1=0,attr2=0 10,id=1,name=aa,index2=a a a,sub_id=1^2^3,sub_attr1=1^2^2,sub_index2=aa^ab^ac;"
                         "cmd=add,attr1=1,attr2=1 11,id=2,name=bb,index2=a b c,sub_id=4,sub_attr1=1,sub_index2=aa;"
                         "cmd=add,attr1=2,attr2=2 22,id=3,name=cc,index2=a c d stop,sub_id=5,sub_attr1=1,sub_index2=ab;"
                         "cmd=add,attr1=3,attr2=3 33,id=4,name=dd,index2=b c d,sub_id=6^7,sub_attr1=2^1,sub_index2=abc^ab";

        IE_NAMESPACE(config)::IndexPartitionOptions options;
        options.GetOnlineConfig().ttl = ttl;
        auto schema = IE_NAMESPACE(testlib)::IndexlibPartitionCreator::CreateSchemaWithSub(mainSchema, subSchema);

        IE_NAMESPACE(partition)::IndexPartitionPtr indexPartition =
            IE_NAMESPACE(testlib)::IndexlibPartitionCreator::CreateIndexPartition(
                    schema, rootPath, docsStr, options, "", true);
        assert(indexPartition.get() != nullptr);
        return indexPartition;
    }


TEST_F(ScanKernelTest, testSimpleProcess) {
    prepareAttributeMap();
    TablePtr outputTable;
    runKernel(outputTable);
    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(4, outputTable->getRowCount());
    ColumnPtr column = outputTable->getColumn("attr1");
    ASSERT_TRUE(column != NULL);
    ColumnData<uint32_t>* attr1 = column->getColumnData<uint32_t>();
    ASSERT_TRUE(attr1 != NULL);
    ASSERT_EQ(0, attr1->get(0));
    ASSERT_EQ(1, attr1->get(1));
    ASSERT_EQ(2, attr1->get(2));
    ASSERT_EQ(3, attr1->get(3));
}

TEST_F(ScanKernelTest, testScanAllWithSub) {
    _attributeMap.clear();
    _attributeMap["table_type"] = string("normal");
    _attributeMap["table_name"] = _tableName;
    _attributeMap["db_name"] = string("default");
    _attributeMap["catalog_name"] = string("default");
    _attributeMap["hash_fields"] = ParseJson(string(R"json(["id"])json"));
    _attributeMap["output_fields"] = ParseJson(string(R"json(["$attr1", "$sub_attr1"])json"));
    _attributeMap["nest_table_attrs"] = ParseJson(string(R"json([{"table_name":"sub_invertedTable"}])json"));
    _attributeMap["use_nest_table"] = Any(true);
    TablePtr outputTable;
    runKernel(outputTable);
    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(7, outputTable->getRowCount());
    ColumnPtr column = outputTable->getColumn("attr1");
    ASSERT_TRUE(column != NULL);
    ColumnData<uint32_t>* attr1 = column->getColumnData<uint32_t>();
    ASSERT_TRUE(attr1 != NULL);
    ASSERT_EQ(0, attr1->get(0));
    ASSERT_EQ(0, attr1->get(1));
    ASSERT_EQ(0, attr1->get(2));
    ASSERT_EQ(1, attr1->get(3));
    ASSERT_EQ(2, attr1->get(4));
    ASSERT_EQ(3, attr1->get(5));
    ASSERT_EQ(3, attr1->get(6));
    ColumnPtr column2 = outputTable->getColumn("sub_attr1");
    ASSERT_TRUE(column2 != NULL);
    ColumnData<int32_t>* subAttr1 = column2->getColumnData<int32_t>();
    ASSERT_TRUE(subAttr1 != NULL);
    ASSERT_EQ(1, subAttr1->get(0));
    ASSERT_EQ(2, subAttr1->get(1));
    ASSERT_EQ(2, subAttr1->get(2));
    ASSERT_EQ(1, subAttr1->get(3));
    ASSERT_EQ(1, subAttr1->get(4));
    ASSERT_EQ(2, subAttr1->get(5));
    ASSERT_EQ(1, subAttr1->get(6));
}

TEST_F(ScanKernelTest, testScanAllWithSubNoSubAttr) {
    _attributeMap.clear();
    _attributeMap["table_type"] = string("normal");
    _attributeMap["table_name"] = _tableName;
    _attributeMap["db_name"] = string("default");
    _attributeMap["catalog_name"] = string("default");
    _attributeMap["hash_fields"] = ParseJson(string(R"json(["id"])json"));
    _attributeMap["output_fields"] = ParseJson(string(R"json(["$attr1"])json"));
    _attributeMap["nest_table_attrs"] = ParseJson(string(R"json([{"table_name":"sub_invertedTable"}])json"));
    _attributeMap["use_nest_table"] = Any(true);
    TablePtr outputTable;
    runKernel(outputTable);
    ASSERT_TRUE(outputTable != NULL);
    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(7, outputTable->getRowCount());
    ColumnPtr column = outputTable->getColumn("attr1");
    ASSERT_TRUE(column != NULL);
    ColumnData<uint32_t>* attr1 = column->getColumnData<uint32_t>();
    ASSERT_TRUE(attr1 != NULL);
    ASSERT_EQ(0, attr1->get(0));
    ASSERT_EQ(0, attr1->get(1));
    ASSERT_EQ(0, attr1->get(2));
    ASSERT_EQ(1, attr1->get(3));
    ASSERT_EQ(2, attr1->get(4));
    ASSERT_EQ(3, attr1->get(5));
    ASSERT_EQ(3, attr1->get(6));
}

TEST_F(ScanKernelTest, testScanAllWithSubFilter) {
    _attributeMap.clear();
    _attributeMap["table_type"] = string("normal");
    _attributeMap["table_name"] = _tableName;
    _attributeMap["db_name"] = string("default");
    _attributeMap["catalog_name"] = string("default");
    _attributeMap["hash_fields"] = ParseJson(string(R"json(["id"])json"));
    _attributeMap["output_fields"] = ParseJson(string(R"json(["$attr1"])json"));
    _attributeMap["condition"] = ParseJson(R"json({"op":"=", "params":["$sub_attr1", 2]})json");
    _attributeMap["nest_table_attrs"] = ParseJson(string(R"json([{"table_name":"sub_invertedTable"}])json"));
    _attributeMap["use_nest_table"] = Any(true);
    TablePtr outputTable;
    runKernel(outputTable);
    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(3, outputTable->getRowCount());
    ColumnPtr column = outputTable->getColumn("attr1");
    ASSERT_TRUE(column != NULL);
    ColumnData<uint32_t>* attr1 = column->getColumnData<uint32_t>();
    ASSERT_TRUE(attr1 != NULL);
    ASSERT_EQ(0, attr1->get(0));
    ASSERT_EQ(0, attr1->get(1));
    ASSERT_EQ(3, attr1->get(2));
}

TEST_F(ScanKernelTest, testScanAllWithSubFilter2) {
    _attributeMap.clear();
    _attributeMap["table_type"] = string("normal");
    _attributeMap["table_name"] = _tableName;
    _attributeMap["db_name"] = string("default");
    _attributeMap["catalog_name"] = string("default");
    _attributeMap["hash_fields"] = ParseJson(string(R"json(["id"])json"));
    _attributeMap["output_fields"] = ParseJson(string(R"json(["$attr1", "$sub_attr1"])json"));
    _attributeMap["condition"] = ParseJson(R"json({"op":"=", "params":["$sub_attr1", 2]})json");
    _attributeMap["nest_table_attrs"] = ParseJson(string(R"json([{"table_name":"sub_invertedTable"}])json"));
    _attributeMap["use_nest_table"] = Any(true);
    TablePtr outputTable;
    runKernel(outputTable);
    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(3, outputTable->getRowCount());
    ColumnPtr column = outputTable->getColumn("attr1");
    ASSERT_TRUE(column != NULL);
    ColumnData<uint32_t>* attr1 = column->getColumnData<uint32_t>();
    ASSERT_TRUE(attr1 != NULL);
    ASSERT_EQ(0, attr1->get(0));
    ASSERT_EQ(0, attr1->get(1));
    ASSERT_EQ(3, attr1->get(2));

    ColumnPtr column2 = outputTable->getColumn("sub_attr1");
    ASSERT_TRUE(column2 != NULL);
    ColumnData<int32_t>* subAttr1 = column2->getColumnData<int32_t>();
    ASSERT_TRUE(subAttr1 != NULL);
    ASSERT_EQ(2, subAttr1->get(0));
    ASSERT_EQ(2, subAttr1->get(1));
    ASSERT_EQ(2, subAttr1->get(2));
}

TEST_F(ScanKernelTest, testOutputFieldNotInAttributes) {
    prepareAttributeMap();
    _attributeMap["output_fields"] = ParseJson(string(R"json(["$attr1", "$attr2", "$id", "$name"])json"));
    TablePtr outputTable;
    runKernel(outputTable);

    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(4, outputTable->getRowCount());
    ColumnPtr column = outputTable->getColumn("attr1");
    ASSERT_TRUE(column != NULL);
    ColumnData<uint32_t>* attr1 = column->getColumnData<uint32_t>();
    ASSERT_TRUE(attr1 != NULL);
    ASSERT_EQ(0, attr1->get(0));
    ASSERT_EQ(1, attr1->get(1));
    ASSERT_EQ(2, attr1->get(2));
    ASSERT_EQ(3, attr1->get(3));

    ColumnPtr nameColumn = outputTable->getColumn("name");
    ASSERT_TRUE(nameColumn != NULL);
    ColumnData<MultiChar>* nameAttr = nameColumn->getColumnData<MultiChar>();
    ASSERT_TRUE(nameAttr != NULL);
    ASSERT_EQ(string("null"), string(nameAttr->get(0).data(), nameAttr->get(0).size()));
    ASSERT_EQ(string("null"), string(nameAttr->get(1).data(), nameAttr->get(1).size()));
    ASSERT_EQ(string("null"), string(nameAttr->get(2).data(), nameAttr->get(2).size()));
    ASSERT_EQ(string("null"), string(nameAttr->get(3).data(), nameAttr->get(3).size()));
}

TEST_F(ScanKernelTest, testOutputExprsConst) {
    prepareAttributeMap();
    _attributeMap["output_fields"] = ParseJson(string(R"json(["$attr1", "$attr2", "$id", "$$f0"])json"));
    _attributeMap["output_field_exprs"] = ParseJson(R"json({"$$f0": 1})json");
    TablePtr outputTable;
    runKernel(outputTable);

    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(4, outputTable->getRowCount());
    ColumnPtr column = outputTable->getColumn("$f0");
    ASSERT_TRUE(column != NULL);
    ColumnData<int32_t>* f0 = column->getColumnData<int32_t>();
    ASSERT_TRUE(f0 != NULL);
    ASSERT_EQ(1, f0->get(0));
    ASSERT_EQ(1, f0->get(1));
    ASSERT_EQ(1, f0->get(2));
    ASSERT_EQ(1, f0->get(3));
}

TEST_F(ScanKernelTest, testOutputExprsOnlyConst) {
    prepareAttributeMap();
    _attributeMap["output_fields"] = ParseJson(string(R"json(["$$f0"])json"));
    _attributeMap["output_field_exprs"] = ParseJson(R"json({"$$f0": 1})json");
    TablePtr outputTable;
    runKernel(outputTable);

    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(4, outputTable->getRowCount());
    ColumnPtr column = outputTable->getColumn("$f0");
    ASSERT_TRUE(column != NULL);
    ColumnData<int32_t>* f0 = column->getColumnData<int32_t>();
    ASSERT_TRUE(f0 != NULL);
    ASSERT_EQ(1, f0->get(0));
    ASSERT_EQ(1, f0->get(1));
    ASSERT_EQ(1, f0->get(2));
    ASSERT_EQ(1, f0->get(3));
}

TEST_F(ScanKernelTest, testOutputExprsConstWithType) {
    prepareAttributeMap();
    _attributeMap["output_fields"] = ParseJson(string(R"json(["$$f0", "$$f1", "$$f2", "$$f3", "$attr1"])json"));
    _attributeMap["output_fields_type"] = ParseJson(string(R"json(["BIGINT", "FLOAT", "DOUBLE", "INTEGER", "BIGINT"])json"));
    _attributeMap["output_field_exprs"] = ParseJson(R"json({"$$f0": 1, "$$f1":0.1, "$$f2":0.2, "$$f3":0, "$attr1":1})json");
    TablePtr outputTable;
    runKernel(outputTable);

    ASSERT_TRUE(outputTable != NULL);
    cout << outputTable->getMatchDocAllocator()->toDebugString() <<endl;
    ASSERT_EQ(4, outputTable->getRowCount());
    ColumnPtr column = outputTable->getColumn("$f0");
    ASSERT_TRUE(column != NULL);
    ColumnData<int64_t>* f0 = column->getColumnData<int64_t>();
    ASSERT_TRUE(f0 != NULL);
    ASSERT_EQ(1, f0->get(0));
    ASSERT_EQ(1, f0->get(1));
    ASSERT_EQ(1, f0->get(2));
    ASSERT_EQ(1, f0->get(3));

    column = outputTable->getColumn("$f1");
    ASSERT_TRUE(column != NULL);
    ColumnData<float>* f1 = column->getColumnData<float>();
    ASSERT_TRUE(f1 != NULL);
    ASSERT_FLOAT_EQ(0.1, f1->get(0));
    ASSERT_FLOAT_EQ(0.1, f1->get(1));
    ASSERT_FLOAT_EQ(0.1, f1->get(2));
    ASSERT_FLOAT_EQ(0.1, f1->get(3));

    column = outputTable->getColumn("$f2");
    ASSERT_TRUE(column != NULL);
    ColumnData<double>* f2 = column->getColumnData<double>();
    ASSERT_TRUE(f2 != NULL);
    ASSERT_DOUBLE_EQ(0.2, f2->get(0));
    ASSERT_DOUBLE_EQ(0.2, f2->get(1));
    ASSERT_DOUBLE_EQ(0.2, f2->get(2));
    ASSERT_DOUBLE_EQ(0.2, f2->get(3));

    column = outputTable->getColumn("$f3");
    ASSERT_TRUE(column != NULL);
    ColumnData<int32_t>* f3 = column->getColumnData<int32_t>();
    ASSERT_TRUE(f3 != NULL);
    ASSERT_EQ(0, f3->get(0));
    ASSERT_EQ(0, f3->get(1));
    ASSERT_EQ(0, f3->get(2));
    ASSERT_EQ(0, f3->get(3));
    column = outputTable->getColumn("attr1");
    ASSERT_TRUE(column != NULL);
    ColumnData<uint32_t>* attr1 = column->getColumnData<uint32_t>();
    ASSERT_TRUE(attr1 != NULL);
    ASSERT_EQ(1, attr1->get(0));
    ASSERT_EQ(1, attr1->get(1));
    ASSERT_EQ(1, attr1->get(2));
    ASSERT_EQ(1, attr1->get(3));
}

TEST_F(ScanKernelTest, testOutputExprsConstWithType2) {
    prepareAttributeMap();
    _attributeMap["output_fields"] = ParseJson(string(R"json(["$$f0", "$$f1", "$$f2"])json"));
    _attributeMap["output_fields_type"] = ParseJson(string(R"json(["BIGINT", "BIGINT", "TINYINT"])json"));
    _attributeMap["output_field_exprs"] = ParseJson(R"json({"$$f0": 0, "$$f1":0, "$$f2":0})json");
    TablePtr outputTable;
    runKernel(outputTable);
    ASSERT_TRUE(outputTable != NULL);
    cout << outputTable->getMatchDocAllocator()->toDebugString() <<endl;
    cout << outputTable->getTableSchema().toString() << endl;

    ASSERT_EQ(4, outputTable->getRowCount());
    ColumnPtr column = outputTable->getColumn("$f0");
    ASSERT_TRUE(column != NULL);
    ColumnData<int64_t>* f0 = column->getColumnData<int64_t>();
    ASSERT_TRUE(f0 != NULL);
    ASSERT_EQ(0, f0->get(0));
    ASSERT_EQ(0, f0->get(1));
    ASSERT_EQ(0, f0->get(2));
    ASSERT_EQ(0, f0->get(3));

    column = outputTable->getColumn("$f1");
    ASSERT_TRUE(column != NULL);
    ColumnData<int64_t>* f1 = column->getColumnData<int64_t>();
    ASSERT_TRUE(f1 != NULL);
    ASSERT_EQ(0, f1->get(0));
    ASSERT_EQ(0, f1->get(1));
    ASSERT_EQ(0, f1->get(2));
    ASSERT_EQ(0, f1->get(3));

    column = outputTable->getColumn("$f2");
    ASSERT_TRUE(column != NULL);
    ColumnData<int8_t>* f2 = column->getColumnData<int8_t>();
    ASSERT_TRUE(f2 != NULL);
    ASSERT_EQ(0, f2->get(0));
    ASSERT_EQ(0, f2->get(1));
    ASSERT_EQ(0, f2->get(2));
    ASSERT_EQ(0, f2->get(3));
}


TEST_F(ScanKernelTest, testOutputExprsConstWithSub) {
    prepareAttributeMap();
    _attributeMap["output_fields"] = ParseJson(string(R"json(["$$f0"])json"));
    _attributeMap["output_field_exprs"] = ParseJson(R"json({"$$f0": 1})json");
    _attributeMap["nest_table_attrs"] = ParseJson(string(R"json([{"table_name":"sub_invertedTable"}])json"));
    _attributeMap["use_nest_table"] = true;
    TablePtr outputTable;
    runKernel(outputTable);

    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(7, outputTable->getRowCount());
    ColumnPtr column = outputTable->getColumn("$f0");
    ASSERT_TRUE(column != NULL);
    ColumnData<int32_t>* f0 = column->getColumnData<int32_t>();
    ASSERT_TRUE(f0 != NULL);
    ASSERT_EQ(1, f0->get(0));
    ASSERT_EQ(1, f0->get(1));
    ASSERT_EQ(1, f0->get(2));
    ASSERT_EQ(1, f0->get(3));
    ASSERT_EQ(1, f0->get(4));
    ASSERT_EQ(1, f0->get(5));
    ASSERT_EQ(1, f0->get(6));
}

TEST_F(ScanKernelTest, testOutputExprsWithAs) {
    prepareAttributeMap();
    _attributeMap["output_fields"] = ParseJson(string(R"json(["$attr1", "$attr2", "$id", "$f0"])json"));
    _attributeMap["output_field_exprs"] = ParseJson(R"json({"$f0":{"op":"+", "params":["$attr1", 1]}})json");

    TablePtr outputTable;
    runKernel(outputTable);

    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(4, outputTable->getRowCount());
    ColumnPtr column = outputTable->getColumn("f0");
    ASSERT_TRUE(column != NULL);
    ColumnData<uint32_t>* f0 = column->getColumnData<uint32_t>();
    ASSERT_TRUE(f0 != NULL);
    ASSERT_EQ(1, f0->get(0));
    ASSERT_EQ(2, f0->get(1));
    ASSERT_EQ(3, f0->get(2));
    ASSERT_EQ(4, f0->get(3));
}

TEST_F(ScanKernelTest, testOutputExprsWithAsRename) {
    prepareAttributeMap();
    _attributeMap["output_fields"] = ParseJson(string(R"json(["$attr1", "$b", "$c", "$attr2"])json"));
    _attributeMap["output_field_exprs"] = ParseJson(R"json({
    "$b": "$attr1",
    "$c": "$attr1"})json");

    TablePtr outputTable;
    runKernel(outputTable);

    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(4, outputTable->getRowCount());
    ASSERT_EQ(4, outputTable->getColumnCount());
    vector<string> expected = {"attr1", "b", "c", "attr2"};
    for (size_t i = 0; i < expected.size(); ++i) {
        ASSERT_EQ(expected[i], outputTable->getColumn(i)->getColumnSchema()->getName());
    }
}

TEST_F(ScanKernelTest, testOutputExprsWithAsNotExistField) {
    prepareAttributeMap();
    _attributeMap["output_fields"] = ParseJson(string(R"json(["$attr1", "$attr2", "$id", "$f0"])json"));
    _attributeMap["output_field_exprs"] = ParseJson(R"json({"$f0":{"op":"+", "params":["$attr1_null", 1]}})json");

    TablePtr outputTable;
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    ASSERT_FALSE(tester.compute());
}

TEST_F(ScanKernelTest, testOutputTwoExprsWithAs) {
    prepareAttributeMap();
    _attributeMap["output_fields"] = ParseJson(string(R"json(["$attr1", "$attr2", "$id", "$f0"])json"));
    _attributeMap["output_field_exprs"] = ParseJson(R"json({"$f0":{"op":"*", "params":[{"op":"+", "params":["$attr1", 1]}, 2]}})json");

    TablePtr outputTable;
    runKernel(outputTable);

    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(4, outputTable->getRowCount());
    ColumnPtr column = outputTable->getColumn("f0");
    ASSERT_TRUE(column != NULL);
    ColumnData<uint32_t>* f0 = column->getColumnData<uint32_t>();
    ASSERT_TRUE(f0 != NULL);
    ASSERT_EQ(2, f0->get(0));
    ASSERT_EQ(4, f0->get(1));
    ASSERT_EQ(6, f0->get(2));
    ASSERT_EQ(8, f0->get(3));
}

TEST_F(ScanKernelTest, testOutputTwoExprsWithAs2) {
    prepareAttributeMap();
    _attributeMap["output_fields"] = ParseJson(string(R"json(["$attr1", "$attr1_1"])json"));
    _attributeMap["output_field_exprs"] = ParseJson(R"json({"$attr1_1":"$attr1"})json");

    TablePtr outputTable;
    runKernel(outputTable);

    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(4, outputTable->getRowCount());
    ColumnPtr column = outputTable->getColumn("attr1");
    ASSERT_TRUE(column != NULL);
    ColumnData<uint32_t>* attr1 = column->getColumnData<uint32_t>();
    ASSERT_TRUE(attr1 != NULL);
    ASSERT_EQ(0, attr1->get(0));
    ASSERT_EQ(1, attr1->get(1));
    ASSERT_EQ(2, attr1->get(2));
    ASSERT_EQ(3, attr1->get(3));

    ColumnPtr column2 = outputTable->getColumn("attr1_1");
    ASSERT_TRUE(column2 != NULL);
    attr1 = column2->getColumnData<uint32_t>();
    ASSERT_TRUE(attr1 != NULL);
    ASSERT_EQ(0, attr1->get(0));
    ASSERT_EQ(1, attr1->get(1));
    ASSERT_EQ(2, attr1->get(2));
    ASSERT_EQ(3, attr1->get(3));
}

TEST_F(ScanKernelTest, testOutputExprsCast) {
    prepareAttributeMap();
    _attributeMap["output_field_exprs"] = ParseJson(R"json({"$attr1": {"op":"CAST","cast_type":"BIGINT","type":"UDF","params":[12]}})json");

    TablePtr outputTable;
    runKernel(outputTable);

    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(4, outputTable->getRowCount());
    ColumnPtr column = outputTable->getColumn("attr1");
    ASSERT_TRUE(column != NULL);
    ColumnData<uint32_t>* attr1 = column->getColumnData<uint32_t>();
    ASSERT_TRUE(attr1 != NULL);
    ASSERT_EQ(12, attr1->get(0));
    ASSERT_EQ(12, attr1->get(1));
    ASSERT_EQ(12, attr1->get(2));
    ASSERT_EQ(12, attr1->get(3));
}

TEST_F(ScanKernelTest, testOutputExprsNormal) {
    prepareAttributeMap();
    _attributeMap["output_field_exprs"] = ParseJson(R"json({"$attr1":{"op":"+", "params":["$attr1", 1]}})json");

    TablePtr outputTable;
    runKernel(outputTable);

    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(4, outputTable->getRowCount());
    ColumnPtr column = outputTable->getColumn("attr1");
    ASSERT_TRUE(column != NULL);
    ColumnData<uint32_t>* attr1 = column->getColumnData<uint32_t>();
    ASSERT_TRUE(attr1 != NULL);
    ASSERT_EQ(1, attr1->get(0));
    ASSERT_EQ(2, attr1->get(1));
    ASSERT_EQ(3, attr1->get(2));
    ASSERT_EQ(4, attr1->get(3));
}

TEST_F(ScanKernelTest, testBatchSize) {
    prepareAttributeMap();
    _attributeMap["batch_size"] = autil::legacy::Any(3);
    TablePtr outputTable;
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;

    ASSERT_TRUE(tester.compute()); // kernel compute success
    getOutputTable(tester, outputTable);
    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(3, outputTable->getRowCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "attr1", {0, 1, 2}));

    ASSERT_TRUE(tester.compute()); // kernel compute success
    getOutputTable(tester, outputTable);
    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(1, outputTable->getRowCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "attr1", {3}));

}

TEST_F(ScanKernelTest, testLike) {
    prepareAttributeMap();
    _attributeMap["condition"] = ParseJson(R"json({"op":"LIKE", "params":["$index2", "a"]})json");
    _attributeMap["index_infos"] = ParseJson(string(R"json({"$index2":{"type":"TEXT", "name":"index_2"}})json"));

    TablePtr outputTable;
    runKernel(outputTable);

    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(3, outputTable->getRowCount());
    ColumnPtr column = outputTable->getColumn("attr1");
    ASSERT_TRUE(column != NULL);
    ColumnData<uint32_t>* attr1 = column->getColumnData<uint32_t>();
    ASSERT_TRUE(attr1 != NULL);
    ASSERT_EQ(0, attr1->get(0));
    ASSERT_EQ(1, attr1->get(1));
    ASSERT_EQ(2, attr1->get(2));
}

TEST_F(ScanKernelTest, testFilter) {
    prepareAttributeMap();
    _attributeMap["condition"] = ParseJson(R"json({"op":"=", "params":["$attr1", 1]})json");

    TablePtr outputTable;
    runKernel(outputTable);

    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(1, outputTable->getRowCount());
    ColumnPtr column = outputTable->getColumn("attr1");
    ASSERT_TRUE(column != NULL);
    ColumnData<uint32_t>* attr1 = column->getColumnData<uint32_t>();
    ASSERT_TRUE(attr1 != NULL);
    ASSERT_EQ(1, attr1->get(0));
}

TEST_F(ScanKernelTest, testInFilter) {
    prepareAttributeMap();
    _attributeMap["condition"] = ParseJson(R"json({"op":"IN", "params":["$attr1", 1]})json");
\
    TablePtr outputTable;
    runKernel(outputTable);

    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(1, outputTable->getRowCount());
    ColumnPtr column = outputTable->getColumn("attr1");
    ASSERT_TRUE(column != NULL);
    ColumnData<uint32_t>* attr1 = column->getColumnData<uint32_t>();
    ASSERT_TRUE(attr1 != NULL);
    ASSERT_EQ(1, attr1->get(0));
}

TEST_F(ScanKernelTest, testHaInFilter) {
    prepareAttributeMap();
    _attributeMap["condition"] = ParseJson(R"json({"op":"ha_in", "params":["$attr1", "1"], "type":"UDF"})json");

    TablePtr outputTable;
    runKernel(outputTable);

    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(1, outputTable->getRowCount());
    ColumnPtr column = outputTable->getColumn("attr1");
    ASSERT_TRUE(column != NULL);
    ColumnData<uint32_t>* attr1 = column->getColumnData<uint32_t>();
    ASSERT_TRUE(attr1 != NULL);
    ASSERT_EQ(1, attr1->get(0));
}

TEST_F(ScanKernelTest, testHaInToQuery) {
    prepareAttributeMap();
    _attributeMap["condition"] = ParseJson(R"json({"op":"ha_in", "params":["$name", "aa"], "type":"UDF"})json");
    _attributeMap["index_infos"] = ParseJson(R"json({"$name":{"type":"STRING", "name":"name"}})json");
    TablePtr outputTable;
    runKernel(outputTable);

    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(1, outputTable->getRowCount());
    ColumnPtr column = outputTable->getColumn("attr1");
    ASSERT_TRUE(column != NULL);
    ColumnData<uint32_t>* attr1 = column->getColumnData<uint32_t>();
    ASSERT_TRUE(attr1 != NULL);
    ASSERT_EQ(0, attr1->get(0));
}


TEST_F(ScanKernelTest, testComplexCondition) {
    prepareAttributeMap();
    _attributeMap["condition"] = ParseJson(R"json({"op":"=", "params":[{"op":"+", "params":["$attr1", "$attr1"]}, 2]})json");

    TablePtr outputTable;
    runKernel(outputTable);

    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(1, outputTable->getRowCount());
    ColumnPtr column = outputTable->getColumn("attr1");
    ASSERT_TRUE(column != NULL);
    ColumnData<uint32_t>* attr1 = column->getColumnData<uint32_t>();
    ASSERT_TRUE(attr1 != NULL);
    ASSERT_EQ(1, attr1->get(0));
}

TEST_F(ScanKernelTest, testLikeQuery) {
    prepareAttributeMap();
    _attributeMap["condition"] = ParseJson(R"json({"op":"LIKE", "params":["$index2", "a"]})json");
    _attributeMap["index_infos"] = ParseJson(string(R"json({"$index2":{"type":"TEXT", "name":"index_2"}})json"));

    TablePtr outputTable;
    runKernel(outputTable);

    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(3, outputTable->getRowCount());
    ColumnPtr column = outputTable->getColumn("attr1");
    ASSERT_TRUE(column != NULL);
    ColumnData<uint32_t>* attr1 = column->getColumnData<uint32_t>();
    ASSERT_TRUE(attr1 != NULL);
    ASSERT_EQ(0, attr1->get(0));
    ASSERT_EQ(1, attr1->get(1));
    ASSERT_EQ(2, attr1->get(2));
}

TEST_F(ScanKernelTest, testLikeQueryAndFilter) {
    prepareAttributeMap();
    string indexCondition = R"json({"op":"LIKE", "params":["$index2", "a"]})json"; // query
    string attr1Condition = R"json({"op":">=", "params":["$attr1", 1]})json"; // filter
    _attributeMap["condition"] = ParseJson("{\"op\":\"AND\", \"params\":[" + indexCondition + " , " + attr1Condition + "]}");
    _attributeMap["index_infos"] = ParseJson(R"json({"$index2":{"type":"TEXT", "name":"index_2"}})json");

    TablePtr outputTable;
    runKernel(outputTable);

    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(2, outputTable->getRowCount());
    ColumnPtr column = outputTable->getColumn("attr1");
    ASSERT_TRUE(column != NULL);
    ColumnData<uint32_t>* attr1 = column->getColumnData<uint32_t>();
    ASSERT_TRUE(attr1 != NULL);
    ASSERT_EQ(1, attr1->get(0));
    ASSERT_EQ(2, attr1->get(1));
}

TEST_F(ScanKernelTest, testLikeQueryAndLikeQuery) {
    prepareAttributeMap();
    string indexCondition = R"json({"op":"LIKE", "params":["$index2", "a"]})json"; // query
    string indexCondition2 = R"json({"op":"LIKE", "params":["$index2", "c"]})json"; // query
    string attr1Condition = R"json({"op":">", "params":["$attr1", 1]})json"; // filter
    _attributeMap["condition"] =
        ParseJson("{\"op\":\"AND\", \"params\":[" + indexCondition + " , " +
                  indexCondition2 + " , " + attr1Condition + "]}");
    _attributeMap["index_infos"] = ParseJson(string(R"json({"$index2":{"type":"TEXT", "name":"index_2"}})json"));

    TablePtr outputTable;
    runKernel(outputTable);

    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(1, outputTable->getRowCount());
    ColumnPtr column = outputTable->getColumn("attr1");
    ASSERT_TRUE(column != NULL);
    ColumnData<uint32_t>* attr1 = column->getColumnData<uint32_t>();
    ASSERT_TRUE(attr1 != NULL);
    ASSERT_EQ(2, attr1->get(0));
}

TEST_F(ScanKernelTest, testFilterAndFilter) {
    prepareAttributeMap();
    string attr1Condition = R"json({"op":">=", "params":["$attr1", 1]})json"; // filter
    string attr1Condition2 = R"json({"op":"<=", "params":["$attr1", 2]})json"; // filter
    _attributeMap["condition"] =
        ParseJson("{\"op\":\"AND\", \"params\":[" + attr1Condition2 + " , " +
                  attr1Condition + "]}");
    _attributeMap["index_infos"] = ParseJson(R"json({"$index2":{"type":"TEXT", "name":"index_2"}})json");

    TablePtr outputTable;
    runKernel(outputTable);

    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(2, outputTable->getRowCount());
    ColumnPtr column = outputTable->getColumn("attr1");
    ASSERT_TRUE(column != NULL);
    ColumnData<uint32_t>* attr1 = column->getColumnData<uint32_t>();
    ASSERT_TRUE(attr1 != NULL);
    ASSERT_EQ(1, attr1->get(0));
    ASSERT_EQ(2, attr1->get(1));
}

TEST_F(ScanKernelTest, testTermQueryOrFilter) {
    prepareAttributeMap();
    string indexCondition = R"json({"op":"=", "params":["$id",1]})json"; // query
    string attr1Condition = R"json({"op":">=", "params":["$attr1",2]})json"; // filter
    _attributeMap["condition"] = ParseJson("{\"op\":\"OR\", \"params\":[" + indexCondition + " , " + attr1Condition + "]}");
    _attributeMap["index_infos"] = ParseJson(R"json({"$id":{"type":"NUMBER", "name":"id"}})json");

    TablePtr outputTable;
    runKernel(outputTable);

    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(3, outputTable->getRowCount());
    ColumnPtr column = outputTable->getColumn("attr1");
    ASSERT_TRUE(column != NULL);
    ColumnData<uint32_t>* attr1 = column->getColumnData<uint32_t>();
    ASSERT_TRUE(attr1 != NULL);
    ASSERT_EQ(0, attr1->get(0));
    ASSERT_EQ(2, attr1->get(1));
    ASSERT_EQ(3, attr1->get(2));
}

TEST_F(ScanKernelTest, testLikeQueryOrLikeQuery) {
    prepareAttributeMap();
    string indexCondition = R"json({"op":"LIKE", "params":["$index2", "b"]})json"; // query
    string indexCondition2 = R"json({"op":"LIKE", "params":["$index2", "d"]})json"; // query
    _attributeMap["condition"] =
        ParseJson("{\"op\":\"OR\", \"params\":[" + indexCondition + " , " +
                  indexCondition2 + "]}");
    _attributeMap["index_infos"] = ParseJson(R"json({"$index2":{"type":"TEXT", "name":"index_2"}})json");

    TablePtr outputTable;
    runKernel(outputTable);

    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(3, outputTable->getRowCount());
    ColumnPtr column = outputTable->getColumn("attr1");
    ASSERT_TRUE(column != NULL);
    ColumnData<uint32_t>* attr1 = column->getColumnData<uint32_t>();
    ASSERT_TRUE(attr1 != NULL);
    ASSERT_EQ(1, attr1->get(0));
    ASSERT_EQ(2, attr1->get(1));
    ASSERT_EQ(3, attr1->get(2));
}

TEST_F(ScanKernelTest, testFilterOrFilter) {
    prepareAttributeMap();
    string attr1Condition = R"json({"op":">=", "params":["$attr1", 1]})json"; // filter
    string attr1Condition2 = R"json({"op":"<=", "params":["$attr1", 2]})json"; // filter
    _attributeMap["condition"] =
        ParseJson("{\"op\":\"OR\", \"params\":[" + attr1Condition2 + " , " +
                  attr1Condition + "]}");

    TablePtr outputTable;
    runKernel(outputTable);

    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(4, outputTable->getRowCount());
    ColumnPtr column = outputTable->getColumn("attr1");
    ASSERT_TRUE(column != NULL);
    ColumnData<uint32_t>* attr1 = column->getColumnData<uint32_t>();
    ASSERT_TRUE(attr1 != NULL);
    ASSERT_EQ(0, attr1->get(0));
    ASSERT_EQ(1, attr1->get(1));
    ASSERT_EQ(2, attr1->get(2));
    ASSERT_EQ(3, attr1->get(3));
}

TEST_F(ScanKernelTest, testOrOnAnd) {
    prepareAttributeMap();
    string attr1Condition = R"json({"op":">=", "params":["$attr1", 1]})json"; // filter
    string attr1Condition2 = R"json({"op":"<=", "params":["$attr1", 2]})json"; // filter
    string andCondition = "{\"op\":\"AND\", \"params\":[" +
                          attr1Condition2 + " , " +
                          attr1Condition + "]}";
    string idCondition = R"json({"op":"=", "params":["$id", 4]})json"; // query
    _attributeMap["condition"] =
        ParseJson("{\"op\":\"OR\", \"params\":[" + andCondition + " , " +
                  idCondition + "]}");
    _attributeMap["index_infos"] = ParseJson(R"json({"$id":{"type":"NUMBER", "name":"id"}})json");

    TablePtr outputTable;
    runKernel(outputTable);

    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(3, outputTable->getRowCount());
    ColumnPtr column = outputTable->getColumn("attr1");
    ASSERT_TRUE(column != NULL);
    ColumnData<uint32_t>* attr1 = column->getColumnData<uint32_t>();
    ASSERT_TRUE(attr1 != NULL);
    ASSERT_EQ(1, attr1->get(0));
    ASSERT_EQ(2, attr1->get(1));
    ASSERT_EQ(3, attr1->get(2));
}

TEST_F(ScanKernelTest, testNotCondition) {
    prepareAttributeMap();
    _attributeMap["condition"] = ParseJson(R"json({"op": "NOT", "params": [{"op":"=", "params":["$attr1", 1]}]})json");
    TablePtr outputTable;
    runKernel(outputTable);

    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(3, outputTable->getRowCount());
    ColumnPtr column = outputTable->getColumn("attr1");
    ASSERT_TRUE(column != NULL);
    ColumnData<uint32_t>* attr1 = column->getColumnData<uint32_t>();
    ASSERT_TRUE(attr1 != NULL);
    ASSERT_EQ(0, attr1->get(0));
    ASSERT_EQ(2, attr1->get(1));
    ASSERT_EQ(3, attr1->get(2));
}

TEST_F(ScanKernelTest, testNotQueryCondition) {
    prepareAttributeMap();
    _attributeMap["condition"] = ParseJson(R"json({"op": "NOT", "params": [{"op":"=", "params":["$attr2", 1]}]})json");
    TablePtr outputTable;
    runKernel(outputTable);
    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(3, outputTable->getRowCount());
    ColumnPtr column = outputTable->getColumn("attr1");
    ASSERT_TRUE(column != NULL);
    ColumnData<uint32_t>* attr1 = column->getColumnData<uint32_t>();
    ASSERT_TRUE(attr1 != NULL);
    ASSERT_EQ(0, attr1->get(0));
    ASSERT_EQ(2, attr1->get(1));
    ASSERT_EQ(3, attr1->get(2));
}

TEST_F(ScanKernelTest, testNotNEQueryCondition) {
    prepareAttributeMap();
    _attributeMap["condition"] = ParseJson(R"json({"op": "NOT", "params": [{"op":"!=", "params":["$attr2", 1]}]})json");

    TablePtr outputTable;
    runKernel(outputTable);
    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(1, outputTable->getRowCount());
    ColumnPtr column = outputTable->getColumn("attr1");
    ASSERT_TRUE(column != NULL);
    ColumnData<uint32_t>* attr1 = column->getColumnData<uint32_t>();
    ASSERT_TRUE(attr1 != NULL);
    ASSERT_EQ(1, attr1->get(0));
}

TEST_F(ScanKernelTest, testNotOnAndCondition) {
    prepareAttributeMap();
    string indexCondition = R"json({"op":"LIKE", "params":["$index2", "a"]})json"; // query
    string attr1Condition = R"json({"op":">=", "params":["$attr1", 1]})json"; // filter
    string andCondition = "{\"op\":\"AND\", \"params\":[" + indexCondition + " , " + attr1Condition + "]}";
    string notCondition = "{\"op\":\"NOT\", \"params\":[" + andCondition + "]}";
    _attributeMap["condition"] = ParseJson(notCondition);
    _attributeMap["index_infos"] = ParseJson(R"json({"$index2":{"type":"TEXT", "name":"index_2"}})json");

    TablePtr outputTable;
    runKernel(outputTable);

    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(2, outputTable->getRowCount());
    ColumnPtr column = outputTable->getColumn("attr1");
    ASSERT_TRUE(column != NULL);
    ColumnData<uint32_t>* attr1 = column->getColumnData<uint32_t>();
    ASSERT_TRUE(attr1 != NULL);
    ASSERT_EQ(0, attr1->get(0));
    ASSERT_EQ(3, attr1->get(1));
}

TEST_F(ScanKernelTest, testNotOnOrCondition) {
    prepareAttributeMap();
    string indexCondition = R"json({"op":"=", "params":["$id",1]})json"; // query
    string attr1Condition = R"json({"op":">=", "params":["$attr1",2]})json"; // filter
    string orCondition = "{\"op\":\"OR\", \"params\":[" + indexCondition + " , " + attr1Condition + "]}";
    string notCondition = "{\"op\":\"NOT\", \"params\":[" + orCondition + "]}";
    _attributeMap["condition"] = ParseJson(notCondition);
    _attributeMap["index_infos"] = ParseJson(R"json({"$id":{"type":"NUMBER", "name":"id"}})json");

    TablePtr outputTable;
    runKernel(outputTable);

    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(1, outputTable->getRowCount());
    ColumnPtr column = outputTable->getColumn("attr1");
    ASSERT_TRUE(column != NULL);
    ColumnData<uint32_t>* attr1 = column->getColumnData<uint32_t>();
    ASSERT_TRUE(attr1 != NULL);
    ASSERT_EQ(1, attr1->get(0));
}

TEST_F(ScanKernelTest, testContainToQuery) {
    prepareAttributeMap();
    _attributeMap["condition"] = ParseJson(R"json({"op":"contain", "params":["$name", "bb"], "type":"UDF"})json");
    _attributeMap["index_infos"] = ParseJson(R"json({"$name":{"type":"STRING", "name":"name"}})json");

    TablePtr outputTable;
    runKernel(outputTable);

    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(1, outputTable->getRowCount());
    ColumnPtr column = outputTable->getColumn("attr1");
    ASSERT_TRUE(column != NULL);
    ColumnData<uint32_t>* attr1 = column->getColumnData<uint32_t>();
    ASSERT_TRUE(attr1 != NULL);
    ASSERT_EQ(1, attr1->get(0));
}

TEST_F(ScanKernelTest, testContainToNumberQuery) {
    prepareAttributeMap();
    _attributeMap["condition"] = ParseJson(R"json({"op":"contain", "params":["$id", "2"], "type":"UDF"})json");
    _attributeMap["index_infos"] = ParseJson(R"json({"$id":{"type":"NUMBER", "name":"id"}})json");

    TablePtr outputTable;
    runKernel(outputTable);

    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(1, outputTable->getRowCount());
    ColumnPtr column = outputTable->getColumn("attr1");
    ASSERT_TRUE(column != NULL);
    ColumnData<uint32_t>* attr1 = column->getColumnData<uint32_t>();
    ASSERT_TRUE(attr1 != NULL);
    ASSERT_EQ(1, attr1->get(0));
}

TEST_F(ScanKernelTest, testMatchQuery) {
    prepareAttributeMap();
    _attributeMap["condition"] = ParseJson(R"json({"op":"MATCHINDEX", "params":["a"], "type":"UDF"})json");

    TablePtr outputTable;
    runKernel(outputTable);

    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(3, outputTable->getRowCount());
    ColumnPtr column = outputTable->getColumn("attr1");
    ASSERT_TRUE(column != NULL);
    ColumnData<uint32_t>* attr1 = column->getColumnData<uint32_t>();
    ASSERT_TRUE(attr1 != NULL);
    ASSERT_EQ(0, attr1->get(0));
    ASSERT_EQ(1, attr1->get(1));
    ASSERT_EQ(2, attr1->get(2));
}

TEST_F(ScanKernelTest, testMatchQueryEmpty) {
    prepareAttributeMap();
    _attributeMap["condition"] = ParseJson(R"json({"op":"MATCHINDEX", "params":[""], "type":"UDF"})json");
    TablePtr outputTable;
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    ASSERT_FALSE(tester.compute()); // kernel compute success
}

TEST_F(ScanKernelTest, testMatchQueryToken) {
    prepareAttributeMap();
    _attributeMap["condition"] = ParseJson(R"json({"op":"MATCHINDEX", "params":["a c"], "type":"UDF"})json");

    TablePtr outputTable;
    runKernel(outputTable);

    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(2, outputTable->getRowCount());
    ColumnPtr column = outputTable->getColumn("attr1");
    ASSERT_TRUE(column != NULL);
    ColumnData<uint32_t>* attr1 = column->getColumnData<uint32_t>();
    ASSERT_TRUE(attr1 != NULL);
    ASSERT_EQ(1, attr1->get(0));
    ASSERT_EQ(2, attr1->get(1));
}

TEST_F(ScanKernelTest, testMatchQueryTokenStopWord) {
    prepareAttributeMap();
    _attributeMap["condition"] = ParseJson(R"json({"op":"MATCHINDEX", "params":["a c stop"], "type":"UDF"})json");

    TablePtr outputTable;
    runKernel(outputTable);

    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(2, outputTable->getRowCount());
    ColumnPtr column = outputTable->getColumn("attr1");
    ASSERT_TRUE(column != NULL);
    ColumnData<uint32_t>* attr1 = column->getColumnData<uint32_t>();
    ASSERT_TRUE(attr1 != NULL);
    ASSERT_EQ(1, attr1->get(0));
    ASSERT_EQ(2, attr1->get(1));
}

TEST_F(ScanKernelTest, testMatchQueryWithIndexName) {
    prepareAttributeMap();
    _attributeMap["condition"] = ParseJson(R"json({"op":"MATCHINDEX", "params":["name", "aa"], "type":"UDF"})json");

    TablePtr outputTable;
    runKernel(outputTable);

    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(1, outputTable->getRowCount());
    ColumnPtr column = outputTable->getColumn("attr1");
    ASSERT_TRUE(column != NULL);
    ColumnData<uint32_t>* attr1 = column->getColumnData<uint32_t>();
    ASSERT_TRUE(attr1 != NULL);
    ASSERT_EQ(0, attr1->get(0));
}

TEST_F(ScanKernelTest, testMatchQueryWithParam) {
    prepareAttributeMap();
    _attributeMap["condition"] = ParseJson(R"json({"op":"MATCHINDEX", "params":["index_2", "a stop", "remove_stopwords:false"], "type":"UDF"})json");

    TablePtr outputTable;
    runKernel(outputTable);

    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(1, outputTable->getRowCount());
    ColumnPtr column = outputTable->getColumn("attr1");
    ASSERT_TRUE(column != NULL);
    ColumnData<uint32_t>* attr1 = column->getColumnData<uint32_t>();
    ASSERT_TRUE(attr1 != NULL);
    ASSERT_EQ(2, attr1->get(0));
}

TEST_F(ScanKernelTest, testMatchOrFilter) {
    prepareAttributeMap();
    string indexCondition = R"json({"op":"MATCHINDEX", "params":["a"], "type":"UDF"})json"; //match
    string attr1Condition = R"json({"op":">=", "params":["$attr1", 2]})json"; // filter
    _attributeMap["condition"] =
        ParseJson("{\"op\":\"OR\", \"params\":[" + indexCondition + " , " +
                  attr1Condition + "]}");
    _attributeMap["index_infos"] = ParseJson(R"json({"$id":{"type":"NUMBER", "name":"id"}})json");

    TablePtr outputTable;
    runKernel(outputTable);

    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(4, outputTable->getRowCount());
    ColumnPtr column = outputTable->getColumn("attr1");
    ASSERT_TRUE(column != NULL);
    ColumnData<uint32_t>* attr1 = column->getColumnData<uint32_t>();
    ASSERT_TRUE(attr1 != NULL);
    ASSERT_EQ(0, attr1->get(0));
    ASSERT_EQ(1, attr1->get(1));
    ASSERT_EQ(2, attr1->get(2));
    ASSERT_EQ(3, attr1->get(3));
}

TEST_F(ScanKernelTest, testQuery) {
    prepareAttributeMap();
    _attributeMap["condition"] = ParseJson(R"json({"op":"QUERY", "params":["a OR b c"], "type":"UDF"})json");

    TablePtr outputTable;
    runKernel(outputTable);

    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(3, outputTable->getRowCount());
    ColumnPtr column = outputTable->getColumn("attr1");
    ASSERT_TRUE(column != NULL);
    ColumnData<uint32_t>* attr1 = column->getColumnData<uint32_t>();
    ASSERT_TRUE(attr1 != NULL);
    ASSERT_EQ(1, attr1->get(0));
    ASSERT_EQ(2, attr1->get(1));
    ASSERT_EQ(3, attr1->get(2));
}

TEST_F(ScanKernelTest, testErrorQueryWithFakeAnalyzer) {
    prepareAttributeMap();
    _attributeMap["condition"] =
        ParseJson(R"json({"op":"QUERY", "params":["index_2", "\"\"\"\"", "global_analyzer:fake_analyzer"], "type":"UDF"})json");

    TablePtr outputTable;
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    ASSERT_FALSE(tester.compute()); // kernel compute failed
    ASSERT_EQ("init scan kernel failed, table [invertedTable].",
              tester.getErrorMessage());
}

TEST_F(ScanKernelTest, testQueryEmpty) {
    prepareAttributeMap();
    _attributeMap["condition"] =
        ParseJson(R"json({"op":"QUERY", "params":[""], "type":"UDF"})json");

    TablePtr outputTable;
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    ASSERT_FALSE(tester.compute()); // kernel compute failed
}

TEST_F(ScanKernelTest, testQueryDefaultIndex) {
    prepareAttributeMap();
    _attributeMap["condition"] = ParseJson(R"json({"op":"QUERY", "params":["name", "aa OR bb"], "type":"UDF"})json");

    TablePtr outputTable;
    runKernel(outputTable);

    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(2, outputTable->getRowCount());
    ColumnPtr column = outputTable->getColumn("attr1");
    ASSERT_TRUE(column != NULL);
    ColumnData<uint32_t>* attr1 = column->getColumnData<uint32_t>();
    ASSERT_TRUE(attr1 != NULL);
    ASSERT_EQ(0, attr1->get(0));
    ASSERT_EQ(1, attr1->get(1));
}

TEST_F(ScanKernelTest, testQueryWithParam) {
    prepareAttributeMap();
    _attributeMap["condition"] = ParseJson(R"json({"op":"QUERY", "params":["index_2", "a stop", "remove_stopwords:false"], "type":"UDF"})json");

    TablePtr outputTable;
    runKernel(outputTable);

    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(1, outputTable->getRowCount());
    ColumnPtr column = outputTable->getColumn("attr1");
    ASSERT_TRUE(column != NULL);
    ColumnData<uint32_t>* attr1 = column->getColumnData<uint32_t>();
    ASSERT_TRUE(attr1 != NULL);
    ASSERT_EQ(2, attr1->get(0));
}

TEST_F(ScanKernelTest, testQueryOnlyStopWord) {
    prepareAttributeMap();
    _attributeMap["condition"] = ParseJson(R"json({"op":"QUERY", "params":["stop"], "type":"UDF"})json");

    TablePtr outputTable;
    runKernel(outputTable);

    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(1, outputTable->getRowCount());
    ColumnPtr column = outputTable->getColumn("attr1");
    ASSERT_TRUE(column != NULL);
    ColumnData<uint32_t>* attr1 = column->getColumnData<uint32_t>();
    ASSERT_TRUE(attr1 != NULL);
    ASSERT_EQ(2, attr1->get(0));
}

TEST_F(ScanKernelTest, testQueryOrFilter) {
    prepareAttributeMap();
    string indexCondition = R"json({"op":"QUERY", "params":["name", "aa OR bb"], "type":"UDF"})json"; //query
    string attr1Condition = R"json({"op":">=", "params":["$attr1",2]})json"; // filter
    _attributeMap["condition"] = ParseJson("{\"op\":\"OR\", \"params\":[" + indexCondition + " , " + attr1Condition + "]}");
    _attributeMap["index_infos"] = ParseJson(R"json({"$index2":{"type":"TEXT","name":"index_2"}})json");

    TablePtr outputTable;
    runKernel(outputTable);

    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(4, outputTable->getRowCount());
    ColumnPtr column = outputTable->getColumn("attr1");
    ASSERT_TRUE(column != NULL);
    ColumnData<uint32_t>* attr1 = column->getColumnData<uint32_t>();
    ASSERT_TRUE(attr1 != NULL);
    ASSERT_EQ(0, attr1->get(0));
    ASSERT_EQ(1, attr1->get(1));
    ASSERT_EQ(2, attr1->get(2));
    ASSERT_EQ(3, attr1->get(3));
}

TEST_F(ScanKernelTest, testInitNoMemPool) {
    prepareAttributeMap();

    KernelTesterBuilder builder;
    builder.kernel("ScanKernel");
    builder.output("output0");
    builder.attrs(autil::legacy::ToJsonString(_attributeMap));
    KernelTesterPtr testerPtr(builder.build());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_TRUE(testerPtr->hasError());
    ASSERT_EQ("get mem pool resource failed", testerPtr->getErrorMessage());
}

TEST_F(ScanKernelTest, testInitNoBizResource) {
    prepareAttributeMap();

    KernelTesterBuilder builder;
    builder.resource(RESOURCE_MEMORY_POOL_URI, &_memPoolResource);
    builder.kernel("ScanKernel");
    builder.output("output0");
    builder.attrs(autil::legacy::ToJsonString(_attributeMap));
    KernelTesterPtr testerPtr(builder.build());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_TRUE(testerPtr->hasError());
    ASSERT_EQ("get sql biz resource failed", testerPtr->getErrorMessage());
}

TEST_F(ScanKernelTest, testInitNoQueryResource) {
    prepareAttributeMap();

    KernelTesterBuilder builder;
    builder.resource(RESOURCE_MEMORY_POOL_URI, &_memPoolResource);
    builder.resource("SqlBizResource", _resourceMap["SqlBizResource"]);
    builder.kernel("ScanKernel");
    builder.output("output0");
    builder.attrs(autil::legacy::ToJsonString(_attributeMap));
    KernelTesterPtr testerPtr(builder.build());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_TRUE(testerPtr->hasError());
    ASSERT_EQ("get sql query resource failed", testerPtr->getErrorMessage());
}

TEST_F(ScanKernelTest, testInitWrongTableName) {
    prepareAttributeMap();
    _attributeMap["table_name"] = string("wrong_table");
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    ASSERT_TRUE(testerPtr->hasError());
    ASSERT_EQ("table [wrong_table] not exist.", testerPtr->getErrorMessage());
}

TEST_F(ScanKernelTest, testInitScanColumnNull) {
    prepareAttributeMap();
    _attributeMap["output_fields"] = ParseJson(string(R"json(["wrong_field"])json"));
    TablePtr outputTable;
    runKernel(outputTable);
    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(4, outputTable->getRowCount());
    ColumnPtr column = outputTable->getColumn("wrong_filed");
    ASSERT_TRUE(column == NULL);
}

TEST_F(ScanKernelTest, testInitCreateScanIter) {
    prepareAttributeMap();
    _attributeMap["condition"] = string("xxx");
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    ASSERT_TRUE(testerPtr->hasError());
    ASSERT_EQ("init scan kernel failed, table [invertedTable].", testerPtr->getErrorMessage());
}

TEST_F(ScanKernelTest, testLimit) {
    prepareAttributeMap();
    _attributeMap["limit"] = Any(3);
    TablePtr outputTable;
    runKernel(outputTable);

    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(3, outputTable->getRowCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "attr1", {0, 1, 2}));
}

TEST_F(ScanKernelTest, testLimitBatchSize) {
    prepareAttributeMap();
    _attributeMap["batch_size"] = Any(2);
    _attributeMap["limit"] = Any(3);
    TablePtr outputTable;
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;

    ASSERT_TRUE(tester.compute()); // kernel compute success
    getOutputTable(tester, outputTable);
    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(2, outputTable->getRowCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "attr1", {0, 1}));

    ASSERT_TRUE(tester.compute()); // kernel compute success
    getOutputTable(tester, outputTable);
    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(1, outputTable->getRowCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "attr1", {2}));

}

TEST_F(ScanKernelTest, testInvalidAttribute) {
    prepareAttributeMap();
    _attributeMap.erase("table_name");
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    ASSERT_TRUE(tester.hasError());
    ASSERT_EQ(EC_INVALID_ATTRIBUTE, tester.getErrorCode());
    ASSERT_TRUE(string::npos != tester.getErrorMessage().find("table_name not found"));
}

TEST_F(ScanKernelTest, testOutputWithSubDoc) {
    _attributeMap.clear();
    _attributeMap["table_type"] = string("normal");
    _attributeMap["table_name"] = _tableName;
    _attributeMap["db_name"] = string("default");
    _attributeMap["catalog_name"] = string("default");
    _attributeMap["hash_fields"] = ParseJson(string(R"json(["id"])json"));
    _attributeMap["nest_table_attrs"] = ParseJson(string(R"json([{"table_name":"sub_invertedTable"}])json"));
    _attributeMap["use_nest_table"] = Any(true);
    _attributeMap["output_fields"] = ParseJson(string(R"json(["$sub_id", "$attr1"])json"));
    string subCondition = R"json({"op":">", "params":["$sub_id", 1]})json"; //query
    string attr1Condition = R"json({"op":"<=", "params":["$attr1",2]})json"; // filter
    _attributeMap["condition"] = ParseJson("{\"op\":\"AND\", \"params\":[" + subCondition + " , " + attr1Condition + "]}");
    _attributeMap["index_infos"] = ParseJson(R"json({"$index2":{"type":"TEXT","name":"index_2"}})json");
    TablePtr outputTable;
    runKernel(outputTable);
    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ("sub_id", outputTable->getColumnName(0));
    ASSERT_EQ("attr1", outputTable->getColumnName(1));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "attr1", {0, 0, 1, 2}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int64_t>(outputTable, "sub_id", {2, 3, 4, 5}));
}


TEST_F(ScanKernelTest, testConditionWithSubDoc) {
    _attributeMap.clear();
    _attributeMap["table_type"] = string("normal");
    _attributeMap["table_name"] = _tableName;
    _attributeMap["db_name"] = string("default");
    _attributeMap["catalog_name"] = string("default");
    _attributeMap["hash_fields"] = ParseJson(string(R"json(["id"])json"));
    _attributeMap["nest_table_attrs"] = ParseJson(string(R"json([{"table_name":"sub_invertedTable"}])json"));
    _attributeMap["use_nest_table"] = Any(true);
    _attributeMap["output_fields"] = ParseJson(string(R"json(["$attr1", "$sub_id"])json"));
    string subCondition = R"json({"op":">", "params":["$sub_id", 1]})json"; //query
    string attr1Condition = R"json({"op":"<=", "params":["$attr1",2]})json"; // filter
    _attributeMap["condition"] = ParseJson("{\"op\":\"AND\", \"params\":[" + subCondition + " , " + attr1Condition + "]}");
    _attributeMap["index_infos"] = ParseJson(R"json({"$index2":{"type":"TEXT","name":"index_2"}})json");

    TablePtr outputTable;
    runKernel(outputTable);

    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ("sub_id", outputTable->getColumnName(1));
    ASSERT_EQ("attr1", outputTable->getColumnName(0));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "attr1", {0, 0, 1, 2}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int64_t>(outputTable, "sub_id", {2, 3, 4, 5}));
}

TEST_F(ScanKernelTest, testConditionWithSubDoc2) {
    _attributeMap.clear();
    _attributeMap["table_type"] = string("normal");
    _attributeMap["table_name"] = _tableName;
    _attributeMap["db_name"] = string("default");
    _attributeMap["catalog_name"] = string("default");
    _attributeMap["hash_fields"] = ParseJson(string(R"json(["id"])json"));
    _attributeMap["nest_table_attrs"] = ParseJson(string(R"json([{"table_name":"sub_invertedTable"}])json"));
    _attributeMap["use_nest_table"] = Any(true);
    _attributeMap["output_fields"] = ParseJson(string(R"json(["$attr1", "$sub_id"])json"));
    string subCondition = R"json({"op":"<=", "params":["$sub_id", 1]})json"; //query
    string attr1Condition = R"json({"op":">", "params":["$attr1",2]})json"; // filter
    _attributeMap["condition"] = ParseJson("{\"op\":\"OR\", \"params\":[" + subCondition + " , " + attr1Condition + "]}");
    _attributeMap["index_infos"] = ParseJson(R"json({"$index2":{"type":"TEXT","name":"index_2"}})json");

    TablePtr outputTable;
    runKernel(outputTable);

    ASSERT_TRUE(outputTable != NULL);
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "attr1", {0, 3, 3}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int64_t>(outputTable, "sub_id", {1, 6, 7}));
}

TEST_F(ScanKernelTest, testConditionWithSubDoc3) {
    _attributeMap.clear();
    _attributeMap["table_type"] = string("normal");
    _attributeMap["table_name"] = _tableName;
    _attributeMap["db_name"] = string("default");
    _attributeMap["catalog_name"] = string("default");
    _attributeMap["hash_fields"] = ParseJson(string(R"json(["id"])json"));
    _attributeMap["nest_table_attrs"] = ParseJson(string(R"json([{"table_name":"sub_invertedTable"}])json"));
    _attributeMap["use_nest_table"] = Any(true);
    _attributeMap["output_fields"] = ParseJson(string(R"json(["$attr1", "$sub_id"])json"));
    string subCondition = R"json({"op":">", "params":["$sub_id", 2]})json"; //query
    _attributeMap["condition"] = ParseJson("{\"op\":\"NOT\", \"params\":[" + subCondition + "]}");
    _attributeMap["index_infos"] = ParseJson(R"json({"$index2":{"type":"TEXT","name":"index_2"}})json");

    TablePtr outputTable;
    runKernel(outputTable);

    ASSERT_TRUE(outputTable != NULL);
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "attr1", {0, 0}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int64_t>(outputTable, "sub_id", {1, 2}));
}

END_HA3_NAMESPACE(sql);
