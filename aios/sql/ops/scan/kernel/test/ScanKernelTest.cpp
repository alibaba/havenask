#include "sql/ops/scan/ScanKernel.h"

#include <assert.h>
#include <cstdint>
#include <ext/alloc_traits.h>
#include <iostream>
#include <limits>
#include <map>
#include <memory>
#include <stddef.h>
#include <string>
#include <utility>
#include <vector>

#include "autil/CommonMacros.h"
#include "autil/MultiValueType.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/json.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "build_service/analyzer/AnalyzerFactory.h"
#include "build_service/analyzer/AnalyzerInfos.h"
#include "build_service/analyzer/TokenizerManager.h"
#include "build_service/config/ResourceReader.h"
#include "indexlib/analyzer/AnalyzerDefine.h"
#include "indexlib/analyzer/AnalyzerInfo.h"
#include "indexlib/analyzer/IAnalyzerFactory.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/online_config.h"
#include "indexlib/partition/index_application.h"
#include "indexlib/partition/index_partition.h"
#include "indexlib/testlib/indexlib_partition_creator.h"
#include "matchdoc/MatchDocAllocator.h"
#include "navi/common.h"
#include "navi/engine/Data.h"
#include "navi/engine/Kernel.h"
#include "navi/tester/KernelTester.h"
#include "navi/tester/KernelTesterBuilder.h"
#include "navi/tester/NaviResourceHelper.h"
#include "sql/framework/PushDownOp.h"
#include "sql/ops/scan/NormalScanR.h"
#include "sql/ops/scan/ScanBase.h"
#include "sql/ops/scan/ScanPushDownR.h"
#include "sql/ops/scan/test/FakeTokenizer.h"
#include "sql/ops/test/OpTestBase.h"
#include "sql/resource/AnalyzerFactoryR.h"
#include "suez/turing/expression/util/FieldBoost.h"
#include "table/Column.h"
#include "table/ColumnData.h"
#include "table/ColumnSchema.h"
#include "table/Table.h"
#include "table/TableSchema.h"
#include "unittest/unittest.h"

using namespace std;
using namespace suez::turing;
using namespace table;
using namespace build_service::analyzer;
using namespace indexlibv2::analyzer;
using namespace navi;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;

namespace sql {

class ScanKernelTest : public OpTestBase {
public:
    ScanKernelTest() {
        _needBuildIndex = true;
        _needExprResource = true;
    }
    ~ScanKernelTest() {}

public:
    void SetUp() override {
        OpTestBase::SetUp();
        auto *naviRHelper = getNaviRHelper();
        auto analyzerR = naviRHelper->getOrCreateRes<AnalyzerFactoryR>();
        analyzerR->_factory = initAnalyzerFactory();
    }

private:
    void prepareAttributeMap() {
        _attributeMap.clear();
        _attributeMap["table_type"] = string("normal");
        _attributeMap["table_name"] = _tableName;
        _attributeMap["db_name"] = string("default");
        _attributeMap["catalog_name"] = string("default");
        _attributeMap["hash_fields"] = ParseJson(string(R"json(["id"])json"));
        _attributeMap["output_fields_internal"]
            = ParseJson(string(R"json(["$attr1", "$attr2", "$id"])json"));
        _attributeMap["use_nest_table"] = Any(false);
        _attributeMap["limit"] = Any(1000);
        _attributeMap["hash_fields"] = ParseJson(R"json(["id"])json");
    }
    KernelTesterPtr buildTester(KernelTesterBuilder &builder) {
        setResource(builder);
        builder.kernel("sql.ScanKernel");
        builder.output("output0");
        builder.attrs(autil::legacy::FastToJsonString(_attributeMap));
        builder.logLevel("DEBUG");
        return builder.build();
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
        _tester = buildTester(builder);
        ASSERT_TRUE(_tester.get());
        ASSERT_TRUE(_tester->compute()); // kernel compute success
        getOutputTable(*_tester, outputTable);
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
    indexlib::partition::IndexPartitionPtr makeIndexPartition(const std::string &rootPath,
                                                              const std::string &tableName);
    static std::string fakeAnalyzerName;

private:
    autil::legacy::json::JsonMap _attributeMap;
    KernelTesterPtr _tester;
};

std::string ScanKernelTest::fakeAnalyzerName = "fake_analyzer";

AnalyzerFactoryPtr ScanKernelTest::initAnalyzerFactory() {
    AnalyzerInfosPtr infosPtr(new AnalyzerInfos());
    unique_ptr<AnalyzerInfo> taobaoInfo(new AnalyzerInfo());
    taobaoInfo->setTokenizerConfig(TOKENIZER_TYPE, SIMPLE_ANALYZER);
    taobaoInfo->setTokenizerConfig(DELIMITER, " ");
    taobaoInfo->addStopWord(string("stop"));
    infosPtr->addAnalyzerInfo("taobao_analyzer", *taobaoInfo);

    string aliwsConfig = GET_TEST_DATA_PATH() + string("/default_aliws_conf");
    infosPtr->makeCompatibleWithOldConfig();
    TokenizerManagerPtr tokenizerManagerPtr(
        new TokenizerManager(std::make_shared<build_service::config::ResourceReader>(aliwsConfig)));
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

indexlib::partition::IndexPartitionPtr
ScanKernelTest::makeIndexPartition(const std::string &rootPath, const std::string &tableName) {
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
          "d,sub_id=6^7,sub_attr1=2^1,sub_index2=abc^ab";

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

TEST_F(ScanKernelTest, testSimpleProcess) {
    prepareAttributeMap();
    TablePtr outputTable;
    runKernel(outputTable);
    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(4, outputTable->getRowCount());
    auto column = outputTable->getColumn("attr1");
    ASSERT_TRUE(column != NULL);
    ColumnData<uint32_t> *attr1 = column->getColumnData<uint32_t>();
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
    _attributeMap["output_fields_internal"]
        = ParseJson(string(R"json(["$attr1", "$sub_attr1"])json"));
    _attributeMap["nest_table_attrs"]
        = ParseJson(string(R"json([{"table_name":"sub_invertedTable"}])json"));
    _attributeMap["use_nest_table"] = Any(true);
    TablePtr outputTable;
    runKernel(outputTable);
    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(7, outputTable->getRowCount());
    auto column = outputTable->getColumn("attr1");
    ASSERT_TRUE(column != NULL);
    ColumnData<uint32_t> *attr1 = column->getColumnData<uint32_t>();
    ASSERT_TRUE(attr1 != NULL);
    ASSERT_EQ(0, attr1->get(0));
    ASSERT_EQ(0, attr1->get(1));
    ASSERT_EQ(0, attr1->get(2));
    ASSERT_EQ(1, attr1->get(3));
    ASSERT_EQ(2, attr1->get(4));
    ASSERT_EQ(3, attr1->get(5));
    ASSERT_EQ(3, attr1->get(6));
    auto column2 = outputTable->getColumn("sub_attr1");
    ASSERT_TRUE(column2 != NULL);
    ColumnData<int32_t> *subAttr1 = column2->getColumnData<int32_t>();
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
    _attributeMap["output_fields_internal"] = ParseJson(string(R"json(["$attr1"])json"));
    _attributeMap["nest_table_attrs"]
        = ParseJson(string(R"json([{"table_name":"sub_invertedTable"}])json"));
    _attributeMap["use_nest_table"] = Any(true);
    TablePtr outputTable;
    runKernel(outputTable);
    ASSERT_TRUE(outputTable != NULL);
    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(7, outputTable->getRowCount());
    auto column = outputTable->getColumn("attr1");
    ASSERT_TRUE(column != NULL);
    ColumnData<uint32_t> *attr1 = column->getColumnData<uint32_t>();
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
    _attributeMap["output_fields_internal"] = ParseJson(string(R"json(["$attr1"])json"));
    _attributeMap["push_down_ops"] = ParseJson(R"json([
    {
        "attrs": {
            "condition": {"op":"=", "params":["$sub_attr1", 2]},
            "output_field_exprs": {}
        },
        "op_name": "CalcOp"
    }])json");
    _attributeMap["nest_table_attrs"]
        = ParseJson(string(R"json([{"table_name":"sub_invertedTable"}])json"));
    _attributeMap["use_nest_table"] = Any(true);
    TablePtr outputTable;
    runKernel(outputTable);
    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(3, outputTable->getRowCount());
    auto column = outputTable->getColumn("attr1");
    ASSERT_TRUE(column != NULL);
    ColumnData<uint32_t> *attr1 = column->getColumnData<uint32_t>();
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
    _attributeMap["output_fields_internal"]
        = ParseJson(string(R"json(["$attr1", "$sub_attr1"])json"));
    _attributeMap["push_down_ops"] = ParseJson(R"json([
    {
        "attrs": {
            "condition": {"op":"=", "params":["$sub_attr1", 2]},
            "output_field_exprs": {}
        },
        "op_name": "CalcOp"
    }])json");
    _attributeMap["nest_table_attrs"]
        = ParseJson(string(R"json([{"table_name":"sub_invertedTable"}])json"));
    _attributeMap["use_nest_table"] = Any(true);
    TablePtr outputTable;
    runKernel(outputTable);
    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(3, outputTable->getRowCount());
    auto column = outputTable->getColumn("attr1");
    ASSERT_TRUE(column != NULL);
    ColumnData<uint32_t> *attr1 = column->getColumnData<uint32_t>();
    ASSERT_TRUE(attr1 != NULL);
    ASSERT_EQ(0, attr1->get(0));
    ASSERT_EQ(0, attr1->get(1));
    ASSERT_EQ(3, attr1->get(2));

    auto column2 = outputTable->getColumn("sub_attr1");
    ASSERT_TRUE(column2 != NULL);
    ColumnData<int32_t> *subAttr1 = column2->getColumnData<int32_t>();
    ASSERT_TRUE(subAttr1 != NULL);
    ASSERT_EQ(2, subAttr1->get(0));
    ASSERT_EQ(2, subAttr1->get(1));
    ASSERT_EQ(2, subAttr1->get(2));
}

TEST_F(ScanKernelTest, testOutputFieldNotInAttributes) {
    prepareAttributeMap();
    _attributeMap["table_meta"] = ParseJson(R"json({
        "field_meta": [
            {
                "field_type": "string",
                "field_name": "$name"
            }
        ]
    })json");
    _attributeMap["output_fields_internal"]
        = ParseJson(string(R"json(["$attr1", "$attr2", "$id", "$name"])json"));
    TablePtr outputTable;
    runKernel(outputTable);

    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(4, outputTable->getRowCount());
    auto column = outputTable->getColumn("attr1");
    ASSERT_TRUE(column != NULL);
    ColumnData<uint32_t> *attr1 = column->getColumnData<uint32_t>();
    ASSERT_TRUE(attr1 != NULL);
    ASSERT_EQ(0, attr1->get(0));
    ASSERT_EQ(1, attr1->get(1));
    ASSERT_EQ(2, attr1->get(2));
    ASSERT_EQ(3, attr1->get(3));

    auto nameColumn = outputTable->getColumn("name");
    ASSERT_TRUE(nameColumn != NULL);
    ColumnData<MultiChar> *nameAttr = nameColumn->getColumnData<MultiChar>();
    ASSERT_TRUE(nameAttr != NULL);
    ASSERT_EQ(string("null"), string(nameAttr->get(0).data(), nameAttr->get(0).size()));
    ASSERT_EQ(string("null"), string(nameAttr->get(1).data(), nameAttr->get(1).size()));
    ASSERT_EQ(string("null"), string(nameAttr->get(2).data(), nameAttr->get(2).size()));
    ASSERT_EQ(string("null"), string(nameAttr->get(3).data(), nameAttr->get(3).size()));
}

TEST_F(ScanKernelTest, testOutputExprsConst) {
    prepareAttributeMap();
    _attributeMap["output_fields_internal"]
        = ParseJson(string(R"json(["$attr1", "$attr2", "$id", "$$f0"])json"));
    _attributeMap["push_down_ops"] = ParseJson(R"json([
    {
        "attrs": {
            "output_field_exprs": {"$$f0": 1}
        },
        "op_name": "CalcOp"
    }])json");
    TablePtr outputTable;
    runKernel(outputTable);

    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(4, outputTable->getRowCount());
    auto column = outputTable->getColumn("$f0");
    ASSERT_TRUE(column != NULL);
    ColumnData<int32_t> *f0 = column->getColumnData<int32_t>();
    ASSERT_TRUE(f0 != NULL);
    ASSERT_EQ(1, f0->get(0));
    ASSERT_EQ(1, f0->get(1));
    ASSERT_EQ(1, f0->get(2));
    ASSERT_EQ(1, f0->get(3));
}

TEST_F(ScanKernelTest, testOutputExprsOnlyConst) {
    prepareAttributeMap();
    _attributeMap["output_fields_internal"] = ParseJson(string(R"json(["$$f0"])json"));
    _attributeMap["push_down_ops"] = ParseJson(R"json([
    {
        "attrs": {
            "output_field_exprs": {"$$f0": 1}
        },
        "op_name": "CalcOp"
    }])json");
    TablePtr outputTable;
    runKernel(outputTable);

    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(4, outputTable->getRowCount());
    auto column = outputTable->getColumn("$f0");
    ASSERT_TRUE(column != NULL);
    ColumnData<int32_t> *f0 = column->getColumnData<int32_t>();
    ASSERT_TRUE(f0 != NULL);
    ASSERT_EQ(1, f0->get(0));
    ASSERT_EQ(1, f0->get(1));
    ASSERT_EQ(1, f0->get(2));
    ASSERT_EQ(1, f0->get(3));
}

TEST_F(ScanKernelTest, testOutputExprsConstWithType) {
    prepareAttributeMap();
    _attributeMap["output_fields_internal"]
        = ParseJson(string(R"json(["$$f0", "$$f1", "$$f2", "$$f3", "$attr1"])json"));
    _attributeMap["output_fields_internal_type"]
        = ParseJson(string(R"json(["BIGINT", "FLOAT", "DOUBLE", "INTEGER", "BIGINT"])json"));
    _attributeMap["push_down_ops"] = ParseJson(R"json([
    {
        "attrs": {
            "output_field_exprs": {"$$f0": 1, "$$f1":0.1, "$$f2":0.2, "$$f3":0, "$attr1":1}
        },
        "op_name": "CalcOp"
    }])json");
    TablePtr outputTable;
    runKernel(outputTable);

    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(4, outputTable->getRowCount());
    auto column = outputTable->getColumn("$f0");
    ASSERT_TRUE(column != NULL);
    ColumnData<int64_t> *f0 = column->getColumnData<int64_t>();
    ASSERT_TRUE(f0 != NULL);
    ASSERT_EQ(1, f0->get(0));
    ASSERT_EQ(1, f0->get(1));
    ASSERT_EQ(1, f0->get(2));
    ASSERT_EQ(1, f0->get(3));

    column = outputTable->getColumn("$f1");
    ASSERT_TRUE(column != NULL);
    ColumnData<float> *f1 = column->getColumnData<float>();
    ASSERT_TRUE(f1 != NULL);
    ASSERT_FLOAT_EQ(0.1, f1->get(0));
    ASSERT_FLOAT_EQ(0.1, f1->get(1));
    ASSERT_FLOAT_EQ(0.1, f1->get(2));
    ASSERT_FLOAT_EQ(0.1, f1->get(3));

    column = outputTable->getColumn("$f2");
    ASSERT_TRUE(column != NULL);
    ColumnData<double> *f2 = column->getColumnData<double>();
    ASSERT_TRUE(f2 != NULL);
    ASSERT_DOUBLE_EQ(0.2, f2->get(0));
    ASSERT_DOUBLE_EQ(0.2, f2->get(1));
    ASSERT_DOUBLE_EQ(0.2, f2->get(2));
    ASSERT_DOUBLE_EQ(0.2, f2->get(3));

    column = outputTable->getColumn("$f3");
    ASSERT_TRUE(column != NULL);
    ColumnData<int32_t> *f3 = column->getColumnData<int32_t>();
    ASSERT_TRUE(f3 != NULL);
    ASSERT_EQ(0, f3->get(0));
    ASSERT_EQ(0, f3->get(1));
    ASSERT_EQ(0, f3->get(2));
    ASSERT_EQ(0, f3->get(3));
    column = outputTable->getColumn("attr1");
    ASSERT_TRUE(column != NULL);
    ColumnData<uint32_t> *attr1 = column->getColumnData<uint32_t>();
    ASSERT_TRUE(attr1 != NULL);
    ASSERT_EQ(1, attr1->get(0));
    ASSERT_EQ(1, attr1->get(1));
    ASSERT_EQ(1, attr1->get(2));
    ASSERT_EQ(1, attr1->get(3));
}

TEST_F(ScanKernelTest, testOutputExprsConstWithType2) {
    prepareAttributeMap();
    _attributeMap["output_fields_internal"]
        = ParseJson(string(R"json(["$$f0", "$$f1", "$$f2"])json"));
    _attributeMap["output_fields_internal_type"]
        = ParseJson(string(R"json(["BIGINT", "BIGINT", "TINYINT"])json"));
    _attributeMap["push_down_ops"] = ParseJson(R"json([
    {
        "attrs": {
            "output_field_exprs": {"$$f0": 0, "$$f1":0, "$$f2":0}
        },
        "op_name": "CalcOp"
    }])json");
    TablePtr outputTable;
    runKernel(outputTable);
    ASSERT_TRUE(outputTable != NULL);
    cout << outputTable->getTableSchema().toString() << endl;

    ASSERT_EQ(4, outputTable->getRowCount());
    auto column = outputTable->getColumn("$f0");
    ASSERT_TRUE(column != NULL);
    ColumnData<int64_t> *f0 = column->getColumnData<int64_t>();
    ASSERT_TRUE(f0 != NULL);
    ASSERT_EQ(0, f0->get(0));
    ASSERT_EQ(0, f0->get(1));
    ASSERT_EQ(0, f0->get(2));
    ASSERT_EQ(0, f0->get(3));

    column = outputTable->getColumn("$f1");
    ASSERT_TRUE(column != NULL);
    ColumnData<int64_t> *f1 = column->getColumnData<int64_t>();
    ASSERT_TRUE(f1 != NULL);
    ASSERT_EQ(0, f1->get(0));
    ASSERT_EQ(0, f1->get(1));
    ASSERT_EQ(0, f1->get(2));
    ASSERT_EQ(0, f1->get(3));

    column = outputTable->getColumn("$f2");
    ASSERT_TRUE(column != NULL);
    ColumnData<int8_t> *f2 = column->getColumnData<int8_t>();
    ASSERT_TRUE(f2 != NULL);
    ASSERT_EQ(0, f2->get(0));
    ASSERT_EQ(0, f2->get(1));
    ASSERT_EQ(0, f2->get(2));
    ASSERT_EQ(0, f2->get(3));
}

TEST_F(ScanKernelTest, testOutputExprsConstWithSub) {
    prepareAttributeMap();
    _attributeMap["output_fields_internal"] = ParseJson(string(R"json(["$$f0"])json"));
    _attributeMap["push_down_ops"] = ParseJson(R"json([
    {
        "attrs": {
            "output_field_exprs": {"$$f0": 1}
        },
        "op_name": "CalcOp"
    }])json");
    _attributeMap["nest_table_attrs"]
        = ParseJson(string(R"json([{"table_name":"sub_invertedTable"}])json"));
    _attributeMap["use_nest_table"] = true;
    TablePtr outputTable;
    runKernel(outputTable);

    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(7, outputTable->getRowCount());
    auto column = outputTable->getColumn("$f0");
    ASSERT_TRUE(column != NULL);
    ColumnData<int32_t> *f0 = column->getColumnData<int32_t>();
    ASSERT_TRUE(f0 != NULL);
    ASSERT_EQ(1, f0->get(0));
    ASSERT_EQ(1, f0->get(1));
    ASSERT_EQ(1, f0->get(2));
    ASSERT_EQ(1, f0->get(3));
    ASSERT_EQ(1, f0->get(4));
    ASSERT_EQ(1, f0->get(5));
    ASSERT_EQ(1, f0->get(6));
}

TEST_F(ScanKernelTest, testOutputExprsInnerDocId) {
    prepareAttributeMap();
    _attributeMap["output_fields_internal"] = ParseJson(string(R"json(["$__inner_docid"])json"));
    TablePtr outputTable;
    runKernel(outputTable);

    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(4, outputTable->getRowCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(outputTable, "__inner_docid", {0, 1, 2, 3}));
}

TEST_F(ScanKernelTest, testOutputExprsWithAs) {
    prepareAttributeMap();
    _attributeMap["output_fields_internal"]
        = ParseJson(string(R"json(["$attr1", "$attr2", "$id", "$f0"])json"));
    _attributeMap["push_down_ops"] = ParseJson(R"json([
    {
        "attrs": {
            "output_field_exprs": {"$f0":{"op":"+", "params":["$attr1", 1]}}
        },
        "op_name": "CalcOp"
    }])json");
    TablePtr outputTable;
    runKernel(outputTable);

    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(4, outputTable->getRowCount());
    auto column = outputTable->getColumn("f0");
    ASSERT_TRUE(column != NULL);
    ColumnData<uint32_t> *f0 = column->getColumnData<uint32_t>();
    ASSERT_TRUE(f0 != NULL);
    ASSERT_EQ(1, f0->get(0));
    ASSERT_EQ(2, f0->get(1));
    ASSERT_EQ(3, f0->get(2));
    ASSERT_EQ(4, f0->get(3));
}

TEST_F(ScanKernelTest, testOutputExprsWithAsRename) {
    prepareAttributeMap();
    _attributeMap["output_fields_internal"]
        = ParseJson(string(R"json(["$attr1", "$b", "$c", "$attr2"])json"));
    _attributeMap["push_down_ops"] = ParseJson(R"json([
    {
        "attrs": {
            "output_field_exprs": {"$b": "$attr1","$c": "$attr1"}
        },
        "op_name": "CalcOp"
    }])json");
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
    _attributeMap["output_fields_internal"]
        = ParseJson(string(R"json(["$attr1", "$attr2", "$id", "$f0"])json"));
    _attributeMap["push_down_ops"] = ParseJson(R"json([
    {
        "attrs": {
            "output_field_exprs": {"$f0":{"op":"+", "params":["$attr1_null", 1]}}
        },
        "op_name": "CalcOp"
    }])json");
    TablePtr outputTable;
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    ASSERT_FALSE(tester.compute());
}

TEST_F(ScanKernelTest, testOutputTwoExprsWithAs) {
    prepareAttributeMap();
    _attributeMap["output_fields_internal"]
        = ParseJson(string(R"json(["$attr1", "$attr2", "$id", "$f0"])json"));
    _attributeMap["push_down_ops"] = ParseJson(R"json([
    {
        "attrs": {
            "output_field_exprs": {"$f0":{"op":"*", "params":[{"op":"+", "params":["$attr1", 1]}, 2]}}
        },
        "op_name": "CalcOp"
    }])json");
    TablePtr outputTable;
    runKernel(outputTable);

    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(4, outputTable->getRowCount());
    auto column = outputTable->getColumn("f0");
    ASSERT_TRUE(column != NULL);
    ColumnData<uint32_t> *f0 = column->getColumnData<uint32_t>();
    ASSERT_TRUE(f0 != NULL);
    ASSERT_EQ(2, f0->get(0));
    ASSERT_EQ(4, f0->get(1));
    ASSERT_EQ(6, f0->get(2));
    ASSERT_EQ(8, f0->get(3));
}

TEST_F(ScanKernelTest, testOutputTwoExprsWithAs2) {
    prepareAttributeMap();
    _attributeMap["output_fields_internal"]
        = ParseJson(string(R"json(["$attr1", "$attr1_1"])json"));
    _attributeMap["push_down_ops"] = ParseJson(R"json([
    {
        "attrs": {
            "output_field_exprs": {"$attr1_1":"$attr1"}
        },
        "op_name": "CalcOp"
    }])json");

    TablePtr outputTable;
    runKernel(outputTable);

    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(4, outputTable->getRowCount());
    auto column = outputTable->getColumn("attr1");
    ASSERT_TRUE(column != NULL);
    ColumnData<uint32_t> *attr1 = column->getColumnData<uint32_t>();
    ASSERT_TRUE(attr1 != NULL);
    ASSERT_EQ(0, attr1->get(0));
    ASSERT_EQ(1, attr1->get(1));
    ASSERT_EQ(2, attr1->get(2));
    ASSERT_EQ(3, attr1->get(3));

    auto column2 = outputTable->getColumn("attr1_1");
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
    _attributeMap["push_down_ops"] = ParseJson(R"json([
    {
        "attrs": {
            "output_field_exprs": {"$attr1": {"op":"CAST","cast_type":"BIGINT","type":"UDF","params":[12]}}
        },
        "op_name": "CalcOp"
    }])json");

    TablePtr outputTable;
    runKernel(outputTable);

    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(4, outputTable->getRowCount());
    auto column = outputTable->getColumn("attr1");
    ASSERT_TRUE(column != NULL);
    ColumnData<uint32_t> *attr1 = column->getColumnData<uint32_t>();
    ASSERT_TRUE(attr1 != NULL);
    ASSERT_EQ(12, attr1->get(0));
    ASSERT_EQ(12, attr1->get(1));
    ASSERT_EQ(12, attr1->get(2));
    ASSERT_EQ(12, attr1->get(3));
}

TEST_F(ScanKernelTest, testOutputExprsNormal) {
    prepareAttributeMap();
    _attributeMap["push_down_ops"] = ParseJson(R"json([
    {
        "attrs": {
            "output_field_exprs": {"$attr1":{"op":"+", "params":["$attr1", 1]}}
        },
        "op_name": "CalcOp"
    }])json");

    TablePtr outputTable;
    runKernel(outputTable);

    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(4, outputTable->getRowCount());
    auto column = outputTable->getColumn("attr1");
    ASSERT_TRUE(column != NULL);
    ColumnData<uint32_t> *attr1 = column->getColumnData<uint32_t>();
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
    _attributeMap["push_down_ops"] = ParseJson(R"json([
    {
        "attrs": {
            "condition": {"op":"LIKE", "params":["$index2", "a"]},
            "output_field_exprs": {}
        },
        "op_name": "CalcOp"
    }])json");
    _attributeMap["table_meta"] = ParseJson(R"json({
        "field_meta": [
            {
                "field_type": "TEXT",
                "field_name": "$index2",
                "index_name": "index_2",
                "index_type": "TEXT"
            }
        ]
    })json");

    TablePtr outputTable;
    runKernel(outputTable);

    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(3, outputTable->getRowCount());
    auto column = outputTable->getColumn("attr1");
    ASSERT_TRUE(column != NULL);
    ColumnData<uint32_t> *attr1 = column->getColumnData<uint32_t>();
    ASSERT_TRUE(attr1 != NULL);
    ASSERT_EQ(0, attr1->get(0));
    ASSERT_EQ(1, attr1->get(1));
    ASSERT_EQ(2, attr1->get(2));
}

TEST_F(ScanKernelTest, testFilter) {
    prepareAttributeMap();
    _attributeMap["push_down_ops"] = ParseJson(R"json([
    {
        "attrs": {
            "condition": {"op":"=", "params":["$attr1", 1]},
            "output_field_exprs": {}
        },
        "op_name": "CalcOp"
    }])json");
    TablePtr outputTable;
    runKernel(outputTable);

    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(1, outputTable->getRowCount());
    auto column = outputTable->getColumn("attr1");
    ASSERT_TRUE(column != NULL);
    ColumnData<uint32_t> *attr1 = column->getColumnData<uint32_t>();
    ASSERT_TRUE(attr1 != NULL);
    ASSERT_EQ(1, attr1->get(0));
}

TEST_F(ScanKernelTest, testInFilter) {
    prepareAttributeMap();
    _attributeMap["push_down_ops"] = ParseJson(R"json([
    {
        "attrs": {
            "condition": {"op":"IN", "params":["$attr1", 1]},
            "output_field_exprs": {}
        },
        "op_name": "CalcOp"
    }])json");
    TablePtr outputTable;
    runKernel(outputTable);

    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(1, outputTable->getRowCount());
    auto column = outputTable->getColumn("attr1");
    ASSERT_TRUE(column != NULL);
    ColumnData<uint32_t> *attr1 = column->getColumnData<uint32_t>();
    ASSERT_TRUE(attr1 != NULL);
    ASSERT_EQ(1, attr1->get(0));
}

TEST_F(ScanKernelTest, testHaInFilter) {
    prepareAttributeMap();
    _attributeMap["push_down_ops"] = ParseJson(R"json([
    {
        "attrs": {
            "condition": {"op":"ha_in", "params":["$attr1", "1"], "type":"UDF"},
            "output_field_exprs": {}
        },
        "op_name": "CalcOp"
    }])json");

    TablePtr outputTable;
    runKernel(outputTable);

    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(1, outputTable->getRowCount());
    auto column = outputTable->getColumn("attr1");
    ASSERT_TRUE(column != NULL);
    ColumnData<uint32_t> *attr1 = column->getColumnData<uint32_t>();
    ASSERT_TRUE(attr1 != NULL);
    ASSERT_EQ(1, attr1->get(0));
}

TEST_F(ScanKernelTest, testHaInToQuery) {
    prepareAttributeMap();
    _attributeMap["push_down_ops"] = ParseJson(R"json([
    {
        "attrs": {
            "condition": {"op":"ha_in", "params":["$name", "aa"], "type":"UDF"},
            "output_field_exprs": {}
        },
        "op_name": "CalcOp"
    }])json");
    _attributeMap["table_meta"] = ParseJson(R"json({
        "field_meta": [
            {
                "field_type": "STRING",
                "field_name": "name",
                "index_name": "name",
                "index_type": "STRING"
            }
        ]
    })json");

    TablePtr outputTable;
    runKernel(outputTable);

    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(1, outputTable->getRowCount());
    auto column = outputTable->getColumn("attr1");
    ASSERT_TRUE(column != NULL);
    ColumnData<uint32_t> *attr1 = column->getColumnData<uint32_t>();
    ASSERT_TRUE(attr1 != NULL);
    ASSERT_EQ(0, attr1->get(0));
}

TEST_F(ScanKernelTest, testComplexCondition) {
    prepareAttributeMap();
    _attributeMap["push_down_ops"] = ParseJson(R"json([
    {
        "attrs": {
            "condition": {"op":"=", "params":[{"op":"+", "params":["$attr1", "$attr1"]}, 2]},
            "output_field_exprs": {}
        },
        "op_name": "CalcOp"
    }])json");

    TablePtr outputTable;
    runKernel(outputTable);

    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(1, outputTable->getRowCount());
    auto column = outputTable->getColumn("attr1");
    ASSERT_TRUE(column != NULL);
    ColumnData<uint32_t> *attr1 = column->getColumnData<uint32_t>();
    ASSERT_TRUE(attr1 != NULL);
    ASSERT_EQ(1, attr1->get(0));
}

TEST_F(ScanKernelTest, testLikeQuery) {
    prepareAttributeMap();
    _attributeMap["push_down_ops"] = ParseJson(R"json([
    {
        "attrs": {
            "condition": {"op":"LIKE", "params":["$index2", "a"]},
            "output_field_exprs": {}
        },
        "op_name": "CalcOp"
    }])json");
    _attributeMap["table_meta"] = ParseJson(R"json({
        "field_meta": [
            {
                "field_type": "TEXT",
                "field_name": "$index2",
                "index_name": "index_2",
                "index_type": "TEXT"
            }
        ]
    })json");

    TablePtr outputTable;
    runKernel(outputTable);

    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(3, outputTable->getRowCount());
    auto column = outputTable->getColumn("attr1");
    ASSERT_TRUE(column != NULL);
    ColumnData<uint32_t> *attr1 = column->getColumnData<uint32_t>();
    ASSERT_TRUE(attr1 != NULL);
    ASSERT_EQ(0, attr1->get(0));
    ASSERT_EQ(1, attr1->get(1));
    ASSERT_EQ(2, attr1->get(2));
}

TEST_F(ScanKernelTest, testLikeQueryAndFilter) {
    prepareAttributeMap();
    _attributeMap["push_down_ops"] = ParseJson(R"json([
    {
        "attrs": {
            "condition": {"op":"AND", "params":[
                {"op":"LIKE", "params":["$index2", "a"]},
                {"op":">=", "params":["$attr1", 1]}
            ]},
            "output_field_exprs": {}
        },
        "op_name": "CalcOp"
    }])json");
    _attributeMap["table_meta"] = ParseJson(R"json({
        "field_meta": [
            {
                "field_type": "TEXT",
                "field_name": "$index2",
                "index_name": "index_2",
                "index_type": "TEXT"
            }
        ]
    })json");

    TablePtr outputTable;
    runKernel(outputTable);

    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(2, outputTable->getRowCount());
    auto column = outputTable->getColumn("attr1");
    ASSERT_TRUE(column != NULL);
    ColumnData<uint32_t> *attr1 = column->getColumnData<uint32_t>();
    ASSERT_TRUE(attr1 != NULL);
    ASSERT_EQ(1, attr1->get(0));
    ASSERT_EQ(2, attr1->get(1));
}

TEST_F(ScanKernelTest, testLikeQueryAndLikeQuery) {
    prepareAttributeMap();
    _attributeMap["push_down_ops"] = ParseJson(R"json([
    {
        "attrs": {
            "condition": {"op":"AND", "params":[
                {"op":"LIKE", "params":["$index2", "a"]},
                {"op":"LIKE", "params":["$index2", "c"]},
                {"op":">", "params":["$attr1", 1]}
            ]},
            "output_field_exprs": {}
        },
        "op_name": "CalcOp"
    }])json");
    _attributeMap["table_meta"] = ParseJson(R"json({
        "field_meta": [
            {
                "field_type": "TEXT",
                "field_name": "$index2",
                "index_name": "index_2",
                "index_type": "TEXT"
            }
        ]
    })json");

    TablePtr outputTable;
    runKernel(outputTable);

    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(1, outputTable->getRowCount());
    auto column = outputTable->getColumn("attr1");
    ASSERT_TRUE(column != NULL);
    ColumnData<uint32_t> *attr1 = column->getColumnData<uint32_t>();
    ASSERT_TRUE(attr1 != NULL);
    ASSERT_EQ(2, attr1->get(0));
}

TEST_F(ScanKernelTest, testFilterAndFilter) {
    prepareAttributeMap();
    _attributeMap["push_down_ops"] = ParseJson(R"json([
    {
        "attrs": {
            "condition": {"op":"AND", "params":[
                {"op":">=", "params":["$attr1", 1]},
                {"op":"<=", "params":["$attr1", 2]}
            ]},
            "output_field_exprs": {}
        },
        "op_name": "CalcOp"
    }])json");

    _attributeMap["table_meta"] = ParseJson(R"json({
        "field_meta": [
            {
                "field_type": "TEXT",
                "field_name": "$index2",
                "index_name": "index_2",
                "index_type": "TEXT"
            }
        ]
    })json");

    TablePtr outputTable;
    runKernel(outputTable);

    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(2, outputTable->getRowCount());
    auto column = outputTable->getColumn("attr1");
    ASSERT_TRUE(column != NULL);
    ColumnData<uint32_t> *attr1 = column->getColumnData<uint32_t>();
    ASSERT_TRUE(attr1 != NULL);
    ASSERT_EQ(1, attr1->get(0));
    ASSERT_EQ(2, attr1->get(1));
}

TEST_F(ScanKernelTest, testTermQueryOrFilter) {
    prepareAttributeMap();
    _attributeMap["push_down_ops"] = ParseJson(R"json([
    {
        "attrs": {
            "condition": {"op":"OR", "params":[
                {"op":"=", "params":["$id",1]},
                {"op":">=", "params":["$attr1",2]}
            ]},
            "output_field_exprs": {}
        },
        "op_name": "CalcOp"
    }])json");

    _attributeMap["table_meta"] = ParseJson(R"json({
        "field_meta": [
            {
                "field_type": "NUMBER",
                "field_name": "$id",
                "index_name": "id",
                "index_type": "NUMBER"
            }
        ]
    })json");

    TablePtr outputTable;
    runKernel(outputTable);

    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(3, outputTable->getRowCount());
    auto column = outputTable->getColumn("attr1");
    ASSERT_TRUE(column != NULL);
    ColumnData<uint32_t> *attr1 = column->getColumnData<uint32_t>();
    ASSERT_TRUE(attr1 != NULL);
    ASSERT_EQ(0, attr1->get(0));
    ASSERT_EQ(2, attr1->get(1));
    ASSERT_EQ(3, attr1->get(2));
}

TEST_F(ScanKernelTest, testLikeQueryOrLikeQuery) {
    prepareAttributeMap();
    _attributeMap["push_down_ops"] = ParseJson(R"json([
    {
        "attrs": {
            "condition": {"op":"OR", "params":[
                {"op":"LIKE", "params":["$index2", "b"]},
                {"op":"LIKE", "params":["$index2", "d"]}
            ]},
            "output_field_exprs": {}
        },
        "op_name": "CalcOp"
    }])json");

    _attributeMap["table_meta"] = ParseJson(R"json({
        "field_meta": [
            {
                "field_type": "TEXT",
                "field_name": "$index2",
                "index_name": "index_2",
                "index_type": "TEXT"
            }
        ]
    })json");

    TablePtr outputTable;
    runKernel(outputTable);

    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(3, outputTable->getRowCount());
    auto column = outputTable->getColumn("attr1");
    ASSERT_TRUE(column != NULL);
    ColumnData<uint32_t> *attr1 = column->getColumnData<uint32_t>();
    ASSERT_TRUE(attr1 != NULL);
    ASSERT_EQ(1, attr1->get(0));
    ASSERT_EQ(2, attr1->get(1));
    ASSERT_EQ(3, attr1->get(2));
}

TEST_F(ScanKernelTest, testFilterOrFilter) {
    prepareAttributeMap();
    _attributeMap["push_down_ops"] = ParseJson(R"json([
    {
        "attrs": {
            "condition": {"op":"OR", "params":[
                {"op":">=", "params":["$attr1", 1]},
                {"op":"<=", "params":["$attr1", 2]}
            ]},
            "output_field_exprs": {}
        },
        "op_name": "CalcOp"
    }])json");

    TablePtr outputTable;
    runKernel(outputTable);

    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(4, outputTable->getRowCount());
    auto column = outputTable->getColumn("attr1");
    ASSERT_TRUE(column != NULL);
    ColumnData<uint32_t> *attr1 = column->getColumnData<uint32_t>();
    ASSERT_TRUE(attr1 != NULL);
    ASSERT_EQ(0, attr1->get(0));
    ASSERT_EQ(1, attr1->get(1));
    ASSERT_EQ(2, attr1->get(2));
    ASSERT_EQ(3, attr1->get(3));
}

TEST_F(ScanKernelTest, testOrOnAnd) {
    prepareAttributeMap();
    _attributeMap["push_down_ops"] = ParseJson(R"json([
    {
        "attrs": {
            "condition": {
                "op":"OR",
                "params":[
                    {
                        "op": "AND",
                        "params": [
                            {"op":">=", "params":["$attr1", 1]},
                            {"op":"<=", "params":["$attr1", 2]}
                        ]
                    },
                    {"op":"=", "params":["$id", 4]}
                ]
            },
            "output_field_exprs": {}
        },
        "op_name": "CalcOp"
    }])json");

    _attributeMap["table_meta"] = ParseJson(R"json({
        "field_meta": [
            {
                "field_type": "NUMBER",
                "field_name": "$id",
                "index_name": "id",
                "index_type": "NUMBER"
            }
        ]
    })json");

    TablePtr outputTable;
    runKernel(outputTable);

    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(3, outputTable->getRowCount());
    auto column = outputTable->getColumn("attr1");
    ASSERT_TRUE(column != NULL);
    ColumnData<uint32_t> *attr1 = column->getColumnData<uint32_t>();
    ASSERT_TRUE(attr1 != NULL);
    ASSERT_EQ(1, attr1->get(0));
    ASSERT_EQ(2, attr1->get(1));
    ASSERT_EQ(3, attr1->get(2));
}

TEST_F(ScanKernelTest, testNotCondition) {
    prepareAttributeMap();
    _attributeMap["push_down_ops"] = ParseJson(R"json([
    {
        "attrs": {
            "condition": {"op": "NOT", "params": [{"op":"=", "params":["$attr1", 1]}]},
            "output_field_exprs": {}
        },
        "op_name": "CalcOp"
    }])json");

    TablePtr outputTable;
    runKernel(outputTable);

    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(3, outputTable->getRowCount());
    auto column = outputTable->getColumn("attr1");
    ASSERT_TRUE(column != NULL);
    ColumnData<uint32_t> *attr1 = column->getColumnData<uint32_t>();
    ASSERT_TRUE(attr1 != NULL);
    ASSERT_EQ(0, attr1->get(0));
    ASSERT_EQ(2, attr1->get(1));
    ASSERT_EQ(3, attr1->get(2));
}

TEST_F(ScanKernelTest, testNotQueryCondition) {
    prepareAttributeMap();
    _attributeMap["push_down_ops"] = ParseJson(R"json([
    {
        "attrs": {
            "condition": {"op": "NOT", "params": [{"op":"=", "params":["$attr2", 1]}]},
            "output_field_exprs": {}
        },
        "op_name": "CalcOp"
    }])json");

    TablePtr outputTable;
    runKernel(outputTable);
    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(3, outputTable->getRowCount());
    auto column = outputTable->getColumn("attr1");
    ASSERT_TRUE(column != NULL);
    ColumnData<uint32_t> *attr1 = column->getColumnData<uint32_t>();
    ASSERT_TRUE(attr1 != NULL);
    ASSERT_EQ(0, attr1->get(0));
    ASSERT_EQ(2, attr1->get(1));
    ASSERT_EQ(3, attr1->get(2));
}

TEST_F(ScanKernelTest, testNotNEQueryCondition) {
    prepareAttributeMap();
    _attributeMap["push_down_ops"] = ParseJson(R"json([
    {
        "attrs": {
            "condition": {"op": "NOT", "params": [{"op":"!=", "params":["$attr2", 1]}]},
            "output_field_exprs": {}
        },
        "op_name": "CalcOp"
    }])json");

    TablePtr outputTable;
    runKernel(outputTable);
    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(1, outputTable->getRowCount());
    auto column = outputTable->getColumn("attr1");
    ASSERT_TRUE(column != NULL);
    ColumnData<uint32_t> *attr1 = column->getColumnData<uint32_t>();
    ASSERT_TRUE(attr1 != NULL);
    ASSERT_EQ(1, attr1->get(0));
}

TEST_F(ScanKernelTest, testNotOnAndCondition) {
    prepareAttributeMap();
    _attributeMap["push_down_ops"] = ParseJson(R"json([
    {
        "attrs": {
            "condition": {
                "op":"NOT",
                "params":[
                    {
                        "op": "AND",
                        "params": [
                            {"op":"LIKE", "params":["$index2", "a"]},
                            {"op":">=", "params":["$attr1", 1]}
                        ]
                    }
                ]
            },
            "output_field_exprs": {}
        },
        "op_name": "CalcOp"
    }])json");

    _attributeMap["table_meta"] = ParseJson(R"json({
        "field_meta": [
            {
                "field_type": "TEXT",
                "field_name": "$index2",
                "index_name": "index_2",
                "index_type": "TEXT"
            }
        ]
    })json");

    TablePtr outputTable;
    runKernel(outputTable);

    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(2, outputTable->getRowCount());
    auto column = outputTable->getColumn("attr1");
    ASSERT_TRUE(column != NULL);
    ColumnData<uint32_t> *attr1 = column->getColumnData<uint32_t>();
    ASSERT_TRUE(attr1 != NULL);
    ASSERT_EQ(0, attr1->get(0));
    ASSERT_EQ(3, attr1->get(1));
}

TEST_F(ScanKernelTest, testNotOnOrCondition) {
    prepareAttributeMap();
    _attributeMap["push_down_ops"] = ParseJson(R"json([
    {
        "attrs": {
            "condition": {
                "op":"NOT",
                "params":[
                    {
                        "op": "OR",
                        "params": [
                            {"op":"=", "params":["$id",1]},
                            {"op":">=", "params":["$attr1",2]}
                        ]
                    }
                ]
            },
            "output_field_exprs": {}
        },
        "op_name": "CalcOp"
    }])json");

    _attributeMap["table_meta"] = ParseJson(R"json({
        "field_meta": [
            {
                "field_type": "NUMBER",
                "field_name": "$id",
                "index_name": "id",
                "index_type": "NUMBER"
            }
        ]
    })json");

    TablePtr outputTable;
    runKernel(outputTable);

    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(1, outputTable->getRowCount());
    auto column = outputTable->getColumn("attr1");
    ASSERT_TRUE(column != NULL);
    ColumnData<uint32_t> *attr1 = column->getColumnData<uint32_t>();
    ASSERT_TRUE(attr1 != NULL);
    ASSERT_EQ(1, attr1->get(0));
}

TEST_F(ScanKernelTest, testContainToQuery) {
    prepareAttributeMap();
    _attributeMap["push_down_ops"] = ParseJson(R"json([
    {
        "attrs": {
            "condition": {"op":"contain", "params":["$name", "bb"], "type":"UDF"},
            "output_field_exprs": {}
        },
        "op_name": "CalcOp"
    }])json");

    _attributeMap["table_meta"] = ParseJson(R"json({
        "field_meta": [
            {
                "field_type": "STRING",
                "field_name": "$name",
                "index_name": "name",
                "index_type": "STRING"
            }
        ]
    })json");

    TablePtr outputTable;
    runKernel(outputTable);

    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(1, outputTable->getRowCount());
    auto column = outputTable->getColumn("attr1");
    ASSERT_TRUE(column != NULL);
    ColumnData<uint32_t> *attr1 = column->getColumnData<uint32_t>();
    ASSERT_TRUE(attr1 != NULL);
    ASSERT_EQ(1, attr1->get(0));
}

TEST_F(ScanKernelTest, testContainToNumberQuery) {
    prepareAttributeMap();
    _attributeMap["push_down_ops"] = ParseJson(R"json([
    {
        "attrs": {
            "condition": {"op":"contain", "params":["$id", "2"], "type":"UDF"},
            "output_field_exprs": {}
        },
        "op_name": "CalcOp"
    }])json");

    _attributeMap["table_meta"] = ParseJson(R"json({
        "field_meta": [
            {
                "field_type": "NUMBER",
                "field_name": "$id",
                "index_name": "id",
                "index_type": "NUMBER"
            }
        ]
    })json");

    TablePtr outputTable;
    runKernel(outputTable);

    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(1, outputTable->getRowCount());
    auto column = outputTable->getColumn("attr1");
    ASSERT_TRUE(column != NULL);
    ColumnData<uint32_t> *attr1 = column->getColumnData<uint32_t>();
    ASSERT_TRUE(attr1 != NULL);
    ASSERT_EQ(1, attr1->get(0));
}

TEST_F(ScanKernelTest, testMatchQuery) {
    prepareAttributeMap();
    _attributeMap["push_down_ops"] = ParseJson(R"json([
    {
        "attrs": {
            "condition": {"op":"MATCHINDEX", "params":["a"], "type":"UDF"},
            "output_field_exprs": {}
        },
        "op_name": "CalcOp"
    }])json");

    TablePtr outputTable;
    runKernel(outputTable);

    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(3, outputTable->getRowCount());
    auto column = outputTable->getColumn("attr1");
    ASSERT_TRUE(column != NULL);
    ColumnData<uint32_t> *attr1 = column->getColumnData<uint32_t>();
    ASSERT_TRUE(attr1 != NULL);
    ASSERT_EQ(0, attr1->get(0));
    ASSERT_EQ(1, attr1->get(1));
    ASSERT_EQ(2, attr1->get(2));
}

TEST_F(ScanKernelTest, testMatchQueryEmpty) {
    prepareAttributeMap();
    _attributeMap["push_down_ops"] = ParseJson(R"json([
    {
        "attrs": {
            "condition": {"op":"MATCHINDEX", "params":[""], "type":"UDF"},
            "output_field_exprs": {}
        },
        "op_name": "CalcOp"
    }])json");

    TablePtr outputTable;
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    ASSERT_FALSE(tester.compute()); // kernel compute success
}

TEST_F(ScanKernelTest, testMatchQueryToken) {
    prepareAttributeMap();
    _attributeMap["push_down_ops"] = ParseJson(R"json([
    {
        "attrs": {
            "condition": {"op":"MATCHINDEX", "params":["a c"], "type":"UDF"},
            "output_field_exprs": {}
        },
        "op_name": "CalcOp"
    }])json");

    TablePtr outputTable;
    runKernel(outputTable);

    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(2, outputTable->getRowCount());
    auto column = outputTable->getColumn("attr1");
    ASSERT_TRUE(column != NULL);
    ColumnData<uint32_t> *attr1 = column->getColumnData<uint32_t>();
    ASSERT_TRUE(attr1 != NULL);
    ASSERT_EQ(1, attr1->get(0));
    ASSERT_EQ(2, attr1->get(1));
}

TEST_F(ScanKernelTest, testMatchQueryTokenStopWord) {
    prepareAttributeMap();
    _attributeMap["push_down_ops"] = ParseJson(R"json([
    {
        "attrs": {
            "condition": {"op":"MATCHINDEX", "params":["a c stop"], "type":"UDF"},
            "output_field_exprs": {}
        },
        "op_name": "CalcOp"
    }])json");

    TablePtr outputTable;
    runKernel(outputTable);

    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(2, outputTable->getRowCount());
    auto column = outputTable->getColumn("attr1");
    ASSERT_TRUE(column != NULL);
    ColumnData<uint32_t> *attr1 = column->getColumnData<uint32_t>();
    ASSERT_TRUE(attr1 != NULL);
    ASSERT_EQ(1, attr1->get(0));
    ASSERT_EQ(2, attr1->get(1));
}

TEST_F(ScanKernelTest, testMatchQueryWithIndexName) {
    prepareAttributeMap();
    _attributeMap["push_down_ops"] = ParseJson(R"json([
    {
        "attrs": {
            "condition": {"op":"MATCHINDEX", "params":["name", "aa"], "type":"UDF"},
            "output_field_exprs": {}
        },
        "op_name": "CalcOp"
    }])json");

    TablePtr outputTable;
    runKernel(outputTable);

    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(1, outputTable->getRowCount());
    auto column = outputTable->getColumn("attr1");
    ASSERT_TRUE(column != NULL);
    ColumnData<uint32_t> *attr1 = column->getColumnData<uint32_t>();
    ASSERT_TRUE(attr1 != NULL);
    ASSERT_EQ(0, attr1->get(0));
}

TEST_F(ScanKernelTest, testMatchQueryWithParam) {
    prepareAttributeMap();
    _attributeMap["push_down_ops"] = ParseJson(R"json([
    {
        "attrs": {
            "condition": {"op":"MATCHINDEX", "params":["index_2", "a stop", "remove_stopwords:false"], "type":"UDF"},
            "output_field_exprs": {}
        },
        "op_name": "CalcOp"
    }])json");

    TablePtr outputTable;
    runKernel(outputTable);

    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(1, outputTable->getRowCount());
    auto column = outputTable->getColumn("attr1");
    ASSERT_TRUE(column != NULL);
    ColumnData<uint32_t> *attr1 = column->getColumnData<uint32_t>();
    ASSERT_TRUE(attr1 != NULL);
    ASSERT_EQ(2, attr1->get(0));
}

TEST_F(ScanKernelTest, testMatchOrFilter) {
    prepareAttributeMap();
    _attributeMap["push_down_ops"] = ParseJson(R"json([
    {
        "attrs": {
            "condition": {"op":"OR", "params":[
                {"op":"MATCHINDEX", "params":["a"], "type":"UDF"},
                {"op":">=", "params":["$attr1", 2]}
            ]},
            "output_field_exprs": {}
        },
        "op_name": "CalcOp"
    }])json");

    _attributeMap["table_meta"] = ParseJson(R"json({
        "field_meta": [
            {
                "field_type": "NUMBER",
                "field_name": "$id",
                "index_name": "id",
                "index_type": "NUMBER"
            }
        ]
    })json");

    TablePtr outputTable;
    runKernel(outputTable);

    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(4, outputTable->getRowCount());
    auto column = outputTable->getColumn("attr1");
    ASSERT_TRUE(column != NULL);
    ColumnData<uint32_t> *attr1 = column->getColumnData<uint32_t>();
    ASSERT_TRUE(attr1 != NULL);
    ASSERT_EQ(0, attr1->get(0));
    ASSERT_EQ(1, attr1->get(1));
    ASSERT_EQ(2, attr1->get(2));
    ASSERT_EQ(3, attr1->get(3));
}

TEST_F(ScanKernelTest, testQuery) {
    prepareAttributeMap();
    _attributeMap["push_down_ops"] = ParseJson(R"json([
    {
        "attrs": {
            "condition": {"op":"QUERY", "params":["a OR b c"], "type":"UDF"},
            "output_field_exprs": {}
        },
        "op_name": "CalcOp"
    }])json");

    TablePtr outputTable;
    runKernel(outputTable);

    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(3, outputTable->getRowCount());
    auto column = outputTable->getColumn("attr1");
    ASSERT_TRUE(column != NULL);
    ColumnData<uint32_t> *attr1 = column->getColumnData<uint32_t>();
    ASSERT_TRUE(attr1 != NULL);
    ASSERT_EQ(1, attr1->get(0));
    ASSERT_EQ(2, attr1->get(1));
    ASSERT_EQ(3, attr1->get(2));
}

TEST_F(ScanKernelTest, testQueryEmpty) {
    prepareAttributeMap();
    _attributeMap["push_down_ops"] = ParseJson(R"json([
    {
        "attrs": {
            "condition": {"op":"QUERY", "params":[""], "type":"UDF"},
            "output_field_exprs": {}
        },
        "op_name": "CalcOp"
    }])json");

    TablePtr outputTable;
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    ASSERT_FALSE(tester.compute()); // kernel compute failed
}

TEST_F(ScanKernelTest, testQueryDefaultIndex) {
    prepareAttributeMap();
    _attributeMap["push_down_ops"] = ParseJson(R"json([
    {
        "attrs": {
            "condition": {"op":"QUERY", "params":["name", "aa OR bb"], "type":"UDF"},
            "output_field_exprs": {}
        },
        "op_name": "CalcOp"
    }])json");

    TablePtr outputTable;
    runKernel(outputTable);

    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(2, outputTable->getRowCount());
    auto column = outputTable->getColumn("attr1");
    ASSERT_TRUE(column != NULL);
    ColumnData<uint32_t> *attr1 = column->getColumnData<uint32_t>();
    ASSERT_TRUE(attr1 != NULL);
    ASSERT_EQ(0, attr1->get(0));
    ASSERT_EQ(1, attr1->get(1));
}

TEST_F(ScanKernelTest, testQueryWithParam) {
    prepareAttributeMap();
    _attributeMap["push_down_ops"] = ParseJson(R"json([
    {
        "attrs": {
            "condition": {"op":"QUERY", "params":["index_2", "a stop", "remove_stopwords:false"], "type":"UDF"},
            "output_field_exprs": {}
        },
        "op_name": "CalcOp"
    }])json");

    TablePtr outputTable;
    runKernel(outputTable);

    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(1, outputTable->getRowCount());
    auto column = outputTable->getColumn("attr1");
    ASSERT_TRUE(column != NULL);
    ColumnData<uint32_t> *attr1 = column->getColumnData<uint32_t>();
    ASSERT_TRUE(attr1 != NULL);
    ASSERT_EQ(2, attr1->get(0));
}

TEST_F(ScanKernelTest, testQueryOnlyStopWord) {
    prepareAttributeMap();
    _attributeMap["push_down_ops"] = ParseJson(R"json([
    {
        "attrs": {
            "condition": {"op":"QUERY", "params":["stop"], "type":"UDF"},
            "output_field_exprs": {}
        },
        "op_name": "CalcOp"
    }])json");

    TablePtr outputTable;
    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    auto &tester = *testerPtr;
    ASSERT_FALSE(tester.compute());
    ASSERT_TRUE(outputTable == NULL);
}

TEST_F(ScanKernelTest, testQueryOrFilter) {
    prepareAttributeMap();
    _attributeMap["push_down_ops"] = ParseJson(R"json([
    {
        "attrs": {
            "condition": {"op":"OR", "params":[
                {"op":"QUERY", "params":["name", "aa OR bb"], "type":"UDF"},
                {"op":">=", "params":["$attr1",2]}
            ]},
            "output_field_exprs": {}
        },
        "op_name": "CalcOp"
    }])json");

    _attributeMap["table_meta"] = ParseJson(R"json({
        "field_meta": [
            {
                "field_type": "TEXT",
                "field_name": "$index_2",
                "index_name": "index_2",
                "index_type": "TEXT"
            }
        ]
    })json");

    TablePtr outputTable;
    runKernel(outputTable);

    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(4, outputTable->getRowCount());
    auto column = outputTable->getColumn("attr1");
    ASSERT_TRUE(column != NULL);
    ColumnData<uint32_t> *attr1 = column->getColumnData<uint32_t>();
    ASSERT_TRUE(attr1 != NULL);
    ASSERT_EQ(0, attr1->get(0));
    ASSERT_EQ(1, attr1->get(1));
    ASSERT_EQ(2, attr1->get(2));
    ASSERT_EQ(3, attr1->get(3));
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
    _attributeMap["table_meta"] = ParseJson(R"json({
        "field_meta": [
            {
                "field_type": "int32",
                "field_name": "$wrong_field"
            }
        ]
    })json");
    _attributeMap["output_fields_internal"] = ParseJson(string(R"json(["wrong_field"])json"));
    TablePtr outputTable;
    runKernel(outputTable);
    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ(4, outputTable->getRowCount());
    auto column = outputTable->getColumn("wrong_filed");
    ASSERT_TRUE(column == NULL);
}

TEST_F(ScanKernelTest, testInitCreateScanIter) {
    prepareAttributeMap();
    _attributeMap["push_down_ops"] = ParseJson(R"json([
    {
        "attrs": {
            "condition": "xxx",
            "output_field_exprs": {}
        },
        "op_name": "CalcOp"
    }])json");

    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    ASSERT_TRUE(testerPtr->hasError());
    ASSERT_EQ("table [invertedTable] leaf exprStr [\"xxx\"] is not bool expression",
              testerPtr->getErrorMessage());
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

TEST_F(ScanKernelTest, testOutputWithSubDoc) {
    _attributeMap.clear();
    _attributeMap["table_type"] = string("normal");
    _attributeMap["table_name"] = _tableName;
    _attributeMap["db_name"] = string("default");
    _attributeMap["catalog_name"] = string("default");
    _attributeMap["hash_fields"] = ParseJson(string(R"json(["id"])json"));
    _attributeMap["nest_table_attrs"]
        = ParseJson(string(R"json([{"table_name":"sub_invertedTable"}])json"));
    _attributeMap["use_nest_table"] = Any(true);
    _attributeMap["output_fields_internal"] = ParseJson(string(R"json(["$sub_id", "$attr1"])json"));
    _attributeMap["push_down_ops"] = ParseJson(R"json([
    {
        "attrs": {
            "condition": {"op":"AND", "params":[
                {"op":">", "params":["$sub_id", 1]},
                {"op":"<=", "params":["$attr1",2]}
            ]},
            "output_field_exprs": {}
        },
        "op_name": "CalcOp"
    }])json");

    _attributeMap["table_meta"] = ParseJson(R"json({
        "field_meta": [
            {
                "field_type": "TEXT",
                "field_name": "$index_2",
                "index_name": "index_2",
                "index_type": "TEXT"
            }
        ]
    })json");

    TablePtr outputTable;
    runKernel(outputTable);
    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ("sub_id", outputTable->getColumn(0)->getName());
    ASSERT_EQ("attr1", outputTable->getColumn(1)->getName());
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
    _attributeMap["nest_table_attrs"]
        = ParseJson(string(R"json([{"table_name":"sub_invertedTable"}])json"));
    _attributeMap["use_nest_table"] = Any(true);
    _attributeMap["output_fields_internal"] = ParseJson(string(R"json(["$attr1", "$sub_id"])json"));
    _attributeMap["push_down_ops"] = ParseJson(R"json([
    {
        "attrs": {
            "condition": {"op":"AND", "params":[
                {"op":">", "params":["$sub_id", 1]},
                {"op":"<=", "params":["$attr1",2]}
            ]},
            "output_field_exprs": {}
        },
        "op_name": "CalcOp"
    }])json");

    _attributeMap["table_meta"] = ParseJson(R"json({
        "field_meta": [
            {
                "field_type": "TEXT",
                "field_name": "$index_2",
                "index_name": "index_2",
                "index_type": "TEXT"
            }
        ]
    })json");

    TablePtr outputTable;
    runKernel(outputTable);

    ASSERT_TRUE(outputTable != NULL);
    ASSERT_EQ("sub_id", outputTable->getColumn(1)->getName());
    ASSERT_EQ("attr1", outputTable->getColumn(0)->getName());
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
    _attributeMap["nest_table_attrs"]
        = ParseJson(string(R"json([{"table_name":"sub_invertedTable"}])json"));
    _attributeMap["use_nest_table"] = Any(true);
    _attributeMap["output_fields_internal"] = ParseJson(string(R"json(["$attr1", "$sub_id"])json"));
    _attributeMap["push_down_ops"] = ParseJson(R"json([
    {
        "attrs": {
            "condition": {"op":"OR", "params":[
                {"op":"<=", "params":["$sub_id", 1]},
                {"op":">", "params":["$attr1",2]}
            ]},
            "output_field_exprs": {}
        },
        "op_name": "CalcOp"
    }])json");

    _attributeMap["table_meta"] = ParseJson(R"json({
        "field_meta": [
            {
                "field_type": "TEXT",
                "field_name": "$index_2",
                "index_name": "index_2",
                "index_type": "TEXT"
            }
        ]
    })json");

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
    _attributeMap["nest_table_attrs"]
        = ParseJson(string(R"json([{"table_name":"sub_invertedTable"}])json"));
    _attributeMap["use_nest_table"] = Any(true);
    _attributeMap["output_fields_internal"] = ParseJson(string(R"json(["$attr1", "$sub_id"])json"));
    _attributeMap["push_down_ops"] = ParseJson(R"json([
    {
        "attrs": {
            "condition": {"op":"NOT", "params":[
                {"op":">", "params":["$sub_id", 2]}
            ]},
            "output_field_exprs": {}
        },
        "op_name": "CalcOp"
    }])json");

    _attributeMap["table_meta"] = ParseJson(R"json({
        "field_meta": [
            {
                "field_type": "TEXT",
                "field_name": "$index_2",
                "index_name": "index_2",
                "index_type": "TEXT"
            }
        ]
    })json");

    TablePtr outputTable;
    runKernel(outputTable);

    ASSERT_TRUE(outputTable != NULL);
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "attr1", {0, 0}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int64_t>(outputTable, "sub_id", {1, 2}));
}

TEST_F(ScanKernelTest, testPushDownOps) {
    _attributeMap.clear();
    _attributeMap["table_type"] = string("normal");
    _attributeMap["table_name"] = _tableName;
    _attributeMap["db_name"] = string("default");
    _attributeMap["catalog_name"] = string("default");
    _attributeMap["hash_fields"] = ParseJson(string(R"json(["id"])json"));
    _attributeMap["nest_table_attrs"]
        = ParseJson(string(R"json([{"table_name":"sub_invertedTable"}])json"));
    _attributeMap["use_nest_table"] = Any(true);
    _attributeMap["output_fields_internal"] = ParseJson(string(R"json(["$sub_id", "$attr1"])json"));
    _attributeMap["table_meta"] = ParseJson(R"json({
        "field_meta": [
            {
                "field_type": "TEXT",
                "field_name": "$index_2",
                "index_name": "index_2",
                "index_type": "TEXT"
            }
        ]
    })json");

    _attributeMap["push_down_ops"] = ParseJson(R"json([
    {
        "attrs": {
            "condition": {"op":">", "params":[
                "$attr1", 2
            ]},
            "output_field_exprs": {}
        },
        "op_name": "CalcOp"
    },
    {
        "attrs":{
            "block":false,
            "invocation":{
               "op":"sortTvf",
               "params":[
                   "-attr1",
                   "2",
                   "@table#0"
               ],
               "type":"TVF"
           },
           "output_fields":[
               "$attr1"
           ],
           "output_fields_type":[
               "INTEGER"
           ],
           "scope":"NORMAL",
           "top_distribution":{
               "partition_cnt":0
           },
           "top_properties":{
               "enable_shuffle":false
           }
        },
        "op_name":"TableFunctionScanOp"
    },
    {
        "attrs":{
            "output_field_exprs":{
                "$score":{
                    "op":"+",
                    "params":[
                        "$attr1",
                        1
                    ],
                    "type":"OTHER"
                }
            },
            "output_fields":[
                "$attr1",
                "$score"
            ],
            "output_fields_type":[
                "INTEGER",
                "INTEGER"
            ]
        },
        "op_name":"CalcOp"
    }
    ])json");

    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    auto *kernel = testerPtr->getKernel();
    ScanKernel *scanKernel = dynamic_cast<ScanKernel *>(kernel);
    auto *scanR = dynamic_cast<NormalScanR *>(scanKernel->_scanBase);
    ASSERT_TRUE(scanR);
    ASSERT_EQ(2, scanR->_scanPushDownR->_pushDownOps.size());
    ASSERT_EQ("tvf", scanR->_scanPushDownR->_pushDownOps[0]->getName());
    ASSERT_TRUE(scanR->_scanPushDownR->_pushDownOps[0]->isReuseTable());
    ASSERT_EQ("calc", scanR->_scanPushDownR->_pushDownOps[1]->getName());
    ASSERT_FALSE(scanR->_scanPushDownR->_pushDownOps[1]->isReuseTable());
    ASSERT_TRUE(testerPtr->compute());
    TablePtr outputTable;
    getOutputTable(*testerPtr, outputTable);
    ASSERT_EQ(2, outputTable->getRowCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "attr1", {3, 3}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "score", {4, 4}));
}

TEST_F(ScanKernelTest, testPushDownOps_UnsupportedOp) {
    _attributeMap.clear();
    _attributeMap["table_type"] = string("normal");
    _attributeMap["table_name"] = _tableName;
    _attributeMap["db_name"] = string("default");
    _attributeMap["catalog_name"] = string("default");
    _attributeMap["hash_fields"] = ParseJson(string(R"json(["id"])json"));
    _attributeMap["nest_table_attrs"]
        = ParseJson(string(R"json([{"table_name":"sub_invertedTable"}])json"));
    _attributeMap["use_nest_table"] = Any(true);
    _attributeMap["output_fields_internal"] = ParseJson(string(R"json(["$sub_id", "$attr1"])json"));
    _attributeMap["table_meta"] = ParseJson(R"json({
        "field_meta": [
            {
                "field_type": "TEXT",
                "field_name": "$index_2",
                "index_name": "index_2",
                "index_type": "TEXT"
            }
        ]
    })json");

    _attributeMap["push_down_ops"] = ParseJson(R"json([
    {
        "attrs": {
            "condition": {"op":">", "params":[
                "$attr1", 2
            ]},
            "output_field_exprs": {}
        },
        "op_name": "CalcOp"
    },
    {
        "attrs":{
            "block":false,
            "invocation":{
               "op":"sortTvf",
               "params":[
                   "-attr1",
                   "2",
                   "@table#0"
               ],
               "type":"TVF"
           },
           "output_fields":[
               "$attr1"
           ],
           "output_fields_type":[
               "INTEGER"
           ],
           "scope":"NORMAL",
           "top_distribution":{
               "partition_cnt":0
           },
           "top_properties":{
               "enable_shuffle":false
           }
        },
        "op_name":"MergeOp"
    }
    ])json");

    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    ASSERT_TRUE(testerPtr->hasError());
    ASSERT_EQ("unsupport push down op: MergeOp", testerPtr->getErrorMessage());
}

TEST_F(ScanKernelTest, testPushDownOps_FirstOpNotCalcOp) {
    _attributeMap.clear();
    _attributeMap["table_type"] = string("normal");
    _attributeMap["table_name"] = _tableName;
    _attributeMap["db_name"] = string("default");
    _attributeMap["catalog_name"] = string("default");
    _attributeMap["hash_fields"] = ParseJson(string(R"json(["id"])json"));
    _attributeMap["nest_table_attrs"]
        = ParseJson(string(R"json([{"table_name":"sub_invertedTable"}])json"));
    _attributeMap["use_nest_table"] = Any(true);
    _attributeMap["output_fields_internal"] = ParseJson(string(R"json(["$sub_id", "$attr1"])json"));
    _attributeMap["table_meta"] = ParseJson(R"json({
        "field_meta": [
            {
                "field_type": "TEXT",
                "field_name": "$index_2",
                "index_name": "index_2",
                "index_type": "TEXT"
            }
        ]
    })json");

    _attributeMap["push_down_ops"] = ParseJson(R"json([
    {
        "attrs":{
            "block":false,
            "invocation":{
               "op":"sortTvf",
               "params":[
                   "-attr1",
                   "2",
                   "@table#0"
               ],
               "type":"TVF"
           },
           "output_fields":[
               "$attr1"
           ],
           "output_fields_type":[
               "INTEGER"
           ],
           "scope":"NORMAL",
           "top_distribution":{
               "partition_cnt":0
           },
           "top_properties":{
               "enable_shuffle":false
           }
        },
        "op_name":"TableFunctionScanOp"
    },
    {
        "attrs":{
            "output_field_exprs":{
                "$score":{
                    "op":"+",
                    "params":[
                        "$attr1",
                        1
                    ],
                    "type":"OTHER"
                }
            },
            "output_fields":[
                "$attr1",
                "$score"
            ],
            "output_fields_type":[
                "INTEGER",
                "INTEGER"
            ]
        },
        "op_name":"CalcOp"
    }
    ])json");

    KernelTesterBuilder builder;
    auto testerPtr = buildTester(builder);
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    auto *kernel = testerPtr->getKernel();
    ScanKernel *scanKernel = dynamic_cast<ScanKernel *>(kernel);
    auto *scanR = dynamic_cast<NormalScanR *>(scanKernel->_scanBase);
    ASSERT_TRUE(scanR);
    ASSERT_EQ(2, scanR->_scanPushDownR->_pushDownOps.size());
    ASSERT_EQ("tvf", scanR->_scanPushDownR->_pushDownOps[0]->getName());
    ASSERT_TRUE(scanR->_scanPushDownR->_pushDownOps[0]->isReuseTable());
    ASSERT_EQ("calc", scanR->_scanPushDownR->_pushDownOps[1]->getName());
    ASSERT_FALSE(scanR->_scanPushDownR->_pushDownOps[1]->isReuseTable());
    ASSERT_TRUE(testerPtr->compute());
    TablePtr outputTable;
    getOutputTable(*testerPtr, outputTable);
    ASSERT_EQ(2, outputTable->getRowCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "attr1", {3, 3}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "score", {4, 4}));
}

} // namespace sql
