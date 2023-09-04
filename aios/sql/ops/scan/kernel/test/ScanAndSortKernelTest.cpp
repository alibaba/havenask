#include <algorithm>
#include <assert.h>
#include <cstdint>
#include <iostream>
#include <limits>
#include <map>
#include <memory>
#include <stddef.h>
#include <string>
#include <utility>

#include "autil/CommonMacros.h"
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
#include "navi/common.h"
#include "navi/engine/Data.h"
#include "navi/log/NaviLoggerProvider.h"
#include "navi/tester/KernelTester.h"
#include "navi/tester/KernelTesterBuilder.h"
#include "navi/tester/NaviResourceHelper.h"
#include "sql/framework/PushDownOp.h"
#include "sql/ops/scan/test/FakeTokenizer.h"
#include "sql/ops/test/OpTestBase.h"
#include "sql/resource/AnalyzerFactoryR.h"
#include "suez/turing/expression/util/FieldBoost.h"
#include "table/Table.h"
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

class ScanAndSortKernelTest : public OpTestBase {
public:
    ScanAndSortKernelTest();
    ~ScanAndSortKernelTest();

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

std::string ScanAndSortKernelTest::fakeAnalyzerName = "fake_analyzer";

ScanAndSortKernelTest::ScanAndSortKernelTest() {
    _needBuildIndex = true;
    _needExprResource = true;
}

ScanAndSortKernelTest::~ScanAndSortKernelTest() {}

AnalyzerFactoryPtr ScanAndSortKernelTest::initAnalyzerFactory() {
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
ScanAndSortKernelTest::makeIndexPartition(const std::string &rootPath,
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

TEST_F(ScanAndSortKernelTest, testScanAndSort) {
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
            "condition": {"op":"QUERY", "params":["index_2", "a"], "type":"UDF"},
            "output_field_exprs": {}
        },
        "op_name": "CalcOp"
    },
    {
        "attrs": {
            "order_fields": ["attr1"],
            "directions": ["ASC"],
            "limit": 2,
            "offset": 0
        },
        "op_name": "SortOp"
    }])json");
    TablePtr outputTable;
    navi::NaviLoggerProvider provider("TRACE2");
    ASSERT_NO_FATAL_FAILURE(runKernel(outputTable));
    // EXPECT_EQ("", autil::StringUtil::toString(provider.getTrace("")));
    ASSERT_TRUE(outputTable != NULL);
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "attr1", {0, 1}));
}

TEST_F(ScanAndSortKernelTest, testScanAndSortMultiDims) {
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
            "condition": {"op":"QUERY", "params":["index_2", "'a' OR 'b'"], "type":"UDF"},
            "output_field_exprs": {}
        },
        "op_name": "CalcOp"
    },
    {
        "attrs": {
            "order_fields": ["attr1", "id"],
            "directions": ["ASC", "ASC"],
            "limit": 2,
            "offset": 0
        },
        "op_name": "SortOp"
    }])json");
    TablePtr outputTable;
    navi::NaviLoggerProvider provider("TRACE2");
    ASSERT_NO_FATAL_FAILURE(runKernel(outputTable));
    // EXPECT_EQ("", autil::StringUtil::toString(provider.getTrace("")));
    ASSERT_TRUE(outputTable != NULL);
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "attr1", {0, 1}));
}

} // namespace sql
