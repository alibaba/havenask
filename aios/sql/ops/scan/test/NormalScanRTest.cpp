#include "sql/ops/scan/NormalScanR.h"

#include <algorithm>
#include <assert.h>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "autil/MultiValueType.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/TimeoutTerminator.h"
#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/json.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "autil/result/Result.h"
#include "build_service/analyzer/Token.h"
#include "ha3/common/CommonDef.h"
#include "ha3/common/DocIdsQuery.h"
#include "ha3/common/Query.h"
#include "ha3/common/Term.h"
#include "ha3/common/TermQuery.h"
#include "ha3/search/IndexPartitionReaderWrapper.h"
#include "ha3/search/MatchDataCollectorBase.h"
#include "ha3/search/MatchDataManager.h"
#include "ha3/search/MetaInfo.h"
#include "ha3/search/SimpleMatchData.h"
#include "ha3/search/TermMatchData.h"
#include "indexlib/base/Types.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/online_config.h"
#include "indexlib/index/common/Types.h"
#include "indexlib/index/inverted_index/InvertedIndexSearchTracer.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/index_application.h"
#include "indexlib/partition/index_partition.h"
#include "indexlib/testlib/indexlib_partition_creator.h"
#include "indexlib/util/cache/BlockAccessCounter.h"
#include "matchdoc/CommonDefine.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/Reference.h"
#include "matchdoc/SubDocAccessor.h"
#include "matchdoc/ValueType.h"
#include "navi/common.h"
#include "navi/log/NaviLoggerProvider.h"
#include "navi/tester/NaviResourceHelper.h"
#include "sql/common/FieldInfo.h"
#include "sql/common/common.h"
#include "sql/framework/PushDownOp.h"
#include "sql/ops/calc/CalcInitParamR.h"
#include "sql/ops/scan/AttributeExpressionCreatorR.h"
#include "sql/ops/scan/ScanInitParamR.h"
#include "sql/ops/scan/ScanIterator.h"
#include "sql/ops/scan/ScanIteratorCreatorR.h"
#include "sql/ops/scan/UseSubR.h"
#include "sql/ops/test/OpTestBase.h"
#include "sql/proto/SqlSearchInfo.pb.h"
#include "sql/proto/SqlSearchInfoCollector.h"
#include "sql/proto/SqlSearchInfoCollectorR.h"
#include "sql/resource/TimeoutTerminatorR.h"
#include "suez/turing/expression/framework/AttributeExpression.h"
#include "suez/turing/expression/provider/MetaInfo.h"
#include "suez/turing/expression/provider/TermMetaInfo.h"
#include "suez/turing/expression/provider/common.h"
#include "suez/turing/expression/util/FieldBoost.h"
#include "suez/turing/expression/util/IndexInfos.h"
#include "suez/turing/expression/util/TableInfo.h"
#include "table/Table.h"
#include "table/TableUtil.h"
#include "table/test/MatchDocUtil.h"
#include "unittest/unittest.h"

using namespace std;
using namespace suez::turing;
using namespace matchdoc;
using namespace build_service::analyzer;
using namespace autil;
using namespace table;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace navi;

using namespace isearch::common;
using namespace isearch::search;
using namespace isearch::rank;

namespace sql {

class NormalScanRTest : public OpTestBase {
public:
    NormalScanRTest() {
        _needBuildIndex = true;
        _needExprResource = true;
    }
    ~NormalScanRTest() {}

public:
    void prepareUserIndex() override {
        prepareInvertedTable();
    }
    void prepareInvertedTable() override {
        string tableName = _tableName;
        std::string testPath = _testPath + tableName;
        auto indexPartition = makeIndexPartition(testPath, tableName);
        std::string schemaName = indexPartition->GetSchema()->GetSchemaName();
        _indexPartitionMap.insert(make_pair(schemaName, indexPartition));
    }

    void prepareNormalScanR(NormalScanR &scanR, bool useSub) {
        auto *naviRHelper = getNaviRHelper();
        auto calcIPR = std::make_shared<CalcInitParamR>();
        ASSERT_TRUE(naviRHelper->addExternalRes(calcIPR));
        auto scanIPR = std::make_shared<ScanInitParamR>();
        ASSERT_TRUE(naviRHelper->addExternalRes(scanIPR));
        scanIPR->tableName = _tableName;
        scanIPR->calcInitParamR = calcIPR.get();
        scanR._scanInitParamR = scanIPR.get();
        auto useSubR = std::make_shared<UseSubR>();
        useSubR->_useSub = useSub;
        ASSERT_TRUE(naviRHelper->addExternalRes(useSubR));
        ASSERT_TRUE(naviRHelper->getOrCreateRes(scanR._queryMemPoolR));
        ASSERT_TRUE(naviRHelper->getOrCreateRes(scanR._attributeExpressionCreatorR));
    }

private:
    IndexInfos *
    constructIndexInfos(const std::vector<pair<std::string, indexlib::InvertedIndexType>> &indexs);

    void prepareAttributeMap() {
        _attributeMap.clear();
        _attributeMap["table_type"] = string("normal");
        _attributeMap["table_name"] = _tableName;
        _attributeMap["db_name"] = string("default");
        _attributeMap["catalog_name"] = string("default");
        _attributeMap["output_fields_internal"]
            = ParseJson(string(R"json(["$attr1", "$attr2", "$id"])json"));
        _attributeMap["use_nest_table"] = Any(false);
        _attributeMap["limit"] = Any(1000);
    }
    void checkExpr(const vector<suez::turing::AttributeExpression *> &attributeExpressionVec,
                   const vector<string> &exprStrVec) {
        ASSERT_EQ(attributeExpressionVec.size(), exprStrVec.size());
        for (size_t i = 0; i < exprStrVec.size(); i++) {
            ASSERT_EQ(attributeExpressionVec[i]->getOriginalString(), exprStrVec[i]);
        }
    }

    indexlib::partition::IndexPartitionPtr makeIndexPartition(const std::string &rootPath,
                                                              const std::string &tableName) {
        int64_t ttl = std::numeric_limits<int64_t>::max();
        auto mainSchema = indexlib::testlib::IndexlibPartitionCreator::CreateSchema(
            tableName,
            "attr1:uint32;attr2:int32:true;id:int64;name:string;index2:text", // fields
            "id:primarykey64:id;index_2:text:index2;name:string:name",        // indexs
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
        string docsStr = "cmd=add,attr1=0,attr2=0 10,id=1,name=aa,index2=a a "
                         "a,sub_id=1^2^3,sub_attr1=1^2^2,sub_index2=aa^ab^ac;"
                         "cmd=add,attr1=1,attr2=1 11,id=2,name=bb,index2=a b "
                         "c,sub_id=4,sub_attr1=1,sub_index2=aa;"
                         "cmd=add,attr1=2,attr2=2 22,id=3,name=cc,index2=a c d "
                         "stop,sub_id=5,sub_attr1=1,sub_index2=ab;"
                         "cmd=add,attr1=3,attr2=3 33,id=4,name=dd,index2=b c "
                         "d,sub_id=6^7,sub_attr1=2^1,sub_index2=abc^ab";

        indexlib::config::IndexPartitionOptions options;
        options.GetOnlineConfig().ttl = ttl;
        auto schema = indexlib::testlib::IndexlibPartitionCreator::CreateSchemaWithSub(mainSchema,
                                                                                       subSchema);

        indexlib::partition::IndexPartitionPtr indexPartition
            = indexlib::testlib::IndexlibPartitionCreator::CreateIndexPartition(
                schema, rootPath, docsStr, options, "", true);
        assert(indexPartition.get() != nullptr);
        return indexPartition;
    }

private:
    autil::legacy::json::JsonMap _attributeMap;
};

TEST_F(NormalScanRTest, testInitFailed1) {
    { // init ouput failed
        autil::legacy::json::JsonMap attributeMap;
        attributeMap["table_type"] = string("normal");
        attributeMap["table_name"] = _tableName;
        attributeMap["db_name"] = string("default");
        attributeMap["catalog_name"] = string("default");
        attributeMap["output_fields_internal"]
            = ParseJson(string(R"json(["$attr1", "$attr2", "$id"])json"));
        attributeMap["hash_fields"] = ParseJson(string(R"json(["id"])json"));

        attributeMap["push_down_ops"] = ParseJson(R"json([
        {
            "attrs": {
                "output_field_exprs": {"$attr1":{"op":"+", "paramsxxx":["$attr1", 1]}}
            },
            "op_name": "CalcOp"
        }])json");
        string jsonStr = autil::legacy::FastToJsonString(attributeMap);
        auto *naviRHelper = getNaviRHelper();
        naviRHelper->kernelConfig(jsonStr);
        auto scanR = naviRHelper->getOrCreateRes<NormalScanR>();
        ASSERT_FALSE(scanR);
    }
}

TEST_F(NormalScanRTest, testInitFailed2) {
    { // create iterator failed
        autil::legacy::json::JsonMap attributeMap;
        attributeMap["table_type"] = string("normal");
        attributeMap["table_name"] = _tableName;
        attributeMap["db_name"] = string("default");
        attributeMap["catalog_name"] = string("default");
        attributeMap["hash_fields"] = ParseJson(string(R"json(["id"])json"));
        attributeMap["output_fields_internal"]
            = ParseJson(string(R"json(["$attr1", "$attr2", "$id"])json"));
        attributeMap["push_down_ops"] = ParseJson(R"json([
        {
            "attrs": {
                "condition": {"op":"LIKE", "paramsxxx":["$index2", "a"]}
            },
            "op_name": "CalcOp"
        }])json");
        string jsonStr = autil::legacy::FastToJsonString(attributeMap);
        auto *naviRHelper = getNaviRHelper();
        naviRHelper->kernelConfig(jsonStr);
        auto scanR = naviRHelper->getOrCreateRes<NormalScanR>();
        ASSERT_FALSE(scanR);
    }
}

TEST_F(NormalScanRTest, testInitFailed3) {
    { // default without sub with sub query
        navi::NaviLoggerProvider provider("TRACE2");
        autil::legacy::json::JsonMap attributeMap;
        attributeMap["table_type"] = string("normal");
        attributeMap["table_name"] = _tableName;
        attributeMap["db_name"] = string("default");
        attributeMap["catalog_name"] = string("default");
        attributeMap["hash_fields"] = ParseJson(string(R"json(["id"])json"));
        attributeMap["use_nest_table"] = false;
        attributeMap["output_fields_internal"] = ParseJson(string(R"json(["$attr1"])json"));
        attributeMap["push_down_ops"] = ParseJson(R"json([
        {
            "attrs": {
                "condition": {"op":"=", "params":["$sub_id", 4]}
            },
            "op_name": "CalcOp"
        }])json");
        attributeMap["table_meta"] = ParseJson(R"json({
            "field_meta": [
                {
                    "field_type": "NUMBER",
                    "field_name": "sub_id",
                    "index_name": "sub_id",
                    "index_type": "primarykey64"
                }
            ]
        })json");

        string jsonStr = autil::legacy::FastToJsonString(attributeMap);
        auto *naviRHelper = getNaviRHelper();
        naviRHelper->kernelConfig(jsonStr);
        auto scanR = naviRHelper->getOrCreateRes<NormalScanR>();
        ASSERT_FALSE(scanR);
        // auto traces = provider.getTrace("");
        // CHECK_TRACE_COUNT(1, "use sub query [sub_term:4] without unnest sub table", traces);
        // CHECK_TRACE_COUNT(1, "table [invertedTable] create ha3 scan iterator failed", traces);
    }
}

TEST_F(NormalScanRTest, testInit1) {
    { // default
        autil::legacy::json::JsonMap attributeMap;
        attributeMap["table_type"] = string("normal");
        attributeMap["table_name"] = _tableName;
        attributeMap["db_name"] = string("default");
        attributeMap["catalog_name"] = string("default");
        attributeMap["hash_fields"] = ParseJson(string(R"json(["id"])json"));
        attributeMap["output_fields_internal"] = ParseJson(string(R"json(["$attr1"])json"));
        string jsonStr = autil::legacy::FastToJsonString(attributeMap);
        auto *naviRHelper = getNaviRHelper();
        naviRHelper->kernelConfig(jsonStr);
        auto scanR = naviRHelper->getOrCreateRes<NormalScanR>();
        ASSERT_TRUE(scanR);
        ASSERT_EQ(1 << 18, scanR->_batchSize);
    }
}

TEST_F(NormalScanRTest, testInit2) {
    { // default with sqlBizResource
        autil::legacy::json::JsonMap attributeMap;
        attributeMap["table_type"] = string("normal");
        attributeMap["table_name"] = _tableName;
        attributeMap["db_name"] = string("default");
        attributeMap["catalog_name"] = string("default");
        attributeMap["hash_fields"] = ParseJson(string(R"json(["id"])json"));
        attributeMap["output_fields_internal"]
            = ParseJson(string(R"json(["$attr1","$attr1","$attr1"])json"));
        string jsonStr = autil::legacy::FastToJsonString(attributeMap);
        auto *naviRHelper = getNaviRHelper();
        naviRHelper->kernelConfig(jsonStr);
        auto scanR = naviRHelper->getOrCreateRes<NormalScanR>();
        ASSERT_TRUE(scanR);
        ASSERT_EQ(1 << 18, scanR->_batchSize);
    }
}

TEST_F(NormalScanRTest, testInit3) {
    { // default with sub
        autil::legacy::json::JsonMap attributeMap;
        attributeMap["table_type"] = string("normal");
        attributeMap["table_name"] = _tableName;
        attributeMap["db_name"] = string("default");
        attributeMap["catalog_name"] = string("default");
        attributeMap["hash_fields"] = ParseJson(string(R"json(["id"])json"));
        attributeMap["nest_table_attrs"]
            = ParseJson(string(R"json([{"table_name":"sub_invertedTable"}])json"));
        attributeMap["use_nest_table"] = true;
        attributeMap["output_fields_internal"] = ParseJson(string(R"json(["$attr1"])json"));
        string jsonStr = autil::legacy::FastToJsonString(attributeMap);
        auto *naviRHelper = getNaviRHelper();
        naviRHelper->kernelConfig(jsonStr);
        auto scanR = naviRHelper->getOrCreateRes<NormalScanR>();
        ASSERT_TRUE(scanR);
    }
}

TEST_F(NormalScanRTest, testInit4) {
    { // default with sub
        autil::legacy::json::JsonMap attributeMap;
        attributeMap["table_type"] = string("normal");
        attributeMap["table_name"] = _tableName;
        attributeMap["db_name"] = string("default");
        attributeMap["catalog_name"] = string("default");
        attributeMap["hash_fields"] = ParseJson(string(R"json(["id"])json"));
        attributeMap["nest_table_attrs"]
            = ParseJson(string(R"json([{"table_name":"sub_invertedTable"}])json"));
        attributeMap["use_nest_table"] = true;
        attributeMap["output_fields_internal"]
            = ParseJson(string(R"json(["$attr1", "$sub_attr1"])json"));
        string jsonStr = autil::legacy::FastToJsonString(attributeMap);
        auto *naviRHelper = getNaviRHelper();
        naviRHelper->kernelConfig(jsonStr);
        auto scanR = naviRHelper->getOrCreateRes<NormalScanR>();
        ASSERT_TRUE(scanR);
    }
}

TEST_F(NormalScanRTest, testInit5) {
    { // batch size 100
        autil::legacy::json::JsonMap attributeMap;
        attributeMap["table_type"] = string("normal");
        attributeMap["table_name"] = _tableName;
        attributeMap["db_name"] = string("default");
        attributeMap["catalog_name"] = string("default");
        attributeMap["hash_fields"] = ParseJson(string(R"json(["id"])json"));
        attributeMap["output_fields_internal"] = ParseJson(string(R"json(["$attr1"])json"));
        attributeMap["batch_size"] = Any(100);
        string jsonStr = autil::legacy::FastToJsonString(attributeMap);
        auto *naviRHelper = getNaviRHelper();
        naviRHelper->kernelConfig(jsonStr);
        auto scanR = naviRHelper->getOrCreateRes<NormalScanR>();
        ASSERT_TRUE(scanR);
        ASSERT_EQ(100, scanR->_batchSize);
    }
}

TEST_F(NormalScanRTest, testDoBatchScan1) {
    { // one batch
        autil::legacy::json::JsonMap attributeMap;
        attributeMap["table_type"] = string("normal");
        attributeMap["table_name"] = _tableName;
        attributeMap["db_name"] = string("default");
        attributeMap["catalog_name"] = string("default");
        attributeMap["hash_fields"] = ParseJson(string(R"json(["id"])json"));
        attributeMap["output_fields_internal"] = ParseJson(string(R"json(["$attr1"])json"));
        string jsonStr = autil::legacy::FastToJsonString(attributeMap);
        auto *naviRHelper = getNaviRHelper();
        naviRHelper->kernelConfig(jsonStr);
        auto scanR = naviRHelper->getOrCreateRes<NormalScanR>();
        ASSERT_TRUE(scanR);
        TablePtr table;
        bool eof = false;
        ASSERT_TRUE(scanR->doBatchScan(table, eof));
        ASSERT_TRUE(table != nullptr);
        EXPECT_EQ("{}", scanR->_scanInitParamR->calcInitParamR->outputExprsJson);
        ASSERT_EQ(4, table->getRowCount());
        ASSERT_EQ("0", table->toString(0, 0));
        ASSERT_EQ("1", table->toString(1, 0));
        ASSERT_EQ("2", table->toString(2, 0));
        ASSERT_EQ("3", table->toString(3, 0));
        ASSERT_TRUE(eof);
        ASSERT_EQ(4, scanR->_seekCount);
        ASSERT_EQ(4, scanR->_scanInitParamR->scanInfo.totalscancount());
        table.reset();
        eof = false;
        ASSERT_TRUE(scanR->doBatchScan(table, eof));
        ASSERT_TRUE(table != nullptr);
        ASSERT_EQ(0, table->getRowCount());
        ASSERT_TRUE(eof);
        ASSERT_EQ(4, scanR->_seekCount);
        ASSERT_EQ(4, scanR->_scanInitParamR->scanInfo.totalscancount());
    }
}

TEST_F(NormalScanRTest, testDoBatchScan2) {
    { // limit 3
        autil::legacy::json::JsonMap attributeMap;
        attributeMap["table_type"] = string("normal");
        attributeMap["table_name"] = _tableName;
        attributeMap["db_name"] = string("default");
        attributeMap["catalog_name"] = string("default");
        attributeMap["hash_fields"] = ParseJson(string(R"json(["id"])json"));
        attributeMap["output_fields_internal"] = ParseJson(string(R"json(["$attr1"])json"));
        attributeMap["limit"] = Any(3);
        string jsonStr = autil::legacy::FastToJsonString(attributeMap);
        auto *naviRHelper = getNaviRHelper();
        naviRHelper->kernelConfig(jsonStr);
        auto scanR = naviRHelper->getOrCreateRes<NormalScanR>();
        ASSERT_TRUE(scanR);
        TablePtr table;
        bool eof = false;
        ASSERT_TRUE(scanR->doBatchScan(table, eof));
        ASSERT_TRUE(table != nullptr);
        ASSERT_EQ(3, table->getRowCount());
        ASSERT_EQ("0", table->toString(0, 0));
        ASSERT_EQ("1", table->toString(1, 0));
        ASSERT_EQ("2", table->toString(2, 0));

        ASSERT_TRUE(eof);
        ASSERT_EQ(3, scanR->_seekCount);
        ASSERT_EQ(3, scanR->_scanInitParamR->scanInfo.totalscancount());
        table.reset();
        eof = false;
        ASSERT_TRUE(scanR->doBatchScan(table, eof));
        ASSERT_TRUE(table != nullptr);
        ASSERT_EQ(0, table->getRowCount());
        ASSERT_TRUE(eof);
        ASSERT_EQ(3, scanR->_seekCount);
        ASSERT_EQ(3, scanR->_scanInitParamR->scanInfo.totalscancount());
    }
}

TEST_F(NormalScanRTest, testDoBatchScan3) {
    { // limit 2, seek more than 2, need delete doc
        autil::legacy::json::JsonMap attributeMap;
        attributeMap["table_type"] = string("normal");
        attributeMap["table_name"] = _tableName;
        attributeMap["db_name"] = string("default");
        attributeMap["catalog_name"] = string("default");
        attributeMap["hash_fields"] = ParseJson(string(R"json(["id"])json"));
        attributeMap["output_fields_internal"] = ParseJson(string(R"json(["$attr1"])json"));
        attributeMap["push_down_ops"] = ParseJson(R"json([
        {
            "attrs": {
                "condition": {"op":"!=", "params":["$attr1", 1]}
            },
            "op_name": "CalcOp"
        }])json");
        attributeMap["limit"] = Any(2);
        string jsonStr = autil::legacy::FastToJsonString(attributeMap);
        auto *naviRHelper = getNaviRHelper();
        naviRHelper->kernelConfig(jsonStr);
        auto scanR = naviRHelper->getOrCreateRes<NormalScanR>();
        ASSERT_TRUE(scanR);
        TablePtr table;
        bool eof = false;
        ASSERT_TRUE(scanR->doBatchScan(table, eof));
        ASSERT_TRUE(table != nullptr);
        ASSERT_EQ(2, table->getRowCount());
        ASSERT_EQ("0", table->toString(0, 0));
        ASSERT_EQ("2", table->toString(1, 0));
        ASSERT_TRUE(eof);
        ASSERT_EQ(3, scanR->_seekCount);
        ASSERT_EQ(4, scanR->_scanInitParamR->scanInfo.totalscancount());
    }
}

TEST_F(NormalScanRTest, testDoBatchScan4) {
    { // batch size 2
        autil::legacy::json::JsonMap attributeMap;
        attributeMap["table_type"] = string("normal");
        attributeMap["table_name"] = _tableName;
        attributeMap["db_name"] = string("default");
        attributeMap["catalog_name"] = string("default");
        attributeMap["hash_fields"] = ParseJson(string(R"json(["id"])json"));
        attributeMap["output_fields_internal"] = ParseJson(string(R"json(["$attr1"])json"));
        attributeMap["batch_size"] = Any(2);
        string jsonStr = autil::legacy::FastToJsonString(attributeMap);
        auto *naviRHelper = getNaviRHelper();
        naviRHelper->kernelConfig(jsonStr);
        auto scanR = naviRHelper->getOrCreateRes<NormalScanR>();
        ASSERT_TRUE(scanR);
        TablePtr table;
        bool eof = false;
        ASSERT_TRUE(scanR->doBatchScan(table, eof));
        ASSERT_TRUE(table != nullptr);
        ASSERT_EQ(2, table->getRowCount());
        ASSERT_EQ("0", table->toString(0, 0));
        ASSERT_EQ("1", table->toString(1, 0));
        ASSERT_FALSE(eof);
        ASSERT_EQ(2, scanR->_scanInitParamR->scanInfo.totalscancount());
        ASSERT_EQ(2, scanR->_seekCount);

        ASSERT_TRUE(scanR->doBatchScan(table, eof));
        ASSERT_TRUE(table != nullptr);
        ASSERT_EQ(2, table->getRowCount());
        ASSERT_EQ("2", table->toString(0, 0));
        ASSERT_EQ("3", table->toString(1, 0));
        ASSERT_FALSE(eof);
        ASSERT_EQ(4, scanR->_scanInitParamR->scanInfo.totalscancount());
        ASSERT_EQ(4, scanR->_seekCount);

        ASSERT_TRUE(scanR->doBatchScan(table, eof));
        ASSERT_TRUE(table != nullptr);
        ASSERT_EQ(0, table->getRowCount());
        ASSERT_EQ(4, scanR->_seekCount);
        ASSERT_EQ(4, scanR->_scanInitParamR->scanInfo.totalscancount());
    }
}

TEST_F(NormalScanRTest, testDoBatchScan5) {
    { // batch size 2 limit 3
        autil::legacy::json::JsonMap attributeMap;
        attributeMap["table_type"] = string("normal");
        attributeMap["table_name"] = _tableName;
        attributeMap["db_name"] = string("default");
        attributeMap["catalog_name"] = string("default");
        attributeMap["hash_fields"] = ParseJson(string(R"json(["id"])json"));
        attributeMap["output_fields_internal"] = ParseJson(string(R"json(["$attr1"])json"));
        attributeMap["batch_size"] = Any(2);
        attributeMap["limit"] = Any(3);
        string jsonStr = autil::legacy::FastToJsonString(attributeMap);
        auto *naviRHelper = getNaviRHelper();
        naviRHelper->kernelConfig(jsonStr);
        auto scanR = naviRHelper->getOrCreateRes<NormalScanR>();
        ASSERT_TRUE(scanR);
        TablePtr table;
        bool eof = false;
        ASSERT_TRUE(scanR->doBatchScan(table, eof));
        ASSERT_TRUE(table != nullptr);
        ASSERT_EQ(2, table->getRowCount());
        ASSERT_EQ("0", table->toString(0, 0));
        ASSERT_EQ("1", table->toString(1, 0));
        ASSERT_FALSE(eof);
        ASSERT_EQ(2, scanR->_scanInitParamR->scanInfo.totalscancount());
        ASSERT_EQ(2, scanR->_seekCount);

        ASSERT_TRUE(scanR->doBatchScan(table, eof));
        ASSERT_TRUE(table != nullptr);
        ASSERT_EQ(1, table->getRowCount());
        ASSERT_EQ("2", table->toString(0, 0));
        ASSERT_TRUE(eof);
        ASSERT_EQ(3, scanR->_seekCount);
        ASSERT_EQ(3, scanR->_scanInitParamR->scanInfo.totalscancount());
    }
}

TEST_F(NormalScanRTest, testTimeout) {
    autil::legacy::json::JsonMap attributeMap;
    attributeMap["table_type"] = string("normal");
    attributeMap["table_name"] = _tableName;
    attributeMap["db_name"] = string("default");
    attributeMap["catalog_name"] = string("default");
    attributeMap["hash_fields"] = ParseJson(string(R"json(["id"])json"));
    attributeMap["output_fields_internal"] = ParseJson(string(R"json(["$attr1"])json"));
    attributeMap["enable_scan_timeout"] = false;
    string jsonStr = autil::legacy::FastToJsonString(attributeMap);
    auto *naviRHelper = getNaviRHelper();
    naviRHelper->kernelConfig(jsonStr);
    auto scanR = naviRHelper->getOrCreateRes<NormalScanR>();
    ASSERT_TRUE(scanR);
    scanR->_timeoutTerminatorR->_timeoutTerminator->_startTime = 1;
    scanR->_timeoutTerminatorR->_timeoutTerminator->_timeout = 0;
    scanR->_timeoutTerminatorR->_timeoutTerminator->_expireTime = 0;
    scanR->_timeoutTerminatorR->_timeoutTerminator->_checkMask = 0;
    TablePtr table;
    bool eof = false;
    navi::NaviLoggerProvider provider("ERROR");
    ASSERT_FALSE(scanR->doBatchScan(table, eof));
    auto traces = provider.getTrace("");
    CHECK_TRACE_COUNT(1, "scan table [invertedTable] timeout, abort, info:", traces);
}

TEST_F(NormalScanRTest, testDoBatchScanWithSub1) {
    { // one batch
        autil::legacy::json::JsonMap attributeMap;
        attributeMap["table_type"] = string("normal");
        attributeMap["table_name"] = _tableName;
        attributeMap["db_name"] = string("default");
        attributeMap["catalog_name"] = string("default");
        attributeMap["hash_fields"] = ParseJson(string(R"json(["id"])json"));
        attributeMap["nest_table_attrs"]
            = ParseJson(string(R"json([{"table_name":"sub_invertedTable"}])json"));
        attributeMap["use_nest_table"] = true;
        attributeMap["output_fields_internal"]
            = ParseJson(string(R"json(["$attr1", "$sub_attr1"])json"));
        string jsonStr = autil::legacy::FastToJsonString(attributeMap);
        auto *naviRHelper = getNaviRHelper();
        naviRHelper->kernelConfig(jsonStr);
        auto scanR = naviRHelper->getOrCreateRes<NormalScanR>();
        ASSERT_TRUE(scanR);
        TablePtr table;
        bool eof = false;
        ASSERT_TRUE(scanR->doBatchScan(table, eof));
        ASSERT_TRUE(table != nullptr);

        ASSERT_EQ(2, table->getColumnCount());
        ASSERT_EQ(7, table->getRowCount());
        ASSERT_EQ("0", table->toString(0, 0));
        ASSERT_EQ("0", table->toString(1, 0));
        ASSERT_EQ("0", table->toString(2, 0));
        ASSERT_EQ("1", table->toString(3, 0));
        ASSERT_EQ("2", table->toString(4, 0));
        ASSERT_EQ("3", table->toString(5, 0));
        ASSERT_EQ("3", table->toString(6, 0));

        ASSERT_EQ("1", table->toString(0, 1));
        ASSERT_EQ("2", table->toString(1, 1));
        ASSERT_EQ("2", table->toString(2, 1));
        ASSERT_EQ("1", table->toString(3, 1));
        ASSERT_EQ("1", table->toString(4, 1));
        ASSERT_EQ("2", table->toString(5, 1));
        ASSERT_EQ("1", table->toString(6, 1));

        ASSERT_TRUE(eof);
        ASSERT_EQ(4, scanR->_seekCount);
        ASSERT_EQ(4, scanR->_scanInitParamR->scanInfo.totalscancount());
        table.reset();
        eof = false;
        ASSERT_TRUE(scanR->doBatchScan(table, eof));
        ASSERT_TRUE(table != nullptr);
        ASSERT_EQ(0, table->getRowCount());
        ASSERT_TRUE(eof);
        ASSERT_EQ(4, scanR->_seekCount);
        ASSERT_EQ(4, scanR->_scanInitParamR->scanInfo.totalscancount());
    }
}

TEST_F(NormalScanRTest, testDoBatchScanWithSub2) {
    { // sub attr only
        autil::legacy::json::JsonMap attributeMap;
        attributeMap["table_type"] = string("normal");
        attributeMap["table_name"] = _tableName;
        attributeMap["db_name"] = string("default");
        attributeMap["catalog_name"] = string("default");
        attributeMap["hash_fields"] = ParseJson(string(R"json(["id"])json"));
        attributeMap["use_nest_table"] = true;
        attributeMap["nest_table_attrs"]
            = ParseJson(string(R"json([{"table_name":"sub_invertedTable"}])json"));
        attributeMap["output_fields_internal"] = ParseJson(string(R"json(["$sub_attr1"])json"));
        string jsonStr = autil::legacy::FastToJsonString(attributeMap);
        auto *naviRHelper = getNaviRHelper();
        naviRHelper->kernelConfig(jsonStr);
        auto scanR = naviRHelper->getOrCreateRes<NormalScanR>();
        ASSERT_TRUE(scanR);
        TablePtr table;
        bool eof = false;
        ASSERT_TRUE(scanR->doBatchScan(table, eof));
        ASSERT_TRUE(table != nullptr);

        ASSERT_EQ(1, table->getColumnCount());
        ASSERT_EQ(7, table->getRowCount());
        ASSERT_EQ("1", table->toString(0, 0));
        ASSERT_EQ("2", table->toString(1, 0));
        ASSERT_EQ("2", table->toString(2, 0));
        ASSERT_EQ("1", table->toString(3, 0));
        ASSERT_EQ("1", table->toString(4, 0));
        ASSERT_EQ("2", table->toString(5, 0));
        ASSERT_EQ("1", table->toString(6, 0));

        ASSERT_TRUE(eof);
        ASSERT_EQ(4, scanR->_seekCount);
        ASSERT_EQ(4, scanR->_scanInitParamR->scanInfo.totalscancount());
        table.reset();
        eof = false;
        ASSERT_TRUE(scanR->doBatchScan(table, eof));
        ASSERT_TRUE(table != nullptr);
        ASSERT_EQ(0, table->getRowCount());
        ASSERT_TRUE(eof);
        ASSERT_EQ(4, scanR->_seekCount);
        ASSERT_EQ(4, scanR->_scanInitParamR->scanInfo.totalscancount());
    }
}

TEST_F(NormalScanRTest, testUpdateScanQuery) {
    { // default
        autil::legacy::json::JsonMap attributeMap;
        attributeMap["table_type"] = string("normal");
        attributeMap["table_name"] = _tableName;
        attributeMap["db_name"] = string("default");
        attributeMap["catalog_name"] = string("default");
        attributeMap["hash_fields"] = ParseJson(string(R"json(["id"])json"));
        attributeMap["output_fields_internal"] = ParseJson(string(R"json(["$attr1"])json"));
        string jsonStr = autil::legacy::FastToJsonString(attributeMap);
        auto *naviRHelper = getNaviRHelper();
        naviRHelper->kernelConfig(jsonStr);
        auto scanR = naviRHelper->getOrCreateRes<NormalScanR>();
        ASSERT_TRUE(scanR);
        TablePtr table;
        StreamQueryPtr inputQuery(new StreamQuery());
        QueryPtr query(new TermQuery("a", "index_2", RequiredFields(), ""));
        inputQuery->query = query;
        ASSERT_TRUE(scanR->updateScanQuery(inputQuery));
        bool eof = false;
        ASSERT_TRUE(scanR->doBatchScan(table, eof));
        ASSERT_TRUE(table != nullptr);
        ASSERT_TRUE(eof);
        ASSERT_EQ(3, table->getRowCount());
        ASSERT_EQ("0", table->toString(0, 0));
        ASSERT_EQ("1", table->toString(1, 0));
        ASSERT_EQ("2", table->toString(2, 0));
        ASSERT_EQ(3, scanR->_seekCount);
        ASSERT_EQ(3, scanR->_scanInitParamR->scanInfo.totalscancount());

        ASSERT_TRUE(scanR->updateScanQuery(inputQuery));
        eof = false;
        ASSERT_TRUE(scanR->doBatchScan(table, eof));
        ASSERT_TRUE(table != nullptr);
        ASSERT_TRUE(eof);
        ASSERT_EQ(3, table->getRowCount());
        ASSERT_EQ("0", table->toString(0, 0));
        ASSERT_EQ("1", table->toString(1, 0));
        ASSERT_EQ("2", table->toString(2, 0));
        ASSERT_EQ(6, scanR->_seekCount);
        ASSERT_EQ(6, scanR->_scanInitParamR->scanInfo.totalscancount());

        ASSERT_TRUE(scanR->updateScanQuery(StreamQueryPtr()));
        eof = false;
        ASSERT_TRUE(scanR->doBatchScan(table, eof));
        ASSERT_TRUE(eof);
        ASSERT_EQ(0, table->getRowCount());
        ASSERT_EQ(6, scanR->_seekCount);
        ASSERT_EQ(6, scanR->_scanInitParamR->scanInfo.totalscancount());
    }
}

TEST_F(NormalScanRTest, testUpdateScanQueryWithDocIds) {
    autil::legacy::json::JsonMap attributeMap;
    attributeMap["table_type"] = string("normal");
    attributeMap["table_name"] = _tableName;
    attributeMap["db_name"] = string("default");
    attributeMap["catalog_name"] = string("default");
    attributeMap["hash_fields"] = ParseJson(string(R"json(["id"])json"));
    attributeMap["output_fields_internal"] = ParseJson(string(R"json(["$attr1"])json"));
    string jsonStr = autil::legacy::FastToJsonString(attributeMap);
    auto *naviRHelper = getNaviRHelper();
    naviRHelper->kernelConfig(jsonStr);
    auto scanR = naviRHelper->getOrCreateRes<NormalScanR>();
    ASSERT_TRUE(scanR);
    TablePtr table;
    StreamQueryPtr inputQuery(new StreamQuery());
    std::vector<docid_t> docIds = {2, 0, 3};
    QueryPtr query(new DocIdsQuery(docIds));
    inputQuery->query = query;
    ASSERT_TRUE(scanR->updateScanQuery(inputQuery));
    bool eof = false;
    ASSERT_TRUE(scanR->doBatchScan(table, eof));
    ASSERT_TRUE(table != nullptr);
    ASSERT_TRUE(eof);
    ASSERT_EQ(3, table->getRowCount());
    ASSERT_EQ("2", table->toString(0, 0));
    ASSERT_EQ("0", table->toString(1, 0));
    ASSERT_EQ("3", table->toString(2, 0));
    EXPECT_EQ(3, scanR->_seekCount);
    ASSERT_EQ(3, scanR->_scanInitParamR->scanInfo.totalscancount());
}

TEST_F(NormalScanRTest, testUpdateScanQueryWithSub1) {
    { // with sub attr
        autil::legacy::json::JsonMap attributeMap;
        attributeMap["table_type"] = string("normal");
        attributeMap["table_name"] = _tableName;
        attributeMap["db_name"] = string("default");
        attributeMap["catalog_name"] = string("default");
        attributeMap["hash_fields"] = ParseJson(string(R"json(["id"])json"));
        attributeMap["nest_table_attrs"]
            = ParseJson(string(R"json([{"table_name":"sub_invertedTable"}])json"));
        attributeMap["use_nest_table"] = Any(true);
        attributeMap["output_fields_internal"]
            = ParseJson(string(R"json(["$sub_attr1", "attr1"])json"));
        string jsonStr = autil::legacy::FastToJsonString(attributeMap);
        auto *naviRHelper = getNaviRHelper();
        naviRHelper->kernelConfig(jsonStr);
        auto scanR = naviRHelper->getOrCreateRes<NormalScanR>();
        ASSERT_TRUE(scanR);
        TablePtr table;
        StreamQueryPtr inputQuery(new StreamQuery());
        QueryPtr query(new TermQuery("a", "index_2", RequiredFields(), ""));
        inputQuery->query = query;
        ASSERT_TRUE(scanR->updateScanQuery(inputQuery));
        bool eof = false;
        ASSERT_TRUE(scanR->doBatchScan(table, eof));
        ASSERT_TRUE(table != nullptr);
        ASSERT_TRUE(eof);
        ASSERT_EQ(2, table->getColumnCount());
        ASSERT_EQ(5, table->getRowCount());
        ASSERT_EQ("1", table->toString(0, 0));
        ASSERT_EQ("2", table->toString(1, 0));
        ASSERT_EQ("2", table->toString(2, 0));
        ASSERT_EQ("1", table->toString(3, 0));
        ASSERT_EQ("1", table->toString(4, 0));

        ASSERT_EQ("0", table->toString(0, 1));
        ASSERT_EQ("0", table->toString(1, 1));
        ASSERT_EQ("0", table->toString(2, 1));
        ASSERT_EQ("1", table->toString(3, 1));
        ASSERT_EQ("2", table->toString(4, 1));
        ASSERT_EQ(3, scanR->_seekCount);
        ASSERT_EQ(3, scanR->_scanInitParamR->scanInfo.totalscancount());

        ASSERT_TRUE(scanR->updateScanQuery(inputQuery));
        eof = false;
        ASSERT_TRUE(scanR->doBatchScan(table, eof));
        ASSERT_TRUE(table != nullptr);
        ASSERT_TRUE(eof);
        ASSERT_EQ(2, table->getColumnCount());
        ASSERT_EQ(5, table->getRowCount());
        ASSERT_EQ("1", table->toString(0, 0));
        ASSERT_EQ("2", table->toString(1, 0));
        ASSERT_EQ("2", table->toString(2, 0));
        ASSERT_EQ("1", table->toString(3, 0));
        ASSERT_EQ("1", table->toString(4, 0));

        ASSERT_EQ("0", table->toString(0, 1));
        ASSERT_EQ("0", table->toString(1, 1));
        ASSERT_EQ("0", table->toString(2, 1));
        ASSERT_EQ("1", table->toString(3, 1));
        ASSERT_EQ("2", table->toString(4, 1));
    }
}

TEST_F(NormalScanRTest, testUpdateScanQueryWithSub2) {
    { // with sub attr empty result
        autil::legacy::json::JsonMap attributeMap;
        attributeMap["table_type"] = string("normal");
        attributeMap["table_name"] = _tableName;
        attributeMap["db_name"] = string("default");
        attributeMap["catalog_name"] = string("default");
        attributeMap["hash_fields"] = ParseJson(string(R"json(["id"])json"));
        attributeMap["nest_table_attrs"]
            = ParseJson(string(R"json([{"table_name":"sub_invertedTable"}])json"));
        attributeMap["use_nest_table"] = Any(true);
        attributeMap["output_fields_internal"]
            = ParseJson(string(R"json(["$sub_attr1", "$attr1"])json"));
        string jsonStr = autil::legacy::FastToJsonString(attributeMap);
        auto *naviRHelper = getNaviRHelper();
        naviRHelper->kernelConfig(jsonStr);
        auto scanR = naviRHelper->getOrCreateRes<NormalScanR>();
        ASSERT_TRUE(scanR);
        TablePtr table;
        StreamQueryPtr inputQuery(new StreamQuery());
        QueryPtr query(new TermQuery("aa", "index_2", RequiredFields(), ""));
        inputQuery->query = query;
        ASSERT_TRUE(scanR->updateScanQuery(inputQuery));
        bool eof = false;
        ASSERT_TRUE(scanR->doBatchScan(table, eof));
        ASSERT_TRUE(table != nullptr);
        ASSERT_TRUE(eof);
        ASSERT_EQ(2, table->getColumnCount());
        ASSERT_EQ(0, table->getRowCount());
    }
}

TEST_F(NormalScanRTest, testUpdateScanQueryLimitBatch1) {
    { // limit 2
        autil::legacy::json::JsonMap attributeMap;
        attributeMap["table_type"] = string("normal");
        attributeMap["table_name"] = _tableName;
        attributeMap["db_name"] = string("default");
        attributeMap["catalog_name"] = string("default");
        attributeMap["hash_fields"] = ParseJson(string(R"json(["id"])json"));
        attributeMap["output_fields_internal"] = ParseJson(string(R"json(["$attr1"])json"));
        attributeMap["limit"] = Any(2);
        string jsonStr = autil::legacy::FastToJsonString(attributeMap);
        auto *naviRHelper = getNaviRHelper();
        naviRHelper->kernelConfig(jsonStr);
        auto scanR = naviRHelper->getOrCreateRes<NormalScanR>();
        ASSERT_TRUE(scanR);
        TablePtr table;
        StreamQueryPtr inputQuery(new StreamQuery());
        QueryPtr query(new TermQuery("a", "index_2", RequiredFields(), ""));
        inputQuery->query = query;
        ASSERT_TRUE(scanR->updateScanQuery(inputQuery));
        bool eof = false;
        ASSERT_TRUE(scanR->doBatchScan(table, eof));
        ASSERT_TRUE(table != nullptr);
        ASSERT_TRUE(eof);
        ASSERT_EQ(2, table->getRowCount());
        ASSERT_EQ("0", table->toString(0, 0));
        ASSERT_EQ("1", table->toString(1, 0));
        ASSERT_EQ(2, scanR->_seekCount);
        ASSERT_EQ(2, scanR->_scanInitParamR->scanInfo.totalscancount());

        ASSERT_TRUE(scanR->updateScanQuery(inputQuery));
        eof = false;
        ASSERT_TRUE(scanR->doBatchScan(table, eof));
        ASSERT_TRUE(table != nullptr);
        ASSERT_TRUE(eof);
        ASSERT_EQ(2, scanR->_seekCount);
        ASSERT_EQ(2, scanR->_scanInitParamR->scanInfo.totalscancount());
    }
}

TEST_F(NormalScanRTest, testUpdateScanQueryLimitBatch2) {
    { // batch size 2
        autil::legacy::json::JsonMap attributeMap;
        attributeMap["table_type"] = string("normal");
        attributeMap["table_name"] = _tableName;
        attributeMap["db_name"] = string("default");
        attributeMap["catalog_name"] = string("default");
        attributeMap["hash_fields"] = ParseJson(string(R"json(["id"])json"));
        attributeMap["output_fields_internal"] = ParseJson(string(R"json(["$attr1"])json"));
        attributeMap["batch_size"] = Any(2);
        string jsonStr = autil::legacy::FastToJsonString(attributeMap);
        auto *naviRHelper = getNaviRHelper();
        naviRHelper->kernelConfig(jsonStr);
        auto scanR = naviRHelper->getOrCreateRes<NormalScanR>();
        ASSERT_TRUE(scanR);
        TablePtr table;
        StreamQueryPtr inputQuery(new StreamQuery());
        QueryPtr query(new TermQuery("a", "index_2", RequiredFields(), ""));
        inputQuery->query = query;
        ASSERT_TRUE(scanR->updateScanQuery(inputQuery));
        bool eof = false;
        ASSERT_TRUE(scanR->doBatchScan(table, eof));
        ASSERT_TRUE(table != nullptr);
        ASSERT_FALSE(eof);

        ASSERT_EQ(2, table->getRowCount());
        ASSERT_EQ("0", table->toString(0, 0));
        ASSERT_EQ("1", table->toString(1, 0));
        ASSERT_EQ(2, scanR->_seekCount);
        ASSERT_EQ(2, scanR->_scanInitParamR->scanInfo.totalscancount());

        ASSERT_TRUE(scanR->doBatchScan(table, eof));
        ASSERT_TRUE(table != nullptr);
        ASSERT_TRUE(eof);
        ASSERT_EQ(1, table->getRowCount());
        ASSERT_EQ("2", table->toString(0, 0));
        ASSERT_EQ(3, scanR->_seekCount);
        ASSERT_EQ(3, scanR->_scanInitParamR->scanInfo.totalscancount());
    }
}

TEST_F(NormalScanRTest, testUpdateScanQueryLimitBatch3) {
    { // batch size 2 limit 2
        autil::legacy::json::JsonMap attributeMap;
        attributeMap["table_type"] = string("normal");
        attributeMap["table_name"] = _tableName;
        attributeMap["db_name"] = string("default");
        attributeMap["catalog_name"] = string("default");
        attributeMap["hash_fields"] = ParseJson(string(R"json(["id"])json"));
        attributeMap["output_fields_internal"] = ParseJson(string(R"json(["$attr1"])json"));
        attributeMap["batch_size"] = Any(2);
        attributeMap["limit"] = Any(2);
        string jsonStr = autil::legacy::FastToJsonString(attributeMap);
        auto *naviRHelper = getNaviRHelper();
        naviRHelper->kernelConfig(jsonStr);
        auto scanR = naviRHelper->getOrCreateRes<NormalScanR>();
        ASSERT_TRUE(scanR);
        TablePtr table;
        StreamQueryPtr inputQuery(new StreamQuery());
        QueryPtr query(new TermQuery("a", "index_2", RequiredFields(), ""));
        inputQuery->query = query;
        ASSERT_TRUE(scanR->updateScanQuery(inputQuery));
        bool eof = false;
        ASSERT_TRUE(scanR->doBatchScan(table, eof));
        ASSERT_TRUE(table != nullptr);
        ASSERT_TRUE(eof);
        ASSERT_EQ(2, table->getRowCount());
        ASSERT_EQ("0", table->toString(0, 0));
        ASSERT_EQ("1", table->toString(1, 0));
        ASSERT_EQ(2, scanR->_seekCount);
        ASSERT_EQ(2, scanR->_scanInitParamR->scanInfo.totalscancount());
    }
}

TEST_F(NormalScanRTest, testInitOutputColumnUseSubTrue1) {
    // output expr is empty
    {
        NormalScanR scanR;
        bool useSubFlag = true;
        ASSERT_NO_FATAL_FAILURE(prepareNormalScanR(scanR, useSubFlag));
        auto matchDocAllocator = scanR._attributeExpressionCreatorR->_matchDocAllocator;
        scanR._scanInitParamR->calcInitParamR->outputFields = {"attr1", "sub_attr1"};
        scanR._scanInitParamR->calcInitParamR->outputExprsJson = "";
        scanR._scanInitParamR->hashFields = {"id"};
        ASSERT_TRUE(scanR.initOutputColumn());
        ASSERT_EQ(0, scanR._copyFieldMap.size());
        checkExpr(scanR._attributeExpressionVec, {"attr1", "sub_attr1"});
        auto refVec = matchDocAllocator->getRefBySerializeLevel(SL_ATTRIBUTE);
        ASSERT_EQ(1, refVec.size());
        refVec = matchDocAllocator->getAllSubNeedSerializeReferences(SL_ATTRIBUTE);
        ASSERT_EQ(1, refVec.size());
    }
}

TEST_F(NormalScanRTest, testInitOutputColumnUseSubTrue2) {
    // copy field
    {
        NormalScanR scanR;
        bool useSubFlag = true;
        ASSERT_NO_FATAL_FAILURE(prepareNormalScanR(scanR, useSubFlag));
        auto matchDocAllocator = scanR._attributeExpressionCreatorR->_matchDocAllocator;
        scanR._scanInitParamR->calcInitParamR->outputFields
            = {"attr1", "attr1_copy", "sub_attr1", "sub_attr1_copy"};
        scanR._scanInitParamR->hashFields = {"id"};
        autil::legacy::json::ToString(
            ParseJson(R"json({"$attr1_copy":"$attr1", "$sub_attr1_copy":"$sub_attr1"})json"),
            scanR._scanInitParamR->calcInitParamR->outputExprsJson);
        ASSERT_TRUE(scanR.initOutputColumn());
        ASSERT_EQ(2, scanR._copyFieldMap.size());
        ASSERT_EQ("attr1", scanR._copyFieldMap["attr1_copy"]);
        ASSERT_EQ("sub_attr1", scanR._copyFieldMap["sub_attr1_copy"]);
        checkExpr(scanR._attributeExpressionVec, {"attr1", "sub_attr1"});
        auto refVec = matchDocAllocator->getRefBySerializeLevel(SL_ATTRIBUTE);
        ASSERT_EQ(2, refVec.size());
        refVec = matchDocAllocator->getAllSubNeedSerializeReferences(SL_ATTRIBUTE);
        ASSERT_EQ(2, refVec.size());
    }
}

TEST_F(NormalScanRTest, testInitOutputColumnUseSubTrue3) {
    // const field
    {
        NormalScanR scanR;
        bool useSubFlag = true;
        ASSERT_NO_FATAL_FAILURE(prepareNormalScanR(scanR, useSubFlag));
        auto matchDocAllocator = scanR._attributeExpressionCreatorR->_matchDocAllocator;
        scanR._scanInitParamR->calcInitParamR->outputFields = {"attr1", "$f0", "$f1", "$f2"};
        scanR._scanInitParamR->hashFields = {"id"};
        autil::legacy::json::ToString(
            ParseJson(R"json({"$attr1":1, "$$f0":0, "$$f1":0.1, "$$f2":"st1"})json"),
            scanR._scanInitParamR->calcInitParamR->outputExprsJson);
        ASSERT_TRUE(scanR.initOutputColumn());
        ASSERT_EQ(0, scanR._copyFieldMap.size());
        ASSERT_EQ(6, matchDocAllocator->getReferenceCount());
        checkExpr(scanR._attributeExpressionVec, {"attr1", "$f0", "$f1", "$f2"});
        for (auto expr : scanR._attributeExpressionVec) {
            ASSERT_EQ(suez::turing::ET_CONST, expr->getExpressionType());
        }
        ReferenceBase *refBase = matchDocAllocator->findReferenceWithoutType("$f0");
        ASSERT_EQ(bt_int32, refBase->getValueType().getBuiltinType());
        auto refVec = matchDocAllocator->getRefBySerializeLevel(SL_ATTRIBUTE);
        ASSERT_EQ(4, refVec.size());
    }
}

TEST_F(NormalScanRTest, testInitOutputColumnUseSubTrue4) {
    // const field with type
    {
        NormalScanR scanR;
        bool useSubFlag = true;
        ASSERT_NO_FATAL_FAILURE(prepareNormalScanR(scanR, useSubFlag));
        auto matchDocAllocator = scanR._attributeExpressionCreatorR->_matchDocAllocator;
        scanR._scanInitParamR->calcInitParamR->outputFields = {"$f0", "$f1", "$f2"};
        scanR._scanInitParamR->hashFields = {"id"};
        scanR._scanInitParamR->calcInitParamR->outputFieldsType = {"BIGINT", "DOUBLE", "VARCHAR"};
        autil::legacy::json::ToString(ParseJson(R"json({"$$f0":0, "$$f1":0.1, "$$f2":"st1"})json"),
                                      scanR._scanInitParamR->calcInitParamR->outputExprsJson);
        ASSERT_TRUE(scanR.initOutputColumn());
        ASSERT_EQ(0, scanR._copyFieldMap.size());
        ASSERT_EQ(5, matchDocAllocator->getReferenceCount());
        checkExpr(scanR._attributeExpressionVec, {"$f0", "$f1", "$f2"});
        for (auto expr : scanR._attributeExpressionVec) {
            ASSERT_EQ(suez::turing::ET_CONST, expr->getExpressionType());
        }
        ReferenceBase *refBase = matchDocAllocator->findReferenceWithoutType("$f0");
        ASSERT_EQ(bt_int64, refBase->getValueType().getBuiltinType());
        auto refVec = matchDocAllocator->getRefBySerializeLevel(SL_ATTRIBUTE);
        ASSERT_EQ(3, refVec.size());
    }
}

TEST_F(NormalScanRTest, testInitOutputColumnUseSubTrue5) {
    // null field
    {
        NormalScanR scanR;
        bool useSubFlag = true;
        ASSERT_NO_FATAL_FAILURE(prepareNormalScanR(scanR, useSubFlag));
        auto matchDocAllocator = scanR._attributeExpressionCreatorR->_matchDocAllocator;
        scanR._scanInitParamR->calcInitParamR->outputFields = {"attr1", "attr3"};
        scanR._scanInitParamR->hashFields = {"id"};
        scanR._scanInitParamR->calcInitParamR->outputExprsJson = "";
        scanR._scanInitParamR->fieldInfos = {{"attr1", {}}, {"attr3", {}}};
        ASSERT_TRUE(scanR.initOutputColumn());
        ASSERT_EQ(0, scanR._copyFieldMap.size());
        checkExpr(scanR._attributeExpressionVec, {"attr1", "attr3"});
        auto refVec = matchDocAllocator->getRefBySerializeLevel(SL_ATTRIBUTE);
        ASSERT_EQ(2, refVec.size());
        ASSERT_EQ(suez::turing::ET_CONST, scanR._attributeExpressionVec[1]->getExpressionType());
    }
}

TEST_F(NormalScanRTest, testInitOutputColumnUseSubTrue6) {
    // case op
    {
        NormalScanR scanR;
        bool useSubFlag = true;
        ASSERT_NO_FATAL_FAILURE(prepareNormalScanR(scanR, useSubFlag));
        auto matchDocAllocator = scanR._attributeExpressionCreatorR->_matchDocAllocator;
        scanR._scanInitParamR->calcInitParamR->outputFields
            = {"attr1", "$f0", "$f1", "$f2", "case"};
        scanR._scanInitParamR->hashFields = {"id"};
        scanR._scanInitParamR->calcInitParamR->outputExprsJson
            = R"({"$attr1":1, "$$f0":0, "$$f1":0.1, "$$f2":"st1", "$case":{"op":"CASE","params":[{"op":"=","params":["$id",{"op":"CAST","cast_type":"BIGINT","params":[48],"type":"UDF"}],"type":"OTHER"},"$attr1",{"op":"=","params":["$id",{"op":"CAST","cast_type":"BIGINT","params":[24],"type":"UDF"}],"type":"OTHER"},"$attr1","$attr1"],"type":"OTHER"}})";
        scanR._scanInitParamR->calcInitParamR->outputFieldsType
            = {"BIGINT", "BIGINT", "DECIMAL", "VARCHAR", "BIGINT"};
        ASSERT_TRUE(scanR.initOutputColumn());
        ASSERT_EQ(0, scanR._copyFieldMap.size());
        ASSERT_EQ(7, matchDocAllocator->getReferenceCount());
        ASSERT_EQ(scanR._attributeExpressionVec.size(), 5);
        ReferenceBase *refBase = matchDocAllocator->findReferenceWithoutType("case");
        ASSERT_EQ(bt_int64, refBase->getValueType().getBuiltinType());
        auto refVec = matchDocAllocator->getRefBySerializeLevel(SL_ATTRIBUTE);
        ASSERT_EQ(5, refVec.size());
    }
}

TEST_F(NormalScanRTest, testInitOutputColumnUseSubTrue7) {
    // case op boolean
    {
        NormalScanR scanR;
        bool useSubFlag = true;
        ASSERT_NO_FATAL_FAILURE(prepareNormalScanR(scanR, useSubFlag));
        auto matchDocAllocator = scanR._attributeExpressionCreatorR->_matchDocAllocator;
        scanR._scanInitParamR->hashFields = {"id"};
        scanR._scanInitParamR->calcInitParamR->outputFields
            = {"attr1", "$f0", "$f1", "$f2", "case"};
        scanR._scanInitParamR->calcInitParamR->outputExprsJson
            = R"({"$attr1":1, "$$f0":0, "$$f1":0.1, "$$f2":"st1", "$case":{"op":"CASE","params":[{"op":"=","params":[{"op":"*","params":[{"op":"+","params":["$attr1",1],"type":"OTHER"},2],"type":"OTHER"},{"op":"CAST","cast_type":"BIGINT","params":[48],"type":"UDF"}],"type":"OTHER"},{"op":">","params":["$attr1",10],"type":"OTHER"},{"op":"=","params":[{"op":"*","params":[{"op":"+","params":["$attr1",1],"type":"OTHER"},2],"type":"OTHER"},{"op":"CAST","cast_type":"BIGINT","params":[24],"type":"UDF"}],"type":"OTHER"},{"op":">","params":["$id",10],"type":"OTHER"},{"op":"CAST","cast_type":"BOOLEAN","params":[false],"type":"UDF"}],"type":"OTHER"}})";
        scanR._scanInitParamR->calcInitParamR->outputFieldsType
            = {"BIGINT", "BIGINT", "DECIMAL", "VARCHAR", "BOOLEAN"};
        ASSERT_TRUE(scanR.initOutputColumn());
        ASSERT_EQ(0, scanR._copyFieldMap.size());
        ASSERT_EQ(7, matchDocAllocator->getReferenceCount());
        ASSERT_EQ(scanR._attributeExpressionVec.size(), 5);
        ReferenceBase *refBase = matchDocAllocator->findReferenceWithoutType("case");
        ASSERT_EQ(bt_bool, refBase->getValueType().getBuiltinType());
        auto refVec = matchDocAllocator->getRefBySerializeLevel(SL_ATTRIBUTE);
        ASSERT_EQ(5, refVec.size());
    }
}

TEST_F(NormalScanRTest, testInitOutputColumnUseSubTrue8) {
    // case op string
    {
        NormalScanR scanR;
        bool useSubFlag = true;
        ASSERT_NO_FATAL_FAILURE(prepareNormalScanR(scanR, useSubFlag));
        auto matchDocAllocator = scanR._attributeExpressionCreatorR->_matchDocAllocator;
        scanR._scanInitParamR->hashFields = {"id"};
        scanR._scanInitParamR->calcInitParamR->outputFields
            = {"attr1", "$f0", "$f1", "$f2", "case"};
        scanR._scanInitParamR->calcInitParamR->outputExprsJson
            = R"({"$attr1":1,"$$f0":0,"$$f1":0.1,"$$f2":"st1","$case":{"op":"CASE","params":[{"op":"=","params":[{"op":"*","params":[{"op":"+","params":["$attr1",1],"type":"OTHER"},2],"type":"OTHER"},{"op":"CAST","cast_type":"BIGINT","params":[48],"type":"UDF"}],"type":"OTHER"},"abc",{"op":"=","params":[{"op":"*","params":[{"op":"+","params":["$attr1",1],"type":"OTHER"},2],"type":"OTHER"},{"op":"CAST","cast_type":"BIGINT","params":[24],"type":"UDF"}],"type":"OTHER"},"def","ghi"],"type":"OTHER"}})";
        scanR._scanInitParamR->calcInitParamR->outputFieldsType
            = {"BIGINT", "BIGINT", "DECIMAL", "VARCHAR", "VARCHAR"};
        ASSERT_TRUE(scanR.initOutputColumn());
        ASSERT_EQ(0, scanR._copyFieldMap.size());
        ASSERT_EQ(7, matchDocAllocator->getReferenceCount());
        ASSERT_EQ(scanR._attributeExpressionVec.size(), 5);
        ReferenceBase *refBase = matchDocAllocator->findReferenceWithoutType("case");
        ASSERT_EQ(bt_string, refBase->getValueType().getBuiltinType());
        auto refVec = matchDocAllocator->getRefBySerializeLevel(SL_ATTRIBUTE);
        ASSERT_EQ(5, refVec.size());
    }
}

TEST_F(NormalScanRTest, testInitOutputColumnUseSubFalse1) {
    { // output expr is empty
        NormalScanR scanR;
        bool useSubFlag = false;
        ASSERT_NO_FATAL_FAILURE(prepareNormalScanR(scanR, useSubFlag));
        auto matchDocAllocator = scanR._attributeExpressionCreatorR->_matchDocAllocator;
        scanR._scanInitParamR->hashFields = {"id"};
        scanR._scanInitParamR->calcInitParamR->outputFields = {"attr1", "attr2"};
        scanR._scanInitParamR->calcInitParamR->outputExprsJson = "";
        ASSERT_TRUE(scanR.initOutputColumn());
        ASSERT_EQ(0, scanR._copyFieldMap.size());
        checkExpr(scanR._attributeExpressionVec, {"attr1", "attr2"});
        auto refVec = matchDocAllocator->getRefBySerializeLevel(SL_ATTRIBUTE);
        ASSERT_EQ(2, refVec.size());
    }
}

TEST_F(NormalScanRTest, testInitOutputColumnUseSubFalse2) {
    { // copy field
        NormalScanR scanR;
        bool useSubFlag = false;
        ASSERT_NO_FATAL_FAILURE(prepareNormalScanR(scanR, useSubFlag));
        auto matchDocAllocator = scanR._attributeExpressionCreatorR->_matchDocAllocator;
        scanR._scanInitParamR->hashFields = {"id"};
        scanR._scanInitParamR->calcInitParamR->outputFields = {"attr1", "attr1_copy"};
        autil::legacy::json::ToString(ParseJson(R"json({"$attr1_copy":"$attr1"})json"),
                                      scanR._scanInitParamR->calcInitParamR->outputExprsJson);
        ASSERT_TRUE(scanR.initOutputColumn());
        ASSERT_EQ(1, scanR._copyFieldMap.size());
        ASSERT_EQ("attr1", scanR._copyFieldMap["attr1_copy"]);
        ASSERT_EQ(2, matchDocAllocator->getReferenceCount());
        checkExpr(scanR._attributeExpressionVec, {"attr1"});
        auto refVec = matchDocAllocator->getRefBySerializeLevel(SL_ATTRIBUTE);
        ASSERT_EQ(2, refVec.size());
    }
}

TEST_F(NormalScanRTest, testInitOutputColumnUseSubFalse3) {
    { // const field
        NormalScanR scanR;
        bool useSubFlag = false;
        ASSERT_NO_FATAL_FAILURE(prepareNormalScanR(scanR, useSubFlag));
        auto matchDocAllocator = scanR._attributeExpressionCreatorR->_matchDocAllocator;
        scanR._scanInitParamR->hashFields = {"id"};
        scanR._scanInitParamR->calcInitParamR->outputFields = {"attr1", "$f0", "$f1", "$f2"};
        autil::legacy::json::ToString(
            ParseJson(R"json({"$attr1":1, "$$f0":0, "$$f1":0.1, "$$f2":"st1"})json"),
            scanR._scanInitParamR->calcInitParamR->outputExprsJson);
        ASSERT_TRUE(scanR.initOutputColumn());
        ASSERT_EQ(0, scanR._copyFieldMap.size());
        ASSERT_EQ(4, matchDocAllocator->getReferenceCount());
        checkExpr(scanR._attributeExpressionVec, {"attr1", "$f0", "$f1", "$f2"});
        for (auto expr : scanR._attributeExpressionVec) {
            ASSERT_EQ(suez::turing::ET_CONST, expr->getExpressionType());
        }
        ReferenceBase *refBase = matchDocAllocator->findReferenceWithoutType("$f0");
        ASSERT_EQ(bt_int32, refBase->getValueType().getBuiltinType());
        auto refVec = matchDocAllocator->getRefBySerializeLevel(SL_ATTRIBUTE);
        ASSERT_EQ(4, refVec.size());
    }
}

TEST_F(NormalScanRTest, testInitOutputColumnUseSubFalse4) {
    { // null field
        NormalScanR scanR;
        bool useSubFlag = false;
        ASSERT_NO_FATAL_FAILURE(prepareNormalScanR(scanR, useSubFlag));
        auto matchDocAllocator = scanR._attributeExpressionCreatorR->_matchDocAllocator;
        scanR._scanInitParamR->hashFields = {"id"};
        scanR._scanInitParamR->calcInitParamR->outputFields = {"attr1", "attr3"};
        scanR._scanInitParamR->calcInitParamR->outputExprsJson = "";
        scanR._scanInitParamR->fieldInfos = {{"attr1", {}}, {"attr3", {}}};
        ASSERT_TRUE(scanR.initOutputColumn());
        ASSERT_EQ(0, scanR._copyFieldMap.size());
        checkExpr(scanR._attributeExpressionVec, {"attr1", "attr3"});
        auto refVec = matchDocAllocator->getRefBySerializeLevel(SL_ATTRIBUTE);
        ASSERT_EQ(2, refVec.size());
        ASSERT_EQ(suez::turing::ET_CONST, scanR._attributeExpressionVec[1]->getExpressionType());
    }
}

TEST_F(NormalScanRTest, testInitOutputColumnUseSubFalse5) {
    { // case op
        NormalScanR scanR;
        bool useSubFlag = false;
        ASSERT_NO_FATAL_FAILURE(prepareNormalScanR(scanR, useSubFlag));
        auto matchDocAllocator = scanR._attributeExpressionCreatorR->_matchDocAllocator;
        scanR._scanInitParamR->hashFields = {"id"};
        scanR._scanInitParamR->calcInitParamR->outputFields
            = {"attr1", "$f0", "$f1", "$f2", "case"};
        scanR._scanInitParamR->calcInitParamR->outputExprsJson
            = R"({"$attr1":1, "$$f0":0, "$$f1":0.1, "$$f2":"st1", "$case":{"op":"CASE","params":[{"op":"=","params":["$id",{"op":"CAST","cast_type":"BIGINT","params":[48],"type":"UDF"}],"type":"OTHER"},"$attr1",{"op":"=","params":["$id",{"op":"CAST","cast_type":"BIGINT","params":[24],"type":"UDF"}],"type":"OTHER"},"$attr1","$attr1"],"type":"OTHER"}})";
        scanR._scanInitParamR->calcInitParamR->outputFieldsType
            = {"BIGINT", "BIGINT", "DECIMAL", "VARCHAR", "BIGINT"};
        ASSERT_TRUE(scanR.initOutputColumn());
        ASSERT_EQ(0, scanR._copyFieldMap.size());
        ASSERT_EQ(5, matchDocAllocator->getReferenceCount());
        ASSERT_EQ(scanR._attributeExpressionVec.size(), 5);
        ReferenceBase *refBase = matchDocAllocator->findReferenceWithoutType("case");
        ASSERT_EQ(bt_int64, refBase->getValueType().getBuiltinType());
        auto refVec = matchDocAllocator->getRefBySerializeLevel(SL_ATTRIBUTE);
        ASSERT_EQ(5, refVec.size());
    }
}

TEST_F(NormalScanRTest, testInitOutputColumnUseSubFalse6) {
    { // case op boolean
        NormalScanR scanR;
        bool useSubFlag = false;
        ASSERT_NO_FATAL_FAILURE(prepareNormalScanR(scanR, useSubFlag));
        auto matchDocAllocator = scanR._attributeExpressionCreatorR->_matchDocAllocator;
        scanR._scanInitParamR->hashFields = {"id"};
        scanR._scanInitParamR->calcInitParamR->outputFields
            = {"attr1", "$f0", "$f1", "$f2", "case"};
        scanR._scanInitParamR->calcInitParamR->outputExprsJson
            = R"({"$attr1":1, "$$f0":0, "$$f1":0.1, "$$f2":"st1", "$case":{"op":"CASE","params":[{"op":"=","params":[{"op":"*","params":[{"op":"+","params":["$attr1",1],"type":"OTHER"},2],"type":"OTHER"},{"op":"CAST","cast_type":"BIGINT","params":[48],"type":"UDF"}],"type":"OTHER"},{"op":">","params":["$attr1",10],"type":"OTHER"},{"op":"=","params":[{"op":"*","params":[{"op":"+","params":["$attr1",1],"type":"OTHER"},2],"type":"OTHER"},{"op":"CAST","cast_type":"BIGINT","params":[24],"type":"UDF"}],"type":"OTHER"},{"op":">","params":["$id",10],"type":"OTHER"},{"op":"CAST","cast_type":"BOOLEAN","params":[false],"type":"UDF"}],"type":"OTHER"}})";
        scanR._scanInitParamR->calcInitParamR->outputFieldsType
            = {"BIGINT", "BIGINT", "DECIMAL", "VARCHAR", "BOOLEAN"};
        ASSERT_TRUE(scanR.initOutputColumn());
        ASSERT_EQ(0, scanR._copyFieldMap.size());
        ASSERT_EQ(5, matchDocAllocator->getReferenceCount());
        ASSERT_EQ(scanR._attributeExpressionVec.size(), 5);
        ReferenceBase *refBase = matchDocAllocator->findReferenceWithoutType("case");
        ASSERT_EQ(bt_bool, refBase->getValueType().getBuiltinType());
        auto refVec = matchDocAllocator->getRefBySerializeLevel(SL_ATTRIBUTE);
        ASSERT_EQ(5, refVec.size());
    }
}

TEST_F(NormalScanRTest, testInitOutputColumnUseSubFalse7) {
    { // case op string
        NormalScanR scanR;
        bool useSubFlag = false;
        ASSERT_NO_FATAL_FAILURE(prepareNormalScanR(scanR, useSubFlag));
        auto matchDocAllocator = scanR._attributeExpressionCreatorR->_matchDocAllocator;
        scanR._scanInitParamR->hashFields = {"id"};
        scanR._scanInitParamR->calcInitParamR->outputFields
            = {"attr1", "$f0", "$f1", "$f2", "case"};
        scanR._scanInitParamR->calcInitParamR->outputExprsJson
            = R"({"$attr1":1,"$$f0":0,"$$f1":0.1,"$$f2":"st1","$case":{"op":"CASE","params":[{"op":"=","params":[{"op":"*","params":[{"op":"+","params":["$attr1",1],"type":"OTHER"},2],"type":"OTHER"},{"op":"CAST","cast_type":"BIGINT","params":[48],"type":"UDF"}],"type":"OTHER"},"abc",{"op":"=","params":[{"op":"*","params":[{"op":"+","params":["$attr1",1],"type":"OTHER"},2],"type":"OTHER"},{"op":"CAST","cast_type":"BIGINT","params":[24],"type":"UDF"}],"type":"OTHER"},"def","ghi"],"type":"OTHER"}})";
        scanR._scanInitParamR->calcInitParamR->outputFieldsType
            = {"BIGINT", "BIGINT", "DECIMAL", "VARCHAR", "VARCHAR"};
        ASSERT_TRUE(scanR.initOutputColumn());
        ASSERT_EQ(0, scanR._copyFieldMap.size());
        ASSERT_EQ(5, matchDocAllocator->getReferenceCount());
        ASSERT_EQ(scanR._attributeExpressionVec.size(), 5);
        ReferenceBase *refBase = matchDocAllocator->findReferenceWithoutType("case");
        ASSERT_EQ(bt_string, refBase->getValueType().getBuiltinType());
        auto refVec = matchDocAllocator->getRefBySerializeLevel(SL_ATTRIBUTE);
        ASSERT_EQ(5, refVec.size());
    }
}

TEST_F(NormalScanRTest, testCreateStreamScanIterator) {
    autil::legacy::json::JsonMap attributeMap;
    attributeMap["hash_fields"] = ParseJson(string(R"json(["id"])json"));
    attributeMap["table_type"] = string("normal");
    attributeMap["table_name"] = _tableName;
    attributeMap["db_name"] = string("default");
    attributeMap["catalog_name"] = string("default");
    attributeMap["output_fields_internal"]
        = ParseJson(string(R"json(["$attr1", "$attr2", "$id"])json"));
    attributeMap["limit"] = Any(1000);
    string jsonStr = autil::legacy::FastToJsonString(attributeMap);
    auto *naviRHelper = getNaviRHelper();
    naviRHelper->kernelConfig(jsonStr);
    auto scanR = naviRHelper->getOrCreateRes<NormalScanR>();
    ASSERT_TRUE(scanR);
    // int64_t timeout = TimeUtility::currentTime() / 1000 + 1000;
    // _sqlQueryResource->setTimeoutMs(timeout);

    scanR->_scanInitParamR->hashFields = {"id"};
    { // empty query
        ASSERT_TRUE(scanR->updateScanQuery(StreamQueryPtr()));
        ASSERT_TRUE(scanR->_scanIter == nullptr);
    }
    {
        StreamQueryPtr inputQuery(new StreamQuery());
        QueryPtr query(new TermQuery("aa", "name", RequiredFields(), ""));
        inputQuery->query = query;
        ASSERT_TRUE(scanR->updateScanQuery(inputQuery));
        ASSERT_TRUE(scanR->_scanIter != nullptr);
        vector<MatchDoc> matchDocs;
        bool ret = scanR->_scanIter->batchSeek(10, matchDocs).unwrap();
        ASSERT_TRUE(ret);
        ASSERT_EQ(1, matchDocs.size());
    }
    { // emtpy term
        StreamQueryPtr inputQuery(new StreamQuery());
        QueryPtr query(new TermQuery("aaa", "name", RequiredFields(), ""));
        inputQuery->query = query;
        ASSERT_TRUE(scanR->updateScanQuery(inputQuery));
        ASSERT_TRUE(scanR->_scanIter == nullptr);
    }
    { // and query
        StreamQueryPtr inputQuery(new StreamQuery());
        QueryPtr query(new TermQuery("aa", "name", RequiredFields(), ""));
        inputQuery->query = query;
        scanR->_scanIteratorCreatorR->_baseScanIteratorInfo.query = query;
        ASSERT_TRUE(scanR->updateScanQuery(inputQuery));
        ASSERT_TRUE(scanR->_scanIter != nullptr);
        vector<MatchDoc> matchDocs;
        bool ret = scanR->_scanIter->batchSeek(10, matchDocs).unwrap();
        ASSERT_TRUE(ret);
        ASSERT_EQ(1, matchDocs.size());
    }
}

TEST_F(NormalScanRTest, testCreateTable) {
    autil::legacy::json::JsonMap attributeMap;
    attributeMap["table_type"] = string("normal");
    attributeMap["table_name"] = _tableName;
    attributeMap["output_fields"] = ParseJson(string(R"json([])json"));
    string jsonStr = autil::legacy::FastToJsonString(attributeMap);
    auto *naviRHelper = getNaviRHelper();
    naviRHelper->kernelConfig(jsonStr);
    auto scanR = naviRHelper->getOrCreateRes<NormalScanR>();
    ASSERT_TRUE(scanR);
    { // reuse allocator
        MatchDocAllocatorPtr allocator(new MatchDocAllocator(_poolPtr.get(), false));
        auto ref1 = allocator->declare<int32_t>("aa", SL_ATTRIBUTE);
        vector<MatchDoc> matchDocVec;
        MatchDoc doc = allocator->allocate(1);
        matchDocVec.push_back(doc);
        ref1->set(doc, 10);
        scanR->_scanInitParamR->hashFields = {"id"};
        TablePtr table = scanR->createTable(matchDocVec, allocator, true);
        ASSERT_TRUE(table != nullptr);
        ASSERT_EQ(1, table->getRowCount());
        ASSERT_EQ(1, table->getColumnCount());
        ASSERT_EQ("10", table->toString(0, 0));
    }
    { // copy allocator
        MatchDocAllocatorPtr allocator(new MatchDocAllocator(_poolPtr.get(), true));
        auto ref1 = allocator->declare<int32_t>("aa", SL_ATTRIBUTE);
        vector<MatchDoc> matchDocVec;
        MatchDoc doc = allocator->allocate(1);
        matchDocVec.push_back(doc);
        ref1->set(doc, 10);
        scanR->_scanInitParamR->hashFields = {"id"};
        TablePtr table = scanR->createTable(matchDocVec, allocator, false);
        ASSERT_TRUE(table != nullptr);
        ASSERT_EQ(1, table->getRowCount());
        ASSERT_EQ(1, table->getColumnCount());
        ASSERT_EQ("10", table->toString(0, 0));
    }

    { // has subdoc ,reuse allocator
        MatchDocAllocatorPtr allocator(new MatchDocAllocator(_poolPtr.get(), true));
        auto ref1 = allocator->declare<int32_t>("aa", SL_ATTRIBUTE);
        auto subRef1 = allocator->declareSub<int32_t>("sub_aa", SL_ATTRIBUTE);
        vector<MatchDoc> matchDocVec;
        MatchDoc doc = allocator->allocate(1);
        allocator->allocateSub(doc, 20);
        subRef1->set(doc, 20);
        allocator->allocateSub(doc, 21);
        subRef1->set(doc, 21);
        allocator->allocateSub(doc, 22);
        subRef1->set(doc, 22);
        matchDocVec.push_back(doc);
        ref1->set(doc, 10);
        scanR->_scanInitParamR->hashFields = {"id"};
        scanR->_scanInitParamR->calcInitParamR->outputFields = {"aa", "sub_aa"};
        scanR->_useSubR->_useSub = true;
        TablePtr table = scanR->createTable(matchDocVec, allocator, true);
        ASSERT_TRUE(table != nullptr);
        ASSERT_EQ(3, table->getRowCount());
        ASSERT_EQ(2, table->getColumnCount());
        ASSERT_EQ("10", table->toString(0, 0));
        ASSERT_EQ("20", table->toString(0, 1));
        ASSERT_EQ("10", table->toString(1, 0));
        ASSERT_EQ("21", table->toString(1, 1));
        ASSERT_EQ("10", table->toString(2, 0));
        ASSERT_EQ("22", table->toString(2, 1));
    }
    { // has subdoc , copy allocator
        MatchDocAllocatorPtr allocator(new MatchDocAllocator(_poolPtr.get(), true));
        auto ref1 = allocator->declare<int32_t>("aa", SL_ATTRIBUTE);
        auto subRef1 = allocator->declareSub<int32_t>("sub_aa", SL_ATTRIBUTE);
        vector<MatchDoc> matchDocVec;
        MatchDoc doc = allocator->allocate(1);
        allocator->allocateSub(doc, 20);
        subRef1->set(doc, 20);
        allocator->allocateSub(doc, 21);
        subRef1->set(doc, 21);
        allocator->allocateSub(doc, 22);
        subRef1->set(doc, 22);
        matchDocVec.push_back(doc);
        ref1->set(doc, 10);
        scanR->_scanInitParamR->hashFields = {"id"};
        scanR->_scanInitParamR->calcInitParamR->outputFields = {"aa", "sub_aa"};
        scanR->_useSubR->_useSub = true;
        TablePtr table = scanR->createTable(matchDocVec, allocator, false);
        ASSERT_TRUE(table != nullptr);
        ASSERT_EQ(3, table->getRowCount());
        ASSERT_EQ(2, table->getColumnCount()) << TableUtil::toString(table);
        ASSERT_EQ("10", table->toString(0, 0));
        ASSERT_EQ("20", table->toString(0, 1));
        ASSERT_EQ("10", table->toString(1, 0));
        ASSERT_EQ("21", table->toString(1, 1));
        ASSERT_EQ("10", table->toString(2, 0));
        ASSERT_EQ("22", table->toString(2, 1));
    }
    { // has subdoc , copy allocator , has empty sub
        MatchDocAllocatorPtr allocator(new MatchDocAllocator(_poolPtr.get(), true));
        auto ref1 = allocator->declare<int32_t>("aa", SL_ATTRIBUTE);
        auto subRef1 = allocator->declareSub<int32_t>("sub_aa", SL_ATTRIBUTE);
        vector<MatchDoc> matchDocVec;
        MatchDoc doc = allocator->allocate(1);
        allocator->allocateSub(doc, 20);
        subRef1->set(doc, 20);
        allocator->allocateSub(doc, 21);
        subRef1->set(doc, 21);
        matchDocVec.push_back(doc);
        ref1->set(doc, 10);

        MatchDoc doc2 = allocator->allocate(2);
        ref1->set(doc2, 12);
        matchDocVec.push_back(doc2);
        scanR->_scanInitParamR->hashFields = {"id"};
        scanR->_scanInitParamR->calcInitParamR->outputFields = {"aa", "sub_aa"};
        scanR->_useSubR->_useSub = true;

        TablePtr table = scanR->createTable(matchDocVec, allocator, false);
        ASSERT_TRUE(table != nullptr);
        ASSERT_EQ(3, table->getRowCount());
        ASSERT_EQ(2, table->getColumnCount());
        ASSERT_EQ("10", table->toString(0, 0));
        ASSERT_EQ("20", table->toString(0, 1));
        ASSERT_EQ("10", table->toString(1, 0));
        ASSERT_EQ("21", table->toString(1, 1));
        ASSERT_EQ("12", table->toString(2, 0));
        ASSERT_EQ("0", table->toString(2, 1));
    }
}

TEST_F(NormalScanRTest, testFlattenSubLeftJoin) {
    // has subdoc , copy allocator , has empty sub
    MatchDocAllocatorPtr allocator(new MatchDocAllocator(_poolPtr.get(), true));
    auto ref1 = allocator->declare<int32_t>("aa", SL_ATTRIBUTE);
    auto subRef1 = allocator->declareSub<int32_t>("sub_aa", SL_ATTRIBUTE);
    vector<MatchDoc> matchDocVec;
    MatchDoc doc1 = allocator->allocate(1);
    auto *firstRef = allocator->_firstSub;
    auto *nextRef = allocator->_nextSub;
    ASSERT_NE(nullptr, firstRef);
    ASSERT_NE(nullptr, nextRef);

    auto subDoc1 = allocator->allocateSub(doc1, 20);
    subRef1->set(doc1, 20);
    auto subDoc2 = allocator->allocateSub(doc1, 21);
    subRef1->set(doc1, 21);
    matchDocVec.push_back(doc1);
    ref1->set(doc1, 10);
    ASSERT_EQ(subDoc1, firstRef->get(doc1));
    ASSERT_EQ(subDoc2, nextRef->get(subDoc1));
    ASSERT_EQ(INVALID_MATCHDOC, nextRef->get(subDoc2));

    MatchDoc doc2 = allocator->allocate(2);
    ref1->set(doc2, 12);
    matchDocVec.push_back(doc2);
    ASSERT_EQ(INVALID_MATCHDOC, firstRef->get(doc2));

    NormalScanR scanR;
    ASSERT_EQ(LEFT_JOIN, scanR._nestTableJoinType);

    TablePtr table;
    scanR.flattenSub(allocator, matchDocVec, table);
    ASSERT_TRUE(table != nullptr);
    ASSERT_EQ(3, table->getRowCount());
    ASSERT_EQ(2, table->getColumnCount());
    ASSERT_EQ("10", table->toString(0, 0));
    ASSERT_EQ("20", table->toString(0, 1));
    ASSERT_EQ("10", table->toString(1, 0));
    ASSERT_EQ("21", table->toString(1, 1));
    ASSERT_EQ("12", table->toString(2, 0));
    ASSERT_EQ("0", table->toString(2, 1));

    ASSERT_EQ(20, firstRef->get(table->_rows[0]).docid);
    ASSERT_EQ(21, firstRef->get(table->_rows[1]).docid);
    ASSERT_EQ(-1, firstRef->get(table->_rows[2]).docid);

    ASSERT_EQ(INVALID_MATCHDOC, nextRef->get(firstRef->get(table->_rows[0])));
    ASSERT_EQ(INVALID_MATCHDOC, nextRef->get(firstRef->get(table->_rows[1])));
    ASSERT_EQ(INVALID_MATCHDOC, nextRef->get(firstRef->get(table->_rows[2])));
}

TEST_F(NormalScanRTest, testFlattenSubInnerJoin) {
    // has subdoc , copy allocator , has empty sub
    MatchDocAllocatorPtr allocator(new MatchDocAllocator(_poolPtr.get(), true));
    auto ref1 = allocator->declare<int32_t>("aa", SL_ATTRIBUTE);
    auto subRef1 = allocator->declareSub<int32_t>("sub_aa", SL_ATTRIBUTE);
    vector<MatchDoc> matchDocVec;
    MatchDoc doc1 = allocator->allocate(1);
    auto *firstRef = allocator->_firstSub;
    auto *nextRef = allocator->_nextSub;
    ASSERT_NE(nullptr, firstRef);
    ASSERT_NE(nullptr, nextRef);

    auto subDoc1 = allocator->allocateSub(doc1, 20);
    subRef1->set(doc1, 20);
    auto subDoc2 = allocator->allocateSub(doc1, 21);
    subRef1->set(doc1, 21);
    matchDocVec.push_back(doc1);
    ref1->set(doc1, 10);
    ASSERT_EQ(subDoc1, firstRef->get(doc1));
    ASSERT_EQ(subDoc2, nextRef->get(subDoc1));
    ASSERT_EQ(INVALID_MATCHDOC, nextRef->get(subDoc2));

    MatchDoc doc2 = allocator->allocate(2);
    ref1->set(doc2, 12);
    matchDocVec.push_back(doc2);
    ASSERT_EQ(INVALID_MATCHDOC, firstRef->get(doc2));

    NormalScanR scanR;
    scanR._nestTableJoinType = INNER_JOIN;

    TablePtr table;
    scanR.flattenSub(allocator, matchDocVec, table);
    ASSERT_TRUE(table != nullptr);
    ASSERT_EQ(2, table->getRowCount());
    ASSERT_EQ(2, table->getColumnCount());
    ASSERT_EQ("10", table->toString(0, 0));
    ASSERT_EQ("20", table->toString(0, 1));
    ASSERT_EQ("10", table->toString(1, 0));
    ASSERT_EQ("21", table->toString(1, 1));

    ASSERT_EQ(20, firstRef->get(table->_rows[0]).docid);
    ASSERT_EQ(21, firstRef->get(table->_rows[1]).docid);

    ASSERT_EQ(INVALID_MATCHDOC, nextRef->get(firstRef->get(table->_rows[0])));
    ASSERT_EQ(INVALID_MATCHDOC, nextRef->get(firstRef->get(table->_rows[1])));
}

TEST_F(NormalScanRTest, testCopyColumns) {
    MatchDocAllocatorPtr allocator(new MatchDocAllocator(_poolPtr.get()));
    vector<MatchDoc> leftDocs = _matchDocUtil.createMatchDocs(allocator, 1);
    ASSERT_NO_FATAL_FAILURE(
        _matchDocUtil.extendMatchDocAllocator<uint32_t>(allocator, leftDocs, "fid", {1}));
    ASSERT_NO_FATAL_FAILURE(
        _matchDocUtil.extendMatchDocAllocator(allocator, leftDocs, "path", {"abc"}));
    map<string, string> fieldMap;
    fieldMap["fid0"] = "fid";
    fieldMap["path0"] = "path";
    for (auto pair : fieldMap) {
        ASSERT_TRUE(NormalScanR::appendCopyColumn(pair.second, pair.first, allocator));
    }
    allocator->extend();
    fieldMap["path00"] = "path";
    NormalScanR::copyColumns(fieldMap, leftDocs, allocator);
    Reference<uint32_t> *fid0 = allocator->findReference<uint32_t>("fid0");
    Reference<MultiChar> *path0 = allocator->findReference<MultiChar>("path0");
    ASSERT_TRUE(fid0 != nullptr);
    ASSERT_TRUE(path0 != nullptr);
    MatchDoc matchDoc = leftDocs[0];
    ASSERT_EQ(1, fid0->get(matchDoc));
    ASSERT_EQ("abc", string(path0->get(matchDoc).data(), path0->get(matchDoc).size()));
}

TEST_F(NormalScanRTest, testCopySubColumns) {
    MatchDocAllocatorPtr allocator(new MatchDocAllocator(_poolPtr.get()));
    Reference<int32_t> *ref1 = allocator->declareSub<int32_t>("ref1", 0);
    ASSERT_TRUE(NormalScanR::appendCopyColumn("ref1", "ref2", allocator));
    allocator->extend();
    MatchDoc doc1 = allocator->allocate(1);
    allocator->allocateSub(doc1, 1);
    ref1->set(doc1, 10);
    allocator->allocateSub(doc1, 2);
    ref1->set(doc1, 11);
    NormalScanR::copyColumns({{"ref2", "ref1"}}, {doc1}, allocator);
    Reference<int32_t> *ref2 = allocator->findReference<int32_t>("ref2");
    ASSERT_TRUE(ref2 != nullptr);
    vector<int32_t> subRef2Vals;
    auto accessor = allocator->getSubDocAccessor();
    std::function<void(MatchDoc)> getSubValue
        = [&subRef2Vals, ref2](MatchDoc newDoc) { subRef2Vals.push_back(ref2->get(newDoc)); };
    accessor->foreach (doc1, getSubValue);
    ASSERT_EQ(2, subRef2Vals.size());
    ASSERT_EQ(10, subRef2Vals[0]);
    ASSERT_EQ(11, subRef2Vals[1]);
}

TEST_F(NormalScanRTest, testAppendCopyColumns) {
    { // empty
        MatchDocAllocatorPtr allocator(new MatchDocAllocator(_poolPtr.get()));
        map<string, string> fieldMap;
        for (auto pair : fieldMap) {
            ASSERT_TRUE(NormalScanR::appendCopyColumn(pair.second, pair.first, allocator));
        }
        ASSERT_EQ(0, allocator->getReferenceCount());
    }
    { // ref not found
        MatchDocAllocatorPtr allocator(new MatchDocAllocator(_poolPtr.get()));
        map<string, string> fieldMap;
        fieldMap["not_exist"] = "not_exist1";
        for (auto pair : fieldMap) {
            ASSERT_FALSE(NormalScanR::appendCopyColumn(pair.second, pair.first, allocator));
        }
    }
    { // src dest is same
        MatchDocAllocatorPtr allocator(new MatchDocAllocator(_poolPtr.get()));
        vector<MatchDoc> leftDocs = _matchDocUtil.createMatchDocs(allocator, 1);
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<uint32_t>(allocator, leftDocs, "fid", {1}));
        map<string, string> fieldMap;
        fieldMap["fid"] = "fid";
        for (auto pair : fieldMap) {
            ASSERT_FALSE(NormalScanR::appendCopyColumn(pair.second, pair.first, allocator));
        }
    }
    { //
        MatchDocAllocatorPtr allocator(new MatchDocAllocator(_poolPtr.get()));
        vector<MatchDoc> leftDocs = _matchDocUtil.createMatchDocs(allocator, 1);
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<uint32_t>(allocator, leftDocs, "fid", {1}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator(allocator, leftDocs, "path", {"abc"}));
        map<string, string> fieldMap;
        fieldMap["fid"] = "path";
        for (auto pair : fieldMap) {
            ASSERT_FALSE(NormalScanR::appendCopyColumn(pair.second, pair.first, allocator));
        }
    }
    { // ok
        MatchDocAllocatorPtr allocator(new MatchDocAllocator(_poolPtr.get()));
        vector<MatchDoc> leftDocs = _matchDocUtil.createMatchDocs(allocator, 1);
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<uint32_t>(allocator, leftDocs, "fid", {1}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator(allocator, leftDocs, "path", {"abc"}));
        map<string, string> fieldMap;
        fieldMap["fid0"] = "fid";
        fieldMap["path0"] = "path";
        for (auto pair : fieldMap) {
            ASSERT_TRUE(NormalScanR::appendCopyColumn(pair.second, pair.first, allocator));
        }
        ASSERT_EQ(4, allocator->getReferenceCount());
        ASSERT_TRUE(allocator->findReference<uint32_t>("fid0") != nullptr);
        ASSERT_TRUE(allocator->findReference<MultiChar>("path0") != nullptr);
    }
}

TEST_F(NormalScanRTest, testInitNestTableJoinType) {
    // empty map, default value
    {
        auto paramR = std::make_shared<ScanInitParamR>();
        NormalScanR scanR;
        scanR._scanInitParamR = paramR.get();
        scanR.initNestTableJoinType();
        ASSERT_EQ(LEFT_JOIN, scanR._nestTableJoinType);
    }
    // simple test, inner join
    {
        auto paramR = std::make_shared<ScanInitParamR>();
        NormalScanR scanR;
        scanR._scanInitParamR = paramR.get();
        paramR->hintsMap[SQL_SCAN_HINT] = {{"nestTableJoinType", "inner"}};
        scanR.initNestTableJoinType();
        ASSERT_EQ(INNER_JOIN, scanR._nestTableJoinType);
    }
    // simple test, left join
    {
        auto paramR = std::make_shared<ScanInitParamR>();
        NormalScanR scanR;
        scanR._scanInitParamR = paramR.get();
        paramR->hintsMap[SQL_SCAN_HINT] = {{"nestTableJoinType", "left"}};
        scanR.initNestTableJoinType();
        ASSERT_EQ(LEFT_JOIN, scanR._nestTableJoinType);
    }
}

TEST_F(NormalScanRTest, testMatchData_emptyQuery) {
    autil::legacy::json::JsonMap attributeMap;
    attributeMap["table_type"] = string("normal");
    attributeMap["table_name"] = _tableName;
    attributeMap["db_name"] = string("default");
    attributeMap["catalog_name"] = string("default");
    attributeMap["hash_fields"] = ParseJson(string(R"json(["id"])json"));
    attributeMap["output_fields_internal"] = ParseJson(string(R"json(["$attr1"])json"));
    attributeMap["match_type"] = ParseJson(string(R"json(["simple"])json"));
    attributeMap["push_down_ops"] = ParseJson(R"json([
        {
            "attrs": {
                "condition": {"op":"=", "params":["$id", 1]}
            },
            "op_name": "CalcOp"
        }])json");
    string jsonStr = autil::legacy::FastToJsonString(attributeMap);
    auto *naviRHelper = getNaviRHelper();
    naviRHelper->kernelConfig(jsonStr);
    auto scanR = naviRHelper->getOrCreateRes<NormalScanR>();
    ASSERT_TRUE(scanR);
    TablePtr table;
    bool eof = false;
    ASSERT_TRUE(scanR->doBatchScan(table, eof));
    ASSERT_TRUE(table != nullptr);
    EXPECT_EQ("{}", scanR->_scanInitParamR->calcInitParamR->outputExprsJson);
    ASSERT_EQ(1, table->getRowCount());
    ASSERT_EQ("0", table->toString(0, 0));
    ASSERT_TRUE(scanR->_scanIteratorCreatorR->_matchDataManager->hasMatchData());
    ASSERT_TRUE(scanR->_scanIteratorCreatorR->_matchDataManager->_needMatchData);
    std::shared_ptr<isearch::rank::MetaInfo> metaInfo(new isearch::rank::MetaInfo);
    scanR->_scanIteratorCreatorR->_matchDataManager->getQueryTermMetaInfo(metaInfo.get());
    ASSERT_EQ(0, metaInfo->size());
}

TEST_F(NormalScanRTest, testMatchData) {
    autil::legacy::json::JsonMap attributeMap;
    attributeMap["table_type"] = string("normal");
    attributeMap["table_name"] = _tableName;
    attributeMap["db_name"] = string("default");
    attributeMap["catalog_name"] = string("default");
    attributeMap["hash_fields"] = ParseJson(string(R"json(["id"])json"));
    attributeMap["output_fields_internal"] = ParseJson(string(R"json(["$attr1"])json"));
    attributeMap["match_type"] = ParseJson(string(R"json(["simple"])json"));
    attributeMap["push_down_ops"] = ParseJson(R"json([
        {
            "attrs": {
                "condition": {"op":"=", "params":["$id", 1]}
            },
            "op_name": "CalcOp"
        }])json");
    attributeMap["table_meta"] = ParseJson(R"json({
        "field_meta": [
            {
                "field_type": "NUMBER",
                "field_name": "id",
                "index_name": "id",
                "index_type": "primarykey64"
            }
        ]
    })json");
    string jsonStr = autil::legacy::FastToJsonString(attributeMap);
    auto *naviRHelper = getNaviRHelper();
    naviRHelper->kernelConfig(jsonStr);
    auto scanR = naviRHelper->getOrCreateRes<NormalScanR>();
    ASSERT_TRUE(scanR);
    TablePtr table;
    bool eof = false;
    std::shared_ptr<isearch::rank::MetaInfo> metaInfo(new isearch::rank::MetaInfo);
    scanR->_scanIteratorCreatorR->_matchDataManager->getQueryTermMetaInfo(metaInfo.get());
    ASSERT_EQ(1, metaInfo->size());
    ASSERT_EQ("id", (*metaInfo)[0].getIndexName())
        << (*metaInfo)[0].getTermText() << (*metaInfo)[0].getQueryLabel();
    ASSERT_TRUE(scanR->_scanIteratorCreatorR->_matchDataManager->hasMatchData());
    ASSERT_TRUE(scanR->_scanIteratorCreatorR->_matchDataManager->_needMatchData);
    auto ref = scanR->_scanIteratorCreatorR->_matchDocAllocator->findReference<SimpleMatchData>(
        SIMPLE_MATCH_DATA_REF);
    ASSERT_TRUE(ref);
    ASSERT_TRUE(scanR->doBatchScan(table, eof));
    ASSERT_TRUE(table != nullptr);
    EXPECT_EQ("{}", scanR->_scanInitParamR->calcInitParamR->outputExprsJson);
    ASSERT_EQ(1, table->getRowCount());
    ASSERT_EQ("0", table->toString(0, 0));

    ref = table->getMatchDocAllocator()->findReference<SimpleMatchData>(SIMPLE_MATCH_DATA_REF);
    ASSERT_FALSE(ref);
}

TEST_F(NormalScanRTest, testSubMatchData) {
    navi::NaviLoggerProvider provider("TRACE1");
    autil::legacy::json::JsonMap attributeMap;
    attributeMap["table_type"] = string("normal");
    attributeMap["table_name"] = _tableName;
    attributeMap["db_name"] = string("default");
    attributeMap["catalog_name"] = string("default");
    attributeMap["use_nest_table"] = true;
    attributeMap["nest_table_attrs"]
        = ParseJson(string(R"json([{"table_name":"sub_invertedTable"}])json"));
    attributeMap["hash_fields"] = ParseJson(string(R"json(["id"])json"));
    attributeMap["output_fields_internal"] = ParseJson(string(R"json(["$attr1"])json"));
    attributeMap["match_type"] = ParseJson(string(R"json(["sub"])json"));
    attributeMap["push_down_ops"] = ParseJson(R"json([
        {
            "attrs": {
                "condition": {"op":"=", "params":["$id", 1]}
            },
            "op_name": "CalcOp"
        }])json");
    attributeMap["table_meta"] = ParseJson(R"json({
        "field_meta": [
            {
                "field_type": "NUMBER",
                "field_name": "id",
                "index_name": "id",
                "index_type": "primarykey64"
            }
        ]
    })json");
    string jsonStr = autil::legacy::FastToJsonString(attributeMap);
    auto *naviRHelper = getNaviRHelper();
    naviRHelper->kernelConfig(jsonStr);
    auto scanR = naviRHelper->getOrCreateRes<NormalScanR>();
    ASSERT_TRUE(scanR);
    TablePtr table;
    bool eof = false;
    std::shared_ptr<isearch::rank::MetaInfo> metaInfo(new isearch::rank::MetaInfo);
    scanR->_scanIteratorCreatorR->_matchDataManager->getQueryTermMetaInfo(metaInfo.get());
    ASSERT_EQ(1, metaInfo->size()) << autil::StringUtil::toString(provider.getTrace(""));
    ASSERT_EQ("id", (*metaInfo)[0].getIndexName())
        << (*metaInfo)[0].getTermText() << (*metaInfo)[0].getQueryLabel();
    ASSERT_TRUE(scanR->_scanIteratorCreatorR->_matchDataManager->hasSubMatchData());
    ASSERT_TRUE(scanR->_scanIteratorCreatorR->_matchDataManager->_needMatchData);
    auto ref
        = scanR->_scanIteratorCreatorR->_matchDocAllocator->findSubReference(SIMPLE_MATCH_DATA_REF);
    ASSERT_TRUE(ref);
    ASSERT_TRUE(scanR->doBatchScan(table, eof));
    ASSERT_TRUE(table != nullptr);
    EXPECT_EQ("{}", scanR->_scanInitParamR->calcInitParamR->outputExprsJson);
    ASSERT_EQ(3, table->getRowCount());
    ASSERT_EQ("0", table->toString(0, 0));

    ref = table->getMatchDocAllocator()->findSubReference(SIMPLE_MATCH_DATA_REF);
    ASSERT_FALSE(ref);
}

IndexInfos *NormalScanRTest::constructIndexInfos(
    const std::vector<pair<std::string, indexlib::InvertedIndexType>> &indexs) {
    auto *indexInfos = new IndexInfos();
    for (const auto &pair : indexs) {
        auto *index = new suez::turing::IndexInfo();
        index->setIndexName(pair.first.c_str());
        index->setIndexType(pair.second);
        indexInfos->addIndexInfo(index);
    }
    return indexInfos;
}

class MockIndexPartitionReaderWrapper : public isearch::search::IndexPartitionReaderWrapper {
public:
    MockIndexPartitionReaderWrapper() {}
    ~MockIndexPartitionReaderWrapper() {}

public:
    MOCK_METHOD0(getInvertedTracers, InvertedTracerVector(void));
};

TEST_F(NormalScanRTest, testCollectInvertedInfos) {
    // prepare normal scan
    auto tableInfo = std::make_shared<TableInfo>("inverted_table");
    auto *indexInfos = constructIndexInfos(
        {{"idx0", it_number_int8}, {"idx1", it_number_int16}, {"idx2", it_text}});
    tableInfo->setIndexInfos(indexInfos);
    auto mockIndexPartition = std::make_shared<MockIndexPartitionReaderWrapper>();
    auto *naviRHelper = getNaviRHelper();
    auto attrCreatorR = std::make_shared<AttributeExpressionCreatorR>();
    attrCreatorR->_indexPartitionReaderWrapper = mockIndexPartition;
    attrCreatorR->_tableInfo = tableInfo;
    NormalScanR scanR;
    scanR._attributeExpressionCreatorR = attrCreatorR.get();
    ASSERT_TRUE(naviRHelper->getOrCreateRes(scanR._sqlSearchInfoCollectorR));
    // prepare tracers
    indexlib::util::BlockAccessCounter counter;
    counter.blockCacheHitCount = 1;
    counter.blockCacheMissCount = 1;
    counter.blockCacheReadLatency = 1;
    counter.blockCacheIOCount = 1;
    counter.blockCacheIODataSize = 1;
    indexlib::index::InvertedIndexSearchTracer tracer;
    tracer._dictionaryCounter = counter;
    tracer._postingCounter = counter;
    tracer._searchedSegmentCount = 1;
    tracer._searchedInMemSegmentCount = 1;
    tracer._totalDictLookupCount = 1;
    tracer._totalDictHitCount = 1;
    std::vector<pair<std::string, indexlib::index::InvertedIndexSearchTracer>> tracerVec {
        {"idx0", tracer}, {"idx1", tracer}, {"idx2", tracer}};
    IndexPartitionReaderWrapper::InvertedTracerVector tracers;
    for (const auto &pair : tracerVec) {
        tracers.emplace_back(&pair.first, &pair.second);
    }
    EXPECT_CALL(*mockIndexPartition, getInvertedTracers()).WillOnce(Return(tracers));
    scanR.getInvertedTracers(scanR._tracerMap);
    ASSERT_EQ(2, scanR._tracerMap.size());
    ASSERT_EQ(
        "number:{dictCounter:{blockCacheHitCnt:2, blockCacheMissCnt:2, blockCacheReadLatency:2, "
        "blockCacheIOCnt:2, blockCacheIODataSize:2}, postingCounter:{blockCacheHitCnt:2, "
        "blockCacheMissCnt:2, blockCacheReadLatency:2, blockCacheIOCnt:2, blockCacheIODataSize:2}, "
        "searchedSegCnt:2, searchedInMemSegCnt:2, totalDictLookupCnt:2, "
        "totalDictHitCnt:2};text:{dictCounter:{blockCacheHitCnt:1, blockCacheMissCnt:1, "
        "blockCacheReadLatency:1, blockCacheIOCnt:1, blockCacheIODataSize:1}, "
        "postingCounter:{blockCacheHitCnt:1, blockCacheMissCnt:1, blockCacheReadLatency:1, "
        "blockCacheIOCnt:1, blockCacheIODataSize:1}, searchedSegCnt:1, searchedInMemSegCnt:1, "
        "totalDictLookupCnt:1, totalDictHitCnt:1};",
        autil::StringUtil::toString(scanR._tracerMap));

    // test collectInvertedInfos
    scanR.collectInvertedInfos(scanR._tracerMap);
    auto &searchInfoCollector = *scanR._sqlSearchInfoCollectorR->_collector;
    ASSERT_EQ(2, searchInfoCollector._sqlSearchInfo.invertedinfos().size());
    ASSERT_EQ(
        "invertedInfos { dictionaryInfo { blockCacheHitCount: 2 blockCacheMissCount: 2 "
        "blockCacheReadLatency: 2 blockCacheIOCount: 2 blockCacheIODataSize: 2 } postingInfo { "
        "blockCacheHitCount: 2 blockCacheMissCount: 2 blockCacheReadLatency: 2 blockCacheIOCount: "
        "2 blockCacheIODataSize: 2 } searchedSegmentCount: 2 searchedInMemSegmentCount: 2 "
        "totalDictLookupCount: 2 totalDictHitCount: 2 indexType: \"number\" hashKey: 1891950960 } "
        "invertedInfos { dictionaryInfo { blockCacheHitCount: 1 blockCacheMissCount: 1 "
        "blockCacheReadLatency: 1 blockCacheIOCount: 1 blockCacheIODataSize: 1 } postingInfo { "
        "blockCacheHitCount: 1 blockCacheMissCount: 1 blockCacheReadLatency: 1 blockCacheIOCount: "
        "1 blockCacheIODataSize: 1 } searchedSegmentCount: 1 searchedInMemSegmentCount: 1 "
        "totalDictLookupCount: 1 totalDictHitCount: 1 indexType: \"text\" hashKey: 4076674828 }",
        searchInfoCollector._sqlSearchInfo.ShortDebugString());
}

} // namespace sql
