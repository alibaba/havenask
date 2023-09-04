#include "sql/ops/scan/KVScanR.h"

#include <algorithm>
#include <cstddef>
#include <limits>
#include <map>
#include <memory>
#include <stdint.h>
#include <string>
#include <utility>
#include <vector>

#include "autil/NoCopyable.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "ha3/search/IndexPartitionReaderWrapper.h"
#include "ha3/search/PartitionInfoWrapper.h"
#include "indexlib/base/Status.h"
#include "indexlib/config/BuildConfig.h"
#include "indexlib/config/OnlineConfig.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/framework/ITablet.h"
#include "indexlib/framework/IndexRoot.h"
#include "indexlib/framework/LevelInfo.h"
#include "indexlib/index/attribute/Constant.h"
#include "indexlib/partition/index_application.h"
#include "indexlib/partition/index_partition.h"
#include "indexlib/table/kv_table/test/KVTableTestHelper.h"
#include "indexlib/test/schema_maker.h"
#include "matchdoc/ValueType.h"
#include "matchdoc/VectorDocStorage.h"
#include "navi/common.h"
#include "navi/tester/NaviResourceHelper.h"
#include "sql/common/WatermarkType.h"
#include "sql/framework/PushDownOp.h"
#include "sql/ops/calc/CalcInitParamR.h"
#include "sql/ops/scan/Collector.h"
#include "sql/ops/scan/ScanInitParamR.h"
#include "sql/ops/test/OpTestBase.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/util/TableInfo.h"
#include "suez/turing/expression/util/TableInfoConfigurator.h"
#include "suez/turing/navi/TableInfoR.h"
#include "table/Column.h"
#include "table/ColumnSchema.h"
#include "table/Table.h"
#include "unittest/unittest.h"

namespace autil {
namespace mem_pool {
class Pool;
} // namespace mem_pool
} // namespace autil

using namespace std;
using namespace autil;
using namespace table;
using namespace matchdoc;
using namespace testing;
using namespace suez::turing;
using autil::mem_pool::Pool;
using namespace navi;
using namespace indexlibv2;
using namespace indexlibv2::framework;
using namespace indexlibv2::index;

using namespace isearch::search;
using namespace isearch::common;

#define ASSERT_ERROR_LOG(expr, log)                                                                \
    {                                                                                              \
        navi::NaviLoggerProvider provider("ERROR");                                                \
        expr;                                                                                      \
        auto traces = provider.getTrace("");                                                       \
        CHECK_TRACE_COUNT(1, log, traces);                                                         \
    }

namespace sql {

class MockKVScanForInit : public KVScanR {
public:
    MockKVScanForInit() {}

public:
    MOCK_METHOD0(startLookupCtx, void(void));
};

class KVScanTestR : public OpTestBase {
public:
    KVScanTestR() {
        _tableName = "kv";
        _needBuildIndex = true;
        _needExprResource = true;
        _needKvTable = true;
    }

    void prepareUserIndex() override {
        prepareSqlKvTable();
    }

    void prepareSqlKvTable() {
        std::string tableName, fields, attributes, docs, pkeyField;
        bool enableTTL;
        int64_t ttl;
        {
            prepareKvPk64TableData(tableName, fields, pkeyField, attributes, docs, enableTTL, ttl);
            std::string testPath = _testPath + tableName;
            auto indexPartition = makeIndexPartitionForKvTable(
                testPath, tableName, fields, pkeyField, attributes, docs, enableTTL, ttl);
            std::string schemaName = indexPartition->GetSchema()->GetSchemaName();
            _indexPartitionMap.insert(make_pair(schemaName, indexPartition));
        }
        {
            prepareKvPkStrTableData(tableName, fields, pkeyField, attributes, docs, enableTTL, ttl);
            std::string testPath = _testPath + tableName;
            auto indexPartition = makeIndexPartitionForKvTable(
                testPath, tableName, fields, pkeyField, attributes, docs, enableTTL, ttl);
            std::string schemaName = indexPartition->GetSchema()->GetSchemaName();
            _indexPartitionMap.insert(make_pair(schemaName, indexPartition));
        }
        {
            prepareKvPk64UnmountTableData(
                tableName, fields, pkeyField, attributes, docs, enableTTL, ttl);
            std::string testPath = _testPath + tableName;
            auto indexPartition = makeIndexPartitionForKvTable(
                testPath, tableName, fields, pkeyField, attributes, docs, enableTTL, ttl);
            std::string schemaName = indexPartition->GetSchema()->GetSchemaName();
            _indexPartitionMap.insert(make_pair(schemaName, indexPartition));
        }
        {
            prepareKvPk64TableDataV2(
                tableName, fields, pkeyField, attributes, docs, enableTTL, ttl);
            std::string testPath = _testPath + tableName;
            auto indexPartition = makeIndexPartitionForKvTable(
                testPath, tableName, fields, pkeyField, attributes, docs, enableTTL, ttl);
            std::string schemaName = indexPartition->GetSchema()->GetSchemaName();
            _indexPartitionMap.insert(make_pair(schemaName, indexPartition));
        }
    }

    void prepareKvPk64TableData(string &tableName,
                                string &fields,
                                string &pkeyField,
                                string &attributes,
                                string &docs,
                                bool &enableTTL,
                                int64_t &ttl) {
        tableName = _tableName;
        fields = "attr1:string:true;attr2:uint32;pk:uint64";
        attributes = "attr1;attr2";
        docs = "cmd=add,attr1=00 aa,attr2=50,pk=0,ts=5000000;"
               "cmd=add,attr1=11 bb,attr2=51,pk=1,ts=10000000;"
               "cmd=add,attr1=22 cc,attr2=52,pk=2,ts=15000000;"
               "cmd=add,attr1=33 dd,attr2=53,pk=3,ts=20000000";
        pkeyField = "pk";
        enableTTL = false;
        ttl = std::numeric_limits<int64_t>::max();
    }

    void prepareKvPkStrTableData(std::string &tableName,
                                 std::string &fields,
                                 std::string &pkeyField,
                                 std::string &attributes,
                                 std::string &docs,
                                 bool &enableTTL,
                                 int64_t &ttl) {
        tableName = _tableName + "_str";
        fields = "attr1:uint32;attr2:string:true;pk:string";
        attributes = "attr1;attr2";
        docs = "cmd=add,attr1=0,attr2=00 aa,pk=000,ts=5000000;"
               "cmd=add,attr1=1,attr2=11 bb,pk=111,ts=10000000;"
               "cmd=add,attr1=2,attr2=22 cc,pk=222,ts=15000000;"
               "cmd=add,attr1=3,attr2=33 dd,pk=333,ts=20000000";
        pkeyField = "pk";
        enableTTL = false;
        ttl = std::numeric_limits<int64_t>::max();
    }

    void prepareKvPk64UnmountTableData(string &tableName,
                                       string &fields,
                                       string &pkeyField,
                                       string &attributes,
                                       string &docs,
                                       bool &enableTTL,
                                       int64_t &ttl) {
        tableName = _tableName + "_unmount";
        fields = "attr2:uint32;pk:uint64";
        attributes = "attr2";
        docs = "cmd=add,attr2=50,pk=0,ts=5000000;"
               "cmd=add,attr2=51,pk=1,ts=10000000;"
               "cmd=add,attr2=52,pk=2,ts=15000000;"
               "cmd=add,attr2=53,pk=3,ts=20000000";
        pkeyField = "pk";
        enableTTL = false;
        ttl = std::numeric_limits<int64_t>::max();
    }

    void prepareKvPk64TableDataV2(string &tableName,
                                  string &fields,
                                  string &pkeyField,
                                  string &attributes,
                                  string &docs,
                                  bool &enableTTL,
                                  int64_t &ttl) {
        tableName = _tableName + "_v2";
        fields = "attr1:string:true;attr2:uint32;pk:uint64";
        attributes = "attr1;attr2;pk";
        docs = "cmd=add,attr1=00 aa,attr2=50,pk=0,ts=5000000;"
               "cmd=add,attr1=11 bb,attr2=51,pk=1,ts=10000000;"
               "cmd=add,attr1=22 cc,attr2=52,pk=2,ts=15000000;"
               "cmd=add,attr1=33 dd,attr2=53,pk=3,ts=20000000";
        pkeyField = "pk";
        enableTTL = false;
        ttl = std::numeric_limits<int64_t>::max();
    }

    ScanInitParamR *preparePk64ScanInitParam(const string &conditionJson) {
        auto *naviRHelper = getNaviRHelper();
        auto paramR = std::make_shared<ScanInitParamR>();
        paramR->tableInfoR = naviRHelper->getOrCreateRes<suez::turing::TableInfoR>();
        auto calcR = std::make_shared<CalcInitParamR>();
        paramR->calcInitParamR = calcR.get();
        paramR->tableType = "kv";
        paramR->tableName = _tableName;
        paramR->calcInitParamR->outputFields = {"pk", "attr1", "attr2"};
        paramR->calcInitParamR->outputFieldsType = {"BIGINT", "VARCHAR", "BIGINT"};
        paramR->batchSize = 3;
        paramR->limit = 5;
        paramR->parallelIndex = 0;
        paramR->parallelNum = 1;
        paramR->calcInitParamR->conditionJson = conditionJson;
        paramR->hashType = "HASH";
        paramR->hashFields = {"pk"};
        paramR->usedFields = {"attr2", "attr1", "pk"};
        paramR->targetWatermark = 1111;
        paramR->targetWatermarkType = WatermarkType::WM_TYPE_SYSTEM_TS;
        if (!naviRHelper->addExternalRes(calcR)) {
            return nullptr;
        }
        if (!naviRHelper->addExternalRes(paramR)) {
            return nullptr;
        }
        return paramR.get();
    }

    ScanInitParamR *preparePkStrScanInitParam(const string &conditionJson) {
        auto *naviRHelper = getNaviRHelper();
        auto paramR = std::make_shared<ScanInitParamR>();
        paramR->tableInfoR = naviRHelper->getOrCreateRes<suez::turing::TableInfoR>();
        auto calcR = std::make_shared<CalcInitParamR>();
        paramR->calcInitParamR = calcR.get();
        paramR->tableType = "kv";
        paramR->tableName = _tableName + "_str";
        paramR->calcInitParamR->outputFields = {"pk", "attr1", "attr2"};
        paramR->calcInitParamR->outputFieldsType = {"VARCHAR", "BIGINT", "VARCHAR"};
        paramR->batchSize = 3;
        paramR->limit = 5;
        paramR->parallelIndex = 0;
        paramR->parallelNum = 1;
        paramR->calcInitParamR->conditionJson = conditionJson;
        paramR->hashType = "HASH";
        paramR->hashFields = {"pk"};
        paramR->usedFields = {"attr2", "attr1", "pk"};
        paramR->targetWatermark = 1111;
        paramR->targetWatermarkType = WatermarkType::WM_TYPE_SYSTEM_TS;
        if (!naviRHelper->addExternalRes(calcR)) {
            return nullptr;
        }
        if (!naviRHelper->addExternalRes(paramR)) {
            return nullptr;
        }
        return paramR.get();
    }

    ScanInitParamR *preparePk64UnmountScanInitParam(const string &conditionJson) {
        auto *naviRHelper = getNaviRHelper();
        auto paramR = std::make_shared<ScanInitParamR>();
        paramR->tableInfoR = naviRHelper->getOrCreateRes<suez::turing::TableInfoR>();
        auto calcR = std::make_shared<CalcInitParamR>();
        paramR->calcInitParamR = calcR.get();
        paramR->tableType = "kv";
        paramR->tableName = _tableName + "_unmount";
        paramR->calcInitParamR->outputFields = {"pk", "attr2"};
        paramR->calcInitParamR->outputFieldsType = {"BIGINT", "BIGINT"};
        paramR->batchSize = 3;
        paramR->limit = 5;
        paramR->parallelIndex = 0;
        paramR->parallelNum = 1;
        paramR->calcInitParamR->conditionJson = conditionJson;
        paramR->hashType = "HASH";
        paramR->hashFields = {"pk"};
        paramR->usedFields = {"attr2", "pk"};
        paramR->targetWatermark = 1111;
        paramR->targetWatermarkType = WatermarkType::WM_TYPE_SYSTEM_TS;
        if (!naviRHelper->addExternalRes(calcR)) {
            return nullptr;
        }
        if (!naviRHelper->addExternalRes(paramR)) {
            return nullptr;
        }
        return paramR.get();
    }
};

class KVScanTestRV2 : public KVScanTestR {
    KVScanTestRV2() {
        _tableName = "kv_tablet";
        _needExprResource = true;
        _needBuildTablet = true;
        _needBuildIndex = false;
    }

private:
    void prepareTablet() override {
        ASSERT_NO_FATAL_FAILURE(prepareSqlKvTablet());
    }

    void prepareSqlKvTablet() {
        std::string tableName, fields, attributes, docs, pkeyField;
        bool enableTTL;
        int64_t ttl;
        {
            ASSERT_NO_FATAL_FAILURE(prepareKvPk64TableDataTablet(
                tableName, fields, pkeyField, attributes, docs, enableTTL, ttl));
            std::string testPath = _testPath + tableName;
            framework::IndexRoot indexRoot(testPath, testPath);
            auto tabletSchema = indexlib::test::SchemaMaker::MakeKVTabletSchema(
                fields, pkeyField, attributes, ttl);
            tabletSchema->TEST_SetTableName(tableName);
            auto tabletOptions = CreateMultiColumnOptions();
            auto helper = make_shared<indexlibv2::table::KVTableTestHelper>();
            _dependentHolders.push_back(helper);
            ASSERT_TRUE(helper->Open(indexRoot, tabletSchema, std::move(tabletOptions)).IsOK());
            ASSERT_TRUE(helper->Build(docs).IsOK());
            auto tabletPtr = helper->GetITablet();
            ASSERT_TRUE(tabletPtr);
            _tabletMap.insert(make_pair(tableName, tabletPtr));
        }
    }

    void prepareKvPk64TableDataTablet(string &tableName,
                                      string &fields,
                                      string &pkeyField,
                                      string &attributes,
                                      string &docs,
                                      bool &enableTTL,
                                      int64_t &ttl) {
        tableName = _tableName;
        fields = "attr1:string:true;attr2:uint32;pk:uint64";
        attributes = "attr1;attr2";
        docs = "cmd=add,attr1=00 aa,attr2=50,pk=0,ts=5000000;"
               "cmd=add,attr1=11 bb,attr2=51,pk=1,ts=10000000;"
               "cmd=add,attr1=22 cc,attr2=52,pk=2,ts=15000000;"
               "cmd=add,attr1=33 dd,attr2=53,pk=3,ts=20000000";
        pkeyField = "pk";
        enableTTL = false;
        ttl = std::numeric_limits<int64_t>::max();
    }

    std::shared_ptr<config::TabletOptions> CreateMultiColumnOptions() {
        std::string jsonStr = R"( {
    "online_index_config": {
        "build_config": {
            "sharding_column_num" : 2,
            "level_num" : 3
        }
    }
    } )";
        auto tabletOptions = std::make_shared<indexlibv2::config::TabletOptions>();
        FastFromJsonString(*tabletOptions, jsonStr);
        tabletOptions->SetIsOnline(true);
        tabletOptions->TEST_GetOnlineConfig().TEST_GetBuildConfig().TEST_SetBuildingMemoryLimit(
            64 * 1024 * 1024);
        return tabletOptions;
    }
};

TEST_F(KVScanTestR, testInitFailed1) {
    {
        std::string conditionJson = "";
        ASSERT_TRUE(preparePk64ScanInitParam(conditionJson));
        auto *naviRHelper = getNaviRHelper();
        naviRHelper->kernelConfig(R"json({
		"kv_require_pk": true,
		"kv_async": false
	})json");
        auto *scanR = naviRHelper->getOrCreateRes<KVScanR>();
        ASSERT_FALSE(scanR);
    }
}

TEST_F(KVScanTestR, testInitFailed2) {
    {
        std::string conditionJson
            = "{\"op\" : \"OR\",\"type\" : \"OTHER\",\"params\" : [ {\"op\" : \"=\",\"type\" : "
              "\"OTHER\",\"params\" : [ \"$pk\", {\"op\" : \"CAST\",\"cast_type\" : "
              "\"BIGINT\",\"type\" : \"UDF\",\"params\" : [ 1 ]} ]}, {\"op\" : \"=\",\"type\" : "
              "\"OTHER\",\"params\" : [ \"$pk\", {\"op\" : \"CAST\",\"cast_type\" : "
              "\"BIGINT\",\"type\" : \"UDF\",\"params\" : [ 2 ] } ] }, { \"op\" : \">\", \"type\" "
              ": \"OTHER\", \"params\" : [ \"$pk\", { \"op\" : \"CAST\", \"cast_type\" : "
              "\"BIGINT\", \"type\" : \"UDF\",\"params\" : [ 3 ] } ] } ] }";
        ASSERT_TRUE(preparePk64ScanInitParam(conditionJson));
        auto *naviRHelper = getNaviRHelper();
        naviRHelper->kernelConfig(R"json({
		"kv_require_pk": true,
		"kv_async": false
	})json");
        auto *scanR = naviRHelper->getOrCreateRes<KVScanR>();
        ASSERT_FALSE(scanR);
    }
}

TEST_F(KVScanTestR, testInit1) {
    {
        std::string conditionJson
            = "{\"op\" : \"OR\",\"type\" : \"OTHER\",\"params\" : [ {\"op\" : \"=\",\"type\" : "
              "\"OTHER\",\"params\" : [ \"$pk\", {\"op\" : \"CAST\",\"cast_type\" : "
              "\"BIGINT\",\"type\" : \"UDF\",\"params\" : [ 1 ]} ]}, {\"op\" : \"=\",\"type\" : "
              "\"OTHER\",\"params\" : [ \"$pk\", {\"op\" : \"CAST\",\"cast_type\" : "
              "\"BIGINT\",\"type\" : \"UDF\",\"params\" : [ 2 ] } ] }, { \"op\" : \">\", \"type\" "
              ": \"OTHER\", \"params\" : [ \"$pk\", { \"op\" : \"CAST\", \"cast_type\" : "
              "\"BIGINT\", \"type\" : \"UDF\",\"params\" : [ 3 ] } ] } ] }";
        ASSERT_TRUE(preparePk64ScanInitParam(conditionJson));
        auto *naviRHelper = getNaviRHelper();
        naviRHelper->kernelConfig(R"json({
		"kv_require_pk": false,
		"kv_async": false
	})json");
        auto *scanR = naviRHelper->getOrCreateRes<KVScanR>();
        ASSERT_TRUE(scanR);
        ASSERT_TRUE(NULL != scanR->_pkeyCollector->getPkeyRef());
        ASSERT_EQ(0, scanR->_rawPks.size());
    }
}

TEST_F(KVScanTestR, testInit2) {
    {
        std::string conditionJson
            = "{\"op\" : \"OR\",\"type\" : \"OTHER\",\"params\" : [ {\"op\" : \"=\",\"type\" : "
              "\"OTHER\",\"params\" : [ \"$pk\", {\"op\" : \"CAST\",\"cast_type\" : "
              "\"BIGINT\",\"type\" : \"UDF\",\"params\" : [ 1 ]} ]}, {\"op\" : \"=\",\"type\" : "
              "\"OTHER\",\"params\" : [ \"$pk\", {\"op\" : \"CAST\",\"cast_type\" : "
              "\"BIGINT\",\"type\" : \"UDF\",\"params\" : [ 2 ] } ] }, { \"op\" : \"=\", \"type\" "
              ": \"OTHER\", \"params\" : [ \"$pk\", { \"op\" : \"CAST\", \"cast_type\" : "
              "\"BIGINT\", \"type\" : \"UDF\",\"params\" : [ 3 ] } ] } ] }";
        ASSERT_TRUE(preparePk64ScanInitParam(conditionJson));
        auto *naviRHelper = getNaviRHelper();
        naviRHelper->kernelConfig(R"json({
		"kv_require_pk": false,
		"kv_async": false
	})json");
        auto *scanR = naviRHelper->getOrCreateRes<KVScanR>();
        ASSERT_TRUE(scanR);
        ASSERT_TRUE(NULL != scanR->_pkeyCollector->getPkeyRef());
        ASSERT_EQ(3, scanR->_rawPks.size());
    }
}

TEST_F(KVScanTestR, testInit3) {
    {
        std::string conditionJson
            = "{\"op\" : \"OR\",\"type\" : \"OTHER\",\"params\" : [ {\"op\" : \"=\",\"type\" : "
              "\"OTHER\",\"params\" : [ \"$pk\", {\"op\" : \"CAST\",\"cast_type\" : "
              "\"BIGINT\",\"type\" : \"UDF\",\"params\" : [ 1 ]} ]}, {\"op\" : \"=\",\"type\" : "
              "\"OTHER\",\"params\" : [ \"$pk\", {\"op\" : \"CAST\",\"cast_type\" : "
              "\"BIGINT\",\"type\" : \"UDF\",\"params\" : [ 2 ] } ] }, { \"op\" : \"=\", \"type\" "
              ": \"OTHER\", \"params\" : [ \"$pk\", { \"op\" : \"CAST\", \"cast_type\" : "
              "\"BIGINT\", \"type\" : \"UDF\",\"params\" : [ 3 ] } ] } ] }";
        auto paramR = preparePk64ScanInitParam(conditionJson);
        ASSERT_TRUE(paramR);
        paramR->parallelIndex = 1;
        paramR->parallelNum = 2;
        auto *naviRHelper = getNaviRHelper();
        naviRHelper->kernelConfig(R"json({
		"kv_require_pk": false,
		"kv_async": false
	})json");
        auto *scanR = naviRHelper->getOrCreateRes<KVScanR>();
        ASSERT_TRUE(scanR);
        ASSERT_EQ(1, scanR->_rawPks.size());
        ASSERT_EQ("2", scanR->_rawPks[0]);
    }
}

TEST_F(KVScanTestR, testInit4) {
    // init for string
    {
        std::string conditionJson
            = R"({"op":"AND","type":"OTHER","params":[{"op":"OR","type":"OTHER","params":[{"op":"=","type":"OTHER","params":["$pk","111"]},{"op":"=","type":"OTHER","params":["$pk","222"]},{"op":"=","type":"OTHER","params":["$pk","333"]}]},{"op":"OR","type":"OTHER","params":[{"op":"=","type":"OTHER","params":[{"op":"CAST","cast_type":"INTEGER","type":"UDF","params":["$attr1"]},0]},{"op":"=","type":"OTHER","params":[{"op":"CAST","cast_type":"INTEGER","type":"UDF","params":["$attr1"]},1]},{"op":"=","type":"OTHER","params":[{"op":"CAST","cast_type":"INTEGER","type":"UDF","params":["$attr1"]},2]}]}]})";
        ASSERT_TRUE(preparePkStrScanInitParam(conditionJson));
        string tableName = _tableName + "_str";
        auto *naviRHelper = getNaviRHelper();
        auto *tableInfoR = naviRHelper->getOrCreateRes<TableInfoR>();
        tableInfoR->_tableInfoMapWithoutRel[tableName]
            = suez::turing::TableInfoConfigurator::createFromIndexApp(tableName, _indexApp);
        naviRHelper->kernelConfig(R"json({
		"kv_require_pk": false,
		"kv_async": false
	})json");
        auto *scanR = naviRHelper->getOrCreateRes<KVScanR>();
        ASSERT_TRUE(scanR);
        ASSERT_TRUE(NULL != scanR->_pkeyCollector->getPkeyRef());
        ASSERT_EQ(3, scanR->_rawPks.size());
    }
}

TEST_F(KVScanTestR, testDoBatchScan1) {
    // condition: pk in (1, 2, 300)
    {
        std::string conditionJson
            = "{\"op\" : \"OR\",\"type\" : \"OTHER\",\"params\" : [ {\"op\" : \"=\",\"type\" : "
              "\"OTHER\",\"params\" : [ \"$pk\", {\"op\" : \"CAST\",\"cast_type\" : "
              "\"BIGINT\",\"type\" : \"UDF\",\"params\" : [ 1 ]} ]}, {\"op\" : \"=\",\"type\" : "
              "\"OTHER\",\"params\" : [ \"$pk\", {\"op\" : \"CAST\",\"cast_type\" : "
              "\"BIGINT\",\"type\" : \"UDF\",\"params\" : [ 2 ] } ] }, { \"op\" : \"=\", \"type\" "
              ": \"OTHER\", \"params\" : [ \"$pk\", { \"op\" : \"CAST\", \"cast_type\" : "
              "\"BIGINT\", \"type\" : \"UDF\",\"params\" : [ 300 ] } ] } ] }";
        ASSERT_TRUE(preparePk64ScanInitParam(conditionJson));
        auto *naviRHelper = getNaviRHelper();
        naviRHelper->kernelConfig(R"json({
		"kv_async": false
	})json");
        auto *scanR = naviRHelper->getOrCreateRes<KVScanR>();
        ASSERT_TRUE(scanR);
        TablePtr table;
        bool eof = false;
        // navi::NaviLoggerProvider provider("ERROR");
        ASSERT_TRUE(scanR->batchScan(table, eof));
        ASSERT_TRUE(eof);
        ASSERT_EQ(2, table->getRowCount());
        ASSERT_EQ(3, table->getColumnCount());
        auto column = table->getColumn("pk");
        ASSERT_TRUE(column);
        ASSERT_TRUE(NULL != column->getColumnSchema()
                    && bt_uint64 == column->getColumnSchema()->getType().getBuiltinType());
        ASSERT_EQ("1", column->toString(0));
        ASSERT_EQ("2", column->toString(1));
        column = table->getColumn("attr1");
        ASSERT_TRUE(column);
        ASSERT_TRUE(NULL != column->getColumnSchema()
                    && bt_string == column->getColumnSchema()->getType().getBuiltinType());
        ASSERT_EQ("11^]bb", column->toString(0));
        ASSERT_EQ("22^]cc", column->toString(1));
        column = table->getColumn("attr2");
        ASSERT_TRUE(column);
        ASSERT_TRUE(NULL != column->getColumnSchema()
                    && bt_uint32 == column->getColumnSchema()->getType().getBuiltinType());
        ASSERT_EQ("51", column->toString(0));
        ASSERT_EQ("52", column->toString(1));
    }
}

TEST_F(KVScanTestR, testDoBatchScan2) {
    // condition: pk in (1, 2, 3) and attr2 in (50, 51, 52)
    {
        std::string conditionJson
            = "{\"op\":\"AND\",\"type\":\"OTHER\",\"params\":[{\"op\":\"OR\",\"type\":\"OTHER\","
              "\"params\":[{\"op\":\"=\",\"type\":\"OTHER\",\"params\":[\"$pk\",{\"op\":\"CAST\","
              "\"cast_type\":\"BIGINT\",\"type\":\"UDF\",\"params\":[1]}]},{\"op\":\"=\",\"type\":"
              "\"OTHER\",\"params\":[\"$pk\",{\"op\":\"CAST\",\"cast_type\":\"BIGINT\",\"type\":"
              "\"UDF\",\"params\":[2]}]},{\"op\":\"=\",\"type\":\"OTHER\",\"params\":[\"$pk\",{"
              "\"op\":\"CAST\",\"cast_type\":\"BIGINT\",\"type\":\"UDF\",\"params\":[3]}]}]},{"
              "\"op\":\"OR\",\"type\":\"OTHER\",\"params\":[{\"op\":\"=\",\"type\":\"OTHER\","
              "\"params\":[\"$attr2\",{\"op\":\"CAST\",\"cast_type\":\"BIGINT\",\"type\":\"UDF\","
              "\"params\":[50]}]},{\"op\":\"=\",\"type\":\"OTHER\",\"params\":[\"$attr2\",{\"op\":"
              "\"CAST\",\"cast_type\":\"BIGINT\",\"type\":\"UDF\",\"params\":[51]}]},{\"op\":\"=\","
              "\"type\":\"OTHER\",\"params\":[\"$attr2\",{\"op\":\"CAST\",\"cast_type\":\"BIGINT\","
              "\"type\":\"UDF\",\"params\":[52]}]}]}]}";
        ASSERT_TRUE(preparePk64ScanInitParam(conditionJson));
        auto *naviRHelper = getNaviRHelper();
        naviRHelper->kernelConfig(R"json({
		"kv_async": false
	})json");
        auto *scanR = naviRHelper->getOrCreateRes<KVScanR>();
        ASSERT_TRUE(scanR);
        TablePtr table;
        bool eof = false;
        bool ret = scanR->batchScan(table, eof);
        ASSERT_TRUE(ret);
        ASSERT_TRUE(eof);
        ASSERT_EQ(2, table->getRowCount());
        ASSERT_EQ(3, table->getColumnCount());
        auto column = table->getColumn("pk");
        ASSERT_TRUE(column);
        ASSERT_EQ("1", column->toString(0));
        ASSERT_EQ("2", column->toString(1));
        column = table->getColumn("attr1");
        ASSERT_TRUE(column);
        ASSERT_EQ("11^]bb", column->toString(0));
        ASSERT_EQ("22^]cc", column->toString(1));
        column = table->getColumn("attr2");
        ASSERT_TRUE(column);
        ASSERT_EQ("51", column->toString(0));
        ASSERT_EQ("52", column->toString(1));
    }
}

TEST_F(KVScanTestR, testDoBatchScan3) {
    // condition: pk in (1, 2, 3) and attr2 in (50, 51, 52) and parallelNum=2
    {
        std::string conditionJson
            = "{\"op\":\"AND\",\"type\":\"OTHER\",\"params\":[{\"op\":\"OR\",\"type\":\"OTHER\","
              "\"params\":[{\"op\":\"=\",\"type\":\"OTHER\",\"params\":[\"$pk\",{\"op\":\"CAST\","
              "\"cast_type\":\"BIGINT\",\"type\":\"UDF\",\"params\":[1]}]},{\"op\":\"=\",\"type\":"
              "\"OTHER\",\"params\":[\"$pk\",{\"op\":\"CAST\",\"cast_type\":\"BIGINT\",\"type\":"
              "\"UDF\",\"params\":[2]}]},{\"op\":\"=\",\"type\":\"OTHER\",\"params\":[\"$pk\",{"
              "\"op\":\"CAST\",\"cast_type\":\"BIGINT\",\"type\":\"UDF\",\"params\":[3]}]}]},{"
              "\"op\":\"OR\",\"type\":\"OTHER\",\"params\":[{\"op\":\"=\",\"type\":\"OTHER\","
              "\"params\":[\"$attr2\",{\"op\":\"CAST\",\"cast_type\":\"BIGINT\",\"type\":\"UDF\","
              "\"params\":[50]}]},{\"op\":\"=\",\"type\":\"OTHER\",\"params\":[\"$attr2\",{\"op\":"
              "\"CAST\",\"cast_type\":\"BIGINT\",\"type\":\"UDF\",\"params\":[51]}]},{\"op\":\"=\","
              "\"type\":\"OTHER\",\"params\":[\"$attr2\",{\"op\":\"CAST\",\"cast_type\":\"BIGINT\","
              "\"type\":\"UDF\",\"params\":[52]}]}]}]}";
        auto paramR = preparePk64ScanInitParam(conditionJson);
        ASSERT_TRUE(paramR);
        paramR->parallelIndex = 1;
        paramR->parallelNum = 2;
        auto *naviRHelper = getNaviRHelper();
        naviRHelper->kernelConfig(R"json({
		"kv_async": false
	})json");
        auto *scanR = naviRHelper->getOrCreateRes<KVScanR>();
        ASSERT_TRUE(scanR);
        TablePtr table;
        bool eof = false;
        bool ret = scanR->batchScan(table, eof);
        ASSERT_TRUE(NULL != table);
        ASSERT_TRUE(ret);
        ASSERT_TRUE(eof);
        ASSERT_EQ(1, table->getRowCount());
        ASSERT_EQ(3, table->getColumnCount());
        auto column = table->getColumn("pk");
        ASSERT_TRUE(column);
        ASSERT_EQ("2", column->toString(0));
        column = table->getColumn("attr1");
        ASSERT_TRUE(column);
        ASSERT_EQ("22^]cc", column->toString(0));
        column = table->getColumn("attr2");
        ASSERT_TRUE(column);
        ASSERT_EQ("52", column->toString(0));
    }
}

TEST_F(KVScanTestR, testDoBatchScan4) {
    // condition: pk in (1, 2, 3) and attr2 in (50, 51, 52) + limit 1 + out 2
    {
        std::string conditionJson
            = "{\"op\":\"AND\",\"type\":\"OTHER\",\"params\":[{\"op\":\"OR\",\"type\":\"OTHER\","
              "\"params\":[{\"op\":\"=\",\"type\":\"OTHER\",\"params\":[\"$pk\",{\"op\":\"CAST\","
              "\"cast_type\":\"BIGINT\",\"type\":\"UDF\",\"params\":[1]}]},{\"op\":\"=\",\"type\":"
              "\"OTHER\",\"params\":[\"$pk\",{\"op\":\"CAST\",\"cast_type\":\"BIGINT\",\"type\":"
              "\"UDF\",\"params\":[2]}]},{\"op\":\"=\",\"type\":\"OTHER\",\"params\":[\"$pk\",{"
              "\"op\":\"CAST\",\"cast_type\":\"BIGINT\",\"type\":\"UDF\",\"params\":[3]}]}]},{"
              "\"op\":\"OR\",\"type\":\"OTHER\",\"params\":[{\"op\":\"=\",\"type\":\"OTHER\","
              "\"params\":[\"$attr2\",{\"op\":\"CAST\",\"cast_type\":\"BIGINT\",\"type\":\"UDF\","
              "\"params\":[50]}]},{\"op\":\"=\",\"type\":\"OTHER\",\"params\":[\"$attr2\",{\"op\":"
              "\"CAST\",\"cast_type\":\"BIGINT\",\"type\":\"UDF\",\"params\":[51]}]},{\"op\":\"=\","
              "\"type\":\"OTHER\",\"params\":[\"$attr2\",{\"op\":\"CAST\",\"cast_type\":\"BIGINT\","
              "\"type\":\"UDF\",\"params\":[52]}]}]}]}";
        auto paramR = preparePk64ScanInitParam(conditionJson);
        ASSERT_TRUE(paramR);
        paramR->limit = 1;
        paramR->calcInitParamR->outputFields = {"pk", "attr2"};
        paramR->calcInitParamR->outputFieldsType = {"BIGINT", "BIGINT"};
        auto *naviRHelper = getNaviRHelper();
        naviRHelper->kernelConfig(R"json({
		"kv_async": false
	})json");
        auto *scanR = naviRHelper->getOrCreateRes<KVScanR>();
        ASSERT_TRUE(scanR);
        TablePtr table;
        bool eof = false;
        bool ret = scanR->batchScan(table, eof);
        ASSERT_TRUE(ret);
        ASSERT_TRUE(eof);
        ASSERT_EQ(1, table->getRowCount());
        ASSERT_EQ(2, table->getColumnCount());
        auto column = table->getColumn("pk");
        ASSERT_TRUE(column);
        ASSERT_EQ("1", column->toString(0));
        column = table->getColumn("attr1");
        ASSERT_FALSE(column);
        column = table->getColumn("attr2");
        ASSERT_TRUE(column);
        ASSERT_EQ("51", column->toString(0));
    }
}

TEST_F(KVScanTestR, testDoBatchScan5) {
    // condition: pk in ("111", "222", "333") and attr1 in (0, 1, 2)
    {
        std::string conditionJson
            = R"({"op":"AND","type":"OTHER","params":[{"op":"OR","type":"OTHER","params":[{"op":"=","type":"OTHER","params":["$pk","111"]},{"op":"=","type":"OTHER","params":["$pk","222"]},{"op":"=","type":"OTHER","params":["$pk","333"]}]},{"op":"OR","type":"OTHER","params":[{"op":"=","type":"OTHER","params":[{"op":"CAST","cast_type":"INTEGER","type":"UDF","params":["$attr1"]},0]},{"op":"=","type":"OTHER","params":[{"op":"CAST","cast_type":"INTEGER","type":"UDF","params":["$attr1"]},1]},{"op":"=","type":"OTHER","params":[{"op":"CAST","cast_type":"INTEGER","type":"UDF","params":["$attr1"]},2]}]}]})";
        ASSERT_TRUE(preparePkStrScanInitParam(conditionJson));
        string tableName = _tableName + "_str";
        auto *naviRHelper = getNaviRHelper();
        auto *tableInfoR = naviRHelper->getOrCreateRes<TableInfoR>();
        tableInfoR->_tableInfoMapWithoutRel[tableName]
            = suez::turing::TableInfoConfigurator::createFromIndexApp(tableName, _indexApp);
        naviRHelper->kernelConfig(R"json({
		"kv_async": false
	})json");
        auto *scanR = naviRHelper->getOrCreateRes<KVScanR>();
        ASSERT_TRUE(scanR);
        TablePtr table;
        bool eof = false;
        bool ret = scanR->batchScan(table, eof);
        ASSERT_TRUE(ret);
        ASSERT_TRUE(eof);
        ASSERT_EQ(2, table->getRowCount());
        ASSERT_EQ(3, table->getColumnCount());
        auto column = table->getColumn("pk");
        ASSERT_TRUE(column);
        ASSERT_EQ("111", column->toString(0));
        ASSERT_EQ("222", column->toString(1));
        column = table->getColumn("attr1");
        ASSERT_TRUE(column);
        ASSERT_EQ("1", column->toString(0));
        ASSERT_EQ("2", column->toString(1));
        column = table->getColumn("attr2");
        ASSERT_TRUE(column);
        ASSERT_EQ("11^]bb", column->toString(0));
        ASSERT_EQ("22^]cc", column->toString(1));
    }
}

TEST_F(KVScanTestR, testDoBatchScan6) {
    // condition: pk in (1, 2, 3) for unmount
    {
        std::string conditionJson
            = "{\"op\" : \"OR\",\"type\" : \"OTHER\",\"params\" : [ {\"op\" : \"=\",\"type\" : "
              "\"OTHER\",\"params\" : [ \"$pk\", {\"op\" : \"CAST\",\"cast_type\" : "
              "\"BIGINT\",\"type\" : \"UDF\",\"params\" : [ 1 ]} ]}, {\"op\" : \"=\",\"type\" : "
              "\"OTHER\",\"params\" : [ \"$pk\", {\"op\" : \"CAST\",\"cast_type\" : "
              "\"BIGINT\",\"type\" : \"UDF\",\"params\" : [ 2 ] } ] }, { \"op\" : \"=\", \"type\" "
              ": \"OTHER\", \"params\" : [ \"$pk\", { \"op\" : \"CAST\", \"cast_type\" : "
              "\"BIGINT\", \"type\" : \"UDF\",\"params\" : [ 3 ] } ] } ] }";
        preparePk64UnmountScanInitParam(conditionJson);
        string tableName = _tableName + "_unmount";
        auto *naviRHelper = getNaviRHelper();
        auto *tableInfoR = naviRHelper->getOrCreateRes<TableInfoR>();
        tableInfoR->_tableInfoMapWithoutRel[tableName]
            = suez::turing::TableInfoConfigurator::createFromIndexApp(tableName, _indexApp);
        naviRHelper->kernelConfig(R"json({
		"kv_async": false
	})json");
        auto *scanR = naviRHelper->getOrCreateRes<KVScanR>();
        ASSERT_TRUE(scanR);
        TablePtr table;
        bool eof = false;
        bool ret = scanR->batchScan(table, eof);
        ASSERT_TRUE(ret);
        ASSERT_TRUE(eof);
        ASSERT_EQ(3, table->getRowCount());
        ASSERT_EQ(2, table->getColumnCount());
        auto column = table->getColumn("pk");
        ASSERT_TRUE(column);
        ASSERT_EQ("1", column->toString(0));
        ASSERT_EQ("2", column->toString(1));
        ASSERT_EQ("3", column->toString(2));
        column = table->getColumn("attr2");
        ASSERT_TRUE(column);
        ASSERT_EQ("51", column->toString(0));
        ASSERT_EQ("52", column->toString(1));
        ASSERT_EQ("53", column->toString(2));
    }
}

TEST_F(KVScanTestR, testDoBatchScan7) {
    // test kv with pk attribute
    {
        std::string conditionJson
            = "{\"op\":\"AND\",\"type\":\"OTHER\",\"params\":[{\"op\":\"OR\",\"type\":\"OTHER\","
              "\"params\":[{\"op\":\"=\",\"type\":\"OTHER\",\"params\":[\"$pk\",{\"op\":\"CAST\","
              "\"cast_type\":\"BIGINT\",\"type\":\"UDF\",\"params\":[1]}]},{\"op\":\"=\",\"type\":"
              "\"OTHER\",\"params\":[\"$pk\",{\"op\":\"CAST\",\"cast_type\":\"BIGINT\",\"type\":"
              "\"UDF\",\"params\":[2]}]},{\"op\":\"=\",\"type\":\"OTHER\",\"params\":[\"$pk\",{"
              "\"op\":\"CAST\",\"cast_type\":\"BIGINT\",\"type\":\"UDF\",\"params\":[3]}]}]},{"
              "\"op\":\"OR\",\"type\":\"OTHER\",\"params\":[{\"op\":\"=\",\"type\":\"OTHER\","
              "\"params\":[\"$attr2\",{\"op\":\"CAST\",\"cast_type\":\"BIGINT\",\"type\":\"UDF\","
              "\"params\":[50]}]},{\"op\":\"=\",\"type\":\"OTHER\",\"params\":[\"$attr2\",{\"op\":"
              "\"CAST\",\"cast_type\":\"BIGINT\",\"type\":\"UDF\",\"params\":[51]}]},{\"op\":\"=\","
              "\"type\":\"OTHER\",\"params\":[\"$attr2\",{\"op\":\"CAST\",\"cast_type\":\"BIGINT\","
              "\"type\":\"UDF\",\"params\":[52]}]}]}]}";
        auto paramR = preparePk64ScanInitParam(conditionJson);
        ASSERT_TRUE(paramR);
        paramR->limit = 1;
        paramR->calcInitParamR->outputFields = {"pk", "attr2"};
        paramR->calcInitParamR->outputFieldsType = {"BIGINT", "BIGINT"};
        auto tableName = _tableName + "_v2";
        paramR->tableName = tableName;
        auto *naviRHelper = getNaviRHelper();
        auto *tableInfoR = naviRHelper->getOrCreateRes<TableInfoR>();
        tableInfoR->_tableInfoMapWithoutRel[tableName]
            = suez::turing::TableInfoConfigurator::createFromIndexApp(tableName, _indexApp);
        naviRHelper->kernelConfig(R"json({
		"kv_async": false
	})json");
        auto *scanR = naviRHelper->getOrCreateRes<KVScanR>();
        ASSERT_TRUE(scanR);
        TablePtr table;
        bool eof = false;
        bool ret = scanR->batchScan(table, eof);
        ASSERT_TRUE(ret);
        ASSERT_TRUE(eof);
        ASSERT_EQ(1, table->getRowCount());
        ASSERT_EQ(2, table->getColumnCount());
        auto column = table->getColumn("pk");
        ASSERT_TRUE(column);
        ASSERT_EQ("1", column->toString(0));
        column = table->getColumn("attr1");
        ASSERT_FALSE(column);
        column = table->getColumn("attr2");
        ASSERT_TRUE(column);
        ASSERT_EQ("51", column->toString(0));
    }
}

TEST_F(KVScanTestR, testUpdateScanQuery) {
    std::string conditionJson
        = "{\"op\" : \"OR\",\"type\" : \"OTHER\",\"params\" : [ {\"op\" : \"=\",\"type\" : "
          "\"OTHER\",\"params\" : [ \"$pk\", {\"op\" : \"CAST\",\"cast_type\" : "
          "\"BIGINT\",\"type\" : \"UDF\",\"params\" : [ 1 ]} ]}, {\"op\" : \"=\",\"type\" : "
          "\"OTHER\",\"params\" : [ \"$pk\", {\"op\" : \"CAST\",\"cast_type\" : "
          "\"BIGINT\",\"type\" : \"UDF\",\"params\" : [ 2 ] } ] }, { \"op\" : \"=\", \"type\" : "
          "\"OTHER\", \"params\" : [ \"$pk\", { \"op\" : \"CAST\", \"cast_type\" : \"BIGINT\", "
          "\"type\" : \"UDF\",\"params\" : [ 3 ] } ] } ] }";
    auto paramR = preparePk64ScanInitParam(conditionJson);
    ASSERT_TRUE(paramR);
    paramR->limit = 10000;
    auto *naviRHelper = getNaviRHelper();
    naviRHelper->kernelConfig(R"json({
		"kv_async": false
	})json");
    auto *scanR = naviRHelper->getOrCreateRes<KVScanR>();
    ASSERT_TRUE(scanR);
    TablePtr table;
    bool eof = false;
    // first scan
    ASSERT_TRUE(scanR->batchScan(table, eof));
    ASSERT_TRUE(eof);
    ASSERT_EQ(3, table->getRowCount());
    ASSERT_EQ(3, table->getColumnCount());
    auto column = table->getColumn("pk");
    ASSERT_TRUE(column);
    ASSERT_EQ("1", column->toString(0));
    ASSERT_EQ("2", column->toString(1));
    ASSERT_EQ("3", column->toString(2));
    column = table->getColumn("attr1");
    ASSERT_TRUE(column);
    ASSERT_EQ("11^]bb", column->toString(0));
    ASSERT_EQ("22^]cc", column->toString(1));
    ASSERT_EQ("33^]dd", column->toString(2));
    column = table->getColumn("attr2");
    ASSERT_TRUE(column);
    ASSERT_EQ("51", column->toString(0));
    ASSERT_EQ("52", column->toString(1));
    ASSERT_EQ("53", column->toString(2));
    // update pks
    StreamQueryPtr inputQuery(new StreamQuery());
    inputQuery->primaryKeys = {"3", "3", "2"};
    ASSERT_TRUE(scanR->updateScanQuery(inputQuery));
    ASSERT_EQ(2, scanR->_rawPks.size());
    ASSERT_EQ("3", scanR->_rawPks[0]);
    ASSERT_EQ("2", scanR->_rawPks[1]);
    TablePtr table2;
    bool eof2 = false;
    ASSERT_TRUE(scanR->batchScan(table2, eof2));
    ASSERT_TRUE(eof2);
    ASSERT_EQ(2, table2->getRowCount());
    ASSERT_EQ(3, table2->getColumnCount());
    column = table2->getColumn("pk");
    ASSERT_TRUE(column);
    ASSERT_EQ("3", column->toString(0));
    ASSERT_EQ("2", column->toString(1));
    column = table2->getColumn("attr1");
    ASSERT_TRUE(column);
    ASSERT_EQ("33^]dd", column->toString(0));
    ASSERT_EQ("22^]cc", column->toString(1));
    column = table2->getColumn("attr2");
    ASSERT_TRUE(column);
    ASSERT_EQ("53", column->toString(0));
    ASSERT_EQ("52", column->toString(1));
    inputQuery.reset();
    ASSERT_TRUE(scanR->updateScanQuery(inputQuery));
    TablePtr table3;
    bool eof3 = false;
    ASSERT_TRUE(scanR->batchScan(table3, eof3));
    ASSERT_TRUE(eof3);
    ASSERT_EQ(0, table3->getRowCount());
    ASSERT_EQ(3, table3->getColumnCount());
}

TEST_F(KVScanTestRV2, testDoBatchScan1) {
    // condition: pk in (1, 2, 300)
    {
        std::string conditionJson
            = "{\"op\" : \"OR\",\"type\" : \"OTHER\",\"params\" : [ {\"op\" : \"=\",\"type\" : "
              "\"OTHER\",\"params\" : [ \"$pk\", {\"op\" : \"CAST\",\"cast_type\" : "
              "\"BIGINT\",\"type\" : \"UDF\",\"params\" : [ 1 ]} ]}, {\"op\" : \"=\",\"type\" : "
              "\"OTHER\",\"params\" : [ \"$pk\", {\"op\" : \"CAST\",\"cast_type\" : "
              "\"BIGINT\",\"type\" : \"UDF\",\"params\" : [ 2 ] } ] }, { \"op\" : \"=\", \"type\" "
              ": \"OTHER\", \"params\" : [ \"$pk\", { \"op\" : \"CAST\", \"cast_type\" : "
              "\"BIGINT\", \"type\" : \"UDF\",\"params\" : [ 300 ] } ] } ] }";
        ASSERT_TRUE(preparePk64ScanInitParam(conditionJson));
        auto *naviRHelper = getNaviRHelper();
        naviRHelper->kernelConfig(R"json({
		"kv_async": false
	})json");
        auto *scanR = naviRHelper->getOrCreateRes<KVScanR>();
        ASSERT_TRUE(scanR);
        TablePtr table;
        bool eof = false;
        bool ret = scanR->batchScan(table, eof);
        ASSERT_TRUE(ret);
        ASSERT_TRUE(eof);
        ASSERT_EQ(2, table->getRowCount());
        ASSERT_EQ(3, table->getColumnCount());
        auto column = table->getColumn("pk");
        ASSERT_TRUE(column);
        ASSERT_TRUE(NULL != column->getColumnSchema()
                    && bt_uint64 == column->getColumnSchema()->getType().getBuiltinType());
        ASSERT_EQ("1", column->toString(0));
        ASSERT_EQ("2", column->toString(1));
        column = table->getColumn("attr1");
        ASSERT_TRUE(column);
        ASSERT_TRUE(NULL != column->getColumnSchema()
                    && bt_string == column->getColumnSchema()->getType().getBuiltinType());
        ASSERT_EQ("11^]bb", column->toString(0));
        ASSERT_EQ("22^]cc", column->toString(1));
        column = table->getColumn("attr2");
        ASSERT_TRUE(column);
        ASSERT_TRUE(NULL != column->getColumnSchema()
                    && bt_uint32 == column->getColumnSchema()->getType().getBuiltinType());
        ASSERT_EQ("51", column->toString(0));
        ASSERT_EQ("52", column->toString(1));
    }
}

TEST_F(KVScanTestRV2, testDoBatchScan2) {
    // condition: pk in (1, 2, 3) and attr2 in (50, 51, 52)
    {
        std::string conditionJson
            = "{\"op\":\"AND\",\"type\":\"OTHER\",\"params\":[{\"op\":\"OR\",\"type\":\"OTHER\","
              "\"params\":[{\"op\":\"=\",\"type\":\"OTHER\",\"params\":[\"$pk\",{\"op\":\"CAST\","
              "\"cast_type\":\"BIGINT\",\"type\":\"UDF\",\"params\":[1]}]},{\"op\":\"=\",\"type\":"
              "\"OTHER\",\"params\":[\"$pk\",{\"op\":\"CAST\",\"cast_type\":\"BIGINT\",\"type\":"
              "\"UDF\",\"params\":[2]}]},{\"op\":\"=\",\"type\":\"OTHER\",\"params\":[\"$pk\",{"
              "\"op\":\"CAST\",\"cast_type\":\"BIGINT\",\"type\":\"UDF\",\"params\":[3]}]}]},{"
              "\"op\":\"OR\",\"type\":\"OTHER\",\"params\":[{\"op\":\"=\",\"type\":\"OTHER\","
              "\"params\":[\"$attr2\",{\"op\":\"CAST\",\"cast_type\":\"BIGINT\",\"type\":\"UDF\","
              "\"params\":[50]}]},{\"op\":\"=\",\"type\":\"OTHER\",\"params\":[\"$attr2\",{\"op\":"
              "\"CAST\",\"cast_type\":\"BIGINT\",\"type\":\"UDF\",\"params\":[51]}]},{\"op\":\"=\","
              "\"type\":\"OTHER\",\"params\":[\"$attr2\",{\"op\":\"CAST\",\"cast_type\":\"BIGINT\","
              "\"type\":\"UDF\",\"params\":[52]}]}]}]}";
        ASSERT_TRUE(preparePk64ScanInitParam(conditionJson));
        auto *naviRHelper = getNaviRHelper();
        naviRHelper->kernelConfig(R"json({
		"kv_async": false
	})json");
        auto *scanR = naviRHelper->getOrCreateRes<KVScanR>();
        ASSERT_TRUE(scanR);
        TablePtr table;
        bool eof = false;
        bool ret = scanR->batchScan(table, eof);
        ASSERT_TRUE(ret);
        ASSERT_TRUE(eof);
        ASSERT_EQ(2, table->getRowCount());
        ASSERT_EQ(3, table->getColumnCount());
        auto column = table->getColumn("pk");
        ASSERT_TRUE(column);
        ASSERT_EQ("1", column->toString(0));
        ASSERT_EQ("2", column->toString(1));
        column = table->getColumn("attr1");
        ASSERT_TRUE(column);
        ASSERT_EQ("11^]bb", column->toString(0));
        ASSERT_EQ("22^]cc", column->toString(1));
        column = table->getColumn("attr2");
        ASSERT_TRUE(column);
        ASSERT_EQ("51", column->toString(0));
        ASSERT_EQ("52", column->toString(1));
    }
}

TEST_F(KVScanTestRV2, testDoBatchScan3) {
    // condition: pk in (1, 2, 3) and attr2 in (50, 51, 52) and parallelNum=2
    {
        std::string conditionJson
            = "{\"op\":\"AND\",\"type\":\"OTHER\",\"params\":[{\"op\":\"OR\",\"type\":\"OTHER\","
              "\"params\":[{\"op\":\"=\",\"type\":\"OTHER\",\"params\":[\"$pk\",{\"op\":\"CAST\","
              "\"cast_type\":\"BIGINT\",\"type\":\"UDF\",\"params\":[1]}]},{\"op\":\"=\",\"type\":"
              "\"OTHER\",\"params\":[\"$pk\",{\"op\":\"CAST\",\"cast_type\":\"BIGINT\",\"type\":"
              "\"UDF\",\"params\":[2]}]},{\"op\":\"=\",\"type\":\"OTHER\",\"params\":[\"$pk\",{"
              "\"op\":\"CAST\",\"cast_type\":\"BIGINT\",\"type\":\"UDF\",\"params\":[3]}]}]},{"
              "\"op\":\"OR\",\"type\":\"OTHER\",\"params\":[{\"op\":\"=\",\"type\":\"OTHER\","
              "\"params\":[\"$attr2\",{\"op\":\"CAST\",\"cast_type\":\"BIGINT\",\"type\":\"UDF\","
              "\"params\":[50]}]},{\"op\":\"=\",\"type\":\"OTHER\",\"params\":[\"$attr2\",{\"op\":"
              "\"CAST\",\"cast_type\":\"BIGINT\",\"type\":\"UDF\",\"params\":[51]}]},{\"op\":\"=\","
              "\"type\":\"OTHER\",\"params\":[\"$attr2\",{\"op\":\"CAST\",\"cast_type\":\"BIGINT\","
              "\"type\":\"UDF\",\"params\":[52]}]}]}]}";
        auto paramR = preparePk64ScanInitParam(conditionJson);
        ASSERT_TRUE(paramR);
        paramR->parallelIndex = 1;
        paramR->parallelNum = 2;
        auto *naviRHelper = getNaviRHelper();
        naviRHelper->kernelConfig(R"json({
		"kv_async": false
	})json");
        auto *scanR = naviRHelper->getOrCreateRes<KVScanR>();
        ASSERT_TRUE(scanR);
        TablePtr table;
        bool eof = false;
        bool ret = scanR->batchScan(table, eof);
        ASSERT_TRUE(NULL != table);
        ASSERT_TRUE(ret);
        ASSERT_TRUE(eof);
        ASSERT_EQ(1, table->getRowCount());
        ASSERT_EQ(3, table->getColumnCount());
        auto column = table->getColumn("pk");
        ASSERT_TRUE(column);
        ASSERT_EQ("2", column->toString(0));
        column = table->getColumn("attr1");
        ASSERT_TRUE(column);
        ASSERT_EQ("22^]cc", column->toString(0));
        column = table->getColumn("attr2");
        ASSERT_TRUE(column);
        ASSERT_EQ("52", column->toString(0));
    }
}

TEST_F(KVScanTestRV2, testDoBatchScan4) {
    // condition: pk in (1, 2, 3) and attr2 in (50, 51, 52) + limit 1 + out 2
    {
        std::string conditionJson
            = "{\"op\":\"AND\",\"type\":\"OTHER\",\"params\":[{\"op\":\"OR\",\"type\":\"OTHER\","
              "\"params\":[{\"op\":\"=\",\"type\":\"OTHER\",\"params\":[\"$pk\",{\"op\":\"CAST\","
              "\"cast_type\":\"BIGINT\",\"type\":\"UDF\",\"params\":[1]}]},{\"op\":\"=\",\"type\":"
              "\"OTHER\",\"params\":[\"$pk\",{\"op\":\"CAST\",\"cast_type\":\"BIGINT\",\"type\":"
              "\"UDF\",\"params\":[2]}]},{\"op\":\"=\",\"type\":\"OTHER\",\"params\":[\"$pk\",{"
              "\"op\":\"CAST\",\"cast_type\":\"BIGINT\",\"type\":\"UDF\",\"params\":[3]}]}]},{"
              "\"op\":\"OR\",\"type\":\"OTHER\",\"params\":[{\"op\":\"=\",\"type\":\"OTHER\","
              "\"params\":[\"$attr2\",{\"op\":\"CAST\",\"cast_type\":\"BIGINT\",\"type\":\"UDF\","
              "\"params\":[50]}]},{\"op\":\"=\",\"type\":\"OTHER\",\"params\":[\"$attr2\",{\"op\":"
              "\"CAST\",\"cast_type\":\"BIGINT\",\"type\":\"UDF\",\"params\":[51]}]},{\"op\":\"=\","
              "\"type\":\"OTHER\",\"params\":[\"$attr2\",{\"op\":\"CAST\",\"cast_type\":\"BIGINT\","
              "\"type\":\"UDF\",\"params\":[52]}]}]}]}";
        auto paramR = preparePk64ScanInitParam(conditionJson);
        ASSERT_TRUE(paramR);
        paramR->limit = 1;
        paramR->calcInitParamR->outputFields = {"pk", "attr2"};
        paramR->calcInitParamR->outputFieldsType = {"BIGINT", "BIGINT"};
        auto *naviRHelper = getNaviRHelper();
        naviRHelper->kernelConfig(R"json({
		"kv_async": false
	})json");
        auto *scanR = naviRHelper->getOrCreateRes<KVScanR>();
        ASSERT_TRUE(scanR);
        TablePtr table;
        bool eof = false;
        bool ret = scanR->batchScan(table, eof);
        ASSERT_TRUE(ret);
        ASSERT_TRUE(eof);
        ASSERT_EQ(1, table->getRowCount());
        ASSERT_EQ(2, table->getColumnCount());
        auto column = table->getColumn("pk");
        ASSERT_TRUE(column);
        ASSERT_EQ("1", column->toString(0));
        column = table->getColumn("attr1");
        ASSERT_FALSE(column);
        column = table->getColumn("attr2");
        ASSERT_TRUE(column);
        ASSERT_EQ("51", column->toString(0));
    }
}

} // namespace sql
