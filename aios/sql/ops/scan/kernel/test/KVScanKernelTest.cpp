
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <map>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "autil/HashFunctionBase.h"
#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/json.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "indexlib/base/Status.h"
#include "indexlib/config/BuildConfig.h"
#include "indexlib/config/OnlineConfig.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/framework/ITablet.h"
#include "indexlib/framework/IndexRoot.h"
#include "indexlib/framework/Locator.h"
#include "indexlib/index/attribute/Constant.h"
#include "indexlib/partition/index_application.h"
#include "indexlib/partition/index_partition.h"
#include "indexlib/table/kv_table/test/KVTableTestHelper.h"
#include "indexlib/test/schema_maker.h"
#include "matchdoc/ValueType.h"
#include "navi/common.h"
#include "navi/tester/KernelTester.h"
#include "navi/tester/KernelTesterBuilder.h"
#include "sql/common/WatermarkType.h"
#include "sql/ops/scan/udf_to_query/UdfToQuery.h"
#include "sql/ops/test/OpTestBase.h"
#include "table/Column.h"
#include "table/ColumnSchema.h"
#include "table/Row.h"
#include "table/Table.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace autil;
using namespace table;
using namespace navi;
using namespace suez::turing;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace matchdoc;
using namespace indexlibv2::framework;
using namespace indexlibv2::index;

namespace sql {

class KVScanKernelTest : public OpTestBase {
public:
    KVScanKernelTest() {
        _tableName = "kvtable";
        _needBuildIndex = true;
        _needExprResource = true;
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

    KernelTesterPtr buildTester(KernelTesterBuilder &builder) {
        setResource(builder);
        builder.kernel("sql.AsyncScanKernel");
        builder.output("output0");
        builder.attrs(autil::legacy::FastToJsonString(_attributeMap));
        return builder.build();
    }

    void prepareAttributeMap() {
        _attributeMap["table_type"] = string("kv");
        _attributeMap["table_name"] = _tableName;
        _attributeMap["db_name"] = string("default");
        _attributeMap["catalog_name"] = string("default");
        _attributeMap["use_nest_table"] = Any(false);
        _attributeMap["target_watermark"] = Any(1111);
        _attributeMap["target_watermark_type"] = Any((int64_t)WatermarkType::WM_TYPE_SYSTEM_TS);
        _attributeMap["hash_type"] = string("HASH");
        _attributeMap["hash_fields"] = ParseJson(R"json(["pk"])json");
    }

    void runNormalCase(const std::string &conditionJson,
                       const std::string &outputFieldsJson,
                       const std::string &usedFieldsJson,
                       table::TablePtr &table,
                       bool &eof) {
        prepareAttributeMap();
        _attributeMap["push_down_ops"]
            = ParseJson(R"([{"attrs": {"condition":)" + conditionJson
                        + R"(,"output_field_exprs": {}},"op_name":"CalcOp"}])");
        _attributeMap["output_fields_internal"] = ParseJson(outputFieldsJson);
        _attributeMap["used_fields"] = ParseJson(usedFieldsJson);
        _attributeMap["limit"] = Any(10000000);
        KernelTesterBuilder builder;
        auto tester = buildTester(builder);
        ASSERT_TRUE(tester);
        ASSERT_NO_FATAL_FAILURE(asyncRunKernel(*tester, table, eof));
    }
};

class KVScanKernelTestV2 : public KVScanKernelTest {
    KVScanKernelTestV2() {
        _tableName = "kv_tablet";
        _needExprResource = true;
        _needBuildTablet = true;
        _needBuildIndex = false;
    }

private:
    void prepareTablet() override {
        prepareSqlKvTablet();
    }

    void prepareSqlKvTablet() {
        std::string tableName, fields, attributes, docs, pkeyField;
        bool enableTTL;
        int64_t ttl;
        {
            prepareKvPk64TableDataTablet(
                tableName, fields, pkeyField, attributes, docs, enableTTL, ttl);
            std::string testPath = _testPath + tableName;
            indexlibv2::framework::IndexRoot indexRoot(testPath, testPath);
            auto tabletSchema = indexlib::test::SchemaMaker::MakeKVTabletSchema(
                fields, pkeyField, attributes, ttl);
            tabletSchema->TEST_SetTableName(tableName);
            auto tabletOptions = CreateMultiColumnOptions();
            auto helper = make_shared<indexlibv2::table::KVTableTestHelper>();
            _dependentHolders.push_back(helper);
            ASSERT_TRUE(helper->Open(indexRoot, tabletSchema, std::move(tabletOptions)).IsOK());
            ASSERT_TRUE(helper->Build(docs).IsOK());
            auto tabletPtr = helper->GetITablet();
            ASSERT_TRUE(_tabletMap.insert(make_pair(tableName, tabletPtr)).second);
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

    std::shared_ptr<indexlibv2::config::TabletOptions> CreateMultiColumnOptions() {
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

TEST_F(KVScanKernelTest, testSimple) {
    std::string conditionJson
        = "{\"op\" : \"OR\",\"type\" : \"OTHER\",\"params\" : [ {\"op\" : \"=\",\"type\" : "
          "\"OTHER\",\"params\" : [ \"$pk\", {\"op\" : \"CAST\",\"cast_type\" : "
          "\"BIGINT\",\"type\" : \"UDF\",\"params\" : [ 1 ]} ]}, {\"op\" : \"=\",\"type\" : "
          "\"OTHER\",\"params\" : [ \"$pk\", {\"op\" : \"CAST\",\"cast_type\" : "
          "\"BIGINT\",\"type\" : \"UDF\",\"params\" : [ 2 ] } ] }, { \"op\" : \"=\", \"type\" "
          ": \"OTHER\", \"params\" : [ \"$pk\", { \"op\" : \"CAST\", \"cast_type\" : "
          "\"BIGINT\", \"type\" : \"UDF\",\"params\" : [ 300 ] } ] } ] }";
    table::TablePtr table;
    bool eof;

    ASSERT_NO_FATAL_FAILURE(runNormalCase(conditionJson,
                                          R"json(["$pk", "$attr1", "$attr2"])json",
                                          R"json(["$pk", "$attr1", "$attr2"])json",
                                          table,
                                          eof));

    ASSERT_TRUE(eof);
    ASSERT_NE(nullptr, table);
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

TEST_F(KVScanKernelTestV2, testSimple) {
    std::string conditionJson
        = "{\"op\" : \"OR\",\"type\" : \"OTHER\",\"params\" : [ {\"op\" : \"=\",\"type\" : "
          "\"OTHER\",\"params\" : [ \"$pk\", {\"op\" : \"CAST\",\"cast_type\" : "
          "\"BIGINT\",\"type\" : \"UDF\",\"params\" : [ 1 ]} ]}, {\"op\" : \"=\",\"type\" : "
          "\"OTHER\",\"params\" : [ \"$pk\", {\"op\" : \"CAST\",\"cast_type\" : "
          "\"BIGINT\",\"type\" : \"UDF\",\"params\" : [ 2 ] } ] }, { \"op\" : \"=\", \"type\" "
          ": \"OTHER\", \"params\" : [ \"$pk\", { \"op\" : \"CAST\", \"cast_type\" : "
          "\"BIGINT\", \"type\" : \"UDF\",\"params\" : [ 300 ] } ] } ] }";
    table::TablePtr table;
    bool eof;

    ASSERT_NO_FATAL_FAILURE(runNormalCase(conditionJson,
                                          R"json(["$pk", "$attr1", "$attr2"])json",
                                          R"json(["$pk", "$attr1", "$attr2"])json",
                                          table,
                                          eof));

    ASSERT_TRUE(eof);
    ASSERT_NE(nullptr, table);
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

} // namespace sql
