#include "sql/ops/scan/KKVScanR.h"

#include <algorithm>
#include <cstddef>
#include <limits>
#include <map>
#include <memory>
#include <stdint.h>
#include <string>
#include <utility>
#include <vector>

#include "autil/TimeUtility.h"
#include "ha3/search/IndexPartitionReaderWrapper.h"
#include "ha3/search/PartitionInfoWrapper.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/partition/index_application.h"
#include "indexlib/partition/index_partition.h"
#include "matchdoc/ValueType.h"
#include "matchdoc/VectorDocStorage.h"
#include "navi/common.h"
#include "navi/tester/NaviResourceHelper.h"
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

using namespace std;
using namespace autil;
using namespace table;
using namespace matchdoc;
using namespace testing;
using namespace suez::turing;
using namespace navi;

using namespace isearch::search;
using namespace isearch::common;

namespace sql {

class KKVScanRTest : public OpTestBase {
public:
    KKVScanRTest() {
        _tableName = "kkv";
        _needBuildIndex = true;
        _needExprResource = true;
        _needKkvTable = true;
    }

    void prepareUserIndex() override {
        prepareSqlKkvTable();
    }

    void prepareSqlKkvTable() {
        std::string tableName, fields, attributes, docs, pkeyField, skeyField;
        int64_t ttl;
        {
            prepareKkvPk64TableData(tableName, fields, pkeyField, skeyField, attributes, docs, ttl);
            std::string testPath = _testPath + tableName;
            auto indexPartition = makeIndexPartitionForKkvTable(
                testPath, tableName, fields, pkeyField, skeyField, attributes, docs, ttl);
            std::string schemaName = indexPartition->GetSchema()->GetSchemaName();
            _indexPartitionMap.insert(make_pair(schemaName, indexPartition));
        }
        {
            prepareKkvPkStrTableData(
                tableName, fields, pkeyField, skeyField, attributes, docs, ttl);
            std::string testPath = _testPath + tableName;
            auto indexPartition = makeIndexPartitionForKkvTable(
                testPath, tableName, fields, pkeyField, skeyField, attributes, docs, ttl);
            std::string schemaName = indexPartition->GetSchema()->GetSchemaName();
            // _bizInfo->_itemTableName = schemaName;
            _indexPartitionMap.insert(make_pair(schemaName, indexPartition));
        }
        {
            prepareKkvPkStrTableDataWithPkAttr(
                tableName, fields, pkeyField, skeyField, attributes, docs, ttl);
            std::string testPath = _testPath + tableName;
            auto indexPartition = makeIndexPartitionForKkvTable(
                testPath, tableName, fields, pkeyField, skeyField, attributes, docs, ttl);
            std::string schemaName = indexPartition->GetSchema()->GetSchemaName();
            // _bizInfo->_itemTableName = schemaName;
            _indexPartitionMap.insert(make_pair(schemaName, indexPartition));
        }
    }

    void prepareKkvPk64TableData(string &tableName,
                                 string &fields,
                                 string &pkeyField,
                                 string &skeyField,
                                 string &attributes,
                                 string &docs,
                                 int64_t &ttl) {
        tableName = _tableName;
        fields = "sk:int64;attr1:string:true;attr2:uint32;pk:uint64";
        attributes = "sk;attr1;attr2";
        docs = "cmd=add,sk=100,attr1=00 aa,attr2=50,pk=0,ts=5000000;"
               "cmd=add,sk=101,attr1=00 aa,attr2=50,pk=0,ts=5000000;"
               "cmd=add,sk=200,attr1=11 bb,attr2=51,pk=1,ts=10000000;"
               "cmd=add,sk=201,attr1=11 bb,attr2=51,pk=1,ts=10000000;"
               "cmd=add,sk=300,attr1=22 cc,attr2=52,pk=2,ts=15000000;"
               "cmd=add,sk=301,attr1=22 cc,attr2=52,pk=2,ts=15000000;"
               "cmd=add,sk=400,attr1=33 dd,attr2=53,pk=3,ts=20000000;"
               "cmd=add,sk=401,attr1=33 dd,attr2=53,pk=3,ts=20000000";
        pkeyField = "pk";
        skeyField = "sk";
        ttl = std::numeric_limits<int64_t>::max();
    }

    void prepareKkvPkStrTableData(std::string &tableName,
                                  std::string &fields,
                                  std::string &pkeyField,
                                  std::string &skeyField,
                                  std::string &attributes,
                                  std::string &docs,
                                  int64_t &ttl) {
        tableName = _tableName + "_str";
        fields = "sk:string;attr1:uint32;attr2:string:true;pk:string";
        attributes = "sk;attr1;attr2";
        docs = "cmd=add,sk=000_0,attr1=0,attr2=00 aa,pk=000,ts=5000000;"
               "cmd=add,sk=000_1,attr1=0,attr2=00 aa,pk=000,ts=5000000;"
               "cmd=add,sk=111_0,attr1=1,attr2=11 bb,pk=111,ts=10000000;"
               "cmd=add,sk=111_1,attr1=1,attr2=11 bb,pk=111,ts=10000000;"
               "cmd=add,sk=222_0,attr1=2,attr2=22 cc,pk=222,ts=15000000;"
               "cmd=add,sk=222_1,attr1=2,attr2=22 cc,pk=222,ts=15000000;"
               "cmd=add,sk=333_0,attr1=3,attr2=33 dd,pk=333,ts=20000000"
               "cmd=add,sk=333_1,attr1=3,attr2=33 dd,pk=333,ts=20000000";
        pkeyField = "pk";
        skeyField = "sk";
        ttl = std::numeric_limits<int64_t>::max();
    }

    void prepareKkvPkStrTableDataWithPkAttr(std::string &tableName,
                                            std::string &fields,
                                            std::string &pkeyField,
                                            std::string &skeyField,
                                            std::string &attributes,
                                            std::string &docs,
                                            int64_t &ttl) {
        tableName = _tableName + "_pkAttrStr";
        fields = "sk:string;attr1:uint32;attr2:string:true;pk:string";
        attributes = "pk;sk;attr1;attr2";
        docs = "cmd=add,sk=000_0,attr1=0,attr2=00 aa,pk=000,ts=5000000;"
               "cmd=add,sk=000_1,attr1=0,attr2=00 aa,pk=000,ts=5000000;"
               "cmd=add,sk=111_0,attr1=1,attr2=11 bb,pk=111,ts=10000000;"
               "cmd=add,sk=111_1,attr1=1,attr2=11 bb,pk=111,ts=10000000;"
               "cmd=add,sk=222_0,attr1=2,attr2=22 cc,pk=222,ts=15000000;"
               "cmd=add,sk=222_1,attr1=2,attr2=22 cc,pk=222,ts=15000000;"
               "cmd=add,sk=333_0,attr1=3,attr2=33 dd,pk=333,ts=20000000"
               "cmd=add,sk=333_1,attr1=3,attr2=33 dd,pk=333,ts=20000000";
        pkeyField = "pk";
        skeyField = "sk";
        ttl = std::numeric_limits<int64_t>::max();
    }

    void preparePk64ScanInitParam(const string &conditionJson) {
        auto *naviRHelper = getNaviRHelper();
        auto paramR = std::make_shared<ScanInitParamR>();
        paramR->tableInfoR = naviRHelper->getOrCreateRes<suez::turing::TableInfoR>();
        auto calcR = std::make_shared<CalcInitParamR>();
        paramR->calcInitParamR = calcR.get();
        paramR->tableType = "kkv";
        paramR->tableName = _tableName;
        paramR->calcInitParamR->outputFields = {"pk", "sk", "attr1", "attr2"};
        paramR->calcInitParamR->outputFieldsType = {"BIGINT", "BIGINT", "VARCHAR", "BIGINT"};
        paramR->batchSize = 3;
        paramR->limit = 5;
        paramR->parallelIndex = 0;
        paramR->parallelNum = 1;
        paramR->calcInitParamR->conditionJson = conditionJson;
        paramR->hashType = "HASH";
        paramR->hashFields = {"pk"};
        paramR->usedFields = {"attr2", "attr1", "pk", "sk"};
        ASSERT_TRUE(naviRHelper->addExternalRes(calcR));
        ASSERT_TRUE(naviRHelper->addExternalRes(paramR));
    }

    void preparePkStrScanInitParam(const string &conditionJson) {
        auto *naviRHelper = getNaviRHelper();
        auto paramR = std::make_shared<ScanInitParamR>();
        paramR->tableInfoR = naviRHelper->getOrCreateRes<suez::turing::TableInfoR>();
        auto calcR = std::make_shared<CalcInitParamR>();
        paramR->calcInitParamR = calcR.get();
        paramR->tableType = "kkv";
        paramR->tableName = _tableName + "_str";
        paramR->calcInitParamR->outputFields = {"pk", "sk", "attr1", "attr2"};
        paramR->calcInitParamR->outputFieldsType = {"VARCHAR", "VARCHAR", "BIGINT", "VARCHAR"};
        paramR->batchSize = 3;
        paramR->limit = 5;
        paramR->parallelIndex = 0;
        paramR->parallelNum = 1;
        paramR->calcInitParamR->conditionJson = conditionJson;
        paramR->hashType = "HASH";
        paramR->hashFields = {"pk"};
        paramR->usedFields = {"attr2", "attr1", "pk", "sk"};
        ASSERT_TRUE(naviRHelper->addExternalRes(calcR));
        ASSERT_TRUE(naviRHelper->addExternalRes(paramR));
    }
};

TEST_F(KKVScanRTest, testInitFailed1) {
    {
        std::string conditionJson = "";
        ASSERT_NO_FATAL_FAILURE(preparePk64ScanInitParam(conditionJson));
        auto *naviRHelper = getNaviRHelper();
        auto *scanR = naviRHelper->getOrCreateRes<KKVScanR>();
        ASSERT_FALSE(scanR);
    }
}

TEST_F(KKVScanRTest, testInitFailed2) {
    {
        std::string conditionJson
            = "{\"op\" : \"OR\",\"type\" : \"OTHER\",\"params\" : [ {\"op\" : \"=\",\"type\" : "
              "\"OTHER\",\"params\" : [ \"$pk\", {\"op\" : \"CAST\",\"cast_type\" : "
              "\"BIGINT\",\"type\" : \"UDF\",\"params\" : [ 1 ]} ]}, {\"op\" : \"=\",\"type\" : "
              "\"OTHER\",\"params\" : [ \"$pk\", {\"op\" : \"CAST\",\"cast_type\" : "
              "\"BIGINT\",\"type\" : \"UDF\",\"params\" : [ 2 ] } ] }, { \"op\" : \">\", \"type\" "
              ": \"OTHER\", \"params\" : [ \"$pk\", { \"op\" : \"CAST\", \"cast_type\" : "
              "\"BIGINT\", \"type\" : \"UDF\",\"params\" : [ 3 ] } ] } ] }";
        ASSERT_NO_FATAL_FAILURE(preparePk64ScanInitParam(conditionJson));
        auto *naviRHelper = getNaviRHelper();
        auto *scanR = naviRHelper->getOrCreateRes<KKVScanR>();
        ASSERT_FALSE(scanR);
    }
}

TEST_F(KKVScanRTest, testInit1) {
    {
        std::string conditionJson = "";
        ASSERT_NO_FATAL_FAILURE(preparePk64ScanInitParam(conditionJson));
        auto *naviRHelper = getNaviRHelper();
        naviRHelper->kernelConfig(R"json({"kkv_require_pk": false})json");
        auto *scanR = naviRHelper->getOrCreateRes<KKVScanR>();
        ASSERT_TRUE(scanR);
        ASSERT_TRUE(NULL != scanR->_pkeyCollector->getPkeyRef());
        ASSERT_EQ(0, scanR->_rawPks.size());
    }
}

TEST_F(KKVScanRTest, testInit2) {
    {
        std::string conditionJson
            = "{\"op\" : \"OR\",\"type\" : \"OTHER\",\"params\" : [ {\"op\" : \"=\",\"type\" : "
              "\"OTHER\",\"params\" : [ \"$pk\", {\"op\" : \"CAST\",\"cast_type\" : "
              "\"BIGINT\",\"type\" : \"UDF\",\"params\" : [ 1 ]} ]}, {\"op\" : \"=\",\"type\" : "
              "\"OTHER\",\"params\" : [ \"$pk\", {\"op\" : \"CAST\",\"cast_type\" : "
              "\"BIGINT\",\"type\" : \"UDF\",\"params\" : [ 2 ] } ] }, { \"op\" : \">\", \"type\" "
              ": \"OTHER\", \"params\" : [ \"$pk\", { \"op\" : \"CAST\", \"cast_type\" : "
              "\"BIGINT\", \"type\" : \"UDF\",\"params\" : [ 3 ] } ] } ] }";
        ASSERT_NO_FATAL_FAILURE(preparePk64ScanInitParam(conditionJson));
        auto *naviRHelper = getNaviRHelper();
        naviRHelper->kernelConfig(R"json({"kkv_require_pk": false})json");
        auto *scanR = naviRHelper->getOrCreateRes<KKVScanR>();
        ASSERT_TRUE(scanR);
        ASSERT_TRUE(NULL != scanR->_pkeyCollector->getPkeyRef());
        ASSERT_EQ(0, scanR->_rawPks.size());
    }
}

TEST_F(KKVScanRTest, testInit3) {
    {
        std::string conditionJson
            = "{\"op\" : \"OR\",\"type\" : \"OTHER\",\"params\" : [ {\"op\" : \"=\",\"type\" : "
              "\"OTHER\",\"params\" : [ \"$pk\", {\"op\" : \"CAST\",\"cast_type\" : "
              "\"BIGINT\",\"type\" : \"UDF\",\"params\" : [ 1 ]} ]}, {\"op\" : \"=\",\"type\" : "
              "\"OTHER\",\"params\" : [ \"$pk\", {\"op\" : \"CAST\",\"cast_type\" : "
              "\"BIGINT\",\"type\" : \"UDF\",\"params\" : [ 2 ] } ] }, { \"op\" : \"=\", \"type\" "
              ": \"OTHER\", \"params\" : [ \"$pk\", { \"op\" : \"CAST\", \"cast_type\" : "
              "\"BIGINT\", \"type\" : \"UDF\",\"params\" : [ 3 ] } ] } ] }";
        ASSERT_NO_FATAL_FAILURE(preparePk64ScanInitParam(conditionJson));
        auto *naviRHelper = getNaviRHelper();
        auto *paramR = naviRHelper->getOrCreateRes<ScanInitParamR>();
        paramR->parallelIndex = 0;
        paramR->parallelNum = 2;
        auto *scanR = naviRHelper->getOrCreateRes<KKVScanR>();
        ASSERT_TRUE(scanR);
        ASSERT_EQ(2, scanR->_rawPks.size());
        ASSERT_EQ("1", scanR->_rawPks[0]);
        ASSERT_EQ("3", scanR->_rawPks[1]);
    }
}

TEST_F(KKVScanRTest, testInit4) {
    // init for string
    {
        std::string conditionJson
            = R"({"op":"AND","type":"OTHER","params":[{"op":"OR","type":"OTHER","params":[{"op":"=","type":"OTHER","params":["$pk","111"]},{"op":"=","type":"OTHER","params":["$pk","222"]},{"op":"=","type":"OTHER","params":["$pk","333"]}]},{"op":"OR","type":"OTHER","params":[{"op":"=","type":"OTHER","params":[{"op":"CAST","cast_type":"INTEGER","type":"UDF","params":["$attr1"]},0]},{"op":"=","type":"OTHER","params":[{"op":"CAST","cast_type":"INTEGER","type":"UDF","params":["$attr1"]},1]},{"op":"=","type":"OTHER","params":[{"op":"CAST","cast_type":"INTEGER","type":"UDF","params":["$attr1"]},2]}]}]})";
        ASSERT_NO_FATAL_FAILURE(preparePkStrScanInitParam(conditionJson));
        string tableName = _tableName + "_str";
        auto *naviRHelper = getNaviRHelper();
        auto *tableInfoR = naviRHelper->getOrCreateRes<TableInfoR>();
        tableInfoR->_tableInfoMapWithoutRel[tableName]
            = suez::turing::TableInfoConfigurator::createFromIndexApp(tableName, _indexApp);
        auto *scanR = naviRHelper->getOrCreateRes<KKVScanR>();
        ASSERT_TRUE(scanR);
        ASSERT_EQ(3, scanR->_rawPks.size());
    }
}

TEST_F(KKVScanRTest, testInit5) {
    {
        std::string conditionJson
            = R"({"op":"AND","type":"OTHER","params":[{"op":"OR","type":"OTHER","params":[{"op":"=","type":"OTHER","params":["$pk","111"]},{"op":"=","type":"OTHER","params":["$pk","222"]},{"op":"=","type":"OTHER","params":["$pk","333"]}]},{"op":"OR","type":"OTHER","params":[{"op":"=","type":"OTHER","params":[{"op":"CAST","cast_type":"INTEGER","type":"UDF","params":["$attr1"]},0]},{"op":"=","type":"OTHER","params":[{"op":"CAST","cast_type":"INTEGER","type":"UDF","params":["$attr1"]},1]},{"op":"=","type":"OTHER","params":[{"op":"CAST","cast_type":"INTEGER","type":"UDF","params":["$attr1"]},2]}]}]})";
        ASSERT_NO_FATAL_FAILURE(preparePkStrScanInitParam(conditionJson));
        auto *naviRHelper = getNaviRHelper();
        auto *paramR = naviRHelper->getOrCreateRes<ScanInitParamR>();
        std::string tableName = _tableName + "_pkAttrStr";
        paramR->tableName = tableName;
        auto *tableInfoR = naviRHelper->getOrCreateRes<TableInfoR>();
        tableInfoR->_tableInfoMapWithoutRel[tableName]
            = suez::turing::TableInfoConfigurator::createFromIndexApp(tableName, _indexApp);
        auto *scanR = naviRHelper->getOrCreateRes<KKVScanR>();
        ASSERT_TRUE(scanR);
        ASSERT_EQ(3, scanR->_rawPks.size());
    }
}

TEST_F(KKVScanRTest, testDoBatchScan1) {
    // condition: pk in (1, 2, 3)
    {
        std::string conditionJson
            = "{\"op\" : \"OR\",\"type\" : \"OTHER\",\"params\" : [ {\"op\" : \"=\",\"type\" : "
              "\"OTHER\",\"params\" : [ \"$pk\", {\"op\" : \"CAST\",\"cast_type\" : "
              "\"BIGINT\",\"type\" : \"UDF\",\"params\" : [ 1 ]} ]}, {\"op\" : \"=\",\"type\" : "
              "\"OTHER\",\"params\" : [ \"$pk\", {\"op\" : \"CAST\",\"cast_type\" : "
              "\"BIGINT\",\"type\" : \"UDF\",\"params\" : [ 2 ] } ] }, { \"op\" : \"=\", \"type\" "
              ": \"OTHER\", \"params\" : [ \"$pk\", { \"op\" : \"CAST\", \"cast_type\" : "
              "\"BIGINT\", \"type\" : \"UDF\",\"params\" : [ 3 ] } ] } ] }";
        ASSERT_NO_FATAL_FAILURE(preparePk64ScanInitParam(conditionJson));
        auto *naviRHelper = getNaviRHelper();
        auto *scanR = naviRHelper->getOrCreateRes<KKVScanR>();
        ASSERT_TRUE(scanR);
        TablePtr table;
        bool eof = false;
        bool ret = scanR->batchScan(table, eof);
        ASSERT_TRUE(ret);
        ASSERT_TRUE(eof);
        ASSERT_EQ(5, table->getRowCount());
        ASSERT_EQ(4, table->getColumnCount());
        auto column = table->getColumn("pk");
        ASSERT_TRUE(column);
        ASSERT_TRUE(NULL != column->getColumnSchema()
                    && bt_uint64 == column->getColumnSchema()->getType().getBuiltinType());
        ASSERT_EQ("1", column->toString(0));
        ASSERT_EQ("1", column->toString(1));
        ASSERT_EQ("2", column->toString(2));
        ASSERT_EQ("2", column->toString(3));
        ASSERT_EQ("3", column->toString(4));
        column = table->getColumn("sk");
        ASSERT_TRUE(column);
        ASSERT_TRUE(NULL != column->getColumnSchema()
                    && bt_int64 == column->getColumnSchema()->getType().getBuiltinType());
        ASSERT_EQ("200", column->toString(0));
        ASSERT_EQ("201", column->toString(1));
        ASSERT_EQ("300", column->toString(2));
        ASSERT_EQ("301", column->toString(3));
        ASSERT_EQ("400", column->toString(4));
        column = table->getColumn("attr1");
        ASSERT_TRUE(column);
        ASSERT_TRUE(NULL != column->getColumnSchema()
                    && bt_string == column->getColumnSchema()->getType().getBuiltinType());
        ASSERT_EQ("11^]bb", column->toString(0));
        ASSERT_EQ("11^]bb", column->toString(1));
        ASSERT_EQ("22^]cc", column->toString(2));
        ASSERT_EQ("22^]cc", column->toString(3));
        ASSERT_EQ("33^]dd", column->toString(4));
        column = table->getColumn("attr2");
        ASSERT_TRUE(column);
        ASSERT_TRUE(NULL != column->getColumnSchema()
                    && bt_uint32 == column->getColumnSchema()->getType().getBuiltinType());
        ASSERT_EQ("51", column->toString(0));
        ASSERT_EQ("51", column->toString(1));
        ASSERT_EQ("52", column->toString(2));
        ASSERT_EQ("52", column->toString(3));
        ASSERT_EQ("53", column->toString(4));
    }
}

TEST_F(KKVScanRTest, testDoBatchScan2) {
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
        ASSERT_NO_FATAL_FAILURE(preparePk64ScanInitParam(conditionJson));
        auto *naviRHelper = getNaviRHelper();
        auto *scanR = naviRHelper->getOrCreateRes<KKVScanR>();
        ASSERT_TRUE(scanR);
        TablePtr table;
        bool eof = false;
        bool ret = scanR->batchScan(table, eof);
        ASSERT_TRUE(ret);
        ASSERT_TRUE(eof);
        ASSERT_EQ(4, table->getRowCount());
        ASSERT_EQ(4, table->getColumnCount());
        auto column = table->getColumn("pk");
        ASSERT_TRUE(column);
        ASSERT_EQ("1", column->toString(0));
        ASSERT_EQ("1", column->toString(1));
        ASSERT_EQ("2", column->toString(2));
        ASSERT_EQ("2", column->toString(3));
        column = table->getColumn("sk");
        ASSERT_TRUE(column);
        ASSERT_EQ("200", column->toString(0));
        ASSERT_EQ("201", column->toString(1));
        ASSERT_EQ("300", column->toString(2));
        ASSERT_EQ("301", column->toString(3));
        column = table->getColumn("attr1");
        ASSERT_TRUE(column);
        ASSERT_EQ("11^]bb", column->toString(0));
        ASSERT_EQ("11^]bb", column->toString(1));
        ASSERT_EQ("22^]cc", column->toString(2));
        ASSERT_EQ("22^]cc", column->toString(3));
        column = table->getColumn("attr2");
        ASSERT_TRUE(column);
        ASSERT_EQ("51", column->toString(0));
        ASSERT_EQ("51", column->toString(1));
        ASSERT_EQ("52", column->toString(2));
        ASSERT_EQ("52", column->toString(3));
    }
}

TEST_F(KKVScanRTest, testDoBatchScan3) {
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
        ASSERT_NO_FATAL_FAILURE(preparePk64ScanInitParam(conditionJson));
        auto *naviRHelper = getNaviRHelper();
        auto *paramR = naviRHelper->getOrCreateRes<ScanInitParamR>();
        paramR->parallelIndex = 0;
        paramR->parallelNum = 2;
        auto *scanR = naviRHelper->getOrCreateRes<KKVScanR>();
        ASSERT_TRUE(scanR);
        TablePtr table;
        bool eof = false;
        bool ret = scanR->batchScan(table, eof);
        ASSERT_TRUE(ret);
        ASSERT_TRUE(eof);
        ASSERT_EQ(2, table->getRowCount());
        ASSERT_EQ(4, table->getColumnCount());
        auto column = table->getColumn("pk");
        ASSERT_TRUE(column);
        ASSERT_EQ("1", column->toString(0));
        ASSERT_EQ("1", column->toString(1));
        column = table->getColumn("sk");
        ASSERT_TRUE(column);
        ASSERT_EQ("200", column->toString(0));
        ASSERT_EQ("201", column->toString(1));
        column = table->getColumn("attr1");
        ASSERT_TRUE(column);
        ASSERT_EQ("11^]bb", column->toString(0));
        ASSERT_EQ("11^]bb", column->toString(1));
        column = table->getColumn("attr2");
        ASSERT_TRUE(column);
        ASSERT_EQ("51", column->toString(0));
        ASSERT_EQ("51", column->toString(1));
    }
}

TEST_F(KKVScanRTest, testDoBatchScan4) {
    // condition: pk in (1, 2, 3) and attr2 in (50, 51, 52) + limit 3 + out 2
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
        ASSERT_NO_FATAL_FAILURE(preparePk64ScanInitParam(conditionJson));
        auto *naviRHelper = getNaviRHelper();
        auto *paramR = naviRHelper->getOrCreateRes<ScanInitParamR>();
        paramR->limit = 3;
        paramR->calcInitParamR->outputFields = {"sk", "attr2"};
        paramR->calcInitParamR->outputFieldsType = {"BIGINT", "BIGINT"};
        auto *scanR = naviRHelper->getOrCreateRes<KKVScanR>();
        ASSERT_TRUE(scanR);
        TablePtr table;
        bool eof = false;
        bool ret = scanR->batchScan(table, eof);
        ASSERT_TRUE(ret);
        ASSERT_TRUE(eof);
        ASSERT_EQ(3, table->getRowCount());
        ASSERT_EQ(2, table->getColumnCount());
        auto column = table->getColumn("sk");
        ASSERT_TRUE(column);
        ASSERT_EQ("200", column->toString(0));
        ASSERT_EQ("201", column->toString(1));
        ASSERT_EQ("300", column->toString(2));
        column = table->getColumn("attr2");
        ASSERT_TRUE(column);
        ASSERT_EQ("51", column->toString(0));
        ASSERT_EQ("51", column->toString(1));
        ASSERT_EQ("52", column->toString(2));
    }
}

TEST_F(KKVScanRTest, testDoBatchScan5) {
    // condition: pk in ("111", "222", "333") and attr1 in (0, 1, 2)
    {
        std::string conditionJson
            = R"({"op":"AND","type":"OTHER","params":[{"op":"OR","type":"OTHER","params":[{"op":"=","type":"OTHER","params":["$pk","111"]},{"op":"=","type":"OTHER","params":["$pk","222"]},{"op":"=","type":"OTHER","params":["$pk","333"]}]},{"op":"OR","type":"OTHER","params":[{"op":"=","type":"OTHER","params":[{"op":"CAST","cast_type":"INTEGER","type":"UDF","params":["$attr1"]},0]},{"op":"=","type":"OTHER","params":[{"op":"CAST","cast_type":"INTEGER","type":"UDF","params":["$attr1"]},1]},{"op":"=","type":"OTHER","params":[{"op":"CAST","cast_type":"INTEGER","type":"UDF","params":["$attr1"]},2]}]}]})";
        ASSERT_NO_FATAL_FAILURE(preparePkStrScanInitParam(conditionJson));
        auto *naviRHelper = getNaviRHelper();
        string tableName = _tableName + "_str";
        auto *tableInfoR = naviRHelper->getOrCreateRes<TableInfoR>();
        tableInfoR->_tableInfoMapWithoutRel[tableName]
            = suez::turing::TableInfoConfigurator::createFromIndexApp(tableName, _indexApp);
        auto *scanR = naviRHelper->getOrCreateRes<KKVScanR>();
        ASSERT_TRUE(scanR);
        TablePtr table;
        bool eof = false;
        bool ret = scanR->batchScan(table, eof);
        ASSERT_TRUE(ret);
        ASSERT_TRUE(eof);
        ASSERT_EQ(4, table->getRowCount());
        ASSERT_EQ(4, table->getColumnCount());
        auto column = table->getColumn("pk");
        ASSERT_TRUE(column);
        ASSERT_EQ("111", column->toString(0));
        ASSERT_EQ("111", column->toString(1));
        ASSERT_EQ("222", column->toString(2));
        ASSERT_EQ("222", column->toString(3));
        column = table->getColumn("sk");
        ASSERT_TRUE(column);
        ASSERT_EQ("111_0", column->toString(0));
        ASSERT_EQ("111_1", column->toString(1));
        ASSERT_EQ("222_0", column->toString(2));
        ASSERT_EQ("222_1", column->toString(3));
        column = table->getColumn("attr1");
        ASSERT_TRUE(column);
        ASSERT_EQ("1", column->toString(0));
        ASSERT_EQ("1", column->toString(1));
        ASSERT_EQ("2", column->toString(2));
        ASSERT_EQ("2", column->toString(3));
        column = table->getColumn("attr2");
        ASSERT_TRUE(column);
        ASSERT_EQ("11^]bb", column->toString(0));
        ASSERT_EQ("11^]bb", column->toString(1));
        ASSERT_EQ("22^]cc", column->toString(2));
        ASSERT_EQ("22^]cc", column->toString(3));
    }
}

TEST_F(KKVScanRTest, testDoBatchScan6) {
    {
        std::string conditionJson
            = R"({"op":"AND","type":"OTHER","params":[{"op":"OR","type":"OTHER","params":[{"op":"=","type":"OTHER","params":["$pk","111"]},{"op":"=","type":"OTHER","params":["$pk","222"]},{"op":"=","type":"OTHER","params":["$pk","333"]}]},{"op":"OR","type":"OTHER","params":[{"op":"=","type":"OTHER","params":[{"op":"CAST","cast_type":"INTEGER","type":"UDF","params":["$attr1"]},0]},{"op":"=","type":"OTHER","params":[{"op":"CAST","cast_type":"INTEGER","type":"UDF","params":["$attr1"]},1]},{"op":"=","type":"OTHER","params":[{"op":"CAST","cast_type":"INTEGER","type":"UDF","params":["$attr1"]},2]}]}]})";
        ASSERT_NO_FATAL_FAILURE(preparePkStrScanInitParam(conditionJson));
        std::string tableName = _tableName + "_pkAttrStr";
        auto *naviRHelper = getNaviRHelper();
        auto *paramR = naviRHelper->getOrCreateRes<ScanInitParamR>();
        paramR->tableName = tableName;
        auto *tableInfoR = naviRHelper->getOrCreateRes<TableInfoR>();
        tableInfoR->_tableInfoMapWithoutRel[tableName]
            = suez::turing::TableInfoConfigurator::createFromIndexApp(tableName, _indexApp);
        auto *scanR = naviRHelper->getOrCreateRes<KKVScanR>();
        ASSERT_TRUE(scanR);
        TablePtr table;
        bool eof = false;
        bool ret = scanR->batchScan(table, eof);
        ASSERT_TRUE(ret);
        ASSERT_TRUE(eof);
        ASSERT_EQ(4, table->getRowCount());
        ASSERT_EQ(4, table->getColumnCount());
        auto column = table->getColumn("pk");
        ASSERT_TRUE(column);
        ASSERT_EQ("111", column->toString(0));
        ASSERT_EQ("111", column->toString(1));
        ASSERT_EQ("222", column->toString(2));
        ASSERT_EQ("222", column->toString(3));
        column = table->getColumn("sk");
        ASSERT_TRUE(column);
        ASSERT_EQ("111_0", column->toString(0));
        ASSERT_EQ("111_1", column->toString(1));
        ASSERT_EQ("222_0", column->toString(2));
        ASSERT_EQ("222_1", column->toString(3));
        column = table->getColumn("attr1");
        ASSERT_TRUE(column);
        ASSERT_EQ("1", column->toString(0));
        ASSERT_EQ("1", column->toString(1));
        ASSERT_EQ("2", column->toString(2));
        ASSERT_EQ("2", column->toString(3));
        column = table->getColumn("attr2");
        ASSERT_TRUE(column);
        ASSERT_EQ("11^]bb", column->toString(0));
        ASSERT_EQ("11^]bb", column->toString(1));
        ASSERT_EQ("22^]cc", column->toString(2));
        ASSERT_EQ("22^]cc", column->toString(3));
    }
}

TEST_F(KKVScanRTest, testUpdateScanQuery) {
    std::string conditionJson
        = "{\"op\" : \"OR\",\"type\" : \"OTHER\",\"params\" : [ {\"op\" : \"=\",\"type\" : "
          "\"OTHER\",\"params\" : [ \"$pk\", {\"op\" : \"CAST\",\"cast_type\" : "
          "\"BIGINT\",\"type\" : \"UDF\",\"params\" : [ 1 ]} ]}, {\"op\" : \"=\",\"type\" : "
          "\"OTHER\",\"params\" : [ \"$pk\", {\"op\" : \"CAST\",\"cast_type\" : "
          "\"BIGINT\",\"type\" : \"UDF\",\"params\" : [ 2 ] } ] }, { \"op\" : \"=\", \"type\" : "
          "\"OTHER\", \"params\" : [ \"$pk\", { \"op\" : \"CAST\", \"cast_type\" : \"BIGINT\", "
          "\"type\" : \"UDF\",\"params\" : [ 3 ] } ] } ] }";
    ASSERT_NO_FATAL_FAILURE(preparePk64ScanInitParam(conditionJson));
    auto *naviRHelper = getNaviRHelper();
    auto *paramR = naviRHelper->getOrCreateRes<ScanInitParamR>();
    paramR->limit = 10000;
    auto *scanR = naviRHelper->getOrCreateRes<KKVScanR>();
    ASSERT_TRUE(scanR);
    TablePtr table;
    bool eof = false;
    // first scan
    ASSERT_TRUE(scanR->batchScan(table, eof));
    ASSERT_TRUE(eof);
    ASSERT_EQ(6, table->getRowCount());
    ASSERT_EQ(4, table->getColumnCount());
    auto column = table->getColumn("pk");
    ASSERT_TRUE(column);
    ASSERT_EQ("1", column->toString(0));
    ASSERT_EQ("1", column->toString(1));
    ASSERT_EQ("2", column->toString(2));
    ASSERT_EQ("2", column->toString(3));
    ASSERT_EQ("3", column->toString(4));
    ASSERT_EQ("3", column->toString(5));
    column = table->getColumn("sk");
    ASSERT_TRUE(column);
    ASSERT_EQ("200", column->toString(0));
    ASSERT_EQ("201", column->toString(1));
    ASSERT_EQ("300", column->toString(2));
    ASSERT_EQ("301", column->toString(3));
    ASSERT_EQ("400", column->toString(4));
    ASSERT_EQ("401", column->toString(5));
    column = table->getColumn("attr1");
    ASSERT_TRUE(column);
    ASSERT_EQ("11^]bb", column->toString(0));
    ASSERT_EQ("11^]bb", column->toString(1));
    ASSERT_EQ("22^]cc", column->toString(2));
    ASSERT_EQ("22^]cc", column->toString(3));
    ASSERT_EQ("33^]dd", column->toString(4));
    ASSERT_EQ("33^]dd", column->toString(5));
    column = table->getColumn("attr2");
    ASSERT_TRUE(column);
    ASSERT_EQ("51", column->toString(0));
    ASSERT_EQ("51", column->toString(1));
    ASSERT_EQ("52", column->toString(2));
    ASSERT_EQ("52", column->toString(3));
    ASSERT_EQ("53", column->toString(4));
    ASSERT_EQ("53", column->toString(5));
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
    ASSERT_EQ(4, table2->getRowCount());
    ASSERT_EQ(4, table2->getColumnCount());
    column = table2->getColumn("pk");
    ASSERT_TRUE(column);
    ASSERT_EQ("3", column->toString(0));
    ASSERT_EQ("3", column->toString(1));
    ASSERT_EQ("2", column->toString(2));
    ASSERT_EQ("2", column->toString(3));
    column = table2->getColumn("sk");
    ASSERT_TRUE(column);
    ASSERT_EQ("400", column->toString(0));
    ASSERT_EQ("401", column->toString(1));
    ASSERT_EQ("300", column->toString(2));
    ASSERT_EQ("301", column->toString(3));
    column = table2->getColumn("attr1");
    ASSERT_TRUE(column);
    ASSERT_EQ("33^]dd", column->toString(0));
    ASSERT_EQ("33^]dd", column->toString(1));
    ASSERT_EQ("22^]cc", column->toString(2));
    ASSERT_EQ("22^]cc", column->toString(3));
    column = table2->getColumn("attr2");
    ASSERT_TRUE(column);
    ASSERT_EQ("53", column->toString(0));
    ASSERT_EQ("53", column->toString(1));
    ASSERT_EQ("52", column->toString(2));
    ASSERT_EQ("52", column->toString(3));
    inputQuery.reset();
    TablePtr table3;
    bool eof3 = false;
    ASSERT_TRUE(scanR->updateScanQuery(inputQuery));
    ASSERT_TRUE(scanR->batchScan(table3, eof3));
    ASSERT_TRUE(eof3);
    ASSERT_EQ(0, table3->getRowCount());
    ASSERT_EQ(4, table3->getColumnCount());
}

} // namespace sql
