#include "sql/resource/IquanR.h"

#include <algorithm>
#include <exception>
#include <ext/alloc_traits.h>
#include <map>
#include <memory>
#include <mutex>
#include <ostream>
#include <stddef.h>
#include <string>
#include <unordered_map>
#include <vector>

#include "autil/EnvUtil.h"
#include "autil/Log.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/json.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "fslib/util/FileUtil.h"
#include "ha3/util/TypeDefine.h"
#include "indexlib/config/ITabletSchema.h"
#include "indexlib/index_base/schema_adapter.h"
#include "iquan/common/Common.h"
#include "iquan/common/Status.h"
#include "iquan/common/catalog/LayerTableDef.h"
#include "iquan/common/catalog/TableDef.h"
#include "iquan/common/catalog/TableModel.h"
#include "iquan/jni/IquanEnv.h"
#include "iquan/jni/JvmType.h"
#include "navi/log/NaviLoggerProvider.h"
#include "navi/log/test/NaviLoggerProviderTestUtil.h"
#include "navi/tester/NaviResourceHelper.h"
#include "sql/common/IndexPartitionSchemaConverter.h"
#include "sql/common/common.h"
#include "sql/resource/Ha3TableInfoR.h"
#include "sql/resource/SqlConfig.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace iquan;
using namespace autil::legacy;
using namespace autil;
using namespace navi;

namespace sql {
static std::once_flag iquanFlag;

class IquanRTest : public TESTBASE {
public:
    void setUp() override;
    void tearDown() override;

private:
    template <typename T>
    void loadFromFile(const std::string &fileName, T &json) {
        std::string jsonFile = GET_TEST_DATA_PATH() + fileName;
        std::string jsonStr;
        ASSERT_TRUE(fslib::util::FileUtil::readFile(jsonFile, jsonStr));
        try {
            autil::legacy::FastFromJsonString(json, jsonStr);
        } catch (const std::exception &e) { ASSERT_TRUE(false) << e.what(); }
    }

    unique_ptr<TableModel> getTableModelFromSchema(const string &schemaFile) const {
        std::string jsonFile = GET_TEST_DATA_PATH() + schemaFile;
        std::string jsonStr;
        if (!fslib::util::FileUtil::readFile(jsonFile, jsonStr)) {
            return {};
        }
        auto schemaPtr = indexlib::index_base::SchemaAdapter::LoadSchema(jsonStr);
        if (!schemaPtr) {
            return {};
        }
        TableDef tableDef;
        IndexPartitionSchemaConverter::convert(std::move(schemaPtr), tableDef);
        tableDef.distribution.partitionCnt = 1;
        auto tableModelPtr = make_unique<TableModel>();
        tableModelPtr->tableContent = tableDef;
        return tableModelPtr;
    }

    void readTestFile(const std::string fileName, std::string &jsonStr) {
        std::string jsonFile = GET_TEST_DATA_PATH() + fileName;
        ASSERT_TRUE(fslib::util::FileUtil::readFile(jsonFile, jsonStr));
    }

    void prepLoadLogicTables(unique_ptr<IquanR> &iquanRPtr, const string &dbName);
    void prepLoadLayerTables(unique_ptr<IquanR> &iquanRPtr, const string &dbName);
    void prepLoadExternalTableModels(unique_ptr<IquanR> &iquanRPtr);
    void testUpdateCatalogDefFailedStage1(unique_ptr<IquanR> &iquanRPtr);
    void testUpdateCatalogDefFlushStage1(unique_ptr<IquanR> &iquanRPtr);
    void testUpdateCatalogDefFailedStage2(unique_ptr<IquanR> &iquanRPtr);
    void testUpdateCatalogDefFlushStage2(unique_ptr<IquanR> &iquanRPtr);
    void testUpdateCatalogDefFailedStage3(unique_ptr<IquanR> &iquanRPtr);
    void testUpdateCatalogDefFlushStage3(unique_ptr<IquanR> &iquanRPtr);
    void testUpdateCatalogDefFailedStage4(unique_ptr<IquanR> &iquanRPtr);
    void testUpdateCatalogDefFlushStage4(unique_ptr<IquanR> &iquanRPtr);
    void testUpdateCatalogDefFailedStage5(unique_ptr<IquanR> &iquanRPtr);
    void testUpdateCatalogDefFlushStage5(unique_ptr<IquanR> &iquanRPtr);
    void testUpdateCatalogDefFailedStage6(unique_ptr<IquanR> &iquanRPtr);
    void testUpdateCatalogDefFlushStage6(unique_ptr<IquanR> &iquanRPtr);

private:
    AUTIL_LOG_DECLARE();
    std::unique_ptr<ExternalTableConfigR> _exterTableConfigRPtr;
};

AUTIL_LOG_SETUP(resource, IquanRTest);

void IquanRTest::setUp() {
    std::call_once(iquanFlag, [&]() {
        // make iquan search for iquan.jar from workdir
        EnvGuard _(BINARY_PATH, ".");
        Status status = IquanEnv::jvmSetup(jt_hdfs_jvm, {}, "");
        ASSERT_TRUE(status.ok()) << "can not start jvm" << std::endl << status.errorMessage();
    });
}

void IquanRTest::tearDown() {}

TEST_F(IquanRTest, testSimple) {
    string iquanConfig = string(R"json({"iquan_jni_config":{}, "iquan_client_config":{}})json");
    navi::NaviResourceHelper naviRes;
    naviRes.config(IquanR::RESOURCE_ID, iquanConfig);
    IquanR *iquanR = naviRes.getOrCreateRes<IquanR>();
    ASSERT_TRUE(iquanR);
    ASSERT_TRUE(iquanR->_sqlClient);
    ASSERT_TRUE(iquanR->_gigMetaR);
}

void IquanRTest::prepLoadLogicTables(unique_ptr<IquanR> &iquanRPtr, const string &dbName) {
    string logicTablePath = "sql_resource/logical_table.json";
    map<string, vector<TableModel>> logicTables;
    ASSERT_NO_FATAL_FAILURE(IquanRTest::loadFromFile(logicTablePath, logicTables));
    map<string, vector<TableModel>> dbNameToLogicTables;
    for (auto &p : logicTables) {
        dbNameToLogicTables[dbName].insert(
            dbNameToLogicTables[dbName].end(), p.second.begin(), p.second.end());
    }
    ASSERT_FALSE(dbNameToLogicTables.empty());
    iquanRPtr->_logicTables = std::move(dbNameToLogicTables);
}

TEST_F(IquanRTest, testLoadLogicTablesSuccess) {
    auto iquanRPtr = make_unique<IquanR>();
    prepLoadLogicTables(iquanRPtr, "db1");
    LocationSign locationSign = {1, "qrs", "qrs"};
    CatalogDefs catalogDefs;
    ASSERT_TRUE(iquanRPtr->loadLogicTables(catalogDefs, locationSign));
    string expectStr
        = R"Json({"catalogs":[{"catalog_name":"default","databases":[{"database_name":"system","tables":[],"layer_tables":[],"functions":[]},{"database_name":"db1","tables":[{"table_content_type":"","table_content":{"table_name":"trigger_table","table_type":"logical","fields":[{"field_name":"trigger_id","field_type":{"type":"int64","extend_infos":{}},"is_attribute":false},{"field_name":"trigger_str","field_type":{"type":"string","extend_infos":{}},"is_attribute":false},{"field_name":"trigger_score_float","field_type":{"type":"float","extend_infos":{}},"is_attribute":false},{"field_name":"trigger_score_double","field_type":{"type":"double","extend_infos":{}},"is_attribute":false},{"field_name":"trigger_score_int","field_type":{"type":"int32","extend_infos":{}},"is_attribute":false},{"field_name":"trigger_score_long","field_type":{"type":"int64","extend_infos":{}},"is_attribute":false}],"summary_fields":[],"sub_tables":[],"distribution":{"partition_cnt":1,"hash_fields":["trigger_id"],"hash_function":"HASH64","hash_params":{}},"sort_desc":[],"properties":{},"indexes":{}}}],"layer_tables":[],"functions":[]}],"locations":[{"partition_cnt":1,"node_name":"qrs","node_type":"qrs","tables":[{"database_name":"db1","table_name":"trigger_table"}],"equivalent_hash_fields":[],"join_info":[]}]}]})Json";
    ASSERT_EQ(expectStr, FastToJsonString(catalogDefs));
}

TEST_F(IquanRTest, testLoadLogicTablesFailed1) {
    // load logical table failed by mismatch table type
    auto iquanRPtr = make_unique<IquanR>();
    prepLoadLogicTables(iquanRPtr, "db1");
    LocationSign locationSign = {1, "qrs", "qrs"};
    (iquanRPtr->_logicTables).begin()->second[0].tableContent.tableType = "not_logical";
    navi::NaviLoggerProvider provider("ERROR");
    CatalogDefs catalogDefs;
    ASSERT_FALSE(iquanRPtr->loadLogicTables(catalogDefs, locationSign));
    ASSERT_NO_FATAL_FAILURE(NaviLoggerProviderTestUtil::checkTraceCount(
        1, "tableType mismatch, expect: logical, actual: not_logical", provider));
}

TEST_F(IquanRTest, testLoadLogicTablesFailed2) {
    // load logical table failed by repeated table name
    auto iquanRPtr = make_unique<IquanR>();
    prepLoadLogicTables(iquanRPtr, "db1");
    LocationSign locationSign = {1, "qrs", "qrs"};
    auto &logicTablesVec = (iquanRPtr->_logicTables).begin()->second;
    ASSERT_FALSE(logicTablesVec.empty());
    logicTablesVec.push_back(logicTablesVec.back());
    CatalogDefs catalogDefs;
    navi::NaviLoggerProvider provider("ERROR");
    ASSERT_FALSE(iquanRPtr->loadLogicTables(catalogDefs, locationSign));
    ASSERT_NO_FATAL_FAILURE(NaviLoggerProviderTestUtil::checkTraceCount(
        1, "add table [trigger_table] to database [db1] failed", provider));
}

TEST_F(IquanRTest, testLoadLogicTablesFailed3) {
    // load logical table failed by repeated table identity
    auto iquanRPtr = make_unique<IquanR>();
    prepLoadLogicTables(iquanRPtr, "db1");
    LocationSign locationSign = {1, "qrs", "qrs"};
    auto dbName = (iquanRPtr->_logicTables).begin()->first;
    auto tableName = (iquanRPtr->_logicTables).begin()->second[0].tableName();
    CatalogDefs catalogDefs;
    TableIdentity tableIdentity(dbName, tableName);
    ASSERT_TRUE(catalogDefs.catalog(SQL_DEFAULT_CATALOG_NAME)
                    .location(locationSign)
                    .addTableIdentity(tableIdentity));
    navi::NaviLoggerProvider provider("ERROR");
    ASSERT_FALSE(iquanRPtr->loadLogicTables(catalogDefs, locationSign));
    ASSERT_NO_FATAL_FAILURE(NaviLoggerProviderTestUtil::checkTraceCount(
        1, "add table identity [db1] [trigger_table] failed", provider));
}

void IquanRTest::prepLoadLayerTables(unique_ptr<IquanR> &iquanRPtr, const string &dbName) {
    string layerTablePath = "sql_resource/layer_tables.json";
    map<string, vector<LayerTableModel>> layerTables;
    ASSERT_NO_FATAL_FAILURE(loadFromFile(layerTablePath, layerTables));
    map<string, vector<LayerTableModel>> dbNameToLayerTables;
    for (auto &p : layerTables) {
        dbNameToLayerTables[dbName].insert(
            dbNameToLayerTables[dbName].end(), p.second.begin(), p.second.end());
    }
    ASSERT_FALSE(dbNameToLayerTables.empty());
    iquanRPtr->_layerTables = dbNameToLayerTables;
}

TEST_F(IquanRTest, testLoadLayerTablesSuccess) {
    auto iquanRPtr = make_unique<IquanR>();
    prepLoadLayerTables(iquanRPtr, "db1");
    LocationSign locationSign = {1, "qrs", "qrs"};
    CatalogDefs catalogDefs;
    ASSERT_TRUE(iquanRPtr->loadLayerTables(catalogDefs, locationSign));
    string expectStr
        = R"Json({"catalogs":[{"catalog_name":"default","databases":[{"database_name":"system","tables":[],"layer_tables":[],"functions":[]},{"database_name":"db1","tables":[],"layer_tables":[{"layer_table_name":"layer_table_tj_shop_01","content":{"layer_format":[{"field_name":"ts","layer_method":"range","value_type":"int"},{"field_name":"md5","layer_method":"grouping","value_type":"string"}],"layers":[{"database_name":"db1","table_name":"layer_table_t1","layer_info":{"md5":["123456"],"ts":[[45,78]]}},{"database_name":"db1","table_name":"layer_table_t2","layer_info":{"md5":["123456"],"ts":[[99,145]]}},{"database_name":"db1","table_name":"layer_table_t3","layer_info":{"md5":["-123456"],"ts":[[0,38]]}}],"properties":{}}},{"layer_table_name":"layer_table_tj_shop_02","content":{"layer_format":[{"field_name":"ts","layer_method":"range","value_type":"int"}],"layers":[{"database_name":"db1","table_name":"layer_table_t1","layer_info":{"ts":[[45,78]]}},{"database_name":"db1","table_name":"layer_table_t2","layer_info":{"ts":[[99,145]]}},{"database_name":"db1","table_name":"layer_table_t3","layer_info":{"ts":[[0,38]]}}],"properties":{}}},{"layer_table_name":"layer_table_order","content":{"layer_format":[{"field_name":"is_online","layer_method":"grouping","value_type":"int"}],"layers":[{"database_name":"db1","table_name":"layer_table_t1","layer_info":{"is_online":[0]}},{"database_name":"db1","table_name":"layer_table_t2","layer_info":{"is_online":[1]}}],"properties":{"distinct":{"params":{"default":"ARBITRARY","primary":"max"},"type":"agg"}}}},{"layer_table_name":"layer_table_order_simple","content":{"layer_format":[{"field_name":"is_online","layer_method":"grouping","value_type":"int"}],"layers":[{"database_name":"db1","table_name":"layer_table_simple_01","layer_info":{"is_online":[0]}},{"database_name":"db1","table_name":"layer_table_simple_02","layer_info":{"is_online":[1]}}],"properties":{"distinct":{"params":{"default":"ARBITRARY","primary":"max"},"type":"agg"}}}},{"layer_table_name":"layer_table_simple_three","content":{"layer_format":[{"field_name":"ds","layer_method":"grouping","value_type":"int"}],"layers":[{"database_name":"db1","table_name":"layer_table_simple_01","layer_info":{"ds":[0]}},{"database_name":"db1","table_name":"layer_table_simple_02","layer_info":{"ds":[1]}},{"database_name":"db1","table_name":"layer_table_simple_03","layer_info":{"ds":[2]}}],"properties":{"distinct":{"params":{"default":"ARBITRARY","primary":"max"},"type":"agg"}}}}],"functions":[]}],"locations":[{"partition_cnt":1,"node_name":"qrs","node_type":"qrs","tables":[{"database_name":"db1","table_name":"layer_table_tj_shop_01"},{"database_name":"db1","table_name":"layer_table_tj_shop_02"},{"database_name":"db1","table_name":"layer_table_order"},{"database_name":"db1","table_name":"layer_table_order_simple"},{"database_name":"db1","table_name":"layer_table_simple_three"}],"equivalent_hash_fields":[],"join_info":[]}]}]})Json";
    ASSERT_EQ(expectStr, FastToJsonString(catalogDefs));
}

TEST_F(IquanRTest, testLoadLayerTablesFailed1) {
    auto iquanRPtr = make_unique<IquanR>();
    prepLoadLayerTables(iquanRPtr, "db1");
    LocationSign locationSign = {1, "qrs", "qrs"};
    // load layer table failed by repeated table name
    auto &layerTablesVec = (iquanRPtr->_layerTables).begin()->second;
    layerTablesVec.push_back(layerTablesVec.back());
    navi::NaviLoggerProvider provider("ERROR");
    CatalogDefs catalogDefs;
    ASSERT_FALSE(iquanRPtr->loadLayerTables(catalogDefs, locationSign));
    string errorTrace
        = "add table [" + layerTablesVec.back().layerTableName + "] to database [db1] failed";
    ASSERT_NO_FATAL_FAILURE(NaviLoggerProviderTestUtil::checkTraceCount(1, errorTrace, provider));
}

TEST_F(IquanRTest, testLoadLayerTablesFailed2) {
    // load layer table failed by repeated table identity
    auto iquanRPtr = make_unique<IquanR>();
    prepLoadLayerTables(iquanRPtr, "db1");
    LocationSign locationSign = {1, "qrs", "qrs"};
    auto dbName = (iquanRPtr->_layerTables).begin()->first;
    auto tableName = (iquanRPtr->_layerTables).begin()->second[0].tableName();
    navi::NaviLoggerProvider provider("ERROR");
    CatalogDefs catalogDefs;
    TableIdentity tableIdentity(dbName, tableName);
    ASSERT_TRUE(catalogDefs.catalog(SQL_DEFAULT_CATALOG_NAME)
                    .location(locationSign)
                    .addTableIdentity(tableIdentity));
    ASSERT_FALSE(iquanRPtr->loadLayerTables(catalogDefs, locationSign));
    string errorTrace = "add table identity [" + dbName + "] [" + tableName + "] failed";
    ASSERT_NO_FATAL_FAILURE(NaviLoggerProviderTestUtil::checkTraceCount(1, errorTrace, provider));
}

void IquanRTest::prepLoadExternalTableModels(unique_ptr<IquanR> &iquanRPtr) {
    iquanRPtr->_configPath = GET_TEST_DATA_PATH();
    _exterTableConfigRPtr = make_unique<ExternalTableConfigR>();
    iquanRPtr->_externalTableConfigR = _exterTableConfigRPtr.get();
}

TEST_F(IquanRTest, testFillExternalTableModelsSuccess) {
    auto iquanRPtr = make_unique<IquanR>();
    prepLoadExternalTableModels(iquanRPtr);
    LocationSign locationSign = {1, "qrs", "qrs"};
    string tableName = "extern_simple4";
    TableConfig tableConfig;
    tableConfig.schemaFile = "sql_resource/tablet_schema.json";
    iquanRPtr->_externalTableConfigR->tableConfigMap = {{tableName, tableConfig}};
    CatalogDefs catalogDefs;
    ASSERT_TRUE(iquanRPtr->fillExternalTableModels(catalogDefs, locationSign));
    // db name, table type, table name
    ASSERT_EQ(1, catalogDefs.catalogs.size());
    const auto &catalogDef = catalogDefs.catalogs[0];
    ASSERT_EQ(2, catalogDef.databases.size());
    const auto &databases = catalogDef.databases;
    ASSERT_EQ(SQL_DEFAULT_EXTERNAL_DATABASE_NAME, databases[1].dbName);
    const auto &tables = databases[1].tables;
    ASSERT_EQ(1, tables.size());
    ASSERT_EQ(sql::SCAN_EXTERNAL_TABLE_TYPE, tables[0].tableType());
    ASSERT_EQ(tableName, tables[0].tableName());
}

TEST_F(IquanRTest, testFillExternalTableModelsFailed1) {
    // read external schema file failed
    auto iquanRPtr = make_unique<IquanR>();
    prepLoadExternalTableModels(iquanRPtr);
    LocationSign locationSign = {1, "qrs", "qrs"};
    string tableName = "not_exist";
    TableConfig tableConfig;
    tableConfig.schemaFile = "sql_resource/not_exist_schema.json";
    iquanRPtr->_externalTableConfigR->tableConfigMap = {{tableName, tableConfig}};
    navi::NaviLoggerProvider provider("ERROR");
    CatalogDefs catalogDefs;
    ASSERT_FALSE(iquanRPtr->fillExternalTableModels(catalogDefs, locationSign));
    string errorTrace
        = "read file ["
          + fslib::util::FileUtil::joinFilePath(iquanRPtr->_configPath, tableConfig.schemaFile)
          + "] failed";
    ASSERT_NO_FATAL_FAILURE(NaviLoggerProviderTestUtil::checkTraceCount(1, errorTrace, provider));
}

TEST_F(IquanRTest, testFillExternalTableModelsFailed2) {
    // load external schema failed
    auto iquanRPtr = make_unique<IquanR>();
    prepLoadExternalTableModels(iquanRPtr);
    LocationSign locationSign = {1, "qrs", "qrs"};
    string tableName = "invalid";
    TableConfig tableConfig;
    tableConfig.schemaFile = "sql_resource/invalid_schema.json";
    iquanRPtr->_externalTableConfigR->tableConfigMap = {{tableName, tableConfig}};
    navi::NaviLoggerProvider provider("ERROR");
    CatalogDefs catalogDefs;
    ASSERT_FALSE(iquanRPtr->fillExternalTableModels(catalogDefs, locationSign));
    string errorTrace = "get external table index schema failed, table[" + tableName + "] config["
                        + FastToJsonString(tableConfig, true) + "]";
    ASSERT_NO_FATAL_FAILURE(NaviLoggerProviderTestUtil::checkTraceCount(1, errorTrace, provider));
}

TEST_F(IquanRTest, testFillExternalTableModelsFailed3) {
    // fill external normal table falied by repeated table name
    auto iquanRPtr = make_unique<IquanR>();
    prepLoadExternalTableModels(iquanRPtr);
    LocationSign locationSign = {1, "qrs", "qrs"};
    string tableName = "extern_simple4";
    TableConfig tableConfig;
    tableConfig.schemaFile = "sql_resource/tablet_schema.json";
    iquanRPtr->_externalTableConfigR->tableConfigMap = {{tableName, tableConfig}};
    CatalogDefs catalogDefs;
    string dbName = SQL_DEFAULT_EXTERNAL_DATABASE_NAME;
    TableModel fakeTableModel;
    fakeTableModel.tableContent.tableName = tableName;
    ASSERT_TRUE(
        catalogDefs.catalog(SQL_DEFAULT_CATALOG_NAME).database(dbName).addTable(fakeTableModel));
    navi::NaviLoggerProvider provider("ERROR");
    ASSERT_FALSE(iquanRPtr->fillExternalTableModels(catalogDefs, locationSign));
    string errorTrace = "add table [" + tableName + "] to database ["
                        + SQL_DEFAULT_EXTERNAL_DATABASE_NAME + "] failed";
    ASSERT_NO_FATAL_FAILURE(NaviLoggerProviderTestUtil::checkTraceCount(1, errorTrace, provider));
}

TEST_F(IquanRTest, testFillExternalTableModelsFailed4) {
    // fill external normal table falied by repeated table identity
    auto iquanRPtr = make_unique<IquanR>();
    prepLoadExternalTableModels(iquanRPtr);
    LocationSign locationSign = {1, "qrs", "qrs"};
    string tableName = "extern_simple4";
    TableConfig tableConfig;
    tableConfig.schemaFile = "sql_resource/tablet_schema.json";
    iquanRPtr->_externalTableConfigR->tableConfigMap = {{tableName, tableConfig}};
    CatalogDefs catalogDefs;
    string dbName = SQL_DEFAULT_EXTERNAL_DATABASE_NAME;
    TableIdentity tableIdentity {dbName, tableName};
    ASSERT_TRUE(catalogDefs.catalog(SQL_DEFAULT_CATALOG_NAME)
                    .location(locationSign)
                    .addTableIdentity(tableIdentity));
    navi::NaviLoggerProvider provider("ERROR");
    ASSERT_FALSE(iquanRPtr->fillExternalTableModels(catalogDefs, locationSign));
    string errorTrace = "add table identity [" + dbName + "] [" + tableName + "] failed";
    ASSERT_NO_FATAL_FAILURE(NaviLoggerProviderTestUtil::checkTraceCount(1, errorTrace, provider));
}

TEST_F(IquanRTest, testGetGigCatalogDef) {
    auto iquanRPtr = make_unique<IquanR>();
    auto gigMetaRPtr = make_unique<GigMetaR>();
    iquanRPtr->_gigMetaR = gigMetaRPtr.get();
    auto &bizMetaInfos = gigMetaRPtr->_bizMetaInfos;
    // empty bizMetaInfos
    {
        CatalogDefs catalogDefs;
        ASSERT_TRUE(iquanRPtr->getGigCatalogDef(catalogDefs));
        ASSERT_TRUE(catalogDefs.catalogs.empty());
    }
    // empty versions
    {
        multi_call::BizMetaInfo bizMetaInfo;
        bizMetaInfo.bizName = "default_biz";
        bizMetaInfos.emplace_back(bizMetaInfo);
        CatalogDefs catalogDefs;
        ASSERT_TRUE(iquanRPtr->getGigCatalogDef(catalogDefs));
        ASSERT_TRUE(catalogDefs.catalogs.empty());
        bizMetaInfos.clear();
    }
    // No ha3_table_info_r meta
    {
        multi_call::VersionInfo versionInfo;
        versionInfo.metas.reset(
            new multi_call::MetaMap({{"not" + sql::Ha3TableInfoR::RESOURCE_ID, "metaStr"}}));
        multi_call::BizMetaInfo bizMetaInfo;
        bizMetaInfo.bizName = "default_biz";
        bizMetaInfo.versions = {{1, versionInfo}};
        bizMetaInfos.emplace_back(bizMetaInfo);
        CatalogDefs catalogDefs;
        ASSERT_TRUE(iquanRPtr->getGigCatalogDef(catalogDefs));
        ASSERT_TRUE(catalogDefs.catalogs.empty());
        bizMetaInfos.clear();
    }
    // parse meta str failed
    {
        multi_call::VersionInfo versionInfo;
        versionInfo.metas.reset(
            new multi_call::MetaMap({{sql::Ha3TableInfoR::RESOURCE_ID, "invalid_json"}}));
        multi_call::BizMetaInfo bizMetaInfo;
        bizMetaInfo.bizName = "default_biz";
        bizMetaInfo.versions = {{1, versionInfo}};
        bizMetaInfos.emplace_back(bizMetaInfo);
        CatalogDefs catalogDefs;
        navi::NaviLoggerProvider provider("ERROR");
        ASSERT_FALSE(iquanRPtr->getGigCatalogDef(catalogDefs));
        ASSERT_NO_FATAL_FAILURE(NaviLoggerProviderTestUtil::checkTraceCount(
            1, "parse gig catalog failed, metaStr: invalid_json", provider));
        ASSERT_TRUE(catalogDefs.catalogs.empty());
        bizMetaInfos.clear();
    }
    string catalogName = SQL_DEFAULT_CATALOG_NAME;
    string dbName = "database";
    // single biz, single version meta
    {
        string schemaFile = "sql_resource/tablet_schema.json";
        auto tableModelPtr = getTableModelFromSchema(schemaFile);
        ASSERT_TRUE(tableModelPtr);
        CatalogDefs expectCatalogDefs;
        ASSERT_TRUE(
            expectCatalogDefs.catalog(catalogName).database(dbName).addTable(*tableModelPtr));
        multi_call::VersionInfo versionInfo;
        string metaStr = FastToJsonString(expectCatalogDefs);
        versionInfo.metas.reset(
            new multi_call::MetaMap({{sql::Ha3TableInfoR::RESOURCE_ID, metaStr}}));
        multi_call::BizMetaInfo bizMetaInfo;
        bizMetaInfo.bizName = "default_biz";
        bizMetaInfo.versions = {{1, versionInfo}};
        bizMetaInfos.emplace_back(bizMetaInfo);
        CatalogDefs catalogDefs;
        ASSERT_TRUE(iquanRPtr->getGigCatalogDef(catalogDefs));
        ASSERT_EQ(metaStr, FastToJsonString(catalogDefs));
        bizMetaInfos.clear();
    }
    // single biz, multi version meta, only extract the head version
    {
        string schemaFile = "sql_resource/tablet_schema.json";
        auto tableModelPtr = getTableModelFromSchema(schemaFile);
        ASSERT_TRUE(tableModelPtr);
        CatalogDefs expectCatalogDefs;
        ASSERT_TRUE(
            expectCatalogDefs.catalog(catalogName).database(dbName).addTable(*tableModelPtr));
        multi_call::VersionInfo versionInfo;
        string metaStr = FastToJsonString(expectCatalogDefs);
        versionInfo.metas.reset(
            new multi_call::MetaMap({{sql::Ha3TableInfoR::RESOURCE_ID, metaStr}}));
        multi_call::BizMetaInfo bizMetaInfo;
        bizMetaInfo.bizName = "default_biz";
        bizMetaInfo.versions = {{1, versionInfo}, {2, versionInfo}};
        bizMetaInfos.emplace_back(bizMetaInfo);
        CatalogDefs catalogDefs;
        ASSERT_TRUE(iquanRPtr->getGigCatalogDef(catalogDefs));
        ASSERT_EQ(metaStr, FastToJsonString(catalogDefs));
        bizMetaInfos.clear();
    }
    // multi biz, merge each biz info
    {
        string schemaFile1 = "sql_resource/tablet_schema.json";
        auto tableModel1Ptr = getTableModelFromSchema(schemaFile1);
        ASSERT_TRUE(tableModel1Ptr);
        string schemaFile2 = "sql_resource/kv_schema.json";
        auto tableModel2Ptr = getTableModelFromSchema(schemaFile2);
        ASSERT_TRUE(tableModel2Ptr);
        CatalogDefs expectCatalogDefs1;
        ASSERT_TRUE(
            expectCatalogDefs1.catalog(catalogName).database(dbName).addTable(*tableModel1Ptr));
        CatalogDefs expectCatalogDefs2;
        ASSERT_TRUE(
            expectCatalogDefs2.catalog(catalogName).database(dbName).addTable(*tableModel2Ptr));
        multi_call::VersionInfo versionInfo1;
        string metaStr1 = FastToJsonString(expectCatalogDefs1);
        versionInfo1.metas.reset(
            new multi_call::MetaMap({{sql::Ha3TableInfoR::RESOURCE_ID, metaStr1}}));
        multi_call::VersionInfo versionInfo2;
        string metaStr2 = FastToJsonString(expectCatalogDefs2);
        versionInfo2.metas.reset(
            new multi_call::MetaMap({{sql::Ha3TableInfoR::RESOURCE_ID, metaStr2}}));
        ASSERT_TRUE(expectCatalogDefs1.merge(expectCatalogDefs2));
        multi_call::BizMetaInfo bizMetaInfo1;
        bizMetaInfo1.bizName = "biz_1";
        bizMetaInfo1.versions = {{1, versionInfo1}};
        bizMetaInfos.emplace_back(bizMetaInfo1);
        multi_call::BizMetaInfo bizMetaInfo2;
        bizMetaInfo2.bizName = "biz_2";
        bizMetaInfo2.versions = {{1, versionInfo2}};
        bizMetaInfos.emplace_back(bizMetaInfo2);
        CatalogDefs catalogDefs;
        ASSERT_TRUE(iquanRPtr->getGigCatalogDef(catalogDefs));
        ASSERT_EQ(FastToJsonString(expectCatalogDefs1), FastToJsonString(catalogDefs));
    }
}

TEST_F(IquanRTest, testInitIquanClient) {
    auto iquanRPtr = make_unique<IquanR>();
    JniConfig &jniConfig = iquanRPtr->_jniConfig;
    // init iquan client failed
    {
        jniConfig.tableConfig.summaryTableSuffix = "";
        navi::NaviLoggerProvider provider("ERROR");
        ASSERT_FALSE(iquanRPtr->initIquanClient());
        string errorTrace = string("init sql client failed, error msg: ")
                            + "jniConfig is invalid: " + FastToJsonString(jniConfig);
        ASSERT_NO_FATAL_FAILURE(
            NaviLoggerProviderTestUtil::checkTraceCount(1, errorTrace, provider));
    }
    // init iquan client success
    {
        jniConfig.tableConfig.summaryTableSuffix = SUMMARY_TABLE_SUFFIX;
        ASSERT_TRUE(iquanRPtr->initIquanClient());
        ASSERT_TRUE(iquanRPtr->_sqlClient);
    }
}

void IquanRTest::testUpdateCatalogDefFailedStage1(unique_ptr<IquanR> &iquanRPtr) {
    // failed by null sql client
    navi::NaviLoggerProvider provider("ERROR");
    ASSERT_FALSE(iquanRPtr->updateCatalogDef());
    ASSERT_NO_FATAL_FAILURE(NaviLoggerProviderTestUtil::checkTraceCount(
        1, "update failed, sql client empty", provider));
}

void IquanRTest::testUpdateCatalogDefFlushStage1(unique_ptr<IquanR> &iquanRPtr) {
    // flush sql client
    ASSERT_TRUE(iquanRPtr->initIquanClient());
}

void IquanRTest::testUpdateCatalogDefFailedStage2(unique_ptr<IquanR> &iquanRPtr) {
    // failed by getGigCatalogDef failed
    auto &bizMetaInfos = iquanRPtr->_gigMetaR->_bizMetaInfos;
    multi_call::VersionInfo versionInfo;
    versionInfo.metas.reset(
        new multi_call::MetaMap({{sql::Ha3TableInfoR::RESOURCE_ID, "invalid_json"}}));
    multi_call::BizMetaInfo bizMetaInfo;
    bizMetaInfo.bizName = "default_biz";
    bizMetaInfo.versions = {{1, versionInfo}};
    bizMetaInfos.emplace_back(bizMetaInfo);
    navi::NaviLoggerProvider provider("ERROR");
    ASSERT_FALSE(iquanRPtr->updateCatalogDef());
    ASSERT_NO_FATAL_FAILURE(NaviLoggerProviderTestUtil::checkTraceCount(
        1, "parse gig catalog failed, metaStr: invalid_json", provider));
}

void IquanRTest::testUpdateCatalogDefFlushStage2(unique_ptr<IquanR> &iquanRPtr) {
    // flush biz meta info
    auto &bizMetaInfos = iquanRPtr->_gigMetaR->_bizMetaInfos;
    bizMetaInfos.clear();
    string catalogName = SQL_DEFAULT_CATALOG_NAME;
    string dbName = "db1";
    string schemaFile = "sql_resource/tablet_schema.json";
    auto tableModelPtr = getTableModelFromSchema(schemaFile);
    ASSERT_TRUE(tableModelPtr);
    CatalogDefs catalogDefs;
    ASSERT_TRUE(catalogDefs.catalog(catalogName).database(dbName).addTable(*tableModelPtr));
    multi_call::VersionInfo versionInfo;
    versionInfo.metas.reset(new multi_call::MetaMap(
        {{sql::Ha3TableInfoR::RESOURCE_ID, FastToJsonString(catalogDefs)}}));
    multi_call::BizMetaInfo bizMetaInfo;
    bizMetaInfo.bizName = "default_biz";
    bizMetaInfo.versions = {{1, versionInfo}};
    bizMetaInfos.emplace_back(bizMetaInfo);
}

void IquanRTest::testUpdateCatalogDefFailedStage3(unique_ptr<IquanR> &iquanRPtr) {
    // failed by load logial table failed
    prepLoadLogicTables(iquanRPtr, "db1");
    auto &logicTables = iquanRPtr->_logicTables;
    logicTables.begin()->second[0].tableContent.tableType = "not_logical";
    navi::NaviLoggerProvider provider("ERROR");
    ASSERT_FALSE(iquanRPtr->updateCatalogDef());
    ASSERT_NO_FATAL_FAILURE(NaviLoggerProviderTestUtil::checkTraceCount(
        1, "tableType mismatch, expect: logical, actual: not_logical", provider));
}

void IquanRTest::testUpdateCatalogDefFlushStage3(unique_ptr<IquanR> &iquanRPtr) {
    // flush logical table
    (iquanRPtr->_logicTables).begin()->second[0].tableContent.tableType
        = isearch::SQL_LOGICTABLE_TYPE;
}

void IquanRTest::testUpdateCatalogDefFailedStage4(unique_ptr<IquanR> &iquanRPtr) {
    // failed by load layer table
    prepLoadLayerTables(iquanRPtr, "db1");
    auto &layerTables = iquanRPtr->_layerTables;
    auto &layerTablesVec = layerTables.begin()->second;
    layerTablesVec.push_back(layerTablesVec.back());
    navi::NaviLoggerProvider provider("ERROR");
    ASSERT_FALSE(iquanRPtr->updateCatalogDef());
    string errorTrace
        = "add table [" + layerTablesVec.back().layerTableName + "] to database [db1] failed";
    ASSERT_NO_FATAL_FAILURE(NaviLoggerProviderTestUtil::checkTraceCount(1, errorTrace, provider));
}

void IquanRTest::testUpdateCatalogDefFlushStage4(unique_ptr<IquanR> &iquanRPtr) {
    // flush layer table
    iquanRPtr->_layerTables.clear();
}

void IquanRTest::testUpdateCatalogDefFailedStage5(unique_ptr<IquanR> &iquanRPtr) {
    // failed by load external table failed
    iquanRPtr->_configPath = GET_TEST_DATA_PATH();
    // auto exterTableConfigRPtr = make_unique<ExternalTableConfigR>();
    // iquanRPtr->_externalTableConfigR = exterTableConfigRPtr.get();
    string tableName = "not_exist";
    TableConfig tableConfig;
    tableConfig.schemaFile = "sql_resource/not_exist_schema.json";
    iquanRPtr->_externalTableConfigR->tableConfigMap = {{tableName, tableConfig}};
    navi::NaviLoggerProvider provider("ERROR");
    ASSERT_FALSE(iquanRPtr->updateCatalogDef());
    string errorTrace
        = "read file ["
          + fslib::util::FileUtil::joinFilePath(iquanRPtr->_configPath, tableConfig.schemaFile)
          + "] failed";
    ASSERT_NO_FATAL_FAILURE(NaviLoggerProviderTestUtil::checkTraceCount(1, errorTrace, provider));
}

void IquanRTest::testUpdateCatalogDefFlushStage5(unique_ptr<IquanR> &iquanRPtr) {
    // flush external table
    string externTableName = "extern_simple4";
    TableConfig tableConfig;
    tableConfig.schemaFile = "sql_resource/tablet_schema.json";
    iquanRPtr->_externalTableConfigR->tableConfigMap = {{externTableName, tableConfig}};
}

void IquanRTest::testUpdateCatalogDefFailedStage6(unique_ptr<IquanR> &iquanRPtr) {
    // failed by fill function models failed
    auto &udfModelR = iquanRPtr->_udfModelR;
    ASSERT_FALSE(udfModelR->_dbUdfMap["system"].empty());
    auto &funcVec = udfModelR->_dbUdfMap["system"];
    funcVec.push_back(funcVec.back());
    navi::NaviLoggerProvider provider("ERROR");
    ASSERT_FALSE(iquanRPtr->updateCatalogDef());
    ASSERT_NO_FATAL_FAILURE(
        NaviLoggerProviderTestUtil::checkTraceCount(1, "fillFunctionModels failed", provider));
}

void IquanRTest::testUpdateCatalogDefFlushStage6(unique_ptr<IquanR> &iquanRPtr) {
    // flush udf_model_r
    auto &funcVec = iquanRPtr->_udfModelR->_dbUdfMap["system"];
    funcVec.pop_back();
}

TEST_F(IquanRTest, testUpdateCatalogDef) {
    auto iquanRPtr = make_unique<IquanR>();

    auto gigMetaR = make_unique<GigMetaR>();
    iquanRPtr->_gigMetaR = gigMetaR.get();

    auto exterTableConfigRPtr = make_unique<ExternalTableConfigR>();
    iquanRPtr->_externalTableConfigR = exterTableConfigRPtr.get();

    string udfModelRStr;
    ASSERT_NO_FATAL_FAILURE(readTestFile("sql_resource/udf_model_r.json", udfModelRStr));
    NaviResourceHelper naviRes;
    ASSERT_TRUE(naviRes.config(udfModelRStr));
    auto udfModelR = naviRes.getOrCreateRes<UdfModelR>();
    ASSERT_TRUE(udfModelR);
    iquanRPtr->_udfModelR = udfModelR;

    testUpdateCatalogDefFailedStage1(iquanRPtr);
    testUpdateCatalogDefFlushStage1(iquanRPtr);
    testUpdateCatalogDefFailedStage2(iquanRPtr);
    testUpdateCatalogDefFlushStage2(iquanRPtr);
    testUpdateCatalogDefFailedStage3(iquanRPtr);
    testUpdateCatalogDefFlushStage3(iquanRPtr);
    testUpdateCatalogDefFailedStage4(iquanRPtr);
    testUpdateCatalogDefFlushStage4(iquanRPtr);
    testUpdateCatalogDefFailedStage5(iquanRPtr);
    testUpdateCatalogDefFlushStage5(iquanRPtr);
    testUpdateCatalogDefFailedStage6(iquanRPtr);
    testUpdateCatalogDefFlushStage6(iquanRPtr);
    // update catalog defs success
    ASSERT_TRUE(iquanRPtr->updateCatalogDef());
}

TEST_F(IquanRTest, testInitSuccess) {
    string iquanConfig = string(R"json({"iquan_jni_config":{}, "iquan_client_config":{}})json");
    navi::NaviResourceHelper naviRes;
    naviRes.config(IquanR::RESOURCE_ID, iquanConfig);
    IquanR *iquanR = naviRes.getOrCreateRes<IquanR>();
    ASSERT_TRUE(iquanR);
    ASSERT_TRUE(iquanR->_sqlClient);
    ASSERT_TRUE(iquanR->_gigMetaR);
}

TEST_F(IquanRTest, testInitFailedBySqlClient) {
    string iquanConfig = string(
        R"json({"iquan_jni_config":{"table_config":{"summary_suffix":""}}, "iquan_client_config":{}})json");
    navi::NaviResourceHelper naviRes;
    naviRes.config(IquanR::RESOURCE_ID, iquanConfig);
    IquanR *iquanR = naviRes.getOrCreateRes<IquanR>();
    ASSERT_FALSE(iquanR);
}

TEST_F(IquanRTest, testInitFailedByUpdateCatalogs) {
    string iquanConfig = string(
        R"json({"iquan_jni_config":{}, "iquan_client_config":{}, "logic_tables":{"table_content": {"table_name": "invalid", "table_type": "invalid", "field": [], "distribution": {}, "properties": {}}}})json");
    navi::NaviResourceHelper naviRes;
    naviRes.config(IquanR::RESOURCE_ID, iquanConfig);
    IquanR *iquanR = naviRes.getOrCreateRes<IquanR>();
    ASSERT_FALSE(iquanR);
}
} // namespace sql
