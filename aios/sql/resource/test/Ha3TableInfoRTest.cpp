#include "sql/resource/Ha3TableInfoR.h"

#include "fslib/util/FileUtil.h"
#include "indexlib/config/ITabletSchema.h"
#include "indexlib/index_base/schema_adapter.h"
#include "indexlib/partition/join_cache/join_field.h"
#include "iquan/common/Common.h"
#include "iquan/common/catalog/CatalogDef.h"
#include "navi/log/NaviLoggerProvider.h"
#include "navi/log/test/NaviLoggerProviderTestUtil.h"
#include "navi/tester/KernelTesterBuilder.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace iquan;
using namespace autil::legacy;
using namespace navi;

namespace sql {

class MockIndexPartitionReader : public indexlib::partition::IndexPartitionReader {
public:
    MockIndexPartitionReader() {}
    MockIndexPartitionReader(indexlib::config::IndexPartitionSchemaPtr schema)
        : indexlib::partition::IndexPartitionReader(schema) {}
    ~MockIndexPartitionReader() {}

public:
    void Open(const indexlib::index_base::PartitionDataPtr &partitionData) override {
        return;
    }

    const indexlib::index::SummaryReaderPtr &GetSummaryReader() const override {
        return RET_EMPTY_SUMMARY_READER;
    }

    const indexlib::index::SourceReaderPtr &GetSourceReader() const override {
        return RET_EMPTY_SOURCE_READER;
    }

    const indexlib::index::AttributeReaderPtr &
    GetAttributeReader(const std::string &field) const override {
        return _attributeReaderPtr;
    }

    virtual const indexlib::index::PackAttributeReaderPtr &
    GetPackAttributeReader(const std::string &packAttrName) const override {
        return _packAttributeReaderPtr;
    }

    virtual const indexlib::index::PackAttributeReaderPtr &
    GetPackAttributeReader(packattrid_t packAttrId) const override {
        return _packAttributeReaderPtr;
    }

    virtual const indexlib::index::AttributeReaderContainerPtr &
    GetAttributeReaderContainer() const override {
        return _attributeReaderContainerPtr;
    }

    indexlib::index::InvertedIndexReaderPtr GetInvertedIndexReader() const override {
        return nullptr;
    }

    const indexlib::index::InvertedIndexReaderPtr &
    GetInvertedIndexReader(const std::string &indexName) const override {
        return RET_EMPTY_INDEX_READER;
    }

    const indexlib::index::InvertedIndexReaderPtr &
    GetInvertedIndexReader(indexid_t indexId) const override {
        return RET_EMPTY_INDEX_READER;
    }

    const indexlib::index::PrimaryKeyIndexReaderPtr &GetPrimaryKeyReader() const override {
        return RET_EMPTY_PRIMARY_KEY_READER;
    }

    indexlib::index_base::Version GetVersion() const override {
        return indexlib::index_base::Version();
    }
    indexlib::index_base::Version GetOnDiskVersion() const override {
        return indexlib::index_base::Version();
    }

    const indexlib::index::DeletionMapReaderPtr &GetDeletionMapReader() const override {
        return RET_EMPTY_DELETIONMAP_READER;
    }

    indexlib::index::PartitionInfoPtr GetPartitionInfo() const override {
        return nullptr;
    }

    indexlib::partition::IndexPartitionReaderPtr GetSubPartitionReader() const override {
        return nullptr;
    }

    bool GetSortedDocIdRanges(const indexlib::table::DimensionDescriptionVector &dimensions,
                              const indexlib::DocIdRange &rangeLimits,
                              indexlib::DocIdRangeVector &resultRanges) const override {
        return false;
    }

    const AccessCounterMap &GetIndexAccessCounters() const override {
        static AccessCounterMap map;
        return map;
    }

    const AccessCounterMap &GetAttributeAccessCounters() const override {
        static AccessCounterMap map;
        return map;
    }

private:
    indexlib::index::AttributeReaderPtr _attributeReaderPtr;
    indexlib::index::PackAttributeReaderPtr _packAttributeReaderPtr;
    indexlib::index::AttributeReaderContainerPtr _attributeReaderContainerPtr;
};

class Ha3TableInfoRTest : public TESTBASE {
public:
    void setUp() override;
    void tearDown() override;

public:
    template <typename T>
    bool loadFromFile(const std::string &filePath, T &json) {
        std::string jsonFile = GET_TEST_DATA_PATH() + filePath;
        std::string jsonStr;
        if (!fslib::util::FileUtil::readFile(jsonFile, jsonStr)) {
            return false;
        }
        try {
            autil::legacy::FromJsonString(json, jsonStr);
        } catch (const std::exception &e) { return false; }
        return true;
    }

    map<string, suez::TableDefConfig> fillTableConfigMap(const vector<string> &tableNames) {
        suez::TableDefConfig tableDefConfig;
        if (!loadFromFile("sql_resource/cluster.json", tableDefConfig)) {
            return {};
        }
        map<string, suez::TableDefConfig> ret;
        for (const auto &name : tableNames) {
            ret[name] = tableDefConfig;
        }
        return ret;
    }

    indexlib::partition::JoinRelationMap fillJoinRelations(const vector<string> &tableNames) {
        indexlib::partition::JoinRelationMap ret;
        for (size_t i = 0; i < tableNames.size(); ++i) {
            indexlib::partition::JoinField joinField;
            joinField.fieldName = "field" + to_string(i);
            joinField.joinTableName = tableNames[1 - i];
            ret[tableNames[i]] = {joinField};
        }
        return ret;
    }

    vector<shared_ptr<indexlibv2::config::ITabletSchema>>
    fillTableSchemas(const vector<string> &schemaPaths) {
        vector<shared_ptr<indexlibv2::config::ITabletSchema>> ret;
        for (size_t i = 0; i < schemaPaths.size(); ++i) {
            std::string jsonFile = GET_TEST_DATA_PATH() + schemaPaths[i];
            std::string jsonStr;
            if (!fslib::util::FileUtil::readFile(jsonFile, jsonStr)) {
                return {};
            }
            auto schemaPtr = indexlib::index_base::SchemaAdapter::LoadSchema(jsonStr);
            if (!schemaPtr) {
                return {};
            }
            ret.emplace_back(std::move(schemaPtr));
        }
        return ret;
    }

    vector<shared_ptr<indexlibv2::config::ITabletSchema>>
    fillResourceAndGetTableSchemas(const vector<string> &schemaPaths) {
        auto tableSchemas = fillTableSchemas(schemaPaths);
        if (tableSchemas.empty()) {
            return {};
        }
        vector<string> tableNames;
        for (auto &schema : tableSchemas) {
            tableNames.push_back(schema->GetTableName());
        }
        _ha3TableInfoR->_ha3ClusterDefR->_tableConfigMap = fillTableConfigMap(tableNames);
        if (_ha3TableInfoR->_ha3ClusterDefR->_tableConfigMap.empty()) {
            return {};
        }
        if (tableNames.size() == 2) {
            _ha3TableInfoR->_tableInfoR->_joinRelations = fillJoinRelations(tableNames);
        }
        return tableSchemas;
    }

private:
    unique_ptr<Ha3TableInfoR> _ha3TableInfoR;
    unique_ptr<Ha3ClusterDefR> _clusterDefR;
    unique_ptr<suez::turing::TableInfoR> _tableInfoR;
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(resource, Ha3TableInfoRTest);

void Ha3TableInfoRTest::setUp() {
    _clusterDefR.reset(new Ha3ClusterDefR());
    _tableInfoR.reset(new suez::turing::TableInfoR());
    _ha3TableInfoR.reset(new Ha3TableInfoR());
    _ha3TableInfoR->_tableInfoR = _tableInfoR.get();
    _ha3TableInfoR->_ha3ClusterDefR = _clusterDefR.get();
    _ha3TableInfoR->_zoneName = "zone1";
    _ha3TableInfoR->_partCount = 1;
}

void Ha3TableInfoRTest::tearDown() {}

TEST_F(Ha3TableInfoRTest, testInit) {
    // failed by get table schemas
    {
        navi::ResourceInitContext ctx;
        navi::NaviLoggerProvider provider("ERROR");
        std::vector<std::shared_ptr<indexlibv2::config::ITabletSchema>> tableSchemas;
        ASSERT_EQ(navi::EC_ABORT, _ha3TableInfoR->init(ctx));
        ASSERT_NO_FATAL_FAILURE(
            NaviLoggerProviderTestUtil::checkTraceCount(1, "empty index app map", provider));
    }

    // prepare table schema for init test
    auto indexAppPtr = make_shared<indexlib::partition::IndexApplication>();
    _tableInfoR->_id2IndexAppMap.insert({0, indexAppPtr});
    auto &tableReaderContainer = indexAppPtr->mReaderContainer;
    indexlib::partition::IndexPartitionReaderPtr indexPartitionReaderPtr
        = make_shared<MockIndexPartitionReader>();
    vector<string> schemaPaths = {"sql_resource/tablet_schema.json"};
    indexPartitionReaderPtr->mTabletSchema = fillTableSchemas(schemaPaths).back();
    tableReaderContainer.mReaders.push_back(indexPartitionReaderPtr);

    // failed by generate meta
    {
        navi::NaviLoggerProvider provider("ERROR");
        navi::ResourceInitContext ctx;
        ASSERT_EQ(navi::EC_ABORT, _ha3TableInfoR->init(ctx));
        string errorTrace = "fillTableDef from cluster def failed, zoneName ["
                            + _ha3TableInfoR->_zoneName + "] tableName ["
                            + indexPartitionReaderPtr->mTabletSchema->GetTableName() + "]";
        ASSERT_NO_FATAL_FAILURE(
            NaviLoggerProviderTestUtil::checkTraceCount(1, errorTrace, provider));
    }
    // init success
    {
        vector<string> tableNames = {indexPartitionReaderPtr->mTabletSchema->GetTableName()};
        _clusterDefR->_tableConfigMap = fillTableConfigMap(tableNames);
        navi::ResourceInitContext ctx;
        ASSERT_EQ(navi::EC_NONE, _ha3TableInfoR->init(ctx));
    }
}

TEST_F(Ha3TableInfoRTest, testAddJoinInfo) {
    indexlib::partition::JoinRelationMap joinRelations = {{"t1", {{"f1", "aux1", false, false}}}};
    CatalogDefs catalogDefs;
    iquan::LocationDef locationDef;
    locationDef.sign.nodeName = locationDef.sign.nodeType = "node";
    locationDef.sign.partitionCnt = 2;
    locationDef.tableIdentities.push_back({"db_a", "t1"});
    TableModel tableModel;
    catalogDefs.catalog("default").database("db_a").addTable(tableModel);
    catalogDefs.catalog("default")
        .location(locationDef.sign)
        .addTableIdentity({"db_a", tableModel.tableName()});

    auto &defaultLocationDef = catalogDefs.catalog("default").location(locationDef.sign);

    {
        ASSERT_TRUE(Ha3TableInfoR::addJoinInfo(
            "default", "db_a", locationDef.sign, "not_exist", joinRelations, catalogDefs));
        ASSERT_EQ(0, defaultLocationDef.joinInfos.size());
    }
    {
        ASSERT_TRUE(Ha3TableInfoR::addJoinInfo(
            "default", "db_a", locationDef.sign, "t1", joinRelations, catalogDefs));
        ASSERT_EQ(1, defaultLocationDef.joinInfos.size());
        ASSERT_EQ("t1", defaultLocationDef.joinInfos[0].mainTable.tableName);
        ASSERT_EQ("aux1", defaultLocationDef.joinInfos[0].auxTable.tableName);
        ASSERT_EQ("db_a", defaultLocationDef.joinInfos[0].mainTable.dbName);
        ASSERT_EQ("db_a", defaultLocationDef.joinInfos[0].auxTable.dbName);
        ASSERT_EQ("f1", defaultLocationDef.joinInfos[0].joinField);
    }
}

TEST_F(Ha3TableInfoRTest, testAddInnerDocId) {
    TableDef tableDef;
    Ha3TableInfoR::addInnerDocId(tableDef);
    ASSERT_EQ(1, tableDef.fields.size());
    std::string expectStr = R"json({
"field_name":
  "__inner_docid",
"field_type":
  {
  "extend_infos":
    {
    },
  "type":
    "INTEGER"
  },
"index_name":
  "__inner_docid",
"index_type":
  "PRIMARYKEY64",
"is_attribute":
  false
})json";
    ASSERT_EQ(expectStr, autil::legacy::ToJsonString(tableDef.fields[0]));
}

TEST_F(Ha3TableInfoRTest, testFillSummaryTable) {
    DatabaseDef database;
    ASSERT_TRUE(loadFromFile("sql_test/table_model_json/simple_summary_table.json", database));

    auto &tableModels = database.tables;
    ASSERT_EQ(1, tableModels.size());
    auto &tableModel = tableModels[0];
    auto &tableDef = tableModel.tableContent;
    ASSERT_EQ("simple", tableModel.tableName());
    ASSERT_EQ(11, tableDef.summaryFields.size());
    std::vector<FieldDef> summaryFields = tableDef.summaryFields;

    TableModel summaryTable;
    ASSERT_TRUE(Ha3TableInfoR::fillSummaryTable(tableModel, summaryTable));

    const auto &summaryTableDef = summaryTable.tableContent;
    ASSERT_EQ("simple_summary_", summaryTableDef.tableName);
    ASSERT_EQ(IQUAN_TABLE_TYPE_SUMMARY, summaryTableDef.tableType);
    ASSERT_EQ(autil::legacy::ToJsonString(summaryFields),
              autil::legacy::ToJsonString(summaryTableDef.fields));
    ASSERT_TRUE(tableModel.tableContent.summaryFields.empty());
    ASSERT_TRUE(summaryTableDef.summaryFields.empty());

    ASSERT_TRUE(tableDef.summaryFields.empty());
}

TEST_F(Ha3TableInfoRTest, testFillSummaryTableFailed) {
    DatabaseDef database;
    ASSERT_TRUE(loadFromFile("sql_test/table_model_json/simple_khronos_table.json", database));
    auto &tableModels = database.tables;
    ASSERT_EQ(1, tableModels.size());
    auto &tableModel = tableModels[0];
    ASSERT_EQ("simple", tableModel.tableContent.tableName);
    ASSERT_EQ(9, tableModel.tableContent.fields.size());
    TableModel summaryTable;
    ASSERT_FALSE(Ha3TableInfoR::fillSummaryTable(tableModel, summaryTable));
}

TEST_F(Ha3TableInfoRTest, testGenerateMeta) {
    vector<string> schemaPaths = {"sql_resource/tablet_schema.json", "sql_resource/kv_schema.json"};
    auto tableSchemas = fillResourceAndGetTableSchemas(schemaPaths);
    ASSERT_EQ(2, tableSchemas.size());
    auto &clusterDefR = _ha3TableInfoR->_ha3ClusterDefR;
    auto &tableInfoR = _ha3TableInfoR->_tableInfoR;
    // generate meta with no failure and no summary&khronos table
    { ASSERT_TRUE(_ha3TableInfoR->generateMeta(tableSchemas)); }
    // fill table def failed
    {
        auto tableConfigMapBk = clusterDefR->_tableConfigMap;
        clusterDefR->_tableConfigMap.clear();
        navi::NaviLoggerProvider provider("ERROR");
        ASSERT_FALSE(_ha3TableInfoR->generateMeta(tableSchemas));
        ASSERT_NO_FATAL_FAILURE(NaviLoggerProviderTestUtil::checkTraceCount(
            1,
            "fillTableDef from cluster def failed, zoneName [zone1] tableName [simple4]",
            provider));
        clusterDefR->_tableConfigMap = tableConfigMapBk;
    }
    // add join info failed
    {
        auto joinRelationsBk = tableInfoR->_joinRelations;
        auto &joinFieldsVec = tableInfoR->_joinRelations["simple4"];
        joinFieldsVec.push_back(joinFieldsVec.back());
        navi::NaviLoggerProvider provider("ERROR");
        ASSERT_FALSE(_ha3TableInfoR->generateMeta(tableSchemas));
        ASSERT_NO_FATAL_FAILURE(NaviLoggerProviderTestUtil::checkTraceCount(
            1,
            "add join info failed, main table id [zone1_simple4] aux table id [zone1_kv2] join "
            "field [field0]",
            provider));
        tableInfoR->_joinRelations = joinRelationsBk;
    }
    // add table and table identity failed
    {
        vector<string> schemaPaths = {"sql_resource/kv_schema.json", "sql_resource/kv_schema.json"};
        auto tableSchemas = fillTableSchemas(schemaPaths);
        ASSERT_EQ(2, tableSchemas.size());
        navi::NaviLoggerProvider provider("ERROR");
        ASSERT_FALSE(_ha3TableInfoR->generateMeta(tableSchemas));
        ASSERT_NO_FATAL_FAILURE(
            NaviLoggerProviderTestUtil::checkTraceCount(1, "add table [kv2] failed", provider));
    }
}

TEST_F(Ha3TableInfoRTest, testGenerateMeta_fillSummaryTable) {
    vector<string> schemaPaths = {"sql_resource/tablet_schema.json"};
    auto tableSchemas = fillResourceAndGetTableSchemas(schemaPaths);
    ASSERT_EQ(1, tableSchemas.size());
    // fill summary success
    { ASSERT_TRUE(_ha3TableInfoR->generateMeta(tableSchemas)); }
    // fill summary failed
    {
        vector<string> schemaPaths
            = {"sql_resource/tablet_schema.json", "sql_resource/tablet_schema.json"};
        auto tableSchemas = fillTableSchemas(schemaPaths);
        ASSERT_EQ(2, tableSchemas.size());
        navi::NaviLoggerProvider provider("ERROR");
        ASSERT_FALSE(_ha3TableInfoR->generateMeta(tableSchemas));
        ASSERT_NO_FATAL_FAILURE(NaviLoggerProviderTestUtil::checkTraceCount(
            1, "add table [simple4_summary_] failed", provider));
    }
}

TEST_F(Ha3TableInfoRTest, testGetMeta) {
    vector<string> schemaPaths = {"sql_resource/kv_schema.json"};
    auto tableSchemas = fillResourceAndGetTableSchemas(schemaPaths);
    ASSERT_EQ(1, tableSchemas.size());
    ASSERT_TRUE(_ha3TableInfoR->generateMeta(tableSchemas));
    string expectStr
        = R"Json({"catalogs":[{"catalog_name":"default","databases":[{"database_name":"system","tables":[],"layer_tables":[],"functions":[]},{"database_name":"zone1","tables":[{"table_content_type":"","table_content":{"table_name":"kv2","table_type":"kv","fields":[{"field_name":"cat_id","index_type":"PRIMARY_KEY","index_name":"cat_id","field_type":{"type":"INTEGER","extend_infos":{}},"is_attribute":true},{"field_name":"company_id","field_type":{"type":"INTEGER","extend_infos":{}},"is_attribute":true},{"field_name":"category_name","field_type":{"type":"STRING","extend_infos":{}},"is_attribute":true},{"field_name":"category_description","field_type":{"type":"STRING","extend_infos":{}},"is_attribute":true},{"field_name":"auction_count","field_type":{"type":"INTEGER","extend_infos":{}},"is_attribute":true}],"summary_fields":[],"sub_tables":[],"distribution":{"partition_cnt":1,"hash_fields":["nid"],"hash_function":"HASH","hash_params":{}},"sort_desc":[],"properties":{},"indexes":{}}}],"layer_tables":[],"functions":[]}],"locations":[{"partition_cnt":1,"node_name":"zone1","node_type":"searcher","tables":[{"database_name":"zone1","table_name":"kv2"}],"equivalent_hash_fields":[],"join_info":[]}]}]})Json";
    string meta = "";
    ASSERT_TRUE(_ha3TableInfoR->getMeta(meta));
    ASSERT_EQ(expectStr, meta);
}
} // namespace sql
