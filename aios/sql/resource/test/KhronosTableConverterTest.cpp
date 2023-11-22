#include "sql/resource/KhronosTableConverter.h"

#include "autil/legacy/legacy_jsonizable.h"
#include "fslib/util/FileUtil.h"
#include "iquan/jni/IquanEnv.h"
#include "iquan/jni/JvmType.h"
#include "navi/tester/KernelTesterBuilder.h"
#include "sql/common/common.h"
#include "sql/resource/SqlConfig.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace iquan;
using namespace autil::legacy;

namespace sql {
static std::once_flag iquanFlag;

class KhronosTableConverterTest : public TESTBASE {
public:
    void setUp() override;
    void tearDown() override;

private:
    template <typename T>
    void loadFromFile(const std::string &fileName, T &json) {
        std::string testdataPath = GET_TEST_DATA_PATH() + "sql_test/table_model_json/";
        std::string jsonFile = testdataPath + fileName;
        std::string jsonStr;
        ASSERT_TRUE(fslib::util::FileUtil::readFile(jsonFile, jsonStr));
        try {
            autil::legacy::FromJsonString(json, jsonStr);
        } catch (const std::exception &e) { ASSERT_TRUE(false) << e.what(); }
    }

    static void checkFields(const std::vector<std::string> &fieldNames,
                            const std::vector<std::string> &fieldTypeNames,
                            const std::vector<std::string> &indexTypes,
                            const std::vector<std::string> &indexNames,
                            std::vector<FieldDef> &actuals);
    void getTableModel(const std::string &catalogFilePath, TableModel &tableModel);

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(resource, KhronosTableConverterTest);

void KhronosTableConverterTest::setUp() {}

void KhronosTableConverterTest::tearDown() {}

void KhronosTableConverterTest::checkFields(const std::vector<std::string> &fieldNames,
                                            const std::vector<std::string> &fieldTypeNames,
                                            const std::vector<std::string> &indexTypes,
                                            const std::vector<std::string> &indexNames,
                                            std::vector<FieldDef> &actuals) {
    ASSERT_EQ(fieldNames.size(), actuals.size());
    ASSERT_EQ(fieldTypeNames.size(), actuals.size());
    ASSERT_EQ(indexTypes.size(), actuals.size());
    ASSERT_EQ(indexNames.size(), actuals.size());
    for (size_t i = 0; i < actuals.size(); i++) {
        ASSERT_EQ(fieldNames[i], actuals[i].fieldName);
        ASSERT_EQ(fieldTypeNames[i], actuals[i].fieldType.typeName);
        ASSERT_EQ(indexTypes[i], actuals[i].indexType);
        ASSERT_EQ(indexNames[i], actuals[i].indexName);
        if (fieldTypeNames[i] == "MAP") {
            ASSERT_EQ("string", AnyCast<string>(actuals[i].fieldType.keyType["type"]));
            ASSERT_EQ("string", AnyCast<string>(actuals[i].fieldType.valueType["type"]));
        }
    }
}

void KhronosTableConverterTest::getTableModel(const std::string &catalogFilePath,
                                              TableModel &tableModel) {
    CatalogDefs srcCatalogDefs;
    ASSERT_NO_FATAL_FAILURE(loadFromFile(catalogFilePath, srcCatalogDefs));
    ASSERT_EQ(1, srcCatalogDefs.catalogs.size());
    auto &srcCatalogDef = srcCatalogDefs.catalogs[0];
    ASSERT_EQ(1, srcCatalogDef.databases.size());
    auto &srcDatabase = srcCatalogDef.databases[0];
    ASSERT_EQ(1, srcDatabase.tables.size());
    auto &srcTableModels = srcDatabase.tables;
    ASSERT_EQ(1, srcTableModels.size());
    tableModel = srcTableModels[0];
}

TEST_F(KhronosTableConverterTest, testFillKhronosTableV3) {
    TableModel srcTableModel;
    getTableModel("simple_khronos_catalog.json", srcTableModel);
    string dbName = "db1";
    LocationSign locationSign = {1, "qrs", "qrs"};
    CatalogDefs catalogDefs;
    ASSERT_TRUE(KhronosTableConverter::fillKhronosLogicTableV3(
        srcTableModel, dbName, locationSign, catalogDefs));
    ASSERT_EQ(1, catalogDefs.catalogs.size());
    const auto &catalogDef = catalogDefs.catalogs[0];
    ASSERT_EQ(2, catalogDef.databases.size());
    const auto &database = catalogDef.databases[1];
    ASSERT_EQ(dbName, database.dbName);
    ASSERT_EQ(1, database.tables.size());
    ASSERT_EQ(1, catalogDef.locations.size());
    const auto &location = catalogDef.locations[0];
    ASSERT_EQ(
        R"json({"partition_cnt":1,"node_name":"qrs","node_type":"qrs","tables":[{"database_name":"db1","table_name":"simple"}],"equivalent_hash_fields":[],"join_info":[]})json",
        autil::legacy::FastToJsonString(location));
    const auto &tableModel = database.tables[0];
    ASSERT_EQ(IQUAN_TABLE_TYPE_KHRONOS_SERIES_DATA_V3, tableModel.tableType());
}

TEST_F(KhronosTableConverterTest, testFillKhronosTableV3ExternTable) {
    TableModel srcTableModel;
    getTableModel("simple_khronos_catalog.json", srcTableModel);
    srcTableModel.tableContent.tableType = SCAN_EXTERNAL_TABLE_TYPE;
    string dbName = "db1";
    LocationSign locationSign = {1, "qrs", "qrs"};
    CatalogDefs catalogDefs;
    ASSERT_TRUE(KhronosTableConverter::fillKhronosLogicTableV3(
        srcTableModel, dbName, locationSign, catalogDefs));
    ASSERT_EQ(1, catalogDefs.catalogs.size());
    const auto &catalogDef = catalogDefs.catalogs[0];
    ASSERT_EQ(2, catalogDef.databases.size());
    const auto &database = catalogDef.databases[1];
    ASSERT_EQ(dbName, database.dbName);
    ASSERT_EQ(1, database.tables.size());
    ASSERT_EQ(1, catalogDef.locations.size());
    const auto &location = catalogDef.locations[0];
    ASSERT_EQ(
        R"json({"partition_cnt":1,"node_name":"qrs","node_type":"qrs","tables":[{"database_name":"db1","table_name":"simple"}],"equivalent_hash_fields":[],"join_info":[]})json",
        autil::legacy::FastToJsonString(location));
    const auto &tableModel = database.tables[0];
    ASSERT_EQ(SCAN_EXTERNAL_TABLE_TYPE, tableModel.tableType());
}

TEST_F(KhronosTableConverterTest, testFillKhronosTable) {
    TableModel tableModel;
    getTableModel("simple_khronos_catalog.json", tableModel);
    CatalogDefs catalogDefs;
    LocationSign locationSign = {1, "qrs", "qrs"};
    string dbName = "database";
    ASSERT_TRUE(
        KhronosTableConverter::fillKhronosTable(tableModel, dbName, locationSign, catalogDefs));
    ASSERT_EQ(2, catalogDefs.catalogs.size());
    auto &v3CatalogDef = catalogDefs.catalogs[1];
    auto &v2CatalogDef = catalogDefs.catalogs[0];
    ASSERT_EQ(2, v3CatalogDef.databases.size());
    ASSERT_EQ(2, v2CatalogDef.databases.size());
    auto &v3Database = v3CatalogDef.databases[1];
    auto &v2Database = v2CatalogDef.databases[1];
    ASSERT_EQ(2, v3Database.tables.size());
    ASSERT_EQ(2, v2Database.tables.size());
    auto &v3Tables = v3Database.tables;
    auto &v2Tables = v2Database.tables;
    TableModel metaTable = v2Tables[0];
    TableModel seriesTable = v2Tables[1];
    TableModel metaTableV3 = v3Tables[0];
    TableModel seriesTableV3 = v3Tables[1];

    TableDef metaTableDef = metaTable.tableContent;
    TableDef metaTableDefV3 = metaTableV3.tableContent;
    TableDef seriesTableV3Def = seriesTableV3.tableContent;
    TableDef seriesTableDef = seriesTable.tableContent;
    {
        ASSERT_EQ("khronos_meta", metaTable.tableName());
        ASSERT_EQ("khronos_meta", metaTable.tableType());

        // check tableContent
        ASSERT_EQ("khronos_meta", metaTableDef.tableName);
        ASSERT_EQ(5, metaTableDef.fields.size());
        ASSERT_EQ("khronos_meta", metaTableDef.tableType);

        // check fields
        std::vector<std::string> fieldNames {KHRONOS_FIELD_NAME_TS,
                                             KHRONOS_FIELD_NAME_METRIC,
                                             KHRONOS_FIELD_NAME_TAGK,
                                             KHRONOS_FIELD_NAME_TAGV,
                                             KHRONOS_FIELD_NAME_FIELD_NAME};
        std::vector<std::string> fieldTypeNames {"INT32", "STRING", "STRING", "STRING", "STRING"};
        std::vector<std::string> indexTypes {KHRONOS_INDEX_TYPE_TIMESTAMP,
                                             KHRONOS_INDEX_TYPE_METRIC,
                                             KHRONOS_INDEX_TYPE_TAG_KEY,
                                             KHRONOS_INDEX_TYPE_TAG_VALUE,
                                             KHRONOS_INDEX_TYPE_FIELD_NAME};
        std::vector<std::string> indexNames = fieldNames;
        checkFields(fieldNames, fieldTypeNames, indexTypes, indexNames, metaTableDef.fields);

        // check distribution
        ASSERT_EQ(1, metaTableDef.distribution.partitionCnt);
        ASSERT_EQ("serieskey", metaTableDef.distribution.hashFields[0]);
        ASSERT_EQ("HASH", metaTableDef.distribution.hashFunction);

        ASSERT_EQ(0, metaTableDef.properties.size());
    }
    {
        ASSERT_EQ(std::string("simple") + KHRONOS_META_TABLE_NAME_V3_SUFFIX,
                  metaTableV3.tableName());
        ASSERT_EQ("khronos_meta", metaTableV3.tableType());

        // check tableContent
        ASSERT_EQ("simple_meta", metaTableDefV3.tableName);
        ASSERT_EQ(5, metaTableDefV3.fields.size());
        ASSERT_EQ("khronos_meta", metaTableDefV3.tableType);

        // check fields
        std::vector<std::string> fieldNames {KHRONOS_FIELD_NAME_TS,
                                             KHRONOS_FIELD_NAME_METRIC,
                                             KHRONOS_FIELD_NAME_TAGK,
                                             KHRONOS_FIELD_NAME_TAGV,
                                             KHRONOS_FIELD_NAME_FIELD_NAME};
        std::vector<std::string> fieldTypeNames {"INT32", "STRING", "STRING", "STRING", "STRING"};
        std::vector<std::string> indexTypes {KHRONOS_INDEX_TYPE_TIMESTAMP,
                                             KHRONOS_INDEX_TYPE_METRIC,
                                             KHRONOS_INDEX_TYPE_TAG_KEY,
                                             KHRONOS_INDEX_TYPE_TAG_VALUE,
                                             KHRONOS_INDEX_TYPE_FIELD_NAME};
        std::vector<std::string> indexNames = fieldNames;
        checkFields(fieldNames, fieldTypeNames, indexTypes, indexNames, metaTableDefV3.fields);

        // check distribution
        ASSERT_EQ(1, metaTableDefV3.distribution.partitionCnt);
        ASSERT_EQ("serieskey", metaTableDefV3.distribution.hashFields[0]);
        ASSERT_EQ("HASH", metaTableDefV3.distribution.hashFunction);

        ASSERT_EQ(0, metaTableDefV3.properties.size());
    }
    {
        ASSERT_EQ("simple", seriesTableV3.tableName());
        ASSERT_EQ(IQUAN_TABLE_TYPE_KHRONOS_SERIES_DATA_V3, seriesTableV3.tableType());

        // check tableContent
        ASSERT_EQ("simple", seriesTableV3Def.tableName);
        ASSERT_EQ(5, seriesTableV3Def.fields.size());
        ASSERT_EQ(IQUAN_TABLE_TYPE_KHRONOS_SERIES_DATA_V3, seriesTableV3Def.tableType);

        // check fields
        std::vector<std::string> fieldNames {KHRONOS_FIELD_NAME_METRIC,
                                             KHRONOS_FIELD_NAME_TAGS,
                                             KHRONOS_FIELD_NAME_WATERMARK,
                                             KHRONOS_FIELD_NAME_SERIES_VALUE,
                                             KHRONOS_FIELD_NAME_SERIES_STRING};
        std::vector<std::string> fieldTypeNames {"STRING", "MAP", "INT64", "MAP", "MAP"};
        std::vector<std::string> indexTypes {KHRONOS_INDEX_TYPE_METRIC,
                                             KHRONOS_INDEX_TYPE_TAGS,
                                             KHRONOS_INDEX_TYPE_WATERMARK,
                                             KHRONOS_INDEX_TYPE_SERIES_VALUE,
                                             KHRONOS_INDEX_TYPE_SERIES_STRING};
        std::vector<std::string> indexNames = fieldNames;
        checkFields(fieldNames, fieldTypeNames, indexTypes, indexNames, seriesTableV3Def.fields);

        // check distribution
        ASSERT_EQ(1, seriesTableV3Def.distribution.partitionCnt);
        ASSERT_EQ(string("serieskey"), seriesTableV3Def.distribution.hashFields[0]);
        ASSERT_EQ(string("HASH"), seriesTableV3Def.distribution.hashFunction);

        ASSERT_EQ(0, seriesTableV3Def.properties.size());
    }
    {
        ASSERT_EQ(KHRONOS_DATA_TABLE_NAME_SERIES, seriesTable.tableName());
        ASSERT_EQ(IQUAN_TABLE_TYPE_KHRONOS_SERIES_DATA, seriesTable.tableType());

        // check tableContent
        ASSERT_EQ(KHRONOS_DATA_TABLE_NAME_SERIES, seriesTableDef.tableName);
        ASSERT_EQ(7, seriesTableDef.fields.size());
        ASSERT_EQ(IQUAN_TABLE_TYPE_KHRONOS_SERIES_DATA, seriesTableDef.tableType);

        // check fields
        std::vector<std::string> fieldNames {"min_value",
                                             "max_value",
                                             "avg_value",
                                             KHRONOS_FIELD_NAME_TAGS,
                                             KHRONOS_FIELD_NAME_WATERMARK,
                                             KHRONOS_FIELD_NAME_SERIES_VALUE,
                                             KHRONOS_FIELD_NAME_SERIES_STRING};
        std::vector<std::string> fieldTypeNames {
            "STRING", "STRING", "STRING", "MAP", "INT64", "MAP", "MAP"};
        std::vector<std::string> indexTypes {KHRONOS_INDEX_TYPE_SERIES,
                                             KHRONOS_INDEX_TYPE_SERIES,
                                             KHRONOS_INDEX_TYPE_SERIES,
                                             KHRONOS_INDEX_TYPE_TAGS,
                                             KHRONOS_INDEX_TYPE_WATERMARK,
                                             KHRONOS_INDEX_TYPE_SERIES_VALUE,
                                             KHRONOS_INDEX_TYPE_SERIES_STRING};
        std::vector<std::string> indexNames = fieldNames;
        checkFields(fieldNames, fieldTypeNames, indexTypes, indexNames, seriesTableDef.fields);

        // check distribution
        ASSERT_EQ(1, seriesTableDef.distribution.partitionCnt);
        ASSERT_EQ(string("serieskey"), seriesTableDef.distribution.hashFields[0]);
        ASSERT_EQ(string("HASH"), seriesTableDef.distribution.hashFunction);

        ASSERT_EQ(0, seriesTableDef.properties.size());
    }
}

TEST_F(KhronosTableConverterTest, testNewKhronosCatalogInfo) {
    TableModel tableModel;
    getTableModel("simple_new_khronos_catalog.json", tableModel);
    CatalogDefs catalogDefs;
    LocationSign locationSign = {1, "qrs", "qrs"};
    string dbName = "database";
    ASSERT_TRUE(
        KhronosTableConverter::fillKhronosTable(tableModel, dbName, locationSign, catalogDefs));
    ASSERT_EQ(2, catalogDefs.catalogs.size());
    auto &v3CatalogDef = catalogDefs.catalogs[1];
    auto &v2CatalogDef = catalogDefs.catalogs[0];
    ASSERT_EQ(2, v3CatalogDef.databases.size());
    ASSERT_EQ(2, v2CatalogDef.databases.size());
    auto &v3Database = v3CatalogDef.databases[1];
    auto &v2Database = v2CatalogDef.databases[1];
    ASSERT_EQ(2, v3Database.tables.size());
    ASSERT_EQ(2, v2Database.tables.size());
    auto &v3Tables = v3Database.tables;
    auto &v2Tables = v2Database.tables;
    TableModel metaTable = v2Tables[0];
    TableModel seriesTable = v2Tables[1];
    TableModel metaTableV3 = v3Tables[0];
    TableModel seriesTableV3 = v3Tables[1];

    TableDef metaTableDef = metaTable.tableContent;
    TableDef metaTableDefV3 = metaTableV3.tableContent;
    TableDef seriesTableV3Def = seriesTableV3.tableContent;
    TableDef seriesTableDef = seriesTable.tableContent;
    {
        ASSERT_EQ("khronos_meta", metaTable.tableName());
        ASSERT_EQ("khronos_meta", metaTable.tableType());

        // check tableContent
        ASSERT_EQ("khronos_meta", metaTableDef.tableName);
        ASSERT_EQ(5, metaTableDef.fields.size());
        ASSERT_EQ("khronos_meta", metaTableDef.tableType);

        // check fields
        std::vector<std::string> fieldNames {KHRONOS_FIELD_NAME_TS,
                                             KHRONOS_FIELD_NAME_METRIC,
                                             KHRONOS_FIELD_NAME_TAGK,
                                             KHRONOS_FIELD_NAME_TAGV,
                                             KHRONOS_FIELD_NAME_FIELD_NAME};
        std::vector<std::string> fieldTypeNames {"INT32", "STRING", "STRING", "STRING", "STRING"};
        std::vector<std::string> indexTypes {KHRONOS_INDEX_TYPE_TIMESTAMP,
                                             KHRONOS_INDEX_TYPE_METRIC,
                                             KHRONOS_INDEX_TYPE_TAG_KEY,
                                             KHRONOS_INDEX_TYPE_TAG_VALUE,
                                             KHRONOS_INDEX_TYPE_FIELD_NAME};
        std::vector<std::string> indexNames = fieldNames;
        checkFields(fieldNames, fieldTypeNames, indexTypes, indexNames, metaTableDef.fields);

        // check distribution
        ASSERT_EQ(1, metaTableDef.distribution.partitionCnt);
        ASSERT_EQ("serieskey", metaTableDef.distribution.hashFields[0]);
        ASSERT_EQ("HASH", metaTableDef.distribution.hashFunction);
        // ASSERT_TRUE(metaTableDef.joinInfo.empty());
        ASSERT_EQ(0, metaTableDef.properties.size());
    }
    {
        ASSERT_EQ(std::string("simple") + KHRONOS_META_TABLE_NAME_V3_SUFFIX,
                  metaTableV3.tableName());
        ASSERT_EQ("khronos_meta", metaTableV3.tableType());

        // check tableContent
        ASSERT_EQ("simple_meta", metaTableDefV3.tableName);
        ASSERT_EQ(5, metaTableDef.fields.size());
        ASSERT_EQ("khronos_meta", metaTableDefV3.tableType);

        // check fields
        std::vector<std::string> fieldNames {KHRONOS_FIELD_NAME_TS,
                                             KHRONOS_FIELD_NAME_METRIC,
                                             KHRONOS_FIELD_NAME_TAGK,
                                             KHRONOS_FIELD_NAME_TAGV,
                                             KHRONOS_FIELD_NAME_FIELD_NAME};
        std::vector<std::string> fieldTypeNames {"INT32", "STRING", "STRING", "STRING", "STRING"};
        std::vector<std::string> indexTypes {KHRONOS_INDEX_TYPE_TIMESTAMP,
                                             KHRONOS_INDEX_TYPE_METRIC,
                                             KHRONOS_INDEX_TYPE_TAG_KEY,
                                             KHRONOS_INDEX_TYPE_TAG_VALUE,
                                             KHRONOS_INDEX_TYPE_FIELD_NAME};
        std::vector<std::string> indexNames = fieldNames;
        checkFields(fieldNames, fieldTypeNames, indexTypes, indexNames, metaTableDefV3.fields);

        // check distribution
        ASSERT_EQ(1, metaTableDefV3.distribution.partitionCnt);
        ASSERT_EQ("serieskey", metaTableDefV3.distribution.hashFields[0]);
        ASSERT_EQ("HASH", metaTableDefV3.distribution.hashFunction);

        // ASSERT_TRUE(metaTableDefV3.joinInfo.empty());

        ASSERT_EQ(0, metaTableDefV3.properties.size());
    }
    {
        ASSERT_EQ("simple", seriesTableV3.tableName());
        ASSERT_EQ(IQUAN_TABLE_TYPE_KHRONOS_SERIES_DATA_V3, seriesTableV3.tableType());

        // check tableContent
        ASSERT_EQ("simple", seriesTableV3Def.tableName);
        ASSERT_EQ(5, seriesTableV3Def.fields.size());
        ASSERT_EQ(IQUAN_TABLE_TYPE_KHRONOS_SERIES_DATA_V3, seriesTableV3Def.tableType);

        // check fields
        std::vector<std::string> fieldNames {KHRONOS_FIELD_NAME_METRIC,
                                             KHRONOS_FIELD_NAME_TAGS,
                                             KHRONOS_FIELD_NAME_WATERMARK,
                                             KHRONOS_FIELD_NAME_SERIES_VALUE,
                                             KHRONOS_FIELD_NAME_SERIES_STRING};
        std::vector<std::string> fieldTypeNames {"STRING", "MAP", "INT64", "MAP", "MAP"};
        std::vector<std::string> indexTypes {KHRONOS_INDEX_TYPE_METRIC,
                                             KHRONOS_INDEX_TYPE_TAGS,
                                             KHRONOS_INDEX_TYPE_WATERMARK,
                                             KHRONOS_INDEX_TYPE_SERIES_VALUE,
                                             KHRONOS_INDEX_TYPE_SERIES_STRING};
        std::vector<std::string> indexNames = fieldNames;
        checkFields(fieldNames, fieldTypeNames, indexTypes, indexNames, seriesTableV3Def.fields);

        // check distribution
        ASSERT_EQ(1, seriesTableV3Def.distribution.partitionCnt);
        ASSERT_EQ(string("serieskey"), seriesTableV3Def.distribution.hashFields[0]);
        ASSERT_EQ(string("HASH"), seriesTableV3Def.distribution.hashFunction);

        // ASSERT_TRUE(seriesTableV3Def.joinInfo.empty());

        ASSERT_EQ(0, seriesTableV3Def.properties.size());
    }
    {
        ASSERT_EQ(KHRONOS_DATA_TABLE_NAME_SERIES, seriesTable.tableName());
        ASSERT_EQ(IQUAN_TABLE_TYPE_KHRONOS_SERIES_DATA, seriesTable.tableType());

        // check tableContent
        ASSERT_EQ(KHRONOS_DATA_TABLE_NAME_SERIES, seriesTableDef.tableName);
        ASSERT_EQ(9, seriesTableDef.fields.size());
        ASSERT_EQ(IQUAN_TABLE_TYPE_KHRONOS_SERIES_DATA, seriesTableDef.tableType);

        // check fields
        std::vector<std::string> fieldNames {"avg_value",
                                             "sum_value",
                                             "max_value",
                                             "min_value",
                                             "count_value",
                                             KHRONOS_FIELD_NAME_TAGS,
                                             KHRONOS_FIELD_NAME_WATERMARK,
                                             KHRONOS_FIELD_NAME_SERIES_VALUE,
                                             KHRONOS_FIELD_NAME_SERIES_STRING};
        std::vector<std::string> fieldTypeNames {
            "STRING", "STRING", "STRING", "STRING", "STRING", "MAP", "INT64", "MAP", "MAP"};
        std::vector<std::string> indexTypes {KHRONOS_INDEX_TYPE_SERIES,
                                             KHRONOS_INDEX_TYPE_SERIES,
                                             KHRONOS_INDEX_TYPE_SERIES,
                                             KHRONOS_INDEX_TYPE_SERIES,
                                             KHRONOS_INDEX_TYPE_SERIES,
                                             KHRONOS_INDEX_TYPE_TAGS,
                                             KHRONOS_INDEX_TYPE_WATERMARK,
                                             KHRONOS_INDEX_TYPE_SERIES_VALUE,
                                             KHRONOS_INDEX_TYPE_SERIES_STRING};
        std::vector<std::string> indexNames = fieldNames;
        checkFields(fieldNames, fieldTypeNames, indexTypes, indexNames, seriesTableDef.fields);

        // check distribution
        ASSERT_EQ(1, seriesTableDef.distribution.partitionCnt);
        ASSERT_EQ(string("serieskey"), seriesTableDef.distribution.hashFields[0]);
        ASSERT_EQ(string("HASH"), seriesTableDef.distribution.hashFunction);

        // ASSERT_TRUE(seriesTableDef.joinInfo.empty());

        ASSERT_EQ(0, seriesTableDef.properties.size());
    }
}
} // namespace sql
