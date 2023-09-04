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
#include "iquan/common/Common.h"
#include "iquan/common/Status.h"
#include "iquan/common/catalog/CatalogInfo.h"
#include "iquan/common/catalog/LayerTableDef.h"
#include "iquan/common/catalog/TableDef.h"
#include "iquan/common/catalog/TableModel.h"
#include "iquan/jni/IquanEnv.h"
#include "iquan/jni/JvmType.h"
#include "navi/tester/NaviResourceHelper.h"
#include "sql/resource/SqlConfig.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace iquan;
using namespace autil::legacy;
using namespace autil;

namespace sql {
static std::once_flag iquanFlag;

class IquanRTest : public TESTBASE {
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
            autil::legacy::FastFromJsonString(json, jsonStr);
        } catch (const std::exception &e) { ASSERT_TRUE(false) << e.what(); }
    }

private:
    AUTIL_LOG_DECLARE();
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
    IquanR *iquanR = nullptr;
    ASSERT_TRUE(naviRes.getOrCreateRes(iquanR));
    ASSERT_TRUE(iquanR->_sqlClient);
}

TEST_F(IquanRTest, testFillSummaryTables) {
    CatalogInfo catalogInfo;
    ASSERT_NO_FATAL_FAILURE(loadFromFile("simple_summary_catalog.json", catalogInfo));
    SqlConfig config;

    auto &tableModels = catalogInfo.tableModels;
    const auto &tableModel = tableModels.tables[0];
    const auto &tableDef = tableModel.tableContent;
    ASSERT_EQ(1, tableModels.tables.size());
    ASSERT_EQ("simple", tableModel.tableName);
    ASSERT_EQ(11, tableDef.summaryFields.size());
    std::vector<FieldDef> summaryFields = tableDef.summaryFields;
    IquanR::fillSummaryTables(catalogInfo.tableModels, config.summaryTables);
    ASSERT_EQ(2, tableModels.tables.size());

    const auto &summaryTable = tableModels.tables[1];
    const auto &summaryTableDef = summaryTable.tableContent;
    ASSERT_EQ("simple_summary_", summaryTableDef.tableName);
    ASSERT_EQ(IQUAN_TABLE_TYPE_SUMMARY, summaryTableDef.tableType);
    ASSERT_EQ(autil::legacy::FastToJsonString(summaryFields),
              autil::legacy::FastToJsonString(summaryTableDef.fields));
    ASSERT_TRUE(tableModels.tables[0].tableContent.summaryFields.empty());
    ASSERT_TRUE(summaryTableDef.summaryFields.empty());
}

TEST_F(IquanRTest, testFillSummaryTablesWithConfig) {
    CatalogInfo catalogInfo;
    ASSERT_NO_FATAL_FAILURE(loadFromFile("simple_khronos_catalog.json", catalogInfo));
    SqlConfig config;
    config.summaryTables = {"simple"};

    auto &tableModels = catalogInfo.tableModels;
    ASSERT_EQ(1, tableModels.tables.size());
    ASSERT_EQ("simple", tableModels.tables[0].tableName);
    ASSERT_EQ("simple", tableModels.tables[0].tableContent.tableName);
    ASSERT_EQ(9, tableModels.tables[0].tableContent.fields.size());
    IquanR::fillSummaryTables(catalogInfo.tableModels, config.summaryTables);
    ASSERT_EQ(2, tableModels.tables.size());

    ASSERT_EQ("simple_summary_", tableModels.tables[1].tableName);
    ASSERT_EQ("simple_summary_", tableModels.tables[1].tableContent.tableName);
    ASSERT_EQ(tableModels.tables[0].tableContent.tableType,
              tableModels.tables[1].tableContent.tableType);
    ASSERT_EQ(autil::legacy::FastToJsonString(tableModels.tables[0].tableContent.fields),
              autil::legacy::FastToJsonString(tableModels.tables[1].tableContent.fields));
}

TEST_F(IquanRTest, testFillKhronosTable) {
    auto checkFields = [](const std::vector<std::string> &fieldNames,
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
    };

    CatalogInfo catalogInfo;
    ASSERT_NO_FATAL_FAILURE(loadFromFile("simple_khronos_catalog.json", catalogInfo));

    auto &tableModels = catalogInfo.tableModels;
    const auto &tableModel = tableModels.tables[0];
    // const auto &tableDef = tableModel.tableContent;
    ASSERT_EQ(1, tableModels.tables.size());
    ASSERT_EQ("simple", tableModel.tableName);
    ASSERT_EQ("customized", tableModel.tableType);

    ASSERT_TRUE(IquanR::fillKhronosTable(catalogInfo.tableModels));
    ASSERT_EQ(4, tableModels.tables.size());

    TableModel metaTable = tableModels.tables[0];
    TableModel metaTableV3 = tableModels.tables[1];
    TableModel seriesTableV3 = tableModels.tables[2];
    TableModel seriesTable = tableModels.tables[3];
    TableDef metaTableDef = metaTable.tableContent;
    TableDef metaTableDefV3 = metaTableV3.tableContent;
    TableDef seriesTableV3Def = seriesTableV3.tableContent;
    TableDef seriesTableDef = seriesTable.tableContent;
    {
        ASSERT_EQ("simple", metaTable.catalogName);
        ASSERT_EQ("simple", metaTable.databaseName);
        ASSERT_EQ("khronos_meta", metaTable.tableName);
        ASSERT_EQ("khronos_meta", metaTable.tableType);
        ASSERT_EQ(1, metaTable.tableVersion);

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

        ASSERT_TRUE(metaTableDef.joinInfo.empty());

        ASSERT_EQ(0, metaTableDef.properties.size());
    }
    {
        ASSERT_EQ(SQL_DEFAULT_CATALOG_NAME, metaTableV3.catalogName);
        ASSERT_EQ("simple", metaTableV3.databaseName);
        ASSERT_EQ(std::string("simple") + KHRONOS_META_TABLE_NAME_V3_SUFFIX, metaTableV3.tableName);
        ASSERT_EQ("khronos_meta", metaTableV3.tableType);
        ASSERT_EQ(1, metaTableV3.tableVersion);

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

        ASSERT_TRUE(metaTableDefV3.joinInfo.empty());

        ASSERT_EQ(0, metaTableDefV3.properties.size());
    }
    {
        ASSERT_EQ(SQL_DEFAULT_CATALOG_NAME, seriesTableV3.catalogName);
        ASSERT_EQ("simple", seriesTableV3.databaseName);
        ASSERT_EQ("simple", seriesTableV3.tableName);
        ASSERT_EQ(IQUAN_TABLE_TYPE_KHRONOS_SERIES_DATA_V3, seriesTableV3.tableType);
        ASSERT_EQ(1, seriesTableV3.tableVersion);

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

        ASSERT_TRUE(seriesTableV3Def.joinInfo.empty());

        ASSERT_EQ(0, seriesTableV3Def.properties.size());
    }
    {
        ASSERT_EQ("simple", seriesTable.catalogName);
        ASSERT_EQ("simple", seriesTable.databaseName);
        ASSERT_EQ(KHRONOS_DATA_TABLE_NAME_SERIES, seriesTable.tableName);
        ASSERT_EQ(IQUAN_TABLE_TYPE_KHRONOS_SERIES_DATA, seriesTable.tableType);
        ASSERT_EQ(1, seriesTable.tableVersion);

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

        ASSERT_TRUE(seriesTableDef.joinInfo.empty());

        ASSERT_EQ(0, seriesTableDef.properties.size());
    }
}

TEST_F(IquanRTest, testDupKhronosCatalogInfo) {
    CatalogInfo catalogInfo;
    ASSERT_NO_FATAL_FAILURE(loadFromFile("simple_khronos_catalog.json", catalogInfo));

    auto &tableModels = catalogInfo.tableModels;
    const auto &tableModel = tableModels.tables[0];
    // const auto &tableDef = tableModel.tableContent;
    ASSERT_EQ(1, tableModels.tables.size());
    ASSERT_EQ("simple", tableModel.tableName);
    ASSERT_EQ("customized", tableModel.tableType);

    ASSERT_TRUE(IquanR::fillKhronosTable(catalogInfo.tableModels));
    ASSERT_EQ(4, tableModels.tables.size());

    std::unordered_map<std::string, CatalogInfo> iquanCatalogInfos;
    IquanR::dupKhronosCatalogInfo(catalogInfo, iquanCatalogInfos);
    ASSERT_EQ(1, iquanCatalogInfos.size());

    auto &dupCatalogInfo = iquanCatalogInfos["simple"];
    ASSERT_EQ("simple", dupCatalogInfo.catalogName);
}

TEST_F(IquanRTest, testNewKhronosCatalogInfo) {
    auto checkFields = [](const std::vector<std::string> &fieldNames,
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
    };

    CatalogInfo catalogInfo;
    ASSERT_NO_FATAL_FAILURE(loadFromFile("simple_new_khronos_catalog.json", catalogInfo));

    auto &tableModels = catalogInfo.tableModels;
    const auto &tableModel = tableModels.tables[0];
    // const auto &tableDef = tableModel.tableContent;
    ASSERT_EQ(1, tableModels.tables.size());
    ASSERT_EQ("simple", tableModel.tableName);
    ASSERT_EQ("khronos", tableModel.tableType);

    ASSERT_TRUE(IquanR::fillKhronosTable(catalogInfo.tableModels));
    ASSERT_EQ(4, tableModels.tables.size());

    TableModel metaTable = tableModels.tables[0];
    TableModel metaTableV3 = tableModels.tables[1];
    TableModel seriesTableV3 = tableModels.tables[2];
    TableModel seriesTable = tableModels.tables[3];
    TableDef metaTableDef = metaTable.tableContent;
    TableDef metaTableDefV3 = metaTableV3.tableContent;
    TableDef seriesTableV3Def = seriesTableV3.tableContent;
    TableDef seriesTableDef = seriesTable.tableContent;
    {
        ASSERT_EQ("simple", metaTable.catalogName);
        ASSERT_EQ("simple", metaTable.databaseName);
        ASSERT_EQ("khronos_meta", metaTable.tableName);
        ASSERT_EQ("khronos_meta", metaTable.tableType);
        ASSERT_EQ(1, metaTable.tableVersion);

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
        ASSERT_TRUE(metaTableDef.joinInfo.empty());
        ASSERT_EQ(0, metaTableDef.properties.size());
    }
    {
        ASSERT_EQ(SQL_DEFAULT_CATALOG_NAME, metaTableV3.catalogName);
        ASSERT_EQ("simple", metaTableV3.databaseName);
        ASSERT_EQ(std::string("simple") + KHRONOS_META_TABLE_NAME_V3_SUFFIX, metaTableV3.tableName);
        ASSERT_EQ("khronos_meta", metaTableV3.tableType);
        ASSERT_EQ(1, metaTableV3.tableVersion);

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

        ASSERT_TRUE(metaTableDefV3.joinInfo.empty());

        ASSERT_EQ(0, metaTableDefV3.properties.size());
    }
    {
        ASSERT_EQ(SQL_DEFAULT_CATALOG_NAME, seriesTableV3.catalogName);
        ASSERT_EQ("simple", seriesTableV3.databaseName);
        ASSERT_EQ("simple", seriesTableV3.tableName);
        ASSERT_EQ(IQUAN_TABLE_TYPE_KHRONOS_SERIES_DATA_V3, seriesTableV3.tableType);
        ASSERT_EQ(1, seriesTableV3.tableVersion);

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

        ASSERT_TRUE(seriesTableV3Def.joinInfo.empty());

        ASSERT_EQ(0, seriesTableV3Def.properties.size());
    }
    {
        ASSERT_EQ("simple", seriesTable.catalogName);
        ASSERT_EQ("simple", seriesTable.databaseName);
        ASSERT_EQ(KHRONOS_DATA_TABLE_NAME_SERIES, seriesTable.tableName);
        ASSERT_EQ(IQUAN_TABLE_TYPE_KHRONOS_SERIES_DATA, seriesTable.tableType);
        ASSERT_EQ(1, seriesTable.tableVersion);

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

        ASSERT_TRUE(seriesTableDef.joinInfo.empty());

        ASSERT_EQ(0, seriesTableDef.properties.size());
    }
}

} // namespace sql
