#include <engine/ResourceInitContext.h>

#include "autil/legacy/legacy_jsonizable.h"
#include "fslib/util/FileUtil.h"
#include "iquan/common/catalog/FunctionModel.h"
#include "iquan/common/catalog/TableModel.h"
#include "iquan/jni/IquanEnv.h"
#include "iquan/jni/JvmType.h"
#include "navi/log/NaviLoggerProvider.h"
#include "navi/log/test/NaviLoggerProviderTestUtil.h"
#include "navi/tester/KernelTesterBuilder.h"
#include "navi/tester/NaviResourceHelper.h"
#include "sql/common/common.h"
#include "sql/ops/tvf/TvfFuncCreatorR.h"
#include "sql/ops/tvf/test/MockTvfFunc.h"
#include "sql/resource/IquanR.h"
#include "sql/resource/SqlConfig.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace iquan;
using namespace navi;
using namespace autil::legacy;

namespace sql {

class UdfModelRTest : public TESTBASE {
public:
    void setUp() override;
    void tearDown() override;

private:
    template <typename T>
    void loadFromFile(const std::string &fileName, T &json) {
        std::string testdataPath = GET_TEST_DATA_PATH() + "sql_resource/";
        std::string jsonFile = testdataPath + fileName;
        std::string jsonStr;
        ASSERT_TRUE(fslib::util::FileUtil::readFile(jsonFile, jsonStr));
        try {
            autil::legacy::FastFromJsonString(json, jsonStr);
        } catch (const std::exception &e) { ASSERT_TRUE(false) << e.what(); }
    }

    void readTestFile(const std::string fileName, std::string &jsonStr) {
        std::string testdataPath = GET_TEST_DATA_PATH() + "sql_resource/";
        std::string jsonFile = testdataPath + fileName;
        ASSERT_TRUE(fslib::util::FileUtil::readFile(jsonFile, jsonStr));
    }

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(resource, UdfModelRTest);

void UdfModelRTest::setUp() {}

void UdfModelRTest::tearDown() {}

class ErrorTvfFuncCreator : public TvfFuncCreatorR {
public:
    ErrorTvfFuncCreator()
        : TvfFuncCreatorR("", SqlTvfProfileInfo("factory_error_tvf", "factory_error_fun")) {}

public:
    std::string getResourceName() const override {
        return RESOURCE_ID;
    }

public:
    static const std::string RESOURCE_ID;

private:
    TvfFunc *createFunction(const SqlTvfProfileInfo &info) override {
        return new MockTvfFunc();
    }
};
const std::string ErrorTvfFuncCreator::RESOURCE_ID = "tvf_test.factory_error_tvf";

TEST_F(UdfModelRTest, testMergeUdfs) {
    FunctionModel udf1;
    ASSERT_NO_FATAL_FAILURE(loadFromFile("udf1.json", udf1));
    FunctionModel udf2;
    ASSERT_NO_FATAL_FAILURE(loadFromFile("udf2.json", udf2));
    FunctionModel udaf1;
    ASSERT_NO_FATAL_FAILURE(loadFromFile("udaf1.json", udaf1));
    // dest is empty, src is empty
    {
        vector<FunctionModel> destModels;
        vector<FunctionModel> srcModels;
        UdfModelR::mergeUdfs(destModels, srcModels);
        EXPECT_TRUE(srcModels.empty());
        EXPECT_TRUE(destModels.empty());
    }
    // dest is not empty, src is empty
    {
        vector<FunctionModel> destModels;
        vector<FunctionModel> srcModels;
        destModels.push_back(udf1);
        UdfModelR::mergeUdfs(destModels, srcModels);
        EXPECT_TRUE(srcModels.empty());
        EXPECT_EQ(1, destModels.size());
        EXPECT_EQ("udf1", destModels[0].functionName);
        EXPECT_EQ("UDF", destModels[0].functionType);
    }
    // dest is empty, src is not empty
    {
        vector<FunctionModel> destModels;
        vector<FunctionModel> srcModels;
        srcModels.push_back(udf1);
        UdfModelR::mergeUdfs(destModels, srcModels);
        EXPECT_TRUE(srcModels.empty());
        EXPECT_EQ(1, destModels.size());
        EXPECT_EQ("udf1", destModels[0].functionName);
        EXPECT_EQ("UDF", destModels[0].functionType);
    }
    // dest and src are both not empty, and has no function repeated
    {
        vector<FunctionModel> destModels;
        vector<FunctionModel> srcModels;
        srcModels.push_back(udf1);
        destModels.push_back(udf2);
        UdfModelR::mergeUdfs(destModels, srcModels);
        EXPECT_TRUE(srcModels.empty());
        EXPECT_EQ(2, destModels.size());
        EXPECT_EQ("matchscore2", destModels[0].functionName);
        EXPECT_EQ("UDF", destModels[0].functionType);
        EXPECT_EQ("udf1", destModels[1].functionName);
        EXPECT_EQ("UDF", destModels[1].functionType);
    }
    // dest and src are both not empty, and has no function type repeated
    {
        vector<FunctionModel> destModels;
        vector<FunctionModel> srcModels;
        srcModels.push_back(udf1);
        destModels.push_back(udaf1);
        UdfModelR::mergeUdfs(destModels, srcModels);
        EXPECT_TRUE(srcModels.empty());
        EXPECT_EQ(2, destModels.size());
        EXPECT_EQ("udf1", destModels[0].functionName);
        EXPECT_EQ("UDAF", destModels[0].functionType);
        EXPECT_EQ("udf1", destModels[1].functionName);
        EXPECT_EQ("UDF", destModels[1].functionType);
    }
    // dest and are both not empty, and has function repeated
    {
        vector<FunctionModel> destModels;
        vector<FunctionModel> srcModels;
        srcModels.push_back(udf1);
        srcModels.push_back(udf2);
        destModels.push_back(udf2);
        destModels.push_back(udaf1);
        UdfModelR::mergeUdfs(destModels, srcModels);
        EXPECT_TRUE(srcModels.empty());
        EXPECT_EQ(3, destModels.size());
        EXPECT_EQ("matchscore2", destModels[0].functionName);
        EXPECT_EQ("UDF", destModels[0].functionType);
        EXPECT_EQ("udf1", destModels[1].functionName);
        EXPECT_EQ("UDAF", destModels[1].functionType);
        EXPECT_EQ("udf1", destModels[2].functionName);
        EXPECT_EQ("UDF", destModels[2].functionType);
    }
}

TEST_F(UdfModelRTest, testAddFuntions) {
    FunctionModel udf1;
    ASSERT_NO_FATAL_FAILURE(loadFromFile("udf1.json", udf1));
    FunctionModel udf2;
    ASSERT_NO_FATAL_FAILURE(loadFromFile("udf2.json", udf2));
    FunctionModel udaf1;
    ASSERT_NO_FATAL_FAILURE(loadFromFile("udaf1.json", udaf1));

    // function models is empty
    {
        vector<string> catalogList({SQL_DEFAULT_CATALOG_NAME});
        CatalogDefs catalogDefs;
        vector<FunctionModel> functionModels;
        const std::string dbName = SQL_SYSTEM_DATABASE;
        EXPECT_TRUE(UdfModelR::addFunctions(catalogDefs, catalogList, functionModels, dbName));
        EXPECT_EQ(1, catalogDefs.catalogs.size());
        EXPECT_EQ(SQL_DEFAULT_CATALOG_NAME, catalogDefs.catalogs[0].catalogName);
        EXPECT_EQ(1, catalogDefs.catalogs[0].databases.size());
        EXPECT_EQ(0, catalogDefs.catalogs[0].databases[0].functions.size());
    }
    {
        vector<string> catalogList({SQL_DEFAULT_CATALOG_NAME, "xxx"});
        CatalogDefs catalogDefs;
        vector<FunctionModel> functionModels;
        functionModels.push_back(udf1);
        functionModels.push_back(udf2);
        const std::string dbName = SQL_SYSTEM_DATABASE;
        EXPECT_TRUE(UdfModelR::addFunctions(catalogDefs, catalogList, functionModels, dbName));
        EXPECT_EQ(2, catalogDefs.catalogs.size());
        EXPECT_EQ(SQL_DEFAULT_CATALOG_NAME, catalogDefs.catalogs[0].catalogName);
        EXPECT_EQ(1, catalogDefs.catalogs[0].databases.size());
        EXPECT_EQ(2, catalogDefs.catalogs[0].databases[0].functions.size());
        EXPECT_FALSE(UdfModelR::addFunctions(catalogDefs, catalogList, functionModels, dbName));
    }
}

TEST_F(UdfModelRTest, testInit) {
    NaviResourceHelper naviRes;
    std::string configStr;
    ASSERT_NO_FATAL_FAILURE(readTestFile("udf_model_r.json", configStr));
    ASSERT_TRUE(naviRes.config(configStr));
    auto udfModelR = naviRes.getOrCreateRes<UdfModelR>();
    ASSERT_TRUE(udfModelR);
    EXPECT_EQ("a b c default", autil::StringUtil::toString(udfModelR->_specialCatalogs));
    EXPECT_EQ(1, udfModelR->_dbUdfMap["db1"].size());
    EXPECT_EQ(2, udfModelR->_dbUdfMap["db2"].size());
    EXPECT_EQ(3, udfModelR->_dbUdfMap["system"].size());
    EXPECT_EQ(3, udfModelR->_systemUdfModels.size());
    EXPECT_EQ(0, udfModelR->_defaultUdfModels.size());
}

TEST_F(UdfModelRTest, testFillFunctionModels_udafConverterFailed) {
    navi::NaviLoggerProvider provider("ERROR");
    NaviResourceHelper naviRes;
    std::string configStr;
    readTestFile("udf_model_r.json", configStr);
    ASSERT_TRUE(naviRes.config(configStr));
    auto udfModelR = naviRes.getOrCreateRes<UdfModelR>();
    ASSERT_TRUE(udfModelR);
    FunctionModel errorFunctionModel;
    loadFromFile("udf1.json", errorFunctionModel);
    errorFunctionModel.functionDef.prototypes[0].returnTypes[0].isMulti = false;
    errorFunctionModel.functionDef.prototypes[0].returnTypes[0].type = "";
    udfModelR->_aggFuncFactoryR->_functionModels.push_back(errorFunctionModel);
    iquan::CatalogDefs catalogDefs;
    ASSERT_FALSE(udfModelR->fillFunctionModels(catalogDefs));
    ASSERT_NO_FATAL_FAILURE(
        NaviLoggerProviderTestUtil::checkTraceCount(1, "udaf model converter failed", provider));
}

TEST_F(UdfModelRTest, testFillFunctionModels_RegTvfModelsFailed) {
    navi::NaviLoggerProvider provider("ERROR");
    NaviResourceHelper naviRes;
    std::string configStr;
    readTestFile("udf_model_r.json", configStr);
    ASSERT_TRUE(naviRes.config(configStr));
    auto udfModelR = naviRes.getOrCreateRes<UdfModelR>();
    ASSERT_TRUE(udfModelR);
    ErrorTvfFuncCreator errorCreator;
    udfModelR->_tvfFuncFactoryR->_funcToCreator["tvf_1"] = &errorCreator;
    iquan::CatalogDefs catalogDefs;
    ASSERT_FALSE(udfModelR->fillFunctionModels(catalogDefs));
    ASSERT_NO_FATAL_FAILURE(
        NaviLoggerProviderTestUtil::checkTraceCount(1, "reg tvf models failed", provider));
}

TEST_F(UdfModelRTest, testFillFunctionModels_convertdbUdfMapModelsFailed) {
    navi::NaviLoggerProvider provider("ERROR");
    NaviResourceHelper naviRes;
    std::string configStr;
    readTestFile("udf_model_r.json", configStr);
    ASSERT_TRUE(naviRes.config(configStr));
    auto udfModelR = naviRes.getOrCreateRes<UdfModelR>();
    ASSERT_TRUE(udfModelR);
    FunctionModel errorFunctionModel;
    loadFromFile("udf1.json", errorFunctionModel);
    errorFunctionModel.functionDef.prototypes[0].returnTypes[0].isMulti = false;
    errorFunctionModel.functionDef.prototypes[0].returnTypes[0].type = "";
    udfModelR->_dbUdfMap["system"] = {errorFunctionModel};
    iquan::CatalogDefs catalogDefs;
    ASSERT_FALSE(udfModelR->fillFunctionModels(catalogDefs));
    ASSERT_NO_FATAL_FAILURE(NaviLoggerProviderTestUtil::checkTraceCount(
        1, "db [system] udf model converter failed", provider));
}

TEST_F(UdfModelRTest, testFillFunctionModels) {
    NaviResourceHelper naviRes;
    std::string configStr;
    ASSERT_NO_FATAL_FAILURE(readTestFile("udf_model_r.json", configStr));
    ASSERT_TRUE(naviRes.config(configStr));
    auto udfModelR = naviRes.getOrCreateRes<UdfModelR>();
    ASSERT_TRUE(udfModelR);
    {
        CatalogDefs catalogDefs;
        ASSERT_TRUE(udfModelR->fillFunctionModels(catalogDefs));
        ASSERT_EQ(4, catalogDefs.catalogs.size());
        vector<string> catalogList({"a", "b", "c", "default"});
        for (size_t i = 0; i < catalogList.size(); ++i) {
            auto &catalog = catalogDefs.catalogs[i];
            ASSERT_EQ(catalogList[i], catalog.catalogName);
            ASSERT_EQ(3, catalog.databases.size());
            ASSERT_EQ(SQL_SYSTEM_DATABASE, catalog.databases[0].dbName);
            ASSERT_EQ("db1", catalog.databases[1].dbName);
            ASSERT_EQ("db2", catalog.databases[2].dbName);
            {
                vector<string> functionsName(
                    {"MULTICAST", "MATCHINDEX", "QUERY", "ARBITRARY", "topKTvf"});
                vector<string> functionsType({"UDF", "UDF", "UDF", "UDAF", "TVF"});
                for (size_t j = 0; j < functionsName.size(); ++j) {
                    EXPECT_TRUE(
                        catalog.databases[0].hasFunction(functionsName[j], functionsType[j]))
                        << j;
                }
            }
            {
                EXPECT_EQ(1, catalog.databases[1].functions.size());
                vector<string> functionsName({"rand"});
                vector<string> functionsType({"UDF"});
                for (size_t j = 0; j < functionsName.size(); ++j) {
                    EXPECT_TRUE(
                        catalog.databases[1].hasFunction(functionsName[j], functionsType[j]));
                }
            }
            {
                EXPECT_EQ(2, catalog.databases[2].functions.size());
                vector<string> functionsName({"length", "rand"});
                vector<string> functionsType({"UDF", "UDF"});
                for (size_t j = 0; j < functionsName.size(); ++j) {
                    EXPECT_TRUE(
                        catalog.databases[2].hasFunction(functionsName[j], functionsType[j]));
                }
            }
        }
    }
    {
        // dup agg func
        navi::NaviLoggerProvider provider("ERROR");
        CatalogDefs catalogDefs;
        FunctionModel functionModel;
        functionModel.functionName = "ARBITRARY";
        functionModel.functionType = "UDAF";
        catalogDefs.catalog(SQL_DEFAULT_CATALOG_NAME)
            .database(SQL_SYSTEM_DATABASE)
            .functions.push_back(functionModel);
        ASSERT_FALSE(udfModelR->fillFunctionModels(catalogDefs));
        ASSERT_NO_FATAL_FAILURE(
            NaviLoggerProviderTestUtil::checkTraceCount(1, "add udaf functions failed", provider));
    }
    {
        // dup tvf func
        navi::NaviLoggerProvider provider("ERROR");
        CatalogDefs catalogDefs;
        FunctionModel functionModel;
        functionModel.functionName = "topKTvf";
        functionModel.functionType = "TVF";
        catalogDefs.catalog(SQL_DEFAULT_CATALOG_NAME)
            .database(SQL_SYSTEM_DATABASE)
            .functions.push_back(functionModel);
        ASSERT_FALSE(udfModelR->fillFunctionModels(catalogDefs));
        ASSERT_NO_FATAL_FAILURE(
            NaviLoggerProviderTestUtil::checkTraceCount(1, "add tvf functions failed", provider));
    }
    {
        // dup udf func
        navi::NaviLoggerProvider provider("ERROR");
        CatalogDefs catalogDefs;
        FunctionModel functionModel;
        functionModel.functionName = "QUERY";
        functionModel.functionType = "UDF";
        catalogDefs.catalog(SQL_DEFAULT_CATALOG_NAME)
            .database(SQL_SYSTEM_DATABASE)
            .functions.push_back(functionModel);
        ASSERT_FALSE(udfModelR->fillFunctionModels(catalogDefs));
        ASSERT_NO_FATAL_FAILURE(NaviLoggerProviderTestUtil::checkTraceCount(
            1, "add functions in db [system] failed", provider));
    }
}

} // namespace sql
