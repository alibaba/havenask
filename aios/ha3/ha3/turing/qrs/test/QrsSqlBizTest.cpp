#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include <ha3/turing/qrs/QrsSqlBiz.h>
#include <iquan_jni/api/Iquan.h>

using namespace std;
using namespace testing;

USE_HA3_NAMESPACE(config);

BEGIN_HA3_NAMESPACE(turing);

class QrsSqlBizTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(qrs, QrsSqlBizTest);

void QrsSqlBizTest::setUp() {
}

void QrsSqlBizTest::tearDown() {
}

TEST_F(QrsSqlBizTest, testUpdateFlowControl) {
    QrsSqlBiz biz;
    biz._configAdapter.reset(new config::ConfigAdapter(TEST_DATA_CONF_PATH_VERSION(3)));
    biz._searchService.reset(new multi_call::SearchService());
    biz.updateFlowConfig("");
    auto searchService = biz._searchService;
    // config both
    {
        auto companyConfig = searchService->getFlowConfig("company.default_sql");
        EXPECT_TRUE(companyConfig);
        EXPECT_EQ(20, companyConfig->beginDegradeLatency);
        EXPECT_TRUE(5 != companyConfig->fullDegradeLatency);
    }
    // config none
    {
        auto companyConfig = searchService->getFlowConfig("auction.default_sql");
        EXPECT_TRUE(companyConfig);
        EXPECT_EQ(multi_call::DEFAULT_DEGRADE_LATENCY, companyConfig->beginDegradeLatency);
        EXPECT_EQ(multi_call::DEFAULT_DEGRADE_LATENCY, companyConfig->fullDegradeLatency);
    }
    // config normal one
    {
        auto companyConfig = searchService->getFlowConfig("human.default_sql");
        EXPECT_TRUE(companyConfig);
        EXPECT_EQ(20, companyConfig->beginDegradeLatency);
        EXPECT_EQ(multi_call::DEFAULT_DEGRADE_LATENCY, companyConfig->fullDegradeLatency);
    }
    // config T(abnormal) one
    {
        auto companyConfig = searchService->getFlowConfig("dog.default_sql");
        EXPECT_EQ(multi_call::DEFAULT_DEGRADE_LATENCY, companyConfig->beginDegradeLatency);
        EXPECT_TRUE(companyConfig);
    }
}

// TEST_F(QrsSqlBizTest, testInitSqlClient) {
//     // {
//     //     QrsSqlBiz biz;
//     //     auto status = biz.initSqlClient();
//     //     ASSERT_FALSE(status.ok());
//     //     ASSERT_EQ("update table info failed, error msg: jni exception occured: JNIEnv is null.",
//     //               status.error_message());
//     // }
//     {
//         auto status1 = iquan::IquanEnv::setup(iquan::jt_hdfs_jvm, {}, "");
//         ASSERT_TRUE(status1.ok());
//         ASSERT_EQ("", status1.errorMessage());
//         QrsSqlBiz biz;
//         biz._configAdapter.reset(new config::ConfigAdapter(
//                         TEST_DATA_CONF_PATH_VERSION(11)));
//         biz._resourceReader.reset(new resource_reader::ResourceReader(
//                         TEST_DATA_CONF_PATH_VERSION(11)));
//         auto status2 = biz.initSqlClient();
//         EXPECT_TRUE(status2.ok());
//         EXPECT_EQ("", status2.error_message());
//     }
// }

TEST_F(QrsSqlBizTest, testFillTableInfos) {
    {
        QrsSqlBiz biz;
        biz._configAdapter.reset(new config::ConfigAdapter(
                        TEST_DATA_CONF_PATH_VERSION(11)));
        biz._resourceReader.reset(new resource_reader::ResourceReader(
                        TEST_DATA_CONF_PATH_VERSION(11)));
        vector<string> searcherBizNames;
        ASSERT_TRUE(biz._configAdapter->getClusterNames(searcherBizNames));
        std::vector<iquan::Ha3TableModel> tableInfos;
        auto status = biz.fillHa3TableModels(searcherBizNames, tableInfos);
        ASSERT_TRUE(status.ok());
    }
    {
        QrsSqlBiz biz;
        biz._configAdapter.reset(new config::ConfigAdapter(
                        TEST_DATA_CONF_PATH_VERSION(12)));
        biz._resourceReader.reset(new resource_reader::ResourceReader(
                        TEST_DATA_CONF_PATH_VERSION(12)));
        vector<string> searcherBizNames = {"daogou"};
        std::vector<iquan::Ha3TableModel> tableInfos;
        auto status = biz.fillHa3TableModels(searcherBizNames, tableInfos);
        ASSERT_FALSE(status.ok());
        ASSERT_EQ("biz.json is empty.", status.error_message());
    }
}

TEST_F(QrsSqlBizTest, testFillTableInfosWithSqlTableBlacklist) {
    QrsSqlBiz biz;
    biz._configAdapter.reset(new config::ConfigAdapter(
                    TEST_DATA_CONF_PATH_VERSION(11)));
    biz._resourceReader.reset(new resource_reader::ResourceReader(
                    TEST_DATA_CONF_PATH_VERSION(11)));
    vector<string> searcherBizNames;
    ASSERT_TRUE(biz._configAdapter->getClusterNames(searcherBizNames));
    sort(searcherBizNames.begin(), searcherBizNames.end());
    {
        std::vector<iquan::Ha3TableModel> tableInfos;
        auto status = biz.fillHa3TableModels(searcherBizNames, tableInfos);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(4, tableInfos.size());
        ASSERT_EQ("category", tableInfos[0].databaseName);
        ASSERT_EQ("company", tableInfos[1].databaseName);
        ASSERT_EQ("daogou", tableInfos[2].databaseName);
        ASSERT_EQ("daogou", tableInfos[3].databaseName);
    }
    sap::EnvironUtil::setEnv("sqlTableBlacklist", "daogou2, company , xxx , , ");
    {
        std::vector<iquan::Ha3TableModel> tableInfos;
        auto status = biz.fillHa3TableModels(searcherBizNames, tableInfos);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(2, tableInfos.size());
        ASSERT_EQ("category", tableInfos[0].databaseName);
        ASSERT_EQ("daogou", tableInfos[1].databaseName);
    }
}

TEST_F(QrsSqlBizTest, testGetSearchBizInfoFile) {
    ASSERT_EQ("zones/1/biz.json", QrsSqlBiz::getSearchBizInfoFile("1"));
    ASSERT_EQ("zones/daogou/biz.json", QrsSqlBiz::getSearchBizInfoFile("daogou"));
}

TEST_F(QrsSqlBizTest, testIsDefaultBiz) {
    QrsSqlBiz biz;
    biz._resourceReader.reset(new resource_reader::ResourceReader(
                    TEST_DATA_CONF_PATH_VERSION(12)));
    ASSERT_FALSE(biz.isDefaultBiz("daogou"));
    ASSERT_TRUE(biz.isDefaultBiz("company"));
}

TEST_F(QrsSqlBizTest, testGetSearchDefaultBizJson) {
    {
        QrsSqlBiz biz;
        biz._configAdapter.reset(new config::ConfigAdapter(
                        TEST_DATA_CONF_PATH_VERSION(11)));
        string dbName = "daogou";
        string bizJson = biz.getSearchDefaultBizJson("daogou.default", dbName);
        ASSERT_EQ("daogou", dbName);
        string expectJson = R"json({"dependency_table":["daogou","daogou2"]})json";
        ASSERT_EQ(expectJson, bizJson);
    }
    {
        QrsSqlBiz biz;
        biz._configAdapter.reset(new config::ConfigAdapter(
                        TEST_DATA_CONF_PATH_VERSION(11)));
        string dbName = "company";
        string bizJson = biz.getSearchDefaultBizJson("company.default", dbName);
        string expectJson = R"json({"dependency_table":["company"]})json";
        ASSERT_EQ(expectJson, bizJson);
    }
}

TEST_F(QrsSqlBizTest, testGetDependTables) {
    {
        QrsSqlBiz biz;
        biz._configAdapter.reset(new config::ConfigAdapter(
                        TEST_DATA_CONF_PATH_VERSION(12)));
        biz._resourceReader.reset(new resource_reader::ResourceReader(
                        TEST_DATA_CONF_PATH_VERSION(12)));
        std::vector<string> dependTables;
        string dbName;
        auto status = biz.getDependTables("company", dependTables, dbName);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ("company", dbName);
        ASSERT_EQ(1, dependTables.size());
        ASSERT_EQ("company", dependTables[0]);
    }
    {
        QrsSqlBiz biz;
        biz._configAdapter.reset(new config::ConfigAdapter(
                        TEST_DATA_CONF_PATH_VERSION(12)));
        biz._resourceReader.reset(new resource_reader::ResourceReader(
                        TEST_DATA_CONF_PATH_VERSION(12)));
        std::vector<string> dependTables;
        string dbName;
        auto status = biz.getDependTables("daogou", dependTables, dbName);
        ASSERT_FALSE(status.ok());
        ASSERT_EQ("biz.json is empty.", status.error_message());
    }
    {
        QrsSqlBiz biz;
        biz._configAdapter.reset(new config::ConfigAdapter(
                        TEST_DATA_CONF_PATH_VERSION(12)));
        biz._resourceReader.reset(new resource_reader::ResourceReader(
                        TEST_DATA_CONF_PATH_VERSION(12)));
        std::vector<string> dependTables;
        string dbName;
        auto status = biz.getDependTables("category", dependTables, dbName);
        ASSERT_FALSE(status.ok());
        ASSERT_EQ("parse json string [xxx\n] failed.", status.error_message());
    }
    {
        QrsSqlBiz biz;
        biz._configAdapter.reset(new config::ConfigAdapter(
                        TEST_DATA_CONF_PATH_VERSION(11)));
        biz._resourceReader.reset(new resource_reader::ResourceReader(
                        TEST_DATA_CONF_PATH_VERSION(11)));
        std::vector<string> dependTables;
        string dbName;
        auto status = biz.getDependTables("daogou.default", dependTables, dbName);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ("daogou", dbName);
        ASSERT_EQ(2, dependTables.size());
        ASSERT_EQ("daogou", dependTables[0]);
        ASSERT_EQ("daogou2", dependTables[1]);
    }
}

TEST_F(QrsSqlBizTest, testReadClusterFile) {
    {
        QrsSqlBiz biz;
        biz._configAdapter.reset(new config::ConfigAdapter(
                        TEST_DATA_CONF_PATH_VERSION(11)));
        iquan::Ha3TableModel tableInfo;
        auto status = biz.readClusterFile("daogou", tableInfo);
        ASSERT_TRUE(status.ok());
    }
    {
        QrsSqlBiz biz;
        biz._configAdapter.reset(new config::ConfigAdapter(
                        TEST_DATA_CONF_PATH_VERSION(11)));
        iquan::Ha3TableModel tableInfo;
        auto status = biz.readClusterFile("xxxx", tableInfo);
        ASSERT_FALSE(status.ok());
    }
}

TEST_F(QrsSqlBizTest, testFillFunctionModels) {
    {
        QrsSqlBiz biz;
        biz._resourceReader.reset(new resource_reader::ResourceReader(
                        TEST_DATA_CONF_PATH_VERSION(11)));
        biz._configAdapter.reset(new config::ConfigAdapter(
                        TEST_DATA_CONF_PATH_VERSION(11)));
        iquan::FunctionModels functionInfos;
        iquan::TvfModels tvfModels;
        vector<string> searcherBizNames = {"daogou.default", "xxx"};
        auto status = biz.fillFunctionModels(searcherBizNames, functionInfos, tvfModels);
        ASSERT_FALSE(status.ok());
    }
    {
        QrsSqlBiz biz;
        const string &oldEnv = sap::EnvironUtil::getEnv("binaryPath", "");
        sap::EnvironUtil::setEnv("binaryPath", TEST_DATA_CONF_PATH_VERSION(11));
        biz._resourceReader.reset(new resource_reader::ResourceReader(
                        TEST_DATA_CONF_PATH_VERSION(11)));
        biz._configAdapter.reset(new config::ConfigAdapter(
                        TEST_DATA_CONF_PATH_VERSION(11)));
        iquan::FunctionModels functionInfos;
        iquan::TvfModels tvfModels;
        vector<string> searcherBizNames = {"daogou.default", "xxx"};
        auto status = biz.fillFunctionModels(searcherBizNames, functionInfos, tvfModels);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ("daogou", functionInfos.functions[0].databaseName);
        ASSERT_EQ("sql", functionInfos.functions[0].functionName);
        ASSERT_EQ("xxx", functionInfos.functions[1].databaseName);
        ASSERT_EQ("xxx", functionInfos.functions[1].functionName);
        sap::EnvironUtil::setEnv("binaryPath", oldEnv);
    }
}

TEST_F(QrsSqlBizTest, testInitSqlAggFuncManager) {
    QrsSqlBiz biz;
    resource_reader::ResourceReaderPtr resource(
            new resource_reader::ResourceReader(
                    GET_TEST_DATA_PATH() + "/sql_agg_func2/"));
    SqlAggPluginConfig config;
    std::string content;
    resource->getFileContent("sql.json", content);
    FromJsonString(config, content);
    biz._resourceReader = resource;
    ASSERT_TRUE(biz.initSqlAggFuncManager(config));
    ASSERT_EQ(12, biz._aggFuncManager->_funcToCreator.size());
}

TEST_F(QrsSqlBizTest, testFillTableModels) {
    {
        // file exist and content correct
        QrsSqlBiz biz;
        biz._resourceReader.reset(new resource_reader::ResourceReader(
                        TEST_DATA_CONF_PATH_VERSION(11)));
        biz._configAdapter.reset(new config::ConfigAdapter(
                        TEST_DATA_CONF_PATH_VERSION(11)));
        vector<string> searcherBizNames = {"db1"};
        iquan::TableModels tableModels;
        auto status = biz.fillTableModels(searcherBizNames, tableModels);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(2u, tableModels.tables.size());
        EXPECT_EQ("logical_table1", tableModels.tables[0].tableName);
        EXPECT_EQ("json_default", tableModels.tables[0].tableType);
        EXPECT_EQ("logical_table2", tableModels.tables[1].tableName);
        EXPECT_EQ("json_default", tableModels.tables[1].tableType);
    }
    {
        // file exist but has no content
        QrsSqlBiz biz;
        biz._resourceReader.reset(new resource_reader::ResourceReader(
                        TEST_DATA_CONF_PATH_VERSION(11)));
        biz._configAdapter.reset(new config::ConfigAdapter(
                        TEST_DATA_CONF_PATH_VERSION(11)));
        vector<string> searcherBizNames = {"empty"};
        iquan::TableModels tableModels;
        auto status = biz.fillTableModels(searcherBizNames, tableModels);
        ASSERT_TRUE(status.ok());
    }
    {
        // file not exist
        QrsSqlBiz biz;
        biz._resourceReader.reset(new resource_reader::ResourceReader(
                        TEST_DATA_CONF_PATH_VERSION(11)));
        biz._configAdapter.reset(new config::ConfigAdapter(
                        TEST_DATA_CONF_PATH_VERSION(11)));
        vector<string> searcherBizNames = {"fake"};
        iquan::TableModels tableModels;
        auto status = biz.fillTableModels(searcherBizNames, tableModels);
        ASSERT_TRUE(status.ok());
    }
    {
        // file exist but format illegal
        QrsSqlBiz biz;
        biz._resourceReader.reset(new resource_reader::ResourceReader(
                        TEST_DATA_CONF_PATH_VERSION(11)));
        biz._configAdapter.reset(new config::ConfigAdapter(
                        TEST_DATA_CONF_PATH_VERSION(11)));
        vector<string> searcherBizNames = {"db2"};
        iquan::TableModels tableModels;
        auto status = biz.fillTableModels(searcherBizNames, tableModels);
        ASSERT_FALSE(status.ok());
    }
    {
        // file exist, format correct, but databasename mismatch
        QrsSqlBiz biz;
        biz._resourceReader.reset(new resource_reader::ResourceReader(
                        TEST_DATA_CONF_PATH_VERSION(11)));
        biz._configAdapter.reset(new config::ConfigAdapter(
                        TEST_DATA_CONF_PATH_VERSION(11)));
        vector<string> searcherBizNames = {"db3"};
        iquan::TableModels tableModels;
        auto status = biz.fillTableModels(searcherBizNames, tableModels);
        ASSERT_FALSE(status.ok());
    }
    {
        // file exist, format correct, but tabletype mismatch
        QrsSqlBiz biz;
        biz._resourceReader.reset(new resource_reader::ResourceReader(
                        TEST_DATA_CONF_PATH_VERSION(11)));
        biz._configAdapter.reset(new config::ConfigAdapter(
                        TEST_DATA_CONF_PATH_VERSION(11)));
        vector<string> searcherBizNames = {"db4"};
        iquan::TableModels tableModels;
        auto status = biz.fillTableModels(searcherBizNames, tableModels);
        ASSERT_FALSE(status.ok());
    }
}

END_HA3_NAMESPACE();
