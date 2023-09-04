#include <algorithm>
#include <exception>
#include <ext/alloc_traits.h>
#include <iosfwd>
#include <map>
#include <memory>
#include <mutex>
#include <ostream>
#include <string>
#include <vector>

#include "autil/EnvUtil.h"
#include "autil/Log.h"
#include "autil/UrlEncode.h"
#include "autil/legacy/any.h"
#include "autil/legacy/base64.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/json.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "fslib/util/FileUtil.h"
#include "iquan/common/Common.h"
#include "iquan/common/Status.h"
#include "iquan/common/catalog/TableModel.h"
#include "iquan/jni/DynamicParams.h"
#include "iquan/jni/Iquan.h"
#include "iquan/jni/IquanDqlRequest.h"
#include "iquan/jni/IquanEnv.h"
#include "iquan/jni/JvmType.h"
#include "iquan/jni/SqlPlan.h"
#include "kmonitor/client/MetricsReporter.h"
#include "kmonitor/client/MetricsReporterR.h"
#include "navi/common.h"
#include "navi/engine/Data.h"
#include "navi/tester/KernelTester.h"
#include "navi/tester/KernelTesterBuilder.h"
#include "sql/common/common.h"
#include "sql/data/SqlPlanData.h"
#include "sql/data/SqlQueryRequest.h"
#include "sql/data/SqlRequestData.h"
#include "sql/ops/parser/SqlParseKernel.h"
#include "sql/resource/IquanR.h"
#include "sql/resource/SqlConfig.h"
#include "sql/resource/SqlConfigResource.h"
#include "unittest/unittest.h"

using namespace std;
using namespace navi;
using namespace iquan;
using namespace autil::legacy;
using namespace autil::legacy::json;

namespace sql {
static std::once_flag iquanFlag;

class SqlParseKernelTest : public TESTBASE {
public:
    SqlParseKernelTest();
    ~SqlParseKernelTest();

public:
    void setUp() override;
    void tearDown() override {}

private:
    bool loadTableModels(iquan::TableModels &config);

private:
    std::unique_ptr<iquan::Iquan> _sqlClient;

private:
    AUTIL_LOG_DECLARE();
};
AUTIL_LOG_SETUP(sql, SqlParseKernelTest);
SqlParseKernelTest::SqlParseKernelTest() {}

SqlParseKernelTest::~SqlParseKernelTest() {}
void SqlParseKernelTest::setUp() {
    std::call_once(iquanFlag, [&]() {
        // make iquan search for iquan.jar from workdir
        autil::EnvUtil::setEnv(BINARY_PATH, ".");
        Status status = IquanEnv::jvmSetup(jt_hdfs_jvm, {}, "");
        ASSERT_TRUE(status.ok()) << "can not start jvm" << std::endl << status.errorMessage();
        autil::EnvUtil::setEnv(BINARY_PATH, "");
    });
}

bool SqlParseKernelTest::loadTableModels(iquan::TableModels &config) {
    string simpleTableFile = GET_TEST_DATA_PATH() + "sql_test/table_model_json/simple_table.json";
    string content;
    if (!fslib::util::FileUtil::readFile(simpleTableFile, content)) {
        AUTIL_LOG(WARN, "read file failed, path: %s", simpleTableFile.c_str());
        return false;
    }
    try {
        FastFromJsonString(config, content);
    } catch (const std::exception &e) {
        AUTIL_LOG(WARN, "parse table model failed, content: %s", content.c_str());
        return false;
    }
    return true;
}

TEST_F(SqlParseKernelTest, testCompute) {
    string simpleTableFile = GET_TEST_DATA_PATH() + "sql_test/table_model_json/simple_table.json";
    std::string iquanConfigStr;
    ASSERT_TRUE(IquanR::getIquanConfigFromFile(simpleTableFile, iquanConfigStr));
    string sqlConfigStr
        = string(R"json({"iquan_sql_config":{"catalog_name":"default", "db_name":"default"}})json");
    navi::KernelTesterBuilder builder;
    builder.resourceConfig(IquanR::RESOURCE_ID, iquanConfigStr);
    builder.resourceConfig(SqlConfigResource::RESOURCE_ID, sqlConfigStr);
    builder.kernel("sql.SqlParseKernel");
    builder.input("input0");
    builder.output("output0");
    builder.attrsFromMap({{"db_name", "default"}});
    kmonitor::MetricsReporterPtr metricsReporter(new kmonitor::MetricsReporter("", {}, ""));
    kmonitor::MetricsReporterRPtr metricsReporterR(new kmonitor::MetricsReporterR(metricsReporter));
    builder.resource(metricsReporterR);
    auto tester = builder.build();
    ASSERT_TRUE(tester);
    SqlQueryRequestPtr sqlQuery(new SqlQueryRequest());
    sqlQuery->setSqlQueryStr("select i1 from simple");
    SqlRequestDataPtr inputData(new SqlRequestData(sqlQuery));
    ASSERT_TRUE(tester->setInput("input0", inputData, true));
    ASSERT_TRUE(tester->compute());
    DataPtr outputData;
    bool eof = false;
    ASSERT_TRUE(tester->getOutput("output0", outputData, eof));
    ASSERT_TRUE(outputData != nullptr);
    ASSERT_TRUE(eof);
    SqlPlanData *sqlPlanData = dynamic_cast<SqlPlanData *>(outputData.get());
    ASSERT_TRUE(sqlPlanData);
    ASSERT_TRUE(sqlPlanData->_sqlPlan);
    ASSERT_EQ(3, sqlPlanData->_sqlPlan->opList.size());
}

TEST_F(SqlParseKernelTest, testParseSqlPlan) {
    SqlConfigResourcePtr sqlConfigResource(new SqlConfigResource());
    IquanRPtr iquanR(new IquanR());
    _sqlClient.reset(new Iquan());
    iquanR->_sqlClient.reset(new Iquan());
    iquan::Status status = iquanR->_sqlClient->init(iquanR->_jniConfig, iquanR->_clientConfig);
    ASSERT_TRUE(status.ok());
    iquan::TableModels tableModels;
    ASSERT_TRUE(loadTableModels(tableModels));
    ASSERT_EQ(1, tableModels.tables.size());
    status = iquanR->_sqlClient->updateTables(tableModels);
    ASSERT_TRUE(status.ok());
    SqlParseKernel sqlParser;
    sqlParser._sqlConfigResource = sqlConfigResource.get();
    sqlParser._iquanR = iquanR.get();
    sql::SqlQueryRequest sqlRequest;
    sqlRequest.setSqlParams({{SQL_DATABASE_NAME, "default"}});
    sqlRequest._sqlStr = "select i1 from simple";
    iquan::SqlPlanPtr sqlPlan;
    iquan::IquanDqlRequestPtr iquanRequest;
    ASSERT_TRUE(sqlParser.parseSqlPlan(&sqlRequest, iquanRequest, sqlPlan));
    ASSERT_TRUE(sqlPlan);
    ASSERT_EQ(3, sqlPlan->opList.size());
}

TEST_F(SqlParseKernelTest, testAddIquanSqlParams) {
    SqlConfigResourcePtr sqlConfigResource(new SqlConfigResource());
    SqlParseKernel sqlParser;
    sqlParser._sqlConfigResource = sqlConfigResource.get();
    {
        sql::SqlQueryRequest sqlRequest;
        sqlRequest.setSqlParams({
            {"iquan.first", "first"},
            {"iquan.second", "second"},
            {"iquan.third", "third"},
        });
        iquan::IquanDqlRequest iquanRequest;
        ASSERT_TRUE(sqlParser.addIquanSqlParams(&sqlRequest, iquanRequest));
        ASSERT_EQ(7, iquanRequest.sqlParams.size());
        EXPECT_EQ("first", AnyCast<std::string>(iquanRequest.sqlParams["iquan.first"]));
        EXPECT_EQ("second", AnyCast<std::string>(iquanRequest.sqlParams["iquan.second"]));
        EXPECT_EQ("third", AnyCast<std::string>(iquanRequest.sqlParams["iquan.third"]));
        EXPECT_EQ("jni.post.optimize",
                  AnyCast<std::string>(iquanRequest.sqlParams["iquan.plan.prepare.level"]));
        EXPECT_EQ("true", AnyCast<std::string>(iquanRequest.sqlParams["iquan.plan.cache.enable"]));
        ASSERT_EQ(3, iquanRequest.execParams.size());
        EXPECT_FALSE(iquanRequest.execParams[SQL_IQUAN_EXEC_PARAMS_SOURCE_ID].empty());
        EXPECT_EQ("empty", iquanRequest.execParams[SQL_IQUAN_EXEC_PARAMS_SOURCE_SPEC]);
        EXPECT_EQ("", iquanRequest.execParams[SQL_IQUAN_EXEC_PARAMS_TASK_QUEUE]);
    }
    {
        sql::SqlQueryRequest sqlRequest;
        iquan::IquanDqlRequest iquanRequest;
        sqlRequest.setSqlParams({
            {"skip.iquan.first", "first"},
            {"iquan.second", "second"},
            {"exec.source.id", "123"},
            {"exec.source.spec", "a123"},
            {"exec.task.queue", "sql_default"},
        });
        ASSERT_TRUE(sqlParser.addIquanSqlParams(&sqlRequest, iquanRequest));
        ASSERT_EQ(5, iquanRequest.sqlParams.size());
        EXPECT_EQ("second", AnyCast<std::string>(iquanRequest.sqlParams["iquan.second"]));
        ASSERT_EQ(3, iquanRequest.execParams.size());
        EXPECT_EQ("jni.post.optimize",
                  AnyCast<std::string>(iquanRequest.sqlParams["iquan.plan.prepare.level"]));
        EXPECT_EQ("true", AnyCast<std::string>(iquanRequest.sqlParams["iquan.plan.cache.enable"]));
        EXPECT_EQ("123", iquanRequest.execParams[SQL_IQUAN_EXEC_PARAMS_SOURCE_ID]);
        EXPECT_EQ("a123", iquanRequest.execParams[SQL_IQUAN_EXEC_PARAMS_SOURCE_SPEC]);
        EXPECT_EQ("sql_default", iquanRequest.execParams[SQL_IQUAN_EXEC_PARAMS_TASK_QUEUE]);
    }
}

TEST_F(SqlParseKernelTest, testTransToIquanRequest) {
    SqlConfigResourcePtr sqlConfigResource(new SqlConfigResource());
    sqlConfigResource->_sqlConfig.iquanPlanCacheEnable = true;
    sqlConfigResource->_sqlConfig.iquanPlanPrepareLevel = "test";
    IquanRPtr iquanR(new IquanR());
    SqlParseKernel sqlParser;
    sqlParser._sqlConfigResource = sqlConfigResource.get();
    sqlParser._iquanR = iquanR.get();
    {
        sql::SqlQueryRequest sqlRequest;
        iquan::IquanDqlRequest iquanRequest;
        sqlRequest.setSqlParams({
            {"iquan.first", "first"},
            {"iquan.second", "second"},
            {"iquan.third", "third"},
            {"iquan.source.id", "123"},
            {"exec.source.id", "exec.source.id"},
            {"exec.source.id2", "exec.source.id2"},
        });
        ASSERT_TRUE(sqlParser.transToIquanRequest(&sqlRequest, iquanRequest));
        ASSERT_EQ(10, iquanRequest.sqlParams.size());
        EXPECT_EQ("first", AnyCast<std::string>(iquanRequest.sqlParams["iquan.first"]));
        EXPECT_EQ("second", AnyCast<std::string>(iquanRequest.sqlParams["iquan.second"]));
        EXPECT_EQ("third", AnyCast<std::string>(iquanRequest.sqlParams["iquan.third"]));
        EXPECT_EQ("123", AnyCast<std::string>(iquanRequest.sqlParams["iquan.source.id"]));
        EXPECT_EQ("plan_version_0.0.1",
                  AnyCast<std::string>(iquanRequest.sqlParams["iquan.plan.format.version"]));
        EXPECT_EQ(
            "_summary_",
            AnyCast<std::string>(iquanRequest.sqlParams["iquan.optimizer.table.summary.suffix"]));
        EXPECT_EQ("test", AnyCast<std::string>(iquanRequest.sqlParams["iquan.plan.prepare.level"]));
        EXPECT_EQ("true", AnyCast<std::string>(iquanRequest.sqlParams["iquan.plan.cache.enable"]));

        EXPECT_EQ("exec.source.id", iquanRequest.execParams["exec.source.id"]);
        EXPECT_EQ("exec.source.id2", iquanRequest.execParams["exec.source.id2"]);
    }
    {
        sql::SqlQueryRequest sqlRequest;
        iquan::IquanDqlRequest iquanRequest;
        sqlRequest.setSqlParams({
            {"dynamic_params", "[[100,"},
        });
        ASSERT_FALSE(sqlParser.transToIquanRequest(&sqlRequest, iquanRequest));
    }
}

TEST_F(SqlParseKernelTest, testAddIquanDynamicParams) {
    SqlParseKernel sqlParser;
    {
        sql::SqlQueryRequest sqlRequest;
        iquan::DynamicParams dynamicParams;
        sqlRequest.setSqlParams({
            {"dynamic_params", "[[100, \"abc\"]]"},
        });
        ASSERT_TRUE(sqlParser.addIquanDynamicParams(&sqlRequest, dynamicParams));
        ASSERT_EQ(1u, dynamicParams._array.size());
        ASSERT_EQ(2U, dynamicParams._array[0].size());
        EXPECT_EQ(100, JsonNumberCast<int>(dynamicParams.at(0)));
        EXPECT_EQ("abc", AnyCast<string>(dynamicParams.at(1)));
    }
    {
        sql::SqlQueryRequest sqlRequest;
        iquan::DynamicParams dynamicParams;
        sqlRequest.setSqlParams({
            {"dynamic_params", "%5b%5b100%2c+%22abc%22%5d%5d"},
            {"urlencode_data", "true"},
        });
        ASSERT_TRUE(sqlParser.addIquanDynamicParams(&sqlRequest, dynamicParams));
        ASSERT_EQ(1u, dynamicParams._array.size());
        ASSERT_EQ(2U, dynamicParams._array[0].size());
        EXPECT_EQ(100, JsonNumberCast<int>(dynamicParams.at(0)));
        EXPECT_EQ("abc", AnyCast<string>(dynamicParams.at(1)));
    }
    {
        sql::SqlQueryRequest sqlRequest;
        iquan::DynamicParams dynamicParams;
        sqlRequest.setSqlParams({
            {"dynamic_params", "[[100"},
        });
        ASSERT_FALSE(sqlParser.addIquanDynamicParams(&sqlRequest, dynamicParams));
    }
    {
        sql::SqlQueryRequest sqlRequest;
        iquan::DynamicParams dynamicParams;
        std::string qinfo = autil::legacy::Base64EncodeFast("fg_user:k1\\:v1;k2\\:v2");
        sqlRequest.setSqlParams({
            {"dynamic_params", "[[100, \"" + qinfo + "\"]]"},
        });
        ASSERT_TRUE(sqlParser.addIquanDynamicParams(&sqlRequest, dynamicParams));
        ASSERT_EQ(1u, dynamicParams._array.size());
        ASSERT_EQ(2U, dynamicParams._array[0].size());
        EXPECT_EQ(100, JsonNumberCast<int>(dynamicParams.at(0)));
        EXPECT_EQ(qinfo, AnyCast<string>(dynamicParams.at(1)));
    }
    {
        sql::SqlQueryRequest sqlRequest;
        iquan::DynamicParams dynamicParams;
        std::string qinfo = autil::legacy::Base64EncodeFast("fg_user:k1\\:v1;k2\\:v2");
        sqlRequest.setSqlParams({{"dynamic_params", "[[100, \"" + qinfo + "\", {\"key\":\"k1\"}]]"},
                                 {"dynamic_kv_params", "[{\"k1\":\"v1\"}]"},
                                 {"hint_params", "part:1,2;id:3,4"}});
        ASSERT_TRUE(sqlParser.addIquanDynamicParams(&sqlRequest, dynamicParams));
        EXPECT_EQ(100, JsonNumberCast<int>(dynamicParams.at(0)));
        EXPECT_EQ(qinfo, AnyCast<string>(dynamicParams.at(1)));
        EXPECT_EQ("v1", AnyCast<string>(dynamicParams.at(2)));
        ASSERT_EQ("1,2", dynamicParams.getHintParam("part"));
        ASSERT_EQ("3,4", dynamicParams.getHintParam("id"));
    }
}

TEST_F(SqlParseKernelTest, testAddIquanDynamicKVParams) {
    SqlParseKernel sqlParser;
    {
        sql::SqlQueryRequest sqlRequest;
        iquan::DynamicParams dynamicParams;
        sqlRequest.setSqlParams({{"dynamic_kv_params", "[{\"k1\":\"v1\",\"k2\":2}]"}});
        ASSERT_TRUE(sqlParser.addIquanDynamicKVParams(&sqlRequest, dynamicParams));
        ASSERT_EQ(1u, dynamicParams._map.size());
        ASSERT_EQ(2U, dynamicParams._map[0].size());
        EXPECT_EQ("v1", AnyCast<string>(dynamicParams._map[0]["k1"]));
        EXPECT_EQ(2, AnyCast<int>(dynamicParams._map[0]["k2"]));
    }
    {
        sql::SqlQueryRequest sqlRequest;
        iquan::DynamicParams dynamicParams;
        sqlRequest.setSqlParams({
            {"dynamic_kv_params", autil::UrlEncode::encode("[{\"k1\":\"v1\",\"k2\":2}]")},
            {SQL_URLENCODE_DATA, "true"},
        });
        ASSERT_TRUE(sqlParser.addIquanDynamicKVParams(&sqlRequest, dynamicParams));
        ASSERT_EQ(1u, dynamicParams._map.size());
        ASSERT_EQ(2U, dynamicParams._map[0].size());
        EXPECT_EQ("v1", AnyCast<string>(dynamicParams._map[0]["k1"]));
        EXPECT_EQ(2, AnyCast<int>(dynamicParams._map[0]["k2"]));
    }
}

TEST_F(SqlParseKernelTest, testAddIquanHintParams) {
    SqlParseKernel sqlParser;
    {
        sql::SqlQueryRequest sqlRequest;
        iquan::DynamicParams dynamicParams;
        sqlRequest.setSqlParams({
            {"hint_params", "part:1,2;id:3,4"},
        });
        sqlParser.addIquanHintParams(&sqlRequest, dynamicParams);
        ASSERT_EQ(2u, dynamicParams._hint.size());
        ASSERT_TRUE(dynamicParams.findHintParam("part"));
        ASSERT_EQ("1,2", dynamicParams.getHintParam("part"));
        ASSERT_TRUE(dynamicParams.findHintParam("id"));
        ASSERT_EQ("3,4", dynamicParams.getHintParam("id"));
    }
}

TEST_F(SqlParseKernelTest, testAddUserKv) {
    SqlParseKernel sqlParser;
    {
        std::map<string, string> execParams;
        sqlParser.addUserKv({}, execParams);
        ASSERT_TRUE(execParams.empty());
    }
    {
        std::map<string, string> execParams;
        sqlParser.addUserKv({{SQL_FORBIT_MERGE_SEARCH_INFO, "abc"}}, execParams);
        string targetStr = SQL_FORBIT_MERGE_SEARCH_INFO + string("#abc,");
        ASSERT_EQ(targetStr, execParams[SQL_IQUAN_EXEC_PARAMS_USER_KV]);
    }
    {
        std::map<string, string> execParams;
        execParams[SQL_IQUAN_EXEC_PARAMS_USER_KV] = "AAA";
        sqlParser.addUserKv({{SQL_FORBIT_MERGE_SEARCH_INFO, "abc"}}, execParams);
        string targetStr = string("AAA,") + SQL_FORBIT_MERGE_SEARCH_INFO + string("#abc,");
        ASSERT_EQ(targetStr, execParams[SQL_IQUAN_EXEC_PARAMS_USER_KV]);
    }
}

TEST_F(SqlParseKernelTest, testGetDefaultCatalogName) {
    SqlParseKernel sqlParser;
    SqlConfigResourcePtr sqlConfigResource(new SqlConfigResource());
    sqlParser._sqlConfigResource = sqlConfigResource.get();
    {
        sql::SqlQueryRequest sqlRequest;
        string name = sqlParser.getDefaultCatalogName(&sqlRequest);
        ASSERT_EQ(SQL_DEFAULT_CATALOG_NAME, name);
    }
    {
        sql::SqlQueryRequest sqlRequest;
        sqlRequest.setSqlParams({{SQL_CATALOG_NAME, "AAA"}});
        string name = sqlParser.getDefaultCatalogName(&sqlRequest);
        ASSERT_EQ("AAA", name);
    }
    {
        sql::SqlQueryRequest sqlRequest;
        sqlParser._sqlConfigResource->_sqlConfig.catalogName = "abc";
        string name = sqlParser.getDefaultCatalogName(&sqlRequest);
        ASSERT_EQ("abc", name);
    }
}

TEST_F(SqlParseKernelTest, testGetDefaultDatabaseName) {
    SqlParseKernel sqlParser;
    SqlConfigResourcePtr sqlConfigResource(new SqlConfigResource());
    sqlParser._sqlConfigResource = sqlConfigResource.get();
    {
        sql::SqlQueryRequest sqlRequest;
        string name = sqlParser.getDefaultDatabaseName(&sqlRequest);
        ASSERT_EQ("", name);
    }
    {
        sql::SqlQueryRequest sqlRequest;
        sqlRequest.setSqlParams({{SQL_DATABASE_NAME, "AAA"}});
        string name = sqlParser.getDefaultDatabaseName(&sqlRequest);
        ASSERT_EQ("AAA", name);
    }
    {
        sql::SqlQueryRequest sqlRequest;
        sqlRequest.setSqlParams({{SQL_DATABASE_NAME, "AAA"}});
        sqlParser._dbNameAlias = {{"AAA", "BBB"}};
        string name = sqlParser.getDefaultDatabaseName(&sqlRequest);
        ASSERT_EQ("BBB", name);
        sqlParser._dbNameAlias = {};
    }
    {
        sql::SqlQueryRequest sqlRequest;
        sqlParser._dbName = "abc";
        string name = sqlParser.getDefaultDatabaseName(&sqlRequest);
        ASSERT_EQ("abc", name);
    }
    {
        sql::SqlQueryRequest sqlRequest;
        sqlParser._dbName = "abc";
        sqlParser._dbNameAlias = {{"AAA", "BBB"}, {"abc", "def"}};
        string name = sqlParser.getDefaultDatabaseName(&sqlRequest);
        ASSERT_EQ("def", name);
        sqlParser._dbNameAlias = {};
    }
}

} // namespace sql
