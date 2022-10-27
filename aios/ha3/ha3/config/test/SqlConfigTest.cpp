#include<unittest/unittest.h>
#include <ha3/config/SqlConfig.h>
#include <suez/turing/common/FileUtil.h>
#include <ha3/test/test.h>
#include <string>
#include <autil/legacy/jsonizable.h>

using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace suez::turing;
BEGIN_HA3_NAMESPACE(config);

class SqlConfigTest : public TESTBASE {
public:
    SqlConfigTest() {}
    ~SqlConfigTest() {}
public:
    void setUp();
    void tearDown();
protected:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(config, SqlConfigTest);

void SqlConfigTest::setUp() {
    HA3_LOG(DEBUG, "setUp!");
}

void SqlConfigTest::tearDown() {
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(SqlConfigTest, testJsonize) {
    HA3_LOG(DEBUG, "Begin Test!");
    string str = FileUtil::readFile(TEST_DATA_PATH"/sql_config_test/simple.json");
    SqlConfig sqlConfig;
    FromJsonString(sqlConfig, str);

    ASSERT_EQ("gg", sqlConfig.dbName);
    ASSERT_EQ("json", sqlConfig.outputFormat);
    ASSERT_EQ(10, sqlConfig.parallelNum);
    ASSERT_EQ(2000, sqlConfig.sqlTimeout);
    ASSERT_EQ(0.1, sqlConfig.timeoutFactor);
    ASSERT_EQ(0.2, sqlConfig.subGraphTimeoutFactor);
    ASSERT_EQ(3, sqlConfig.subGraphThreadLimit);
    ASSERT_EQ(2, sqlConfig.mainGraphThreadLimit);
    ASSERT_EQ(2, sqlConfig.parallelTables.size());
    ASSERT_EQ("1", sqlConfig.parallelTables[0]);
    ASSERT_EQ("2", sqlConfig.parallelTables[1]);
    ASSERT_EQ("aaaaa", sqlConfig.iquanPlanPrepareLevel);
    ASSERT_TRUE(sqlConfig.iquanPlanCacheEnable);
    // test ClientConfig
    iquan::ClientConfig &clientConfig = sqlConfig.clientConfig;
    std::map<std::string, iquan::CacheConfig> &cacheConfigs = clientConfig.cacheConfigs;
    ASSERT_TRUE(clientConfig.debugFlag);
    ASSERT_EQ(1, cacheConfigs.size());
    ASSERT_EQ(100, cacheConfigs["sql.parse"].initialCapcity);
    ASSERT_EQ(200, cacheConfigs["sql.parse"].concurrencyLevel);
    ASSERT_EQ(300, cacheConfigs["sql.parse"].maxSize);
    ASSERT_EQ(400, cacheConfigs["sql.parse"].expireAfterWriteSec);

    // test JniConfig
    iquan::JniConfig &jniConfig = sqlConfig.jniConfig;
    ASSERT_EQ("fake_summary", jniConfig.tableConfig.summaryTableSuffix);

    // test WarmupConfig
    iquan::WarmupConfig &warmupConfig = sqlConfig.warmupConfig;
    ASSERT_EQ(100, warmupConfig.threadNum);
    ASSERT_EQ(200, warmupConfig.warmupSeconds);
    ASSERT_EQ(300, warmupConfig.warmupQueryNum);

    // test KMonConfig
    iquan::KMonConfig &kmonConfig = sqlConfig.kmonConfig;
    ASSERT_EQ(100, kmonConfig.flumePeriod);
    ASSERT_EQ(1000, kmonConfig.flumeQueueCapacity);
    ASSERT_EQ("cluster", kmonConfig.kmonService);
    ASSERT_EQ("pre", kmonConfig.kmonTenantName);
    ASSERT_EQ("ha3", kmonConfig.serviceName);
    ASSERT_EQ("test_topic", kmonConfig.flumeTopic);
    ASSERT_EQ("11.22.33.44:8888", kmonConfig.flumeAddress);
}

TEST_F(SqlConfigTest, testJsonizeEmpty) {
    HA3_LOG(DEBUG, "Begin Test!");
    string str = "{}";
    SqlConfig sqlConfig;
    FromJsonString(sqlConfig, str);

    ASSERT_EQ("", sqlConfig.dbName);
    ASSERT_EQ("", sqlConfig.outputFormat);
    ASSERT_EQ(DEFAULT_PARALLEL_NUM, sqlConfig.parallelNum);
    ASSERT_EQ(DEFAULT_SQL_TIMEOUT, sqlConfig.sqlTimeout);
    ASSERT_EQ(DEFAULT_SQL_TIMEOUT_FACTOR, sqlConfig.timeoutFactor);
    ASSERT_EQ(DEFAULT_SUB_GRAPH_TIMEOUT_FACTOR, sqlConfig.subGraphTimeoutFactor);
    ASSERT_EQ(DEFAULT_SUB_GRAPH_THREAD_LIMIT, sqlConfig.subGraphThreadLimit);
    ASSERT_EQ(DEFAULT_MAIN_GRAPH_THREAD_LIMIT, sqlConfig.mainGraphThreadLimit);
    ASSERT_EQ(0, sqlConfig.parallelTables.size());
    ASSERT_EQ("jni.post.optimize", sqlConfig.iquanPlanPrepareLevel);
    ASSERT_FALSE(sqlConfig.iquanPlanCacheEnable);

    // test ClientConfig
    iquan::ClientConfig &clientConfig = sqlConfig.clientConfig;
    std::map<std::string, iquan::CacheConfig> &cacheConfigs = clientConfig.cacheConfigs;
    ASSERT_FALSE(clientConfig.debugFlag);
    ASSERT_EQ(4, cacheConfigs.size());
    {
        ASSERT_EQ(1024, cacheConfigs["sql.parse"].initialCapcity);
        ASSERT_EQ(8, cacheConfigs["sql.parse"].concurrencyLevel);
        ASSERT_EQ(4096, cacheConfigs["sql.parse"].maxSize);
        ASSERT_EQ(600, cacheConfigs["sql.parse"].expireAfterWriteSec);

        ASSERT_EQ(1024, cacheConfigs["rel.transform"].initialCapcity);
        ASSERT_EQ(8, cacheConfigs["rel.transform"].concurrencyLevel);
        ASSERT_EQ(4096, cacheConfigs["rel.transform"].maxSize);
        ASSERT_EQ(600, cacheConfigs["rel.transform"].expireAfterWriteSec);

        ASSERT_EQ(1024, cacheConfigs["rel.post.optimize"].initialCapcity);
        ASSERT_EQ(8, cacheConfigs["rel.post.optimize"].concurrencyLevel);
        ASSERT_EQ(4096, cacheConfigs["rel.post.optimize"].maxSize);
        ASSERT_EQ(600, cacheConfigs["rel.post.optimize"].expireAfterWriteSec);

        ASSERT_EQ(1024, cacheConfigs["jni.post.optimize"].initialCapcity);
        ASSERT_EQ(8, cacheConfigs["jni.post.optimize"].concurrencyLevel);
        ASSERT_EQ(100 * 1024 * 1024, cacheConfigs["jni.post.optimize"].maxSize);
        ASSERT_EQ(3600, cacheConfigs["jni.post.optimize"].expireAfterWriteSec);
    }

    // test JniConfig
    iquan::JniConfig &jniConfig = sqlConfig.jniConfig;
    ASSERT_EQ("_summary_", jniConfig.tableConfig.summaryTableSuffix);

    // test WarmupConfig
    iquan::WarmupConfig &warmupConfig = sqlConfig.warmupConfig;
    ASSERT_EQ(2, warmupConfig.threadNum);
    ASSERT_EQ(30, warmupConfig.warmupSeconds);
    ASSERT_EQ(1000000, warmupConfig.warmupQueryNum);

    // test KMonConfig
    iquan::KMonConfig &kmonConfig = sqlConfig.kmonConfig;
    ASSERT_EQ(15, kmonConfig.flumePeriod);
    ASSERT_EQ(10000, kmonConfig.flumeQueueCapacity);
    ASSERT_EQ("", kmonConfig.kmonService);
    ASSERT_EQ("", kmonConfig.kmonTenantName);
    ASSERT_EQ("", kmonConfig.serviceName);
    ASSERT_EQ("", kmonConfig.flumeTopic);
    ASSERT_EQ("127.0.0.1:4141", kmonConfig.flumeAddress);
}

END_HA3_NAMESPACE(config);
