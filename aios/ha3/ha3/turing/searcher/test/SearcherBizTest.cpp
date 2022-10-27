#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include <ha3/turing/searcher/SearcherBiz.h>
#include <suez/turing/common/BizInfo.h>
#include <suez/common/PathDefine.h>
#include <autil/legacy/json.h>

using namespace std;
using namespace testing;
using namespace autil::legacy;
using namespace autil::legacy::json;

BEGIN_HA3_NAMESPACE(turing);

class SearcherBizTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
protected:
    config::ConfigAdapterPtr _configAdapterPtr;
private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(searcher, SearcherBizTest);

void SearcherBizTest::setUp() {
    HA3_LOG(DEBUG, "setUp!");
    string configRoot = TEST_DATA_PATH"/SearcherBizConfig/1";
    _configAdapterPtr.reset(new config::ConfigAdapter(configRoot));
}

void SearcherBizTest::tearDown() {
}

TEST_F(SearcherBizTest, testSimple) {
    suez::ServiceInfo serviceInfo;
    serviceInfo._zoneName = "daogou";
    turing::SearcherBiz biz;
    biz._bizName = "default";
    biz._configAdapter = _configAdapterPtr;
    biz._serviceInfo = serviceInfo;
    std::string runtimeDir = GET_TEMPLATE_DATA_PATH();
    std::string expected = R"json({
"arpc_timeout":
  500,
"biz_name":
  "daogou",
"cava_config":
  {
  "alloc_size_limit":
    40,
  "cava_conf":
    "..\/binary\/usr\/local\/etc\/ha3\/ha3_cava_config.json",
  "code_cache_path":
    "",
  "enable":
    false,
  "enable_query_code":
    false,
  "lib_path":
    "",
  "module_cache_size":
    100000,
  "source_path":
    "",
"compile_queue_size" : 512,
"compile_thread_num" : 1,
"compile_thread_ratio" : 0,
"pool_recycle_size_limit" : 20,
"pool_trunk_size" : 10,
"init_size_limit" : 1024
  },
"dependency_table":
  [
    "daogou",
    "company",
    "category"
  ],
"graph_config_path": "$$$",
"item_table_name":
  "sample",
"join_infos":
  [
    {
    "check_cluster_config":
      true,
    "join_cluster":
      "company",
    "join_field":
      "company_id",
    "strong_join":
      false,
    "use_join_cache":
      false,
    "use_join_pk":
      true
    },
    {
    "check_cluster_config":
      true,
    "join_cluster":
      "category",
    "join_field":
      "cat_id",
    "strong_join":
      false,
    "use_join_cache":
      false,
    "use_join_pk":
      true
    }
  ],
"need_log_query":
  false,
"ops_config_path":
  "",
"plugin_config":
  {
  "function_conf":
    "_func_conf_.json",
  "sorter_conf":
    "_sorter_conf_.json"
  },
"pool_config":
  {
  "pool_max_count":
    200,
  "pool_recycle_size_limit":
    20,
  "pool_trunk_size":
    10
  },
"run_options":
  {
  "interOpThreadPool":
    -1
  },
"session_count":
  1,
"version_config":
  {
  "data_version":
    "",
  "protocol_version":
    ""
  }
})json";
    autil::StringUtil::replaceAll(expected, "$$$", runtimeDir + "/../binary/usr/local/etc/ha3/searcher_default.def");

    string actual;
    bool ret = biz.getDefaultBizJson(actual);
    ASSERT_TRUE(ret);
    ASSERT_EQ(ToString(ParseJson(expected)), ToString(ParseJson(actual)));
}

TEST_F(SearcherBizTest, testThreadNumber) {
    suez::ServiceInfo serviceInfo;
    serviceInfo._zoneName = "daogou";
    turing::SearcherBiz biz;
    biz._bizName = "default";
    biz._configAdapter = _configAdapterPtr;
    biz._serviceInfo = serviceInfo;
    std::string bizJson;
    bool ret = biz.getDefaultBizJson(bizJson);
    ASSERT_TRUE(ret);
    suez::turing::BizInfo bizInfo;
    FromJsonString(bizInfo, bizJson);
    ASSERT_EQ(0, bizInfo._sessionConfig.inter_op_parallelism_threads());
}

TEST_F(SearcherBizTest, testGetDefaultGraphDefPath) {
    const string &workDir = suez::PathDefine::getCurrentPath();
    {
        const string &oldEnv = sap::EnvironUtil::getEnv("binaryPath", "");
        sap::EnvironUtil::setEnv("binaryPath", "");
        SearcherBiz biz;
        string expectPath = workDir +
                            "/../binary/usr/local/etc/ha3/searcher_default.def";
        ASSERT_EQ(expectPath, biz.getDefaultGraphDefPath());
        sap::EnvironUtil::setEnv("binaryPath", oldEnv);
    }
    {
        const string &oldEnv = sap::EnvironUtil::getEnv("binaryPath", "");
        sap::EnvironUtil::setEnv("binaryPath", "");
        SearcherBiz biz;
        biz._bizName = "para2";
        string expectPath = workDir +
                            "/../binary/usr/local/etc/ha3/searcher_default.def";
        ASSERT_EQ(expectPath, biz.getDefaultGraphDefPath());
        sap::EnvironUtil::setEnv("binaryPath", oldEnv);
    }
    {
        const string &oldEnv = sap::EnvironUtil::getEnv("binaryPath", "");
        sap::EnvironUtil::setEnv("binaryPath", "test");
        SearcherBiz biz;
        biz._bizName = "para2";
        string expectPath = "test/usr/local/etc/ha3/searcher_default.def";
        ASSERT_EQ(expectPath, biz.getDefaultGraphDefPath());
        sap::EnvironUtil::setEnv("binaryPath", oldEnv);
    }
}

TEST_F(SearcherBizTest, testGetConfigZoneBizName) {
    SearcherBiz biz;
    ASSERT_EQ(string("."), biz.getConfigZoneBizName(""));
    ASSERT_EQ(string("defaultZone."), biz.getConfigZoneBizName("defaultZone"));
    biz._bizName = "test";
    ASSERT_EQ(string("defaultZone.test"), biz.getConfigZoneBizName("defaultZone"));
}

TEST_F(SearcherBizTest, testGetOutConfName) {
    SearcherBiz biz;
    ASSERT_EQ(string(""), biz.getOutConfName(""));
    ASSERT_EQ(string("_func_.json"), biz.getOutConfName("_func_.json"));
    biz._bizName = "test";
    ASSERT_EQ(string("_func_.json"), biz.getOutConfName("_func_.json"));
}

END_HA3_NAMESPACE(turing);
