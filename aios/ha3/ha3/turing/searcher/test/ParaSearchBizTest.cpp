#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include "ha3/turing/searcher/ParaSearchBiz.h"
#include "suez/turing/common/BizInfo.h"
#include <suez/common/PathDefine.h>
#include <autil/legacy/json.h>

using namespace std;
using namespace testing;
using namespace autil::legacy;
using namespace autil::legacy::json;

BEGIN_HA3_NAMESPACE(turing);

class ParaSearchBizTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(searcher, ParaSearchBizTest);

void ParaSearchBizTest::setUp() {
    HA3_LOG(DEBUG, "setUp!");
}

void ParaSearchBizTest::tearDown() {
    HA3_LOG(DEBUG, "tearDown!");
}

TEST_F(ParaSearchBizTest, testGetDefaultGraphDefPath) {
    ParaSearchBiz biz;
    const string &workDir = suez::PathDefine::getCurrentPath();
    {
        const string &oldEnv = sap::EnvironUtil::getEnv("binaryPath", "");
        sap::EnvironUtil::setEnv("binaryPath", "");
        string expectPath = workDir + "/../binary/usr/local/etc/ha3/searcher_.def";
        ASSERT_EQ(expectPath, biz.getDefaultGraphDefPath());
        sap::EnvironUtil::setEnv("binaryPath", oldEnv);
    }
    {
        const string &oldEnv = sap::EnvironUtil::getEnv("binaryPath", "");
        sap::EnvironUtil::setEnv("binaryPath", "");
        biz._bizName = "para2";
        string expectPath = workDir +
                            "/../binary/usr/local/etc/ha3/searcher_para2.def";
        ASSERT_EQ(expectPath, biz.getDefaultGraphDefPath());
        sap::EnvironUtil::setEnv("binaryPath", oldEnv);
    }
    {
        const string &oldEnv = sap::EnvironUtil::getEnv("binaryPath", "");
        sap::EnvironUtil::setEnv("binaryPath", "test");
        biz._bizName = "para2";
        string expectPath = "test/usr/local/etc/ha3/searcher_para2.def";
        ASSERT_EQ(expectPath, biz.getDefaultGraphDefPath());
        sap::EnvironUtil::setEnv("binaryPath", oldEnv);
    }
}

TEST_F(ParaSearchBizTest, testGetConfigZoneBizName) {
    ParaSearchBiz biz;
    ASSERT_EQ(string(".default"), biz.getConfigZoneBizName(""));
    ASSERT_EQ(string("paraZone.default"), biz.getConfigZoneBizName("paraZone"));
}

TEST_F(ParaSearchBizTest, testGetOutConfName) {
    ParaSearchBiz biz;
    ASSERT_EQ(string("para/"), biz.getOutConfName(""));
    ASSERT_EQ(string("para/_func_.json"), biz.getOutConfName("_func_.json"));
    biz._bizName = "test";
    ASSERT_EQ(string("para/test_func_.json"), biz.getOutConfName("_func_.json"));
}

TEST_F(ParaSearchBizTest, testSimple) {
    suez::ServiceInfo serviceInfo;
    serviceInfo._zoneName = "daogou";
    ParaSearchBiz biz;
    biz._bizName = "para_search_4";
    string configRoot = TEST_DATA_PATH"/SearcherBizConfig/2";
    config::ConfigAdapterPtr cap(new config::ConfigAdapter(configRoot));
    biz._configAdapter = cap;
    biz._serviceInfo = serviceInfo;
    string runtimeDir = GET_TEMPLATE_DATA_PATH();

    string expected = R"json({
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
  "compile_queue_size":
    512,
  "compile_thread_num":
    1,
  "compile_thread_ratio":
    0,
  "enable":
    false,
  "enable_query_code":
    false,
  "init_size_limit":
    1024,
  "lib_path":
    "",
  "module_cache_size":
    100000,
  "pool_recycle_size_limit":
    20,
  "pool_trunk_size":
    10,
  "source_path":
    ""
  },
"dependency_table":
  [
    "daogou",
    "company",
    "category"
  ],
"graph_config_path":
  "$$$",
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
    "para\/para_search_4_func_conf_.json",
  "sorter_conf":
    "para\/para_search_4_sorter_conf_.json"
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
    0
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
    autil::StringUtil::replaceAll(expected, "$$$", runtimeDir + "/../binary/usr/local/etc/ha3/searcher_para_search_4.def");

    string actual;
    bool ret = biz.getDefaultBizJson(actual);
    ASSERT_TRUE(ret);
    ASSERT_EQ(ToString(ParseJson(expected)), ToString(ParseJson(actual)));
}

END_HA3_NAMESPACE(turing);
