#include "autil/HashAlgorithm.h"
#include "future_lite/CoroInterface.h"
#include "future_lite/executors/SimpleExecutor.h"
#include "indexlib/config/BuildConfig.h"
#include "indexlib/config/OnlineConfig.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/file_system/test/LoadConfigListCreator.h"
#include "indexlib/framework/IndexRoot.h"
#include "indexlib/table/kkv_table/KKVReaderImpl.h"
#include "indexlib/table/kkv_table/KKVTabletFactory.h"
#include "indexlib/table/kkv_table/KKVTabletSessionReader.h"
#include "indexlib/table/kkv_table/test/KKVTableTestHelper.h"
#include "indexlib/table/kkv_table/test/KKVTabletSchemaMaker.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;
using namespace indexlib;
using namespace indexlib::util;
using namespace indexlib::index_base;
using namespace indexlib::config;
using namespace indexlib::file_system;
using namespace indexlibv2::index;
using namespace indexlibv2::framework;

namespace indexlibv2::table {

class KKVTabletReaderTest : public TESTBASE
{
public:
    KKVTabletReaderTest() {}
    ~KKVTabletReaderTest() {}

public:
    void setUp() override;
    void tearDown() override;

private:
    void PrepareTableOptions(const std::string& jsonStr);
    void PrepareSchema(const std::string& filename);
    void DoTestBuild();
    void DoTestBuildAndSearch();

private:
    framework::IndexRoot _indexRoot;
    KKVTableTestHelper _testHelper;
    std::shared_ptr<autil::mem_pool::Pool> _pool;
    std::shared_ptr<config::ITabletSchema> _schema;
    std::shared_ptr<indexlibv2::config::KKVIndexConfig> _indexConfig;
    std::string _tableOptionsJsonStr;
    std::shared_ptr<config::TabletOptions> _tabletOptions;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(indexlib.index, KKVTabletReaderTest);

void KKVTabletReaderTest::setUp()
{
    setenv("DISABLE_CODEGEN", "true", 1);
    _indexRoot = framework::IndexRoot(GET_TEMP_DATA_PATH(), GET_TEMP_DATA_PATH());
    _pool = std::make_shared<autil::mem_pool::Pool>();

    _tableOptionsJsonStr = R"( {
            "online_index_config": {
                "build_config": {
                    "sharding_column_num" : 2,
                    "level_num" : 3
                }
            }
        } )";
    PrepareTableOptions(_tableOptionsJsonStr);
    PrepareSchema("kkv_schema.json");
}

void KKVTabletReaderTest::tearDown() { unsetenv("DISABLE_CODEGEN"); }

void KKVTabletReaderTest::PrepareTableOptions(const std::string& jsonStr)
{
    _tabletOptions = KKVTableTestHelper::CreateTableOptions(jsonStr);
    _tabletOptions->SetIsOnline(true);
}

void KKVTabletReaderTest::PrepareSchema(const std::string& filename)
{
    _schema = KKVTableTestHelper::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), filename);
    _indexConfig = KKVTableTestHelper::GetIndexConfig(_schema, "pkey");
}

void KKVTabletReaderTest::DoTestBuild()
{
    KKVTableTestHelper& helper = _testHelper;
    ASSERT_TRUE(helper.Open(_indexRoot, _schema, _tabletOptions).IsOK());
    auto tablet = helper.GetITablet();
    ASSERT_TRUE(helper.Build("cmd=add,pkey=1,skey=1,value=1,ts=10;").IsOK());
    ASSERT_TRUE(helper.Build("cmd=add,pkey=2,skey=2,value=2,ts=10;").IsOK());
    ASSERT_TRUE(helper.Build("cmd=add,pkey=3,skey=3,value=3,ts=10;").IsOK());
    ASSERT_TRUE(helper.Build("cmd=add,pkey=3,skey=31,value=31,ts=10;").IsOK());
    ASSERT_TRUE(helper.Build("cmd=add,pkey=4,skey=4,value=4,ts=10;").IsOK());
    ASSERT_TRUE(helper.Build("cmd=delete,pkey=1,ts=100;").IsOK());
    ASSERT_TRUE(helper.Build("cmd=delete,pkey=4,skey=4,ts=100;").IsOK());
    ASSERT_TRUE(helper.Build("cmd=add,pkey=5,skey=5,value=5,ts=10;").IsOK());
    ASSERT_TRUE(helper.Build("cmd=add,pkey=5,skey=51,value=51,ts=10;").IsOK());
    ASSERT_TRUE(helper.Build("cmd=delete,pkey=5,skey=51,ts=100;").IsOK());
    ASSERT_TRUE(tablet->Seal().IsOK());
    ASSERT_TRUE(tablet->NeedCommit());
    auto pair = tablet->Commit(framework::CommitOptions().SetNeedPublish(true));
    ASSERT_TRUE(pair.first.IsOK());
    ASSERT_TRUE(tablet->Reopen(framework::ReopenOptions(), pair.second.GetVersionId()).IsOK());
}

void KKVTabletReaderTest::DoTestBuildAndSearch()
{
    DoTestBuild();
    KKVTableTestHelper& helper = _testHelper;
    ASSERT_TRUE(helper.Query("kkv", "pkey", "1", ""));
    ASSERT_TRUE(helper.Query("kkv", "pkey", "2", "skey=2,value=2"));
    ASSERT_TRUE(helper.Query("kkv", "pkey", "4", ""));
    ASSERT_TRUE(helper.Query("kkv", "pkey", "5", "skey=5,value=5"));
}

TEST_F(KKVTabletReaderTest, TestSimple)
{
    {
        std::string tabletOptionsStr = R"( {
            "online_index_config": {
                "build_config": {
                    "sharding_column_num" : 2,
                    "level_num" : 3,
                    "building_memory_limit_mb": 4,
                    "max_doc_count": 4
                }
            }
        } )";
        PrepareTableOptions(tabletOptionsStr);
        DoTestBuildAndSearch();
    }
    // every build trigger dump
    {
        std::string tabletOptionsStr = R"( {
            "online_index_config": {
                "build_config": {
                    "sharding_column_num" : 2,
                    "level_num" : 3,
                    "building_memory_limit_mb": 4,
                    "max_doc_count": 1
                }
            }
        } )";
        PrepareTableOptions(tabletOptionsStr);
        DoTestBuildAndSearch();
    }
    // without dump
    {
        std::string tabletOptionsStr = R"( {
            "online_index_config": {
                "build_config": {
                    "sharding_column_num" : 2,
                    "level_num" : 3,
                    "building_memory_limit_mb": 4,
                    "max_doc_count": 10240
                }
            }
        } )";
        PrepareTableOptions(tabletOptionsStr);
        DoTestBuildAndSearch();
    }
}

TEST_F(KKVTabletReaderTest, TestBlockCache)
{
    _tableOptionsJsonStr = R"({
    "online_index_config": {
        "build_config": {
            "sharding_column_num": 2,
            "level_num": 3,
            "building_memory_limit_mb": 4,
            "max_doc_count": 4
        },
        "load_config": [
            {
                "file_patterns": [
                    "pkey",
                    "skey",
                    "value"
                ],
                "load_strategy": "cache",
                "load_strategy_param": {
                    "global_cache": true
                }
            }
        ]
    }
    })";
    PrepareTableOptions(_tableOptionsJsonStr);
    DoTestBuild();

    KKVTableTestHelper& helper = _testHelper;

    auto collector = helper.GetMetricsCollector();
    ASSERT_TRUE(helper.Query("kkv", "pkey", "2", "skey=2,value=2"));
    ASSERT_GT(collector->GetBlockCacheMissCount(), 0);
    // may cache hit

    ASSERT_TRUE(helper.Query("kkv", "pkey", "2", "skey=2,value=2"));
    ASSERT_EQ(collector->GetBlockCacheMissCount(), 0);
    ASSERT_GT(collector->GetBlockCacheHitCount(), 0);
}

} // namespace indexlibv2::table
