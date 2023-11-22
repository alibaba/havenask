#include "indexlib/config/BuildConfig.h"
#include "indexlib/config/MergeConfig.h"
#include "indexlib/config/OfflineConfig.h"
#include "indexlib/config/OnlineConfig.h"
#include "indexlib/table/kv_table/test/KVTableTestHelper.h"
#include "indexlib/table/kv_table/test/KVTabletSchemaMaker.h"
#include "unittest/unittest.h"

using namespace std;

namespace indexlibv2 { namespace table {

class KVTabletCompatibleTest : public TESTBASE
{
public:
    KVTabletCompatibleTest();
    ~KVTabletCompatibleTest();

public:
    void setUp() override {}
    void tearDown() override {}
    void TestSimpleProcess();

private:
    std::shared_ptr<config::TabletOptions> CreateMultiShardOptions();

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(indexlib.table, KVTabletCompatibleTest);

KVTabletCompatibleTest::KVTabletCompatibleTest() {}

KVTabletCompatibleTest::~KVTabletCompatibleTest() {}

TEST_F(KVTabletCompatibleTest, TestCompatibleDefaultConfig)
{
    string field = "key:string;value:string";
    // do not delete these comments, to indicate how legacy index was built
    // auto schema = indexlib::test::SchemaMaker::MakeKVSchema(field, "key", "value");
    // auto options = indexlib::config::IndexPartitionOptions();
    // options.GetOnlineConfig().maxRealtimeDumpInterval = 10000000;
    // options.GetBuildConfig(true).buildTotalMemory = 22;
    // options.GetBuildConfig(false).buildTotalMemory = 40;
    // options.GetBuildConfig(false).keepVersionCount = 100;
    // string docString = "cmd=add,key=key1,value=1,ts=101000000;"
    //                    "cmd=add,key=key2,value=2,ts=102000000;"
    //                    "cmd=add,key=key3,value=3,ts=103000000;"
    //                    "cmd=add,key=key,value=31,ts=104000000"
    //                    "cmd=add,key=key,value=33,ts=104000000";
    // indexlib::test::PartitionStateMachine psm;
    // ASSERT_TRUE(psm.Init(schema, options, GET_TEMP_DATA_PATH()));
    // ASSERT_TRUE(psm.Transfer(indexlib::test::BUILD_FULL, docString, "key", "value=33"));

    std::string indexTar = GET_PRIVATE_TEST_DATA_PATH() + "/test_index/TestCompatibleDefaultConfig.tar";
    std::string cmd = "cp " + indexTar + " " + GET_TEMP_DATA_PATH();
    ASSERT_EQ(0, system(cmd.c_str()));
    cmd = "cd " + GET_TEMP_DATA_PATH() + " && tar -xf TestCompatibleDefaultConfig.tar";
    ASSERT_EQ(0, system(cmd.c_str()));

    auto tabletSchema = table::KVTabletSchemaMaker::Make(field, "key", "value");
    auto tabletOptions = std::make_shared<config::TabletOptions>();
    tabletOptions->SetIsOnline(true);
    tabletOptions->SetIsLeader(true);
    tabletOptions->SetFlushLocal(false);
    tabletOptions->SetFlushRemote(true);
    tabletOptions->TEST_GetOnlineConfig().TEST_GetBuildConfig().TEST_SetBuildingMemoryLimit(64 * 1024 * 1024);
    tabletOptions->TEST_GetOfflineConfig().TEST_GetMergeConfig().TEST_SetMergeStrategyStr("shard_based");
    framework::IndexRoot indexRoot(GET_TEMP_DATA_PATH(), GET_TEMP_DATA_PATH());
    KVTableTestHelper mainHelper;
    ASSERT_TRUE(mainHelper.Open(indexRoot, tabletSchema, std::move(tabletOptions), 1).IsOK());
    ASSERT_TRUE(mainHelper.Query("kv", "key", "key", "value=33"));
    ASSERT_TRUE(mainHelper.Query("kv", "key", "key1", "value=1"));
    ASSERT_TRUE(mainHelper.Query("kv", "key", "key2", "value=2"));
    ASSERT_TRUE(mainHelper.Query("kv", "key", "key3", "value=3"));

    string doc1 = "cmd=add,key=key1,value=11,ts=101000000;"
                  "cmd=add,key=key4,value=4,ts=101000000;";
    ASSERT_TRUE(mainHelper.BuildSegment(doc1).IsOK());
    ASSERT_TRUE(mainHelper.Query("kv", "key", "key", "value=33"));
    ASSERT_TRUE(mainHelper.Query("kv", "key", "key1", "value=11"));
    ASSERT_TRUE(mainHelper.Query("kv", "key", "key2", "value=2"));
    ASSERT_TRUE(mainHelper.Query("kv", "key", "key3", "value=3"));
    ASSERT_TRUE(mainHelper.Query("kv", "key", "key4", "value=4"));
}

std::shared_ptr<config::TabletOptions> KVTabletCompatibleTest::CreateMultiShardOptions()
{
    std::string jsonStr = R"( {
    "online_index_config": {
        "build_config": {
            "sharding_column_num" : 2,
            "level_num" : 2
        }
    }
    } )";
    auto tabletOptions = std::make_shared<config::TabletOptions>();
    FromJsonString(*tabletOptions, jsonStr);
    tabletOptions->SetIsOnline(true);
    tabletOptions->SetIsLeader(true);
    tabletOptions->SetFlushLocal(false);
    tabletOptions->SetFlushRemote(true);
    tabletOptions->TEST_GetOnlineConfig().TEST_GetBuildConfig().TEST_SetBuildingMemoryLimit(64 * 1024 * 1024);
    tabletOptions->TEST_GetOfflineConfig().TEST_GetMergeConfig().TEST_SetMergeStrategyStr("shard_based");
    return tabletOptions;
}

TEST_F(KVTabletCompatibleTest, TestCompatibleMergedSegment)
{
    // prepare old index
    string field = "key:string;value:string";
    // do not delete these comments, to indicate how legacy index was built
    // auto schema = indexlib::test::SchemaMaker::MakeKVSchema(field, "key", "value");
    // auto options = indexlib::config::IndexPartitionOptions();
    // options.GetOnlineConfig().maxRealtimeDumpInterval = 10000000;
    // options.GetBuildConfig(true).buildTotalMemory = 22;
    // options.GetBuildConfig(true).shardingColumnNum = 2;
    // options.GetBuildConfig(false).buildTotalMemory = 40;
    // options.GetBuildConfig(false).shardingColumnNum = 2;
    // options.GetBuildConfig(false).levelTopology = indexlibv2::framework::topo_hash_mod;
    // options.GetBuildConfig(false).levelNum = 2;
    // options.GetBuildConfig(false).keepVersionCount = 100;
    // options.GetBuildConfig(false).enablePackageFile = false;
    // options.GetBuildConfig(true).levelTopology = indexlibv2::framework::topo_hash_mod;
    // options.GetBuildConfig(true).levelNum = 2;
    // string docString = "cmd=add,key=key1,value=1,ts=101000000;"
    //                    "cmd=add,key=key2,value=2,ts=102000000;"
    //                    "cmd=add,key=key3,value=3,ts=103000000;"
    //                    "cmd=add,key=key,value=31,ts=104000000"
    //                    "cmd=add,key=key,value=33,ts=104000000";
    // indexlib::test::PartitionStateMachine psm;
    // ASSERT_TRUE(psm.Init(schema, options, GET_TEMP_DATA_PATH()));
    // ASSERT_TRUE(psm.Transfer(indexlib::test::BUILD_FULL, docString, "key", "value=33"));

    std::string indexTar = GET_PRIVATE_TEST_DATA_PATH() + "/test_index/TestCompatibleMergedSegment.tar";
    std::string cmd = "cp " + indexTar + " " + GET_TEMP_DATA_PATH();
    ASSERT_EQ(0, system(cmd.c_str()));
    cmd = "cd " + GET_TEMP_DATA_PATH() + " && tar -xf TestCompatibleMergedSegment.tar";
    ASSERT_EQ(0, system(cmd.c_str()));

    // use new binary load and search
    auto tabletSchema = table::KVTabletSchemaMaker::Make(field, "key", "value");
    auto tabletOptions = CreateMultiShardOptions();
    framework::IndexRoot indexRoot(GET_TEMP_DATA_PATH(), GET_TEMP_DATA_PATH());
    KVTableTestHelper mainHelper;
    ASSERT_TRUE(mainHelper.Open(indexRoot, tabletSchema, std::move(tabletOptions), 1).IsOK());
    ASSERT_TRUE(mainHelper.Query("kv", "key", "key", "value=33"));
    ASSERT_TRUE(mainHelper.Query("kv", "key", "key1", "value=1"));
    ASSERT_TRUE(mainHelper.Query("kv", "key", "key2", "value=2"));
    ASSERT_TRUE(mainHelper.Query("kv", "key", "key3", "value=3"));

    string doc1 = "cmd=add,key=key1,value=11,ts=101000000;"
                  "cmd=add,key=key4,value=4,ts=101000000;";
    ASSERT_TRUE(mainHelper.BuildSegment(doc1).IsOK());
    ASSERT_TRUE(mainHelper.Query("kv", "key", "key", "value=33"));
    ASSERT_TRUE(mainHelper.Query("kv", "key", "key1", "value=11"));
    ASSERT_TRUE(mainHelper.Query("kv", "key", "key2", "value=2"));
    ASSERT_TRUE(mainHelper.Query("kv", "key", "key3", "value=3"));
    ASSERT_TRUE(mainHelper.Query("kv", "key", "key4", "value=4"));

    ASSERT_TRUE(mainHelper.Merge().IsOK());
    ASSERT_TRUE(mainHelper.Query("kv", "key", "key", "value=33"));
    ASSERT_TRUE(mainHelper.Query("kv", "key", "key1", "value=11"));
    ASSERT_TRUE(mainHelper.Query("kv", "key", "key2", "value=2"));
    ASSERT_TRUE(mainHelper.Query("kv", "key", "key3", "value=3"));
    ASSERT_TRUE(mainHelper.Query("kv", "key", "key4", "value=4"));
}

}} // namespace indexlibv2::table
