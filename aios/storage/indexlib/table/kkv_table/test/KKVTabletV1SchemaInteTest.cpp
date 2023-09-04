#include "fslib/fs/ExceptionTrigger.h"
#include "fslib/fs/FileSystem.h"
#include "indexlib/base/PathUtil.h"
#include "indexlib/config/BuildConfig.h"
#include "indexlib/config/OfflineConfig.h"
#include "indexlib/config/OnlineConfig.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/config/UnresolvedSchema.h"
#include "indexlib/file_system/load_config/LoadConfigList.h"
#include "indexlib/framework/Fence.h"
#include "indexlib/framework/Tablet.h"
#include "indexlib/framework/VersionLoader.h"
#include "indexlib/framework/mock/MockIdGenerator.h"
#include "indexlib/framework/test/TabletTestAgent.h"
#include "indexlib/index/IndexFactoryCreator.h"
#include "indexlib/index/kkv/config/KKVIndexConfig.h"
#include "indexlib/table/kkv_table/KKVTabletFactory.h"
#include "indexlib/table/kkv_table/test/KKVTableTestHelper.h"
#include "indexlib/table/kkv_table/test/KKVTabletSchemaMaker.h"
#include "indexlib/table/plain/MultiShardDiskSegment.h"
#include "indexlib/util/testutil/unittest.h"

using namespace std;
using fslib::fs::ExceptionTrigger;
using namespace indexlibv2::index;
using namespace indexlibv2::framework;

namespace indexlibv2 { namespace table {

// TODO(qisa.cb) remove this class after we have fully tested (V1 schema == V2 schema)
class KKVTabletV1SchemaInteTest : public TESTBASE
{
public:
    KKVTabletV1SchemaInteTest() {}
    ~KKVTabletV1SchemaInteTest() = default;

    void setUp() override
    {
        _tableOptionsJsonStr = R"( {
        "online_index_config": {
            "build_config": {
                "sharding_column_num" : 2,
                "level_num" : 3,
                "building_memory_limit_mb": 8,
                "need_deploy_index": false,
                "need_read_remote_index": true
            }
        }
        } )";

        PrepareTableOptions(_tableOptionsJsonStr);
        PrepareSchema("kkv_schema_v1.json");
    }
    std::string docString(std::vector<std::string> docPrefix)
    {
        std::string ret;
        for (auto prefix : docPrefix) {
            _offset++;
            ret += prefix + std::to_string(_offset) + ";";
        }
        return ret;
    }

    std::string docString1()
    {
        std::vector<std::string> docPrefix = {"cmd=add,pkey=1,skey=1,value=v1,ts=1000000,locator=0:",
                                              "cmd=add,pkey=2,skey=2,value=v2,ts=2000000,locator=0:",
                                              "cmd=add,pkey=3,skey=3,value=v3,ts=3000000,locator=0:",
                                              "cmd=add,pkey=321,skey=1132767,value=V321,ts=3000000,locator=0:",
                                              "cmd=add,pkey=4,skey=4,value=v4,ts=4000000,locator=0:"};
        return docString(docPrefix);
    }

    std::string docString2()
    {
        std::vector<std::string> docPrefix = {"cmd=add,pkey=1,skey=11,value=v11,ts=5000000,locator=0:",
                                              "cmd=add,pkey=12,skey=12,value=Av12,ts=6000000,locator=0:",
                                              "cmd=add,pkey=3,skey=31,value=v31,ts=7000000,locator=0:",
                                              "cmd=add,pkey=14,skey=14,value=Av14,ts=8000000,locator=0:",
                                              "cmd=add,pkey=15,skey=15,value=v15,ts=9000000,locator=0:",
                                              "cmd=delete,pkey=15,skey=15,ts=10000000,locator=0:",
                                              "cmd=delete,pkey=2,ts=13000000,locator=0:"};
        return docString(docPrefix);
    }
    std::string docString3()
    {
        std::vector<std::string> docPrefix = {"cmd=add,pkey=1,skey=12,value=v12,ts=12000000,locator=0:",
                                              "cmd=add,pkey=12,skey=121,value=v121,ts=13000000,locator=0:",
                                              "cmd=add,pkey=3,skey=32,value=Av32,ts=14000000,locator=0:",
                                              "cmd=add,pkey=15,skey=15,value=v151,ts=15000000,locator=0:",
                                              "cmd=delete,pkey=3,skey=3,ts=15000000,locator=0:"};
        return docString(docPrefix);
    }
    void PrepareSchema(const std::string& path);
    void PrepareTableOptions(const std::string& jsonStr);

    void tearDown() override {}

private:
    void CheckLevelInfo(const std::shared_ptr<Tablet>& tablet, uint32_t levelCount, uint32_t shardCount,
                        LevelTopology topology, std::string levelSegments);

private:
    std::shared_ptr<config::TabletOptions> _tabletOptions;
    std::string _tableOptionsJsonStr;
    std::shared_ptr<config::ITabletSchema> _schema;
    std::shared_ptr<indexlibv2::config::KKVIndexConfig> _indexConfig;
    int64_t _offset = 0;
};

void KKVTabletV1SchemaInteTest::PrepareTableOptions(const std::string& jsonStr)
{
    _tabletOptions = KKVTableTestHelper::CreateTableOptions(jsonStr);
    _tabletOptions->SetIsOnline(true);
}

void KKVTabletV1SchemaInteTest::PrepareSchema(const std::string& filename)
{
    _schema = KKVTableTestHelper::LoadSchema(GET_PRIVATE_TEST_DATA_PATH(), filename);
    _indexConfig = KKVTableTestHelper::GetIndexConfig(_schema, "pkey");
}

// levelSegments:levelIdx:segId,segId...;
void KKVTabletV1SchemaInteTest::CheckLevelInfo(const std::shared_ptr<Tablet>& tablet, uint32_t levelCount,
                                               uint32_t shardCount, LevelTopology topology, std::string levelSegmentStr)
{
    auto version = framework::TabletTestAgent(tablet).TEST_GetTabletData()->GetOnDiskVersion();
    auto segDescs = version.GetSegmentDescriptions();
    auto levelInfo = segDescs->GetLevelInfo();
    ASSERT_TRUE(levelInfo);
    ASSERT_EQ(levelCount, levelInfo->GetLevelCount());
    ASSERT_EQ(shardCount, levelInfo->GetShardCount());
    ASSERT_EQ(topology, levelInfo->GetTopology());
    std::vector<std::string> levelSegments;
    autil::StringUtil::fromString(levelSegmentStr, levelSegments, ";");
    for (size_t i = 0; i < levelSegments.size(); i++) {
        std::vector<std::vector<uint32_t>> oneLevelInfos;
        autil::StringUtil::fromString(levelSegments[i], oneLevelInfos, ",", ":");
        uint32_t levelIdx = oneLevelInfos[0][0];
        auto segments = levelInfo->GetSegmentIds(levelIdx);
        auto& expectSegments = oneLevelInfos[1];
        ASSERT_EQ(expectSegments.size(), segments.size());
        for (size_t j = 0; j < segments.size(); j++) {
            ASSERT_EQ(expectSegments[j], segments[j]) << j;
        }
    }
}

TEST_F(KKVTabletV1SchemaInteTest, TestSimple)
{
    framework::IndexRoot indexRoot(GET_TEMP_DATA_PATH(), GET_TEMP_DATA_PATH());
    // test init
    KKVTableTestHelper helper;
    ASSERT_TRUE(helper.Open(indexRoot, _schema, _tabletOptions, INVALID_VERSIONID).IsOK());
    // test build
    ASSERT_TRUE(helper.BuildSegment(docString1()).IsOK());
    ASSERT_TRUE(helper.BuildSegment(docString2()).IsOK());
    // test level info
    auto tablet = helper.GetTablet();
    std::stringstream ss;
    ss << "0:" << framework::Segment::PRIVATE_SEGMENT_ID_MASK + 1 << "," << framework::Segment::PRIVATE_SEGMENT_ID_MASK;
    CheckLevelInfo(tablet, 3, 2, framework::topo_hash_mod, ss.str());

    {
        // check result
        ASSERT_TRUE(helper.Query("kkv", "pkey", "1", "skey=11,value=v11;skey=1,value=v1"));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "2", ""));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "3", "skey=31,value=v31;skey=3,value=v3"));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "4", "skey=4,value=v4"));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "12", "skey=12,value=Av12"));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "14", "skey=14,value=Av14"));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "15", ""));
    }
    {
        // test building segment
        ASSERT_TRUE(helper.Build(docString1()).IsOK());
        ASSERT_TRUE(helper.Query("kkv", "pkey", "1", "skey=1,value=v1;skey=11,value=v11"));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "2", "skey=2,value=v2"));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "3", "skey=3,value=v3;skey=31,value=v31"));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "4", "skey=4,value=v4"));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "12", "skey=12,value=Av12"));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "14", "skey=14,value=Av14"));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "15", ""));
    }
    {
        // test building & dumping segment
        ASSERT_TRUE(tablet->Flush().IsOK());
        ASSERT_TRUE(helper.Build(docString3()).IsOK());
        ASSERT_TRUE(helper.Query("kkv", "pkey", "1", "skey=12,value=v12;skey=1,value=v1;skey=11,value=v11"));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "2", "skey=2,value=v2"));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "3", "skey=32,value=Av32;skey=31,value=v31"));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "4", "skey=4,value=v4"));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "12", "skey=121,value=v121;skey=12,value=Av12"));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "14", "skey=14,value=Av14"));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "15", "skey=15,value=v151"));
    }
}

TEST_F(KKVTabletV1SchemaInteTest, TestBuildNoMemWithDump)
{
    TabletResource resource;
    auto memController = std::make_shared<MemoryQuotaController>("", /*totalQuota=*/128 * 1024 * 1024);
    framework::IndexRoot indexRoot(GET_TEMP_DATA_PATH(), GET_TEMP_DATA_PATH());

    KKVTableTestHelper helper;
    _tabletOptions->SetIsLeader(true);
    _tabletOptions->SetFlushRemote(true);
    _tabletOptions->SetFlushLocal(false);
    _tabletOptions->TEST_GetOnlineConfig().TEST_GetBuildConfig().TEST_SetMaxDocCount(4);

    helper.SetMemoryQuotaController(memController, nullptr);
    ASSERT_TRUE(helper.Open(indexRoot, _schema, _tabletOptions).IsOK());
    ASSERT_TRUE(helper.Build(docString1()).IsOK());
    memController->Allocate(memController->GetFreeQuota() - 20);
    auto buildStatus = helper.Build(docString2());
    ASSERT_TRUE(buildStatus.IsNoMem()) << buildStatus.ToString();
    while (!helper.NeedCommit()) {};
    ASSERT_TRUE(helper.NeedCommit());
    ASSERT_TRUE(helper.Build(docString2()).IsNoMem());
    ASSERT_TRUE(helper.Build(docString2()).IsNoMem());
    auto tablet = helper.GetTablet();
    auto dumper = framework::TabletTestAgent(tablet).TEST_GetTabletDumper();
    ASSERT_EQ(0u, dumper->GetDumpQueueSize());

    auto [status, version] = helper.Commit();
    ASSERT_TRUE(status.IsOK());
    ASSERT_TRUE(helper.Reopen(version).IsOK());
    while (memController->GetFreeQuota() <= 20) {}
    ASSERT_TRUE(memController->GetFreeQuota() > 20);
}

TEST_F(KKVTabletV1SchemaInteTest, TestBuildingSegmentMemCheck)
{
    std::string tableOptionsJsonStr = R"( {
        "online_index_config": {
            "build_config": {
                "sharding_column_num" : 16,
                "level_num" : 3,
                "building_memory_limit_mb": 128,
                "need_deploy_index": false,
                "need_read_remote_index": true
            }
        }
    } )";

    auto tabletOptions = KKVTableTestHelper::CreateTableOptions(tableOptionsJsonStr);
    tabletOptions->SetIsLeader(true);
    tabletOptions->SetFlushRemote(true);
    tabletOptions->SetFlushLocal(false);

    framework::IndexRoot indexRoot(GET_TEMP_DATA_PATH(), GET_TEMP_DATA_PATH());

    KKVTableTestHelper helper;
    ASSERT_TRUE(helper.Open(indexRoot, _schema, tabletOptions).IsOK());
    ASSERT_TRUE(helper.Build(docString1()).IsOK());
    auto tablet = helper.GetTablet();
    auto memController = framework::TabletTestAgent(tablet).TEST_GetTabletMemoryQuotaController();
    auto allocateQuota = memController->GetAllocatedQuota();
    ASSERT_LT(allocateQuota, 128 * 1024 * 1024);
    ASSERT_GT(allocateQuota, 50 * 1024 * 1024);
    ASSERT_TRUE(helper.Build(docString2()).IsOK());
    allocateQuota = memController->GetAllocatedQuota();
    ASSERT_LT(allocateQuota, 128 * 1024 * 1024);
    ASSERT_GT(allocateQuota, 50 * 1024 * 1024);
}

TEST_F(KKVTabletV1SchemaInteTest, TestOnlyBuildingSegment)
{
    framework::IndexRoot indexRoot(GET_TEMP_DATA_PATH(), GET_TEMP_DATA_PATH());
    KKVTableTestHelper helper;
    ASSERT_TRUE(helper.Open(indexRoot, _schema, _tabletOptions).IsOK());
    ASSERT_TRUE(helper.Build(docString1()).IsOK());
    // check result
    ASSERT_TRUE(helper.Query("kkv", "pkey", "1", "skey=1,value=v1"));
    ASSERT_TRUE(helper.Query("kkv", "pkey", "2", "skey=2,value=v2"));
    ASSERT_TRUE(helper.Query("kkv", "pkey", "3", "skey=3,value=v3"));
    ASSERT_TRUE(helper.Query("kkv", "pkey", "4", "skey=4,value=v4"));

    // test dumping & building segment
    ASSERT_TRUE(helper.Flush().IsOK());
    ASSERT_TRUE(helper.Build(docString2()).IsOK());

    ASSERT_TRUE(helper.Query("kkv", "pkey", "1", "skey=11,value=v11;skey=1,value=v1"));
    ASSERT_TRUE(helper.Query("kkv", "pkey", "2", ""));
    ASSERT_TRUE(helper.Query("kkv", "pkey", "3", "skey=31,value=v31;skey=3,value=v3"));
    ASSERT_TRUE(helper.Query("kkv", "pkey", "4", "skey=4,value=v4"));
    ASSERT_TRUE(helper.Query("kkv", "pkey", "12", "skey=12,value=Av12"));
    ASSERT_TRUE(helper.Query("kkv", "pkey", "14", "skey=14,value=Av14"));
    ASSERT_TRUE(helper.Query("kkv", "pkey", "15", ""));
}

TEST_F(KKVTabletV1SchemaInteTest, TestReopenCoverNoRt)
{
    framework::IndexRoot indexRoot(GET_TEMP_DATA_PATH(), GET_TEMP_DATA_PATH());
    KKVTableTestHelper mainHelper;
    _tabletOptions->SetIsLeader(true);
    _tabletOptions->SetFlushLocal(false);
    _tabletOptions->SetFlushRemote(true);

    ASSERT_TRUE(mainHelper.Open(indexRoot, _schema, _tabletOptions).IsOK());
    ASSERT_TRUE(mainHelper.BuildSegment(docString1()).IsOK());

    auto tabletOptions1 = KKVTableTestHelper::CreateTableOptions(_tableOptionsJsonStr);
    tabletOptions1->SetIsLeader(false);
    tabletOptions1->SetFlushLocal(true);
    tabletOptions1->SetFlushRemote(false);

    framework::IndexRoot indexRoot1(GET_TEMP_DATA_PATH() + "/private_dir", GET_TEMP_DATA_PATH());
    KKVTableTestHelper followHelper;
    ASSERT_TRUE(followHelper.Open(indexRoot1, _schema, std::move(tabletOptions1)).IsOK());
    ASSERT_TRUE(followHelper.Build(docString2()).IsOK());
    ASSERT_TRUE(followHelper.Query("kkv", "pkey", "1", "skey=11,value=v11"));
    ASSERT_TRUE(followHelper.Query("kkv", "pkey", "2", ""));
    ASSERT_TRUE(followHelper.Query("kkv", "pkey", "3", "skey=31,value=v31"));
    ASSERT_TRUE(followHelper.Query("kkv", "pkey", "4", ""));
    ASSERT_TRUE(followHelper.Query("kkv", "pkey", "12", "skey=12,value=Av12"));
    ASSERT_TRUE(followHelper.Query("kkv", "pkey", "14", "skey=14,value=Av14"));
    ASSERT_TRUE(followHelper.Query("kkv", "pkey", "15", ""));

    const auto& version = mainHelper.GetCurrentVersion();
    ASSERT_TRUE(followHelper.Reopen(version.GetVersionId()).IsOK());
    ASSERT_TRUE(followHelper.Query("kkv", "pkey", "1", "skey=11,value=v11;skey=1,value=v1"));
    ASSERT_TRUE(followHelper.Query("kkv", "pkey", "2", ""));
    ASSERT_TRUE(followHelper.Query("kkv", "pkey", "3", "skey=31,value=v31;skey=3,value=v3"));
    ASSERT_TRUE(followHelper.Query("kkv", "pkey", "4", "skey=4,value=v4"));
    ASSERT_TRUE(followHelper.Query("kkv", "pkey", "12", "skey=12,value=Av12"));
    ASSERT_TRUE(followHelper.Query("kkv", "pkey", "14", "skey=14,value=Av14"));
    ASSERT_TRUE(followHelper.Query("kkv", "pkey", "15", ""));
}

TEST_F(KKVTabletV1SchemaInteTest, TestReopenCoverPartRt)
{
    framework::IndexRoot indexRoot(GET_TEMP_DATA_PATH(), GET_TEMP_DATA_PATH());
    KKVTableTestHelper mainHelper;
    _tabletOptions->SetIsLeader(true);
    _tabletOptions->SetFlushLocal(false);
    _tabletOptions->SetFlushRemote(true);
    ASSERT_TRUE(mainHelper.Open(indexRoot, _schema, _tabletOptions).IsOK());
    std::string docs = "cmd=add,pkey=1,skey=101,value=v101,ts=5,locator=0:5;"
                       "cmd=add,pkey=4,skey=104,value=v104,ts=6,locator=0:6;"
                       "cmd=add,pkey=2,skey=102,value=v102,ts=6,locator=0:10;";
    ASSERT_TRUE(mainHelper.BuildSegment(docs).IsOK());

    auto tabletOptions1 = KKVTableTestHelper::CreateTableOptions(_tableOptionsJsonStr);
    tabletOptions1->SetIsLeader(false);
    tabletOptions1->SetFlushLocal(true);
    tabletOptions1->SetFlushRemote(false);

    framework::IndexRoot indexRoot1(GET_TEMP_DATA_PATH() + "/private_dir", GET_TEMP_DATA_PATH());
    KKVTableTestHelper followHelper;
    ASSERT_TRUE(followHelper.Open(indexRoot1, _schema, std::move(tabletOptions1)).IsOK());
    ASSERT_TRUE(followHelper.BuildSegment(docString1()).IsOK());
    ASSERT_TRUE(followHelper.BuildSegment(docString2()).IsOK());

    const auto& version = mainHelper.GetCurrentVersion();
    ASSERT_TRUE(followHelper.Reopen(version.GetVersionId()).IsOK());
    ASSERT_TRUE(followHelper.Query("kkv", "pkey", "1", "skey=11,value=v11;skey=101,value=v101"));
    ASSERT_TRUE(followHelper.Query("kkv", "pkey", "2", ""));
    ASSERT_TRUE(followHelper.Query("kkv", "pkey", "3", "skey=31,value=v31"));
    ASSERT_TRUE(followHelper.Query("kkv", "pkey", "4", "skey=104,value=v104"));
    ASSERT_TRUE(followHelper.Query("kkv", "pkey", "12", "skey=12,value=Av12"));
    ASSERT_TRUE(followHelper.Query("kkv", "pkey", "14", "skey=14,value=Av14"));
    ASSERT_TRUE(followHelper.Query("kkv", "pkey", "15", ""));
}

TEST_F(KKVTabletV1SchemaInteTest, TestReopenCoverAllRt)
{
    framework::IndexRoot indexRoot(GET_TEMP_DATA_PATH(), GET_TEMP_DATA_PATH());
    KKVTableTestHelper mainHelper;
    _tabletOptions->SetIsLeader(true);
    _tabletOptions->SetFlushLocal(false);
    _tabletOptions->SetFlushRemote(true);
    ASSERT_TRUE(mainHelper.Open(indexRoot, _schema, _tabletOptions).IsOK());
    std::string docs = "cmd=add,pkey=1,skey=101,value=v101,ts=5,locator=0:5;"
                       "cmd=add,pkey=4,skey=104,value=v104,ts=6,locator=0:6;"
                       "cmd=add,pkey=2,skey=102,value=v102,ts=6,locator=0:12;";
    ASSERT_TRUE(mainHelper.BuildSegment(docs).IsOK());

    auto tabletOptions1 = KKVTableTestHelper::CreateTableOptions(_tableOptionsJsonStr);
    tabletOptions1->SetIsLeader(false);
    tabletOptions1->SetFlushLocal(true);
    tabletOptions1->SetFlushRemote(false);

    framework::IndexRoot indexRoot1(GET_TEMP_DATA_PATH() + "/private_dir", GET_TEMP_DATA_PATH());
    KKVTableTestHelper followHelper;
    ASSERT_TRUE(followHelper.Open(indexRoot1, _schema, std::move(tabletOptions1)).IsOK());
    ASSERT_TRUE(followHelper.BuildSegment(docString1()).IsOK());
    ASSERT_TRUE(followHelper.BuildSegment(docString2()).IsOK());

    const auto& version = mainHelper.GetCurrentVersion();
    ASSERT_TRUE(followHelper.Reopen(version.GetVersionId()).IsOK());
    ASSERT_TRUE(followHelper.Query("kkv", "pkey", "1", "skey=101,value=v101"));
    ASSERT_TRUE(followHelper.Query("kkv", "pkey", "2", "skey=102,value=v102"));
    ASSERT_TRUE(followHelper.Query("kkv", "pkey", "3", ""));
    ASSERT_TRUE(followHelper.Query("kkv", "pkey", "4", "skey=104,value=v104"));
    ASSERT_TRUE(followHelper.Query("kkv", "pkey", "12", ""));
    ASSERT_TRUE(followHelper.Query("kkv", "pkey", "14", ""));
    ASSERT_TRUE(followHelper.Query("kkv", "pkey", "15", ""));
}

TEST_F(KKVTabletV1SchemaInteTest, TestMultiShardBuild)
{
    framework::IndexRoot indexRoot(GET_TEMP_DATA_PATH(), GET_TEMP_DATA_PATH());

    // test init
    KKVTableTestHelper helper;
    ASSERT_TRUE(helper.Open(indexRoot, _schema, _tabletOptions).IsOK());
    // test build
    ASSERT_TRUE(helper.BuildSegment(docString1()).IsOK());
    ASSERT_TRUE(helper.BuildSegment(docString2()).IsOK());
    // test level info
    auto tablet = helper.GetTablet();
    std::stringstream ss;
    ss << "0:" << framework::Segment::PRIVATE_SEGMENT_ID_MASK + 1 << "," << framework::Segment::PRIVATE_SEGMENT_ID_MASK;
    CheckLevelInfo(tablet, 3, 2, framework::topo_hash_mod, ss.str());

    {
        // check result
        ASSERT_TRUE(helper.Query("kkv", "pkey", "1", "skey=11,value=v11;skey=1,value=v1"));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "2", ""));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "3", "skey=31,value=v31;skey=3,value=v3"));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "4", "skey=4,value=v4"));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "12", "skey=12,value=Av12"));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "14", "skey=14,value=Av14"));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "15", ""));
    }
    {
        // test building segment
        ASSERT_TRUE(helper.Build(docString1()).IsOK());
        ASSERT_TRUE(helper.Query("kkv", "pkey", "1", "skey=1,value=v1;skey=11,value=v11"));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "2", "skey=2,value=v2"));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "3", "skey=3,value=v3;skey=31,value=v31"));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "4", "skey=4,value=v4"));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "12", "skey=12,value=Av12"));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "14", "skey=14,value=Av14"));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "15", ""));
    }
    {
        // test building & dumping segment
        ASSERT_TRUE(tablet->Flush().IsOK());
        ASSERT_TRUE(helper.Build(docString3()).IsOK());
        ASSERT_TRUE(helper.Query("kkv", "pkey", "1", "skey=12,value=v12;skey=1,value=v1;skey=11,value=v11"));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "2", "skey=2,value=v2"));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "3", "skey=32,value=Av32;skey=31,value=v31"));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "4", "skey=4,value=v4"));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "12", "skey=121,value=v121;skey=12,value=Av12"));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "14", "skey=14,value=Av14"));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "15", "skey=15,value=v151"));
    }
}

TEST_F(KKVTabletV1SchemaInteTest, TestFollowReopen)
{
    framework::IndexRoot indexRoot(GET_TEMP_DATA_PATH() + "/local", GET_TEMP_DATA_PATH() + "/remote");
    KKVTableTestHelper helper;
    _tabletOptions->SetIsLeader(false);
    _tabletOptions->SetFlushLocal(true);
    _tabletOptions->SetFlushRemote(false);

    ASSERT_TRUE(helper.Open(indexRoot, _schema, _tabletOptions).IsOK());
    ASSERT_EQ(INVALID_VERSIONID, helper.GetCurrentVersion().GetVersionId());
    std::string docs = "cmd=add,pkey=1,skey=1,value=abc,ts=1,locator=0:2;"
                       "cmd=add,pkey=4,skey=4,value=Aijk,ts=2,locator=0:3;"
                       "cmd=add,pkey=2,skey=2,value=Aijk,ts=3,locator=0:4;";
    ASSERT_TRUE(helper.BuildSegment(docs).IsOK());
    ASSERT_EQ(framework::Version::PRIVATE_VERSION_ID_MASK | 0, helper.GetCurrentVersion().GetVersionId());
    ASSERT_TRUE(helper.Query("kkv", "pkey", "1", "skey=1,value=abc"));
    ASSERT_TRUE(helper.Query("kkv", "pkey", "2", "skey=2,value=Aijk"));
    ASSERT_TRUE(helper.Query("kkv", "pkey", "4", "skey=4,value=Aijk"));
    docs = "cmd=add,pkey=7,skey=7,value=abc,ts=4,locator=0:5;"
           "cmd=add,pkey=8,skey=8,value=Aijk,ts=5,locator=0:6;"
           "cmd=add,pkey=9,skey=9,value=Aijk,ts=6,locator=0:7;";
    ASSERT_TRUE(helper.Build(docs).IsOK());
    ASSERT_TRUE(helper.Query("kkv", "pkey", "7", "skey=7,value=abc"));
    ASSERT_TRUE(helper.Query("kkv", "pkey", "8", "skey=8,value=Aijk"));
    ASSERT_TRUE(helper.Query("kkv", "pkey", "9", "skey=9,value=Aijk"));
    auto tablet = std::dynamic_pointer_cast<framework::Tablet>(helper.GetITablet());
    ASSERT_TRUE(tablet);
    auto tabletData = tablet->GetTabletData();
    ASSERT_EQ(1, tabletData->GetIncSegmentCount());
    ASSERT_EQ(2, tabletData->GetSegmentCount());
    auto segment = tabletData->GetSegment(framework::Segment::PRIVATE_SEGMENT_ID_MASK | 0);
    ASSERT_TRUE(segment);
    ASSERT_TRUE(std::dynamic_pointer_cast<plain::MultiShardDiskSegment>(segment));
}

TEST_F(KKVTabletV1SchemaInteTest, TestMultiSKeyChunk)
{
    framework::IndexRoot indexRoot(GET_TEMP_DATA_PATH(), GET_TEMP_DATA_PATH());
    // test init
    KKVTableTestHelper helper;
    ASSERT_TRUE(helper.Open(indexRoot, _schema, _tabletOptions).IsOK());
    std::stringstream expectedQueryResult;
    // build long skey list
    int MAX_SKEY_COUNT = 512;
    for (int i = 0; i < MAX_SKEY_COUNT; ++i) {
        string doc = "cmd=add,pkey=101,skey=" + std::to_string(i) + ",value=v1,ts=1000000,locator=0:1;";
        expectedQueryResult << "skey=" << i << ",value=v1;";
        ASSERT_TRUE(helper.Build(doc).IsOK());
    }
    ASSERT_TRUE(helper.BuildSegment("cmd=add,pkey=102,skey=102,value=v102,ts=1000000,locator=0:1").IsOK());
    {
        ASSERT_TRUE(helper.Query("kkv", "pkey", "101", expectedQueryResult.str()));
        ASSERT_GT(helper.GetMetricsCollector()->GetSKeyChunkCountInBlocks(), 1);
    }
    {
        ASSERT_TRUE(helper.Query("kkv", "pkey", "1", ""));
        ASSERT_TRUE(helper.Build(docString1()).IsOK());
        // check result
        ASSERT_TRUE(helper.Query("kkv", "pkey", "1", "skey=1,value=v1"));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "2", "skey=2,value=v2"));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "3", "skey=3,value=v3"));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "4", "skey=4,value=v4"));
        ASSERT_TRUE(helper.Query("kkv", "pkey", "12", ""));
    }
    {
        ASSERT_TRUE(helper.Query("kkv", "pkey", "101", expectedQueryResult.str()));
        ASSERT_GT(helper.GetMetricsCollector()->GetSKeyChunkCountInBlocks(), 1);
    }
}

}} // namespace indexlibv2::table
