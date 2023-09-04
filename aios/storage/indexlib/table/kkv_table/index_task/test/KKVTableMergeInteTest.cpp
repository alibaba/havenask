#include "fslib/fs/ErrorGenerator.h"
#include "fslib/fs/ExceptionTrigger.h"
#include "indexlib/config/BuildConfig.h"
#include "indexlib/config/MergeConfig.h"
#include "indexlib/config/OfflineConfig.h"
#include "indexlib/config/OnlineConfig.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/framework/Tablet.h"
#include "indexlib/framework/VersionLoader.h"
#include "indexlib/framework/index_task/IndexTaskContext.h"
#include "indexlib/framework/index_task/IndexTaskContextCreator.h"
#include "indexlib/framework/index_task/IndexTaskResourceManager.h"
#include "indexlib/framework/index_task/LocalExecuteEngine.h"
#include "indexlib/framework/test/TabletTestAgent.h"
#include "indexlib/index/primary_key/Common.h"
#include "indexlib/table/index_task/IndexTaskConstant.h"
#include "indexlib/table/index_task/LocalTabletMergeController.h"
#include "indexlib/table/kkv_table/test/KKVTableTestHelper.h"
#include "indexlib/table/kkv_table/test/KKVTabletSchemaMaker.h"
#include "indexlib/util/PathUtil.h"
#include "unittest/unittest.h"

using namespace std;

namespace indexlibv2 { namespace table {

class KKVTableMergeInteTest : public TESTBASE
{
public:
    KKVTableMergeInteTest() {}
    ~KKVTableMergeInteTest() {}

public:
    void setUp() override
    {
        _tabletOptions = CreateMultiShardOptions();

        std::string field = "pkey:string;skey:int8;value:string";
        _tabletSchema = table::KKVTabletSchemaMaker::Make(field, "pkey", "skey", "value", -1);

        auto kkvIndexConfig = _tabletSchema->GetIndexConfig("kkv", "pkey");
        ASSERT_TRUE(kkvIndexConfig);
        _indexConfig = dynamic_pointer_cast<config::KKVIndexConfig>(kkvIndexConfig);
        ASSERT_TRUE(_indexConfig);

        _docs1 = "cmd=add,pkey=1,skey=1,value=v1,ts=1000000,locator=0:1;"
                 "cmd=add,pkey=2,skey=2,value=v2,ts=2000000,locator=0:2;"
                 "cmd=add,pkey=3,skey=3,value=v3,ts=3000000,locator=0:3;"
                 "cmd=add,pkey=321,skey=321,value=V321,ts=3000000,locator=0:3;"
                 "cmd=add,pkey=4,skey=4,value=v4,ts=4000000,locator=0:4;";
        _docs2 = "cmd=add,pkey=1,skey=11,value=v11,ts=5000000,locator=0:5;"
                 "cmd=add,pkey=12,skey=12,value=Av12,ts=6000000,locator=0:6;"
                 "cmd=add,pkey=3,skey=31,value=v31,ts=7000000,locator=0:7;"
                 "cmd=add,pkey=14,skey=14,value=Av14,ts=8000000,locator=0:8;"
                 "cmd=add,pkey=15,skey=15,value=v15,ts=9000000,locator=0:9;"
                 "cmd=delete,pkey=15,skey=15,ts=10000000,locator=0:10;"
                 "cmd=delete,pkey=2,ts=13000000,locator=0:11;";
        _docs3 = "cmd=add,pkey=1,skey=12,value=v12,ts=12000000,locator=0:12;"
                 "cmd=add,pkey=12,skey=121,value=v121,ts=13000000,locator=0:13;"
                 "cmd=add,pkey=3,skey=32,value=Av32,ts=14000000,locator=0:14;"
                 "cmd=add,pkey=15,skey=15,value=v151,ts=15000000,locator=0:15;"
                 "cmd=delete,pkey=3,skey=3,ts=15000000,locator=0:15;";
    }
    void tearDown() override {}

    std::shared_ptr<config::TabletOptions> CreateMultiShardOptions();
    void InnerTestMergeIOException(size_t begin, size_t end);

private:
    void SetIOError(bool enableIOException, size_t normalIOCount);

    std::shared_ptr<config::TabletOptions> _tabletOptions;
    std::shared_ptr<config::ITabletSchema> _tabletSchema;
    std::shared_ptr<indexlibv2::config::KKVIndexConfig> _indexConfig;
    std::string _docs1;
    std::string _docs2;
    std::string _docs3;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(indexlib.table, KKVTableMergeInteTest);

void KKVTableMergeInteTest::SetIOError(bool enableIOException, size_t normalIOCount)
{
    fslib::fs::ErrorGenerator* errorGenerator = fslib::fs::ErrorGenerator::getInstance();
    if (enableIOException) {
        fslib::fs::FileSystem::_useMock = true;
        errorGenerator->openErrorGenerate();
        fslib::fs::ErrorTriggerPtr errorTrigger(new fslib::fs::ErrorTrigger(normalIOCount));
        errorGenerator->setErrorTrigger(errorTrigger);
    } else {
        fslib::fs::FileSystem::_useMock = false;
        errorGenerator->shutDownErrorGenerate();
    }
}

std::shared_ptr<config::TabletOptions> KKVTableMergeInteTest::CreateMultiShardOptions()
{
    std::string jsonStr = R"( {
    "online_index_config": {
        "build_config": {
            "sharding_column_num" : 2,
            "level_num" : 3
        }
    }
    } )";
    auto tabletOptions = std::make_shared<config::TabletOptions>();
    FromJsonString(*tabletOptions, jsonStr);
    tabletOptions->SetIsOnline(true);
    tabletOptions->SetIsLeader(true);
    tabletOptions->SetFlushLocal(false);
    tabletOptions->SetFlushRemote(true);
    // TODO(xinfei.sxf) 1 * 1024 * 1024 使得dump频繁，这个排查一下
    tabletOptions->TEST_GetOnlineConfig().TEST_GetBuildConfig().TEST_SetBuildingMemoryLimit(4 * 1024 * 1024);
    tabletOptions->TEST_GetOnlineConfig().TEST_GetBuildConfig().TEST_SetKeepVersionCount(1);
    tabletOptions->TEST_GetOfflineConfig().TEST_GetBuildConfig().TEST_SetKeepVersionCount(1);
    tabletOptions->TEST_GetOfflineConfig().TEST_GetBuildConfig().TEST_SetFenceTsTolerantDeviation(0);
    tabletOptions->TEST_GetOfflineConfig().TEST_GetMergeConfig().TEST_SetMergeStrategyStr("shard_based");
    return tabletOptions;
}

TEST_F(KKVTableMergeInteTest, TestCleanVersionWithSegmentInDiffFenceDir)
{
    // prepare fence 1 with first build segment
    framework::IndexRoot indexRoot(GET_TEMP_DATA_PATH(), GET_TEMP_DATA_PATH());

    KKVTableTestHelper mainHelper;
    ASSERT_TRUE(mainHelper.Open(indexRoot, _tabletSchema, _tabletOptions).IsOK());
    ASSERT_TRUE(mainHelper.BuildSegment(_docs1).IsOK());
    auto onDiskVersion = mainHelper.GetCurrentVersion();
    std::string fenceName = framework::TabletTestAgent(mainHelper.GetTablet()).TEST_GetFence().GetFenceName();
    // prepare fence 2 with second build segment
    KKVTableTestHelper mainHelper1;
    framework::TabletResource resource;
    ASSERT_TRUE(mainHelper1.Open(indexRoot, _tabletSchema, _tabletOptions, onDiskVersion.GetVersionId()).IsOK());

    ASSERT_TRUE(mainHelper1.BuildSegment(_docs2).IsOK());
    auto fenceName1 = framework::TabletTestAgent(mainHelper1.GetTablet()).TEST_GetFence().GetFenceName();
    ASSERT_NE(fenceName, fenceName1) << "fenceName=" << fenceName << ",fenceName1=" << fenceName1;

    ASSERT_TRUE(mainHelper1.Merge(TableTestHelper::MergeOption::MergeAutoReadOption(true)).IsOK());
    auto rootDir = indexlib::file_system::Directory::GetPhysicalDirectory(GET_TEMP_DATA_PATH())->GetIDirectory();
    ASSERT_TRUE(rootDir->IsExist(fenceName).result);
    auto segmentPath = fenceName + "/" + onDiskVersion.GetSegmentDirName(onDiskVersion[0]);
    ASSERT_TRUE(rootDir->IsExist(segmentPath).result);
    auto mergedOnDiskVersion = mainHelper1.GetCurrentVersion();
    ASSERT_EQ(2u, mergedOnDiskVersion.GetSegmentCount());
    ASSERT_EQ(0u, mergedOnDiskVersion[0]);
    ASSERT_EQ(1u, mergedOnDiskVersion[1]);
    ASSERT_TRUE(mainHelper1.BuildSegment(_docs3).IsOK());
    ASSERT_TRUE(rootDir->IsExist(fenceName).result);
    ASSERT_TRUE(mainHelper1.Merge(TableTestHelper::MergeOption::MergeAutoReadOption(true)).IsOK());
    ASSERT_TRUE(!rootDir->IsExist(fenceName).result);
}

TEST_F(KKVTableMergeInteTest, TestMergeVersionWithNewBuiltSegment)
{
    framework::IndexRoot indexRoot(GET_TEMP_DATA_PATH(), GET_TEMP_DATA_PATH());

    KKVTableTestHelper mainHelper;
    ASSERT_TRUE(mainHelper.Open(indexRoot, _tabletSchema, std::move(_tabletOptions)).IsOK());
    ASSERT_TRUE(mainHelper.BuildSegment(_docs1).IsOK());
    ASSERT_TRUE(mainHelper.BuildSegment(_docs2).IsOK());
    ASSERT_TRUE(mainHelper.Merge(TableTestHelper::MergeOption::MergeAutoReadOption(false)).IsOK());
    ASSERT_TRUE(mainHelper.BuildSegment(_docs3).IsOK());
    const auto& version = mainHelper.GetCurrentVersion();
    ASSERT_TRUE(framework::Version::PUBLIC_VERSION_ID_MASK & version.GetVersionId()) << version.GetVersionId();
    ASSERT_EQ(3u, version.GetSegmentCount());
    ASSERT_TRUE(version.HasSegment(0));
    ASSERT_TRUE(version.HasSegment(1));
    ASSERT_TRUE(version.HasSegment(framework::Segment::PUBLIC_SEGMENT_ID_MASK) + 2);
    // TODO(xinfei.sxf) segment id 3 is where?

    ASSERT_TRUE(mainHelper.Query("kkv", "pkey", "1", "skey=12,value=v12;skey=1,value=v1;skey=11,value=v11;"));
    ASSERT_TRUE(mainHelper.Query("kkv", "pkey", "2", ""));
    ASSERT_TRUE(mainHelper.Query("kkv", "pkey", "3", "skey=32,value=Av32;skey=31,value=v31;"));
    ASSERT_TRUE(mainHelper.Query("kkv", "pkey", "4", "skey=4,value=v4"));
    ASSERT_TRUE(mainHelper.Query("kkv", "pkey", "12", "skey=121,value=v121;skey=12,value=Av12;"));
    ASSERT_TRUE(mainHelper.Query("kkv", "pkey", "14", "skey=14,value=Av14"));
    ASSERT_TRUE(mainHelper.Query("kkv", "pkey", "15", "skey=15,value=v151"));
}

TEST_F(KKVTableMergeInteTest, TestMergeVersionWithNewBuildingSegment)
{
    framework::IndexRoot indexRoot(GET_TEMP_DATA_PATH(), GET_TEMP_DATA_PATH());

    KKVTableTestHelper mainHelper;
    ASSERT_TRUE(mainHelper.Open(indexRoot, _tabletSchema, std::move(_tabletOptions)).IsOK());
    auto tablet = mainHelper.GetITablet();
    ASSERT_TRUE(mainHelper.BuildSegment(_docs1).IsOK());
    ASSERT_TRUE(mainHelper.BuildSegment(_docs2).IsOK());
    ASSERT_TRUE(mainHelper.Merge(TableTestHelper::MergeOption::MergeAutoReadOption(false)).IsOK());
    ASSERT_TRUE(mainHelper.Build(_docs3).IsOK());
    ASSERT_TRUE(tablet->NeedCommit());
    auto [status, versionMeta] = tablet->Commit(framework::CommitOptions().SetNeedPublish(true));
    ASSERT_TRUE(status.IsOK());
    ASSERT_TRUE(mainHelper.Reopen(versionMeta.GetVersionId()).IsOK());
    const auto& version = mainHelper.GetCurrentVersion();
    ASSERT_TRUE(framework::Version::PUBLIC_VERSION_ID_MASK & version.GetVersionId()) << version.GetVersionId();
    ASSERT_EQ(2u, version.GetSegmentCount());
    ASSERT_TRUE(version.HasSegment(0));
    ASSERT_TRUE(version.HasSegment(1));
    // TODO(xinfei.sxf) commit will trigger dump and merge?

    ASSERT_TRUE(mainHelper.Query("kkv", "pkey", "1", "skey=12,value=v12;skey=1,value=v1;skey=11,value=v11;"));
    ASSERT_TRUE(mainHelper.Query("kkv", "pkey", "2", ""));
    ASSERT_TRUE(mainHelper.Query("kkv", "pkey", "3", "skey=32,value=Av32;skey=31,value=v31;"));
    ASSERT_TRUE(mainHelper.Query("kkv", "pkey", "4", "skey=4,value=v4"));
    ASSERT_TRUE(mainHelper.Query("kkv", "pkey", "12", "skey=121,value=v121;skey=12,value=Av12;"));
    ASSERT_TRUE(mainHelper.Query("kkv", "pkey", "14", "skey=14,value=Av14"));
    ASSERT_TRUE(mainHelper.Query("kkv", "pkey", "15", "skey=15,value=v151"));
}

TEST_F(KKVTableMergeInteTest, TestSimple)
{
    // TODO(xinfei.sxf) 在本ut里面启动了两次的merge?
    framework::IndexRoot indexRoot(GET_TEMP_DATA_PATH(), GET_TEMP_DATA_PATH());

    KKVTableTestHelper mainHelper;
    ASSERT_TRUE(mainHelper.Open(indexRoot, _tabletSchema, std::move(_tabletOptions)).IsOK());
    ASSERT_TRUE(mainHelper.BuildSegment(_docs1).IsOK());
    ASSERT_TRUE(mainHelper.BuildSegment(_docs2).IsOK());
    ASSERT_TRUE(mainHelper.BuildSegment(_docs3).IsOK());
    ASSERT_TRUE(mainHelper.Merge(TableTestHelper::MergeOption::MergeAutoReadOption(true)).IsOK());
    const auto& version = mainHelper.GetCurrentVersion();
    ASSERT_TRUE(framework::Version::PUBLIC_VERSION_ID_MASK & version.GetVersionId()) << version.GetVersionId();
    ASSERT_EQ(2u, version.GetSegmentCount());
    // TODO(xinfei.sxf) after merge, is two segment?
    ASSERT_TRUE(version.HasSegment(0));
    ASSERT_TRUE(version.HasSegment(1));

    ASSERT_TRUE(mainHelper.Query("kkv", "pkey", "1", "skey=1,value=v1;skey=11,value=v11;skey=12,value=v12;"));
    ASSERT_TRUE(mainHelper.Query("kkv", "pkey", "2", ""));
    ASSERT_TRUE(mainHelper.Query("kkv", "pkey", "3", "skey=31,value=v31;skey=32,value=Av32;"));
    ASSERT_TRUE(mainHelper.Query("kkv", "pkey", "4", "skey=4,value=v4"));
    ASSERT_TRUE(mainHelper.Query("kkv", "pkey", "12", "skey=12,value=Av12;skey=121,value=v121;"));
    ASSERT_TRUE(mainHelper.Query("kkv", "pkey", "14", "skey=14,value=Av14"));
    ASSERT_TRUE(mainHelper.Query("kkv", "pkey", "15", "skey=15,value=v151"));
}

// TODO(xinfei.sxf) test exception

// TEST_F(KKVTableMergeInteTest, TestMergeException_0) { InnerTestMergeIOException(0, 20); }

// TEST_F(KKVTableMergeInteTest, TestMergeException_1) { InnerTestMergeIOException(200, 220); }

// TEST_F(KKVTableMergeInteTest, TestMergeException_2) { InnerTestMergeIOException(400, 420); }

// TEST_F(KKVTableMergeInteTest, TestMergeException_3) { InnerTestMergeIOException(600, 620); }

// TEST_F(KKVTableMergeInteTest, TestMergeException_4) { InnerTestMergeIOException(800, 820); }

// TEST_F(KKVTableMergeInteTest, TestMergeException_5) { InnerTestMergeIOException(1000, 1020); }

// TEST_F(KKVTableMergeInteTest, TestMergeException_6) { InnerTestMergeIOException(1200, 1220); }

void KKVTableMergeInteTest::InnerTestMergeIOException(size_t begin, size_t end)
{
    int64_t exceptionCount = 0;
    for (size_t i = begin; i < end; i++) {
        std::stringstream root;
        root << GET_TEMP_DATA_PATH() << "/part_" << i;
        framework::IndexRoot indexRoot(root.str(), root.str());

        auto mainHelper = std::make_shared<KKVTableTestHelper>();
        ASSERT_TRUE(mainHelper->Open(indexRoot, _tabletSchema, _tabletOptions).IsOK());
        {
            // prepare segment and version for clean version
            ASSERT_TRUE(mainHelper->BuildSegment(_docs1).IsOK());
            ASSERT_TRUE(mainHelper->BuildSegment(_docs2).IsOK());
            ASSERT_TRUE(mainHelper->BuildSegment(_docs3).IsOK());
            TableTestHelper::StepInfo info;
            info.specifyEpochId = "12345";
            auto status = mainHelper->Merge(true /*auto load merge version*/, &info);
            ASSERT_TRUE(status.IsOK());
            ASSERT_TRUE(mainHelper->BuildSegment(_docs1).IsOK());
            ASSERT_TRUE(mainHelper->BuildSegment(_docs2).IsOK());
            ASSERT_TRUE(mainHelper->BuildSegment(_docs3).IsOK());
            status = mainHelper->Merge(true /*auto load merge version*/, &info);
            ASSERT_TRUE(status.IsOK());
            ASSERT_TRUE(mainHelper->BuildSegment(_docs1).IsOK());
            ASSERT_TRUE(mainHelper->BuildSegment(_docs2).IsOK());
        }
        SetIOError(true, i);
        bool buildError = false;
        if (!mainHelper->BuildSegment(_docs3).IsOK()) {
            SetIOError(false, fslib::fs::ErrorGenerator::NO_EXCEPTION_COUNT);
            exceptionCount++;
            buildError = true;
            auto version = mainHelper->GetCurrentVersion().GetVersionId();
            mainHelper.reset();
            mainHelper = std::make_shared<KKVTableTestHelper>();
            ASSERT_TRUE(mainHelper->Open(indexRoot, _tabletSchema, _tabletOptions, version).IsOK());
            ASSERT_TRUE(mainHelper->BuildSegment(_docs3).IsOK());
        }

        TableTestHelper::StepInfo info;
        info.specifyEpochId = "12345";
        info.specifyMaxMergedSegId = 5;
        info.specifyMaxMergedVersionId = 1;
        auto status = mainHelper->Merge(false /*auto load merge version*/, &info);
        if (status.IsOK() && !buildError) {
            SetIOError(false, fslib::fs::ErrorGenerator::NO_EXCEPTION_COUNT);
            break;
        }
        exceptionCount++;
        SetIOError(false, fslib::fs::ErrorGenerator::NO_EXCEPTION_COUNT);
        status = mainHelper->Merge(true /*auto load merge version*/, &info);
        ASSERT_TRUE(status.IsOK());
        const auto& version = mainHelper->GetCurrentVersion();
        ASSERT_TRUE(framework::Version::PUBLIC_VERSION_ID_MASK & version.GetVersionId()) << version.GetVersionId();
        ASSERT_EQ(2u, version.GetSegmentCount());
        ASSERT_TRUE(version.HasSegment(6));
        ASSERT_TRUE(version.HasSegment(7));

        ASSERT_TRUE(mainHelper->Query("kkv", "pkey", "1", "skey=1,value=v1;skey=11,value=v11;skey=12,value=v12;"));
        ASSERT_TRUE(mainHelper->Query("kkv", "pkey", "2", ""));
        ASSERT_TRUE(mainHelper->Query("kkv", "pkey", "3", "skey=31,value=v31;skey=32,value=Av32;"));
        ASSERT_TRUE(mainHelper->Query("kkv", "pkey", "4", "skey=4,value=v4"));
        ASSERT_TRUE(mainHelper->Query("kkv", "pkey", "12", "skey=12,value=Av12;skey=121,value=v121;"));
        ASSERT_TRUE(mainHelper->Query("kkv", "pkey", "14", "skey=14,value=Av14"));
        ASSERT_TRUE(mainHelper->Query("kkv", "pkey", "15", "skey=15,value=v151"));
    }
    ASSERT_TRUE(exceptionCount > 0);
}

TEST_F(KKVTableMergeInteTest, testCommitVersionAfterMerge)
{
    framework::IndexRoot indexRoot(GET_TEMP_DATA_PATH(), GET_TEMP_DATA_PATH());

    KKVTableTestHelper mainHelper;
    ASSERT_TRUE(mainHelper.Open(indexRoot, _tabletSchema, std::move(_tabletOptions)).IsOK());

    auto buildVersion = [&mainHelper](auto docs) {
        auto status = mainHelper.Build(docs);
        EXPECT_TRUE(status.IsOK());
        status = mainHelper.Flush();
        EXPECT_TRUE(status.IsOK());
        EXPECT_TRUE(mainHelper.NeedCommit());
        auto [s, v] = mainHelper.Commit();
        EXPECT_TRUE(s.IsOK());
        return v;
    };
    auto loadVersion = [indexRoot](versionid_t versionId) {
        framework::Version version;
        auto dir = indexlib::file_system::Directory::GetPhysicalDirectory(indexRoot.GetRemoteRoot());
        EXPECT_TRUE(framework::VersionLoader::GetVersion(dir, versionId, &version).IsOK());
        return version;
    };

    // "cmd=add,pkey=1,skey=1,value=v1,ts=1000000,locator=0:1;"
    //              "cmd=add,pkey=2,skey=2,value=v2,ts=2000000,locator=0:2;"
    //              "cmd=add,pkey=3,skey=3,value=v3,ts=3000000,locator=0:3;"
    //              "cmd=add,pkey=321,skey=321,value=V321,ts=3000000,locator=0:3;"
    //              "cmd=add,pkey=4,skey=4,value=v4,ts=4000000,locator=0:4;";

    std::string docs = "cmd=add,pkey=1,skey=1,value=v1,ts=1000000,locator=0:1;";
    auto buildV0 = buildVersion(docs);
    ASSERT_EQ(framework::Version::PUBLIC_VERSION_ID_MASK | 0, buildV0);
    docs = "cmd=add,pkey=2,skey=2,value=v2,ts=2000000,locator=0:2;";
    auto buildV1 = buildVersion(docs);
    ASSERT_EQ(framework::Version::PUBLIC_VERSION_ID_MASK | 1, buildV1);
    ASSERT_TRUE(mainHelper.Reopen(buildV1).IsOK());
    auto mergeStatus = mainHelper.Merge(TableTestHelper::MergeOption::MergeAutoReadOption(false));
    ASSERT_TRUE(mergeStatus.IsOK()) << mergeStatus.ToString();
    // TODO(xinfei.sxf) need commit ?
    ASSERT_TRUE(mainHelper.NeedCommit());
    auto [status, buildV2] = mainHelper.Commit();
    ASSERT_EQ(framework::Version::PUBLIC_VERSION_ID_MASK | 2, buildV2);
    ASSERT_FALSE(mainHelper.NeedCommit());
    auto version2 = loadVersion(buildV2);
    ASSERT_GT(version2.GetSegmentCount(), 0);
    for (auto [segId, _] : version2) {
        EXPECT_FALSE(framework::Segment::PUBLIC_SEGMENT_ID_MASK & segId) << segId;
        EXPECT_FALSE(framework::Segment::PRIVATE_SEGMENT_ID_MASK & segId) << segId;
    }

    docs = "cmd=add,pkey=3,skey=3,value=v3,ts=3000000,locator=0:3;";
    auto buildV3 = buildVersion(docs);
    ASSERT_TRUE(mainHelper.Reopen(buildV3).IsOK());
    auto version = mainHelper.GetCurrentVersion().Clone();
    ASSERT_EQ(framework::Version::PUBLIC_VERSION_ID_MASK | 3, version.GetVersionId());
    ASSERT_EQ(version2.GetSegmentCount() + 1, version.GetSegmentCount());
    for (auto [segId, _] : version2) {
        ASSERT_TRUE(version.HasSegment(segId));
    }

    docs = "cmd=add,pkey=4,skey=4,value=v4,ts=4000000,locator=0:4;";
    ASSERT_TRUE(mainHelper.BuildSegment(docs).IsOK());
    version = mainHelper.GetCurrentVersion().Clone();
    ASSERT_EQ(framework::Version::PUBLIC_VERSION_ID_MASK | 4, version.GetVersionId());
    ASSERT_EQ(version2.GetSegmentCount() + 2, version.GetSegmentCount());
    for (auto [segId, _] : version2) {
        ASSERT_TRUE(version.HasSegment(segId));
    }

    ASSERT_TRUE(mainHelper.Query("kkv", "pkey", "1", "skey=1,value=v1"));
    ASSERT_TRUE(mainHelper.Query("kkv", "pkey", "2", "skey=2,value=v2"));
    ASSERT_TRUE(mainHelper.Query("kkv", "pkey", "3", "skey=3,value=v3"));
    ASSERT_TRUE(mainHelper.Query("kkv", "pkey", "4", "skey=4,value=v4"));
}

}} // namespace indexlibv2::table
