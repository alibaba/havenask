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
#include "indexlib/table/kv_table/test/KVTableTestHelper.h"
#include "indexlib/table/kv_table/test/KVTabletSchemaMaker.h"
#include "indexlib/util/PathUtil.h"
#include "unittest/unittest.h"

using namespace std;

namespace indexlibv2 { namespace table {

class KVTableMergeInteTest : public TESTBASE
{
public:
    KVTableMergeInteTest() {}
    ~KVTableMergeInteTest() {}

public:
    void setUp() override
    {
        _offset = 0;
        _tabletOptions = CreateMultiShardOptions();
        std::string field = "string1:string;string2:string";
        _tabletSchema = table::KVTabletSchemaMaker::Make(field, "string1", "string2");
    }
    void tearDown() override {}

    std::shared_ptr<config::TabletOptions> CreateMultiShardOptions();
    void InnerTestMergeIOException(size_t begin, size_t end);
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
        std::vector<std::string> docPrefix = {
            "cmd=add,string1=1,string2=abc,ts=1,locator=0:", "cmd=add,string1=2,string2=cde,ts=2,locator=0:",
            "cmd=add,string1=3,string2=fgh,ts=3,locator=0:", "cmd=add,string1=4,string2=ijk,ts=4,locator=0:"};
        return docString(docPrefix);
    }

    std::string docString2()
    {
        std::vector<std::string> docPrefix = {
            "cmd=add,string1=1,string2=ijk,ts=5,locator=0:",  "cmd=add,string1=12,string2=Acde,ts=6,locator=0:",
            "cmd=add,string1=3,string2=Afgh,ts=7,locator=0:", "cmd=add,string1=14,string2=Aijk,ts=8,locator=0:",
            "cmd=add,string1=15,string2=15,ts=9,locator=0:",  "cmd=delete,string1=15,ts=10,locator=0:",
            "cmd=delete,string1=2,ts=13,locator=0:"};
        return docString(docPrefix);
    }
    std::string docString3()
    {
        std::vector<std::string> docPrefix = {
            "cmd=add,string1=1,string2=ijk,ts=12,locator=0:", "cmd=add,string1=12,string2=12,ts=13,locator=0:",
            "cmd=add,string1=3,string2=Afgh,ts=14,locator=0:", "cmd=add,string1=15,string2=15,ts=15,locator=0:"};
        return docString(docPrefix);
    }

private:
    void SetIOError(bool enableIOException, size_t normalIOCount);

    std::shared_ptr<config::TabletOptions> _tabletOptions;
    std::shared_ptr<config::ITabletSchema> _tabletSchema;
    int64_t _offset = 0;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(indexlib.table, KVTableMergeInteTest);

void KVTableMergeInteTest::SetIOError(bool enableIOException, size_t normalIOCount)
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

std::shared_ptr<config::TabletOptions> KVTableMergeInteTest::CreateMultiShardOptions()
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
    tabletOptions->TEST_GetOnlineConfig().TEST_GetBuildConfig().TEST_SetBuildingMemoryLimit(1 * 1024 * 1024);
    tabletOptions->TEST_GetOnlineConfig().TEST_GetBuildConfig().TEST_SetKeepVersionCount(1);
    tabletOptions->TEST_GetOfflineConfig().TEST_GetBuildConfig().TEST_SetKeepVersionCount(1);
    tabletOptions->TEST_GetOfflineConfig().TEST_GetBuildConfig().TEST_SetFenceTsTolerantDeviation(0);
    tabletOptions->TEST_GetOfflineConfig().TEST_GetMergeConfig().TEST_SetMergeStrategyStr("shard_based");
    return tabletOptions;
}

TEST_F(KVTableMergeInteTest, TestCleanVersionWithSegmentInDiffFenceDir)
{
    // prepare fence 1 with first build segment
    framework::IndexRoot indexRoot(GET_TEMP_DATA_PATH(), GET_TEMP_DATA_PATH());

    KVTableTestHelper mainHelper;
    ASSERT_TRUE(mainHelper.Open(indexRoot, _tabletSchema, _tabletOptions).IsOK());
    ASSERT_TRUE(mainHelper.BuildSegment(docString1()).IsOK());
    auto onDiskVersion = mainHelper.GetCurrentVersion();
    std::string fenceName = framework::TabletTestAgent(mainHelper.GetTablet()).TEST_GetFence().GetFenceName();
    // prepare fence 2 with second build segment
    KVTableTestHelper mainHelper1;
    framework::TabletResource resource;
    ASSERT_TRUE(mainHelper1.Open(indexRoot, _tabletSchema, _tabletOptions, onDiskVersion.GetVersionId()).IsOK());

    ASSERT_TRUE(mainHelper1.BuildSegment(docString2()).IsOK());
    auto fenceName1 = framework::TabletTestAgent(mainHelper.GetTablet()).TEST_GetFence().GetFenceName();
    ASSERT_TRUE(mainHelper1.Merge(TableTestHelper::MergeOption::MergeAutoReadOption(true)).IsOK());
    auto rootDir = indexlib::file_system::Directory::GetPhysicalDirectory(GET_TEMP_DATA_PATH())->GetIDirectory();
    ASSERT_TRUE(rootDir->IsExist(fenceName).result);
    auto segmentPath = fenceName + "/" + onDiskVersion.GetSegmentDirName(onDiskVersion[0]);
    ASSERT_TRUE(rootDir->IsExist(segmentPath).result);
    auto mergedOnDiskVersion = mainHelper1.GetCurrentVersion();
    ASSERT_EQ(2u, mergedOnDiskVersion.GetSegmentCount());
    ASSERT_EQ(0u, mergedOnDiskVersion[0]);
    ASSERT_EQ(1u, mergedOnDiskVersion[1]);
    ASSERT_TRUE(mainHelper1.BuildSegment(docString3()).IsOK());
    ASSERT_TRUE(mainHelper1.Merge(TableTestHelper::MergeOption::MergeAutoReadOption(true)).IsOK());
    ASSERT_TRUE(!rootDir->IsExist(fenceName).result);
}

TEST_F(KVTableMergeInteTest, TestMergeVersionWithNewBuiltSegment)
{
    framework::IndexRoot indexRoot(GET_TEMP_DATA_PATH(), GET_TEMP_DATA_PATH());

    KVTableTestHelper mainHelper;
    ASSERT_TRUE(mainHelper.Open(indexRoot, _tabletSchema, std::move(_tabletOptions)).IsOK());
    ASSERT_TRUE(mainHelper.BuildSegment(docString1()).IsOK());
    ASSERT_TRUE(mainHelper.BuildSegment(docString2()).IsOK());
    ASSERT_TRUE(mainHelper.Merge(TableTestHelper::MergeOption::MergeAutoReadOption(false)).IsOK());
    ASSERT_TRUE(mainHelper.BuildSegment(docString3()).IsOK());
    const auto& version = mainHelper.GetCurrentVersion();
    ASSERT_TRUE(framework::Version::PUBLIC_VERSION_ID_MASK & version.GetVersionId()) << version.GetVersionId();
    ASSERT_EQ(3u, version.GetSegmentCount());
    ASSERT_TRUE(version.HasSegment(0));
    ASSERT_TRUE(version.HasSegment(1));
    ASSERT_TRUE(version.HasSegment(framework::Segment::PUBLIC_SEGMENT_ID_MASK) + 2);

    ASSERT_TRUE(mainHelper.Query("kv", "string1", "1", "string2=ijk"));
    ASSERT_TRUE(mainHelper.Query("kv", "string1", "2", ""));
    ASSERT_TRUE(mainHelper.Query("kv", "string1", "3", "string2=Afgh"));
    ASSERT_TRUE(mainHelper.Query("kv", "string1", "4", "string2=ijk"));
    ASSERT_TRUE(mainHelper.Query("kv", "string1", "12", "string2=12"));
    ASSERT_TRUE(mainHelper.Query("kv", "string1", "14", "string2=Aijk"));
    ASSERT_TRUE(mainHelper.Query("kv", "string1", "15", "string2=15"));
}

TEST_F(KVTableMergeInteTest, TestMergeVersionWithNewBuildingSegment)
{
    framework::IndexRoot indexRoot(GET_TEMP_DATA_PATH(), GET_TEMP_DATA_PATH());

    KVTableTestHelper mainHelper;
    ASSERT_TRUE(mainHelper.Open(indexRoot, _tabletSchema, std::move(_tabletOptions)).IsOK());
    auto tablet = mainHelper.GetITablet();
    ASSERT_TRUE(mainHelper.BuildSegment(docString1()).IsOK());
    ASSERT_TRUE(mainHelper.BuildSegment(docString2()).IsOK());
    ASSERT_TRUE(mainHelper.Merge(TableTestHelper::MergeOption::MergeAutoReadOption(false)).IsOK());
    ASSERT_TRUE(mainHelper.Build(docString3()).IsOK());
    ASSERT_TRUE(tablet->NeedCommit());
    auto [status, versionMeta] = tablet->Commit(framework::CommitOptions().SetNeedPublish(true));
    ASSERT_TRUE(status.IsOK());
    ASSERT_TRUE(mainHelper.Reopen(versionMeta.GetVersionId()).IsOK());
    const auto& version = mainHelper.GetCurrentVersion();
    ASSERT_TRUE(framework::Version::PUBLIC_VERSION_ID_MASK & version.GetVersionId()) << version.GetVersionId();
    ASSERT_EQ(2u, version.GetSegmentCount());
    ASSERT_TRUE(version.HasSegment(0));
    ASSERT_TRUE(version.HasSegment(1));

    ASSERT_TRUE(mainHelper.Query("kv", "string1", "1", "string2=ijk"));
    ASSERT_TRUE(mainHelper.Query("kv", "string1", "2", ""));
    ASSERT_TRUE(mainHelper.Query("kv", "string1", "3", "string2=Afgh"));
    ASSERT_TRUE(mainHelper.Query("kv", "string1", "4", "string2=ijk"));
    ASSERT_TRUE(mainHelper.Query("kv", "string1", "12", "string2=12"));
    ASSERT_TRUE(mainHelper.Query("kv", "string1", "14", "string2=Aijk"));
    ASSERT_TRUE(mainHelper.Query("kv", "string1", "15", "string2=15"));
}

TEST_F(KVTableMergeInteTest, TestSimple)
{
    framework::IndexRoot indexRoot(GET_TEMP_DATA_PATH(), GET_TEMP_DATA_PATH());

    KVTableTestHelper mainHelper;
    ASSERT_TRUE(mainHelper.Open(indexRoot, _tabletSchema, std::move(_tabletOptions)).IsOK());
    ASSERT_TRUE(mainHelper.BuildSegment(docString1()).IsOK());
    ASSERT_TRUE(mainHelper.BuildSegment(docString2()).IsOK());
    ASSERT_TRUE(mainHelper.BuildSegment(docString3()).IsOK());
    ASSERT_TRUE(mainHelper.Merge(TableTestHelper::MergeOption::MergeAutoReadOption(true)).IsOK());
    const auto& version = mainHelper.GetCurrentVersion();
    ASSERT_TRUE(framework::Version::PUBLIC_VERSION_ID_MASK & version.GetVersionId()) << version.GetVersionId();
    ASSERT_EQ(2u, version.GetSegmentCount());
    ASSERT_TRUE(version.HasSegment(0));
    ASSERT_TRUE(version.HasSegment(1));

    ASSERT_TRUE(mainHelper.Query("kv", "string1", "1", "string2=ijk"));
    ASSERT_TRUE(mainHelper.Query("kv", "string1", "2", ""));
    ASSERT_TRUE(mainHelper.Query("kv", "string1", "3", "string2=Afgh"));
    ASSERT_TRUE(mainHelper.Query("kv", "string1", "4", "string2=ijk"));
    ASSERT_TRUE(mainHelper.Query("kv", "string1", "12", "string2=12"));
    ASSERT_TRUE(mainHelper.Query("kv", "string1", "14", "string2=Aijk"));
    ASSERT_TRUE(mainHelper.Query("kv", "string1", "15", "string2=15"));
}

TEST_F(KVTableMergeInteTest, TestMergeException_0) { InnerTestMergeIOException(0, 20); }

TEST_F(KVTableMergeInteTest, TestMergeException_1) { InnerTestMergeIOException(200, 220); }

TEST_F(KVTableMergeInteTest, TestMergeException_2) { InnerTestMergeIOException(400, 420); }

TEST_F(KVTableMergeInteTest, TestMergeException_3) { InnerTestMergeIOException(600, 620); }

TEST_F(KVTableMergeInteTest, TestMergeException_4) { InnerTestMergeIOException(800, 820); }

TEST_F(KVTableMergeInteTest, TestMergeException_5) { InnerTestMergeIOException(1000, 1020); }

TEST_F(KVTableMergeInteTest, TestMergeException_6) { InnerTestMergeIOException(1200, 1220); }

void KVTableMergeInteTest::InnerTestMergeIOException(size_t begin, size_t end)
{
    int64_t exceptionCount = 0;
    for (size_t i = begin; i < end; i++) {
        std::stringstream root;
        root << GET_TEMP_DATA_PATH() << "/part_" << i;
        framework::IndexRoot indexRoot(root.str(), root.str());

        auto mainHelper = std::make_shared<KVTableTestHelper>();
        ASSERT_TRUE(mainHelper->Open(indexRoot, _tabletSchema, _tabletOptions).IsOK());
        {
            // prepare segment and version for clean version
            ASSERT_TRUE(mainHelper->BuildSegment(docString1()).IsOK());
            ASSERT_TRUE(mainHelper->BuildSegment(docString2()).IsOK());
            ASSERT_TRUE(mainHelper->BuildSegment(docString3()).IsOK());
            TableTestHelper::StepInfo info;
            info.specifyEpochId = "12345";
            auto status = mainHelper->Merge(true /*auto load merge version*/, &info);
            ASSERT_TRUE(status.IsOK());
            ASSERT_TRUE(mainHelper->BuildSegment(docString1()).IsOK());
            ASSERT_TRUE(mainHelper->BuildSegment(docString2()).IsOK());
            ASSERT_TRUE(mainHelper->BuildSegment(docString3()).IsOK());
            status = mainHelper->Merge(true /*auto load merge version*/, &info);
            ASSERT_TRUE(status.IsOK());
            ASSERT_TRUE(mainHelper->BuildSegment(docString1()).IsOK());
            ASSERT_TRUE(mainHelper->BuildSegment(docString2()).IsOK());
        }
        SetIOError(true, i);
        bool buildError = false;
        if (!mainHelper->BuildSegment(docString3()).IsOK()) {
            SetIOError(false, fslib::fs::ErrorGenerator::NO_EXCEPTION_COUNT);
            exceptionCount++;
            buildError = true;
            auto version = mainHelper->GetCurrentVersion().GetVersionId();
            mainHelper.reset();
            mainHelper = std::make_shared<KVTableTestHelper>();
            ASSERT_TRUE(mainHelper->Open(indexRoot, _tabletSchema, _tabletOptions, version).IsOK());
            ASSERT_TRUE(mainHelper->BuildSegment(docString3()).IsOK());
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

        ASSERT_TRUE(mainHelper->Query("kv", "string1", "1", "string2=ijk"));
        ASSERT_TRUE(mainHelper->Query("kv", "string1", "2", ""));
        ASSERT_TRUE(mainHelper->Query("kv", "string1", "3", "string2=Afgh"));
        ASSERT_TRUE(mainHelper->Query("kv", "string1", "4", "string2=ijk"));
        ASSERT_TRUE(mainHelper->Query("kv", "string1", "12", "string2=12"));
        ASSERT_TRUE(mainHelper->Query("kv", "string1", "14", "string2=Aijk"));
        ASSERT_TRUE(mainHelper->Query("kv", "string1", "15", "string2=15"));
    }
    ASSERT_TRUE(exceptionCount > 0);
}

TEST_F(KVTableMergeInteTest, testCommitVersionAfterMerge)
{
    framework::IndexRoot indexRoot(GET_TEMP_DATA_PATH(), GET_TEMP_DATA_PATH());

    KVTableTestHelper mainHelper;
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

    std::string docs = "cmd=add,string1=1,string2=abc,ts=1,locator=0:1;";
    auto buildV0 = buildVersion(docs);
    ASSERT_EQ(framework::Version::PUBLIC_VERSION_ID_MASK | 0, buildV0);
    docs = "cmd=add,string1=2,string2=abc,ts=2,locator=0:2;";
    auto buildV1 = buildVersion(docs);
    ASSERT_EQ(framework::Version::PUBLIC_VERSION_ID_MASK | 1, buildV1);
    ASSERT_TRUE(mainHelper.Reopen(buildV1).IsOK());
    auto mergeStatus = mainHelper.Merge(TableTestHelper::MergeOption::MergeAutoReadOption(false));
    ASSERT_TRUE(mergeStatus.IsOK()) << mergeStatus.ToString();
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

    docs = "cmd=add,string1=3,string2=abc,ts=3,locator=0:3;";
    auto buildV3 = buildVersion(docs);
    ASSERT_TRUE(mainHelper.Reopen(buildV3).IsOK());
    auto version = mainHelper.GetCurrentVersion().Clone();
    ASSERT_EQ(framework::Version::PUBLIC_VERSION_ID_MASK | 3, version.GetVersionId());
    ASSERT_EQ(version2.GetSegmentCount() + 1, version.GetSegmentCount());
    for (auto [segId, _] : version2) {
        ASSERT_TRUE(version.HasSegment(segId));
    }

    docs = "cmd=add,string1=4,string2=abc,ts=4,locator=0:4;";
    ASSERT_TRUE(mainHelper.BuildSegment(docs).IsOK());
    version = mainHelper.GetCurrentVersion().Clone();
    ASSERT_EQ(framework::Version::PUBLIC_VERSION_ID_MASK | 4, version.GetVersionId());
    ASSERT_EQ(version2.GetSegmentCount() + 2, version.GetSegmentCount());
    for (auto [segId, _] : version2) {
        ASSERT_TRUE(version.HasSegment(segId));
    }

    ASSERT_TRUE(mainHelper.Query("kv", "string1", "1", "string2=abc"));
    ASSERT_TRUE(mainHelper.Query("kv", "string1", "2", "string2=abc"));
    ASSERT_TRUE(mainHelper.Query("kv", "string1", "3", "string2=abc"));
    ASSERT_TRUE(mainHelper.Query("kv", "string1", "4", "string2=abc"));
}

}} // namespace indexlibv2::table
