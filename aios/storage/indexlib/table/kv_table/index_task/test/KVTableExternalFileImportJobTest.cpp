#include "indexlib/table/kv_table/index_task/KVTableExternalFileImportJob.h"

#include <memory>

#include "indexlib/config/BuildConfig.h"
#include "indexlib/config/MergeConfig.h"
#include "indexlib/config/OfflineConfig.h"
#include "indexlib/config/OnlineConfig.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/config/UnresolvedSchema.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/framework/IndexRoot.h"
#include "indexlib/framework/Tablet.h"
#include "indexlib/framework/Version.h"
#include "indexlib/framework/VersionLoader.h"
#include "indexlib/table/kv_table/test/KVTableTestHelper.h"
#include "indexlib/table/kv_table/test/KVTabletSchemaMaker.h"
#include "indexlib/util/PathUtil.h"
#include "unittest/unittest.h"

namespace indexlibv2::table {

class KVTableExternalFileImportJobTest : public TESTBASE
{
public:
    void setUp() override
    {
        _externalRoot = GET_TEMP_DATA_PATH() + "/external/";
        auto status = indexlib::file_system::FslibWrapper::MkDirIfNotExist(_externalRoot).Status();
        ASSERT_TRUE(status.IsOK());
        _internalRoot = GET_TEMP_DATA_PATH() + "/internal/";
        status = indexlib::file_system::FslibWrapper::MkDirIfNotExist(_internalRoot).Status();
        ASSERT_TRUE(status.IsOK());
        _tabletOptions = CreateTabletOptions();
        _tabletSchema = table::KVTabletSchemaMaker::Make("key1:string;value1:string", "key1", "value1");
    }
    void tearDown() override {}

private:
    std::shared_ptr<config::TabletOptions> CreateTabletOptions();

    std::string _externalRoot;
    std::string _internalRoot;
    std::shared_ptr<config::TabletSchema> _tabletSchema;
    std::shared_ptr<config::TabletOptions> _tabletOptions;
};

std::shared_ptr<config::TabletOptions> KVTableExternalFileImportJobTest::CreateTabletOptions()
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
    tabletOptions->TEST_GetOfflineConfig().TEST_GetBuildConfig().TEST_SetKeepVersionCount(1);
    tabletOptions->TEST_GetOfflineConfig().TEST_GetBuildConfig().TEST_SetFenceTsTolerantDeviation(0);
    tabletOptions->TEST_GetOfflineConfig().TEST_GetMergeConfig().TEST_SetMergeStrategyStr("shard_based");
    return tabletOptions;
}

TEST_F(KVTableExternalFileImportJobTest, TestPrepare)
{
    {
        // invalid import file mode
        framework::ImportExternalFileOptions options;
        options.mode = framework::ImportExternalFileOptions::INDEX_MODE;
        KVTableExternalFileImportJob job(_internalRoot, options, 2);
        std::string externalFileName = "key";
        std::string externalFilePath = indexlib::util::PathUtil::JoinPath(_externalRoot, externalFileName);
        ASSERT_TRUE(indexlib::file_system::FslibWrapper::Store(externalFilePath, "content").Status().IsOK());
        ASSERT_FALSE(job.Prepare({externalFilePath}).IsOK());
    }
    {
        // invalid import options
        framework::ImportExternalFileOptions options;
        options.importBehind = true;
        KVTableExternalFileImportJob job(_internalRoot, options, 2);
        std::string externalFileName = "key";
        std::string externalFilePath = indexlib::util::PathUtil::JoinPath(_externalRoot, externalFileName);
        ASSERT_TRUE(indexlib::file_system::FslibWrapper::Store(externalFilePath, "content").Status().IsOK());
        ASSERT_FALSE(job.Prepare({externalFilePath}).IsOK());
    }
    {
        // invalid import options
        framework::ImportExternalFileOptions options;
        options.failIfNotBottomMostLevel = true;
        KVTableExternalFileImportJob job(_internalRoot, options, 2);
        std::string externalFileName = "key";
        std::string externalFilePath = indexlib::util::PathUtil::JoinPath(_externalRoot, externalFileName);
        ASSERT_TRUE(indexlib::file_system::FslibWrapper::Store(externalFilePath, "content").Status().IsOK());
        ASSERT_FALSE(job.Prepare({externalFilePath}).IsOK());
    }
}

TEST_F(KVTableExternalFileImportJobTest, testConvertVersionInPublicRoot)
{
    framework::ImportExternalFileOptions options;
    KVTableExternalFileImportJob job(_internalRoot, options, 2);

    // prepare version in public root
    KVTableTestHelper tablet;
    ASSERT_TABLET_OK(tablet.Open(framework::IndexRoot(_externalRoot, _externalRoot), _tabletSchema, _tabletOptions));
    ASSERT_TABLET_OK(tablet.BuildSegment("cmd=add,key1=1,value1=a,ts=1,locator=0:2;"));
    const auto& version = tablet.GetCurrentVersion();
    std::string versionPath = PathUtil::JoinPath(_externalRoot, version.GetVersionFileName());

    std::vector<InternalFileInfo> internalFileInfos;
    auto status = job.Convert(versionPath, &internalFileInfos);
    ASSERT_TRUE(status.IsOK());
}

TEST_F(KVTableExternalFileImportJobTest, testPrepareMultiVersion)
{
    framework::ImportExternalFileOptions options;
    options.mode = framework::ImportExternalFileOptions::VERSION_MODE;
    KVTableExternalFileImportJob job(_internalRoot, options, 2);

    KVTableTestHelper tablet0;
    std::string indexRoot0 = PathUtil::JoinPath(_externalRoot, "0");
    ASSERT_TABLET_OK(tablet0.Open(framework::IndexRoot(indexRoot0, indexRoot0), _tabletSchema, _tabletOptions));
    ASSERT_TABLET_OK(tablet0.BuildSegment("cmd=add,key1=1,value1=a,ts=1,locator=0:2;"));
    const auto& version0 = tablet0.GetCurrentVersion();
    std::string versionPath0 = PathUtil::JoinPath(indexRoot0, version0.GetVersionFileName());

    KVTableTestHelper tablet1;
    std::string indexRoot1 = PathUtil::JoinPath(_externalRoot, "1");
    ASSERT_TABLET_OK(tablet1.Open(framework::IndexRoot(indexRoot1, indexRoot1), _tabletSchema, _tabletOptions));
    ASSERT_TABLET_OK(tablet1.BuildSegment("cmd=add,key1=1,value1=a,ts=1,locator=0:2;"));
    const auto& version1 = tablet1.GetCurrentVersion();
    std::string versionPath1 = PathUtil::JoinPath(indexRoot1, version1.GetVersionFileName());

    ASSERT_TRUE(job.Prepare({versionPath0, versionPath1}).IsOK());
}

TEST_F(KVTableExternalFileImportJobTest, testPrepareIgnoreDupSegment)
{
    framework::ImportExternalFileOptions options;
    options.mode = framework::ImportExternalFileOptions::VERSION_MODE;
    options.ignoreDuplicateFiles = true;
    KVTableExternalFileImportJob job(_internalRoot, options, 1);

    std::string jsonStr = R"( {
    "online_index_config": {
        "build_config": {
            "sharding_column_num" : 1,
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
    tabletOptions->TEST_GetOfflineConfig().TEST_GetBuildConfig().TEST_SetKeepVersionCount(1);
    tabletOptions->TEST_GetOfflineConfig().TEST_GetBuildConfig().TEST_SetFenceTsTolerantDeviation(0);
    tabletOptions->TEST_GetOfflineConfig().TEST_GetMergeConfig().TEST_SetMergeStrategyStr("shard_based");

    KVTableTestHelper tablet;
    ASSERT_TABLET_OK(tablet.Open(framework::IndexRoot(_externalRoot, _externalRoot), _tabletSchema, tabletOptions));
    ASSERT_TABLET_OK(tablet.BuildSegment("cmd=add,key1=1,value1=old_value,ts=1,locator=0:2;"));
    const auto& version0 = tablet.GetCurrentVersion();
    std::string versionPath0 = PathUtil::JoinPath(_externalRoot, version0.GetVersionFileName());

    ASSERT_TABLET_OK(tablet.BuildSegment("cmd=add,key1=1,value1=new_value,ts=2,locator=0:3;"));
    const auto& version1 = tablet.GetCurrentVersion();
    std::string versionPath1 = PathUtil::JoinPath(_externalRoot, version1.GetVersionFileName());

    ASSERT_TRUE(job.Prepare({versionPath1, versionPath0}).IsOK());
    ASSERT_EQ(job._internalFileInfos.size(), 14);
}

TEST_F(KVTableExternalFileImportJobTest, testPrepareNotIgnoreDupSegment)
{
    framework::ImportExternalFileOptions options;
    options.mode = framework::ImportExternalFileOptions::VERSION_MODE;
    options.ignoreDuplicateFiles = false;
    KVTableExternalFileImportJob job(_internalRoot, options, 1);

    std::string jsonStr = R"( {
    "online_index_config": {
        "build_config": {
            "sharding_column_num" : 1,
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
    tabletOptions->TEST_GetOfflineConfig().TEST_GetBuildConfig().TEST_SetKeepVersionCount(1);
    tabletOptions->TEST_GetOfflineConfig().TEST_GetBuildConfig().TEST_SetFenceTsTolerantDeviation(0);
    tabletOptions->TEST_GetOfflineConfig().TEST_GetMergeConfig().TEST_SetMergeStrategyStr("shard_based");

    KVTableTestHelper tablet;
    ASSERT_TABLET_OK(tablet.Open(framework::IndexRoot(_externalRoot, _externalRoot), _tabletSchema, tabletOptions));
    ASSERT_TABLET_OK(tablet.BuildSegment("cmd=add,key1=1,value1=old_value,ts=1,locator=0:2;"));
    const auto& version0 = tablet.GetCurrentVersion();
    std::string versionPath0 = PathUtil::JoinPath(_externalRoot, version0.GetVersionFileName());

    ASSERT_TABLET_OK(tablet.BuildSegment("cmd=add,key1=1,value1=new_value,ts=2,locator=0:3;"));
    const auto& version1 = tablet.GetCurrentVersion();
    std::string versionPath1 = PathUtil::JoinPath(_externalRoot, version1.GetVersionFileName());

    ASSERT_TRUE(job.Prepare({versionPath1, versionPath0}).IsOK());
    ASSERT_EQ(job._internalFileInfos.size(), 21);
}

} // namespace indexlibv2::table
