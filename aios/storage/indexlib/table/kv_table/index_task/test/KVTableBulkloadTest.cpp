#include "autil/EnvUtil.h"
#include "indexlib/config/BackgroundTaskConfig.h"
#include "indexlib/config/BuildConfig.h"
#include "indexlib/config/MergeConfig.h"
#include "indexlib/config/OfflineConfig.h"
#include "indexlib/config/OnlineConfig.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/config/UnresolvedSchema.h"
#include "indexlib/framework/IndexRoot.h"
#include "indexlib/framework/Tablet.h"
#include "indexlib/framework/Version.h"
#include "indexlib/framework/VersionLoader.h"
#include "indexlib/framework/index_task/IndexTaskContext.h"
#include "indexlib/framework/index_task/IndexTaskContextCreator.h"
#include "indexlib/framework/index_task/IndexTaskResourceManager.h"
#include "indexlib/framework/index_task/LocalExecuteEngine.h"
#include "indexlib/table/index_task/IndexTaskConstant.h"
#include "indexlib/table/index_task/LocalTabletMergeController.h"
#include "indexlib/table/kv_table/test/KVTableTestHelper.h"
#include "indexlib/table/kv_table/test/KVTabletSchemaMaker.h"
#include "indexlib/util/PathUtil.h"
#include "unittest/unittest.h"

namespace indexlibv2::table {

using Directory = indexlib::file_system::Directory;
using Version = framework::Version;
using Segment = framework::Segment;

class KVTableBulkloadTest : public TESTBASE
{
public:
    void setUp() override
    {
        _tabletOptions = CreateMultiShardOptions();
        std::string field = "key1:string;value1:string";
        _tabletSchema = table::KVTabletSchemaMaker::Make(field, "key1", "value1");
    }

    void tearDown() override {}

private:
    std::shared_ptr<config::TabletOptions> CreateMultiShardOptions();

    std::shared_ptr<config::TabletOptions> _tabletOptions;
    std::shared_ptr<config::ITabletSchema> _tabletSchema;
};

std::shared_ptr<config::TabletOptions> KVTableBulkloadTest::CreateMultiShardOptions()
{
    std::string jsonStr = R"( {
    "online_index_config": {
        "build_config": {
            "sharding_column_num" : 2,
            "level_num" : 3
        }
    },
    "offline_index_config": {
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

TEST_F(KVTableBulkloadTest, testSimple)
{
    std::string root0 = PathUtil::JoinPath(GET_TEMP_DATA_PATH(), "0");
    KVTableTestHelper tablet0;
    ASSERT_TABLET_OK(tablet0.Open(framework::IndexRoot(root0, root0), _tabletSchema, _tabletOptions));
    ASSERT_TABLET_OK(tablet0.BuildSegment("cmd=add,key1=1,value1=abc,ts=1,locator=0:2;"
                                          "cmd=add,key1=2,value1=cde,ts=2,locator=0:3;"
                                          "cmd=add,key1=3,value1=fgh,ts=3,locator=0:4;"
                                          "cmd=add,key1=4,value1=ijk,ts=4,locator=0:5;"));

    ASSERT_TABLET_OK(tablet0.BuildSegment("cmd=add,key1=1,value1=ijk,ts=5,locator=0:6;"
                                          "cmd=add,key1=12,value1=Acde,ts=6,locator=0:7;"
                                          "cmd=add,key1=3,value1=Afgh,ts=7,locator=0:8;"
                                          "cmd=add,key1=14,value1=Aijk,ts=8,locator=0:9;"
                                          "cmd=add,key1=15,value1=15,ts=9,locator=0:10;"
                                          "cmd=delete,key1=15,ts=10,locator=0:11;"
                                          "cmd=delete,key1=2,ts=13,locator=0:12;"));

    ASSERT_TABLET_OK(tablet0.BuildSegment("cmd=add,key1=1,value1=ijk,ts=12,locator=0:13;"
                                          "cmd=add,key1=12,value1=12,ts=13,locator=0:14;"
                                          "cmd=add,key1=3,value1=Afgh,ts=14,locator=0:15;"
                                          "cmd=add,key1=15,value1=15,ts=15,locator=0:16;"));

    ASSERT_TABLET_OK(tablet0.Merge(TableTestHelper::MergeOption::MergeAutoReadOption(true)));
    const auto& v0 = tablet0.GetCurrentVersion();
    ASSERT_EQ(v0.GetVersionId(), Version::PUBLIC_VERSION_ID_MASK | 3);
    ASSERT_EQ(v0.GetSegmentCount(), 2u);
    ASSERT_TRUE(v0.HasSegment(0));
    ASSERT_TRUE(v0.HasSegment(1));

    ASSERT_TRUE(tablet0.Query("kv", "key1", "1", "value1=ijk"));
    ASSERT_TRUE(tablet0.Query("kv", "key1", "2", ""));
    ASSERT_TRUE(tablet0.Query("kv", "key1", "3", "value1=Afgh"));
    ASSERT_TRUE(tablet0.Query("kv", "key1", "4", "value1=ijk"));
    ASSERT_TRUE(tablet0.Query("kv", "key1", "12", "value1=12"));
    ASSERT_TRUE(tablet0.Query("kv", "key1", "14", "value1=Aijk"));
    ASSERT_TRUE(tablet0.Query("kv", "key1", "15", "value1=15"));

    std::string versionPath0 = PathUtil::JoinPath(root0, v0.GetVersionFileName());

    KVTableTestHelper tablet1;
    std::string root1 = GET_TEMP_DATA_PATH() + "/1";
    ASSERT_TABLET_OK(tablet1.Open(framework::IndexRoot(root1, root1), _tabletSchema, _tabletOptions));

    std::string bulkloadId = "bulkload_foo";
    std::vector<std::string> externalFiles = {versionPath0};
    auto options = std::make_shared<framework::ImportExternalFileOptions>();
    options->mode = framework::ImportExternalFileOptions::VERSION_MODE;

    ASSERT_TABLET_OK(tablet1.ImportExternalFiles(bulkloadId, externalFiles, options));

    framework::MergeTaskStatus taskStatus;
    ASSERT_TABLET_OK(tablet1.TriggerBulkloadTask(taskStatus));
    ASSERT_EQ(framework::MergeTaskStatus::DONE, taskStatus.code);

    framework::Version targetVersion;
    ASSERT_TABLET_OK(framework::VersionLoader::GetVersion(Directory::GetPhysicalDirectory(root1),
                                                          taskStatus.targetVersion.GetVersionId(), &targetVersion));
    ASSERT_EQ(targetVersion.GetVersionId(), 0);
    auto taskMeta = targetVersion.GetIndexTaskQueue()->Get(framework::BULKLOAD_TASK_TYPE, bulkloadId);
    ASSERT_TRUE(taskMeta);
    ASSERT_EQ(taskMeta->GetState(), framework::IndexTaskMeta::DONE);

    ASSERT_TABLET_OK(
        tablet1.Open(framework::IndexRoot(root1, root1), _tabletSchema, _tabletOptions, targetVersion.GetVersionId()));
    ASSERT_TRUE(tablet1.Query("kv", "key1", "1", "value1=ijk"));
    ASSERT_TRUE(tablet1.Query("kv", "key1", "2", ""));
    ASSERT_TRUE(tablet1.Query("kv", "key1", "3", "value1=Afgh"));
    ASSERT_TRUE(tablet1.Query("kv", "key1", "4", "value1=ijk"));
    ASSERT_TRUE(tablet1.Query("kv", "key1", "12", "value1=12"));
    ASSERT_TRUE(tablet1.Query("kv", "key1", "14", "value1=Aijk"));
    ASSERT_TRUE(tablet1.Query("kv", "key1", "15", "value1=15"));
}

TEST_F(KVTableBulkloadTest, testBulkloadBuildSegmentThenMerge)
{
    std::string root0 = PathUtil::JoinPath(GET_TEMP_DATA_PATH(), "0");
    KVTableTestHelper tablet0;
    ASSERT_TABLET_OK(tablet0.Open(framework::IndexRoot(root0, root0), _tabletSchema, _tabletOptions));
    ASSERT_TABLET_OK(tablet0.BuildSegment("cmd=add,key1=1,value1=abc,ts=1,locator=0:2;"
                                          "cmd=add,key1=2,value1=cde,ts=2,locator=0:3;"
                                          "cmd=add,key1=3,value1=fgh,ts=3,locator=0:4;"
                                          "cmd=add,key1=4,value1=ijk,ts=4,locator=0:5;"));

    ASSERT_TABLET_OK(tablet0.BuildSegment("cmd=add,key1=1,value1=ijk,ts=5,locator=0:6;"
                                          "cmd=add,key1=12,value1=Acde,ts=6,locator=0:7;"
                                          "cmd=add,key1=3,value1=Afgh,ts=7,locator=0:8;"
                                          "cmd=add,key1=14,value1=Aijk,ts=8,locator=0:9;"
                                          "cmd=add,key1=15,value1=15,ts=9,locator=0:10;"
                                          "cmd=delete,key1=15,ts=10,locator=0:11;"
                                          "cmd=delete,key1=2,ts=13,locator=0:12;"));

    ASSERT_TABLET_OK(tablet0.BuildSegment("cmd=add,key1=1,value1=ijk,ts=12,locator=0:13;"
                                          "cmd=add,key1=12,value1=12,ts=13,locator=0:14;"
                                          "cmd=add,key1=3,value1=Afgh,ts=14,locator=0:15;"
                                          "cmd=add,key1=15,value1=15,ts=15,locator=0:16;"));

    const auto& v0 = tablet0.GetCurrentVersion();
    ASSERT_EQ(v0.GetVersionId(), Version::PUBLIC_VERSION_ID_MASK | 2);
    ASSERT_EQ(v0.GetSegmentCount(), 3u);

    ASSERT_TRUE(tablet0.Query("kv", "key1", "1", "value1=ijk"));
    ASSERT_TRUE(tablet0.Query("kv", "key1", "2", ""));
    ASSERT_TRUE(tablet0.Query("kv", "key1", "3", "value1=Afgh"));
    ASSERT_TRUE(tablet0.Query("kv", "key1", "4", "value1=ijk"));
    ASSERT_TRUE(tablet0.Query("kv", "key1", "12", "value1=12"));
    ASSERT_TRUE(tablet0.Query("kv", "key1", "14", "value1=Aijk"));
    ASSERT_TRUE(tablet0.Query("kv", "key1", "15", "value1=15"));

    std::string versionPath0 = PathUtil::JoinPath(root0, v0.GetVersionFileName());

    KVTableTestHelper tablet1;
    std::string root1 = GET_TEMP_DATA_PATH() + "/1";
    ASSERT_TABLET_OK(tablet1.Open(framework::IndexRoot(root1, root1), _tabletSchema, _tabletOptions));

    std::string bulkloadId = "bulkload_foo";
    std::vector<std::string> externalFiles = {versionPath0};
    auto options = std::make_shared<framework::ImportExternalFileOptions>();
    options->mode = framework::ImportExternalFileOptions::VERSION_MODE;

    ASSERT_TABLET_OK(tablet1.ImportExternalFiles(bulkloadId, externalFiles, options));

    framework::MergeTaskStatus taskStatus;
    ASSERT_TABLET_OK(tablet1.TriggerBulkloadTask(taskStatus));
    ASSERT_EQ(framework::MergeTaskStatus::DONE, taskStatus.code);

    framework::Version targetVersion;
    ASSERT_TABLET_OK(framework::VersionLoader::GetVersion(Directory::GetPhysicalDirectory(root1),
                                                          taskStatus.targetVersion.GetVersionId(), &targetVersion));
    ASSERT_EQ(targetVersion.GetVersionId(), 0);
    auto taskMeta = targetVersion.GetIndexTaskQueue()->Get(framework::BULKLOAD_TASK_TYPE, bulkloadId);
    ASSERT_TRUE(taskMeta);
    ASSERT_EQ(taskMeta->GetState(), framework::IndexTaskMeta::DONE);

    ASSERT_TABLET_OK(
        tablet1.Open(framework::IndexRoot(root1, root1), _tabletSchema, _tabletOptions, targetVersion.GetVersionId()));
    ASSERT_TRUE(tablet1.Query("kv", "key1", "1", "value1=ijk"));
    ASSERT_TRUE(tablet1.Query("kv", "key1", "2", ""));
    ASSERT_TRUE(tablet1.Query("kv", "key1", "3", "value1=Afgh"));
    ASSERT_TRUE(tablet1.Query("kv", "key1", "4", "value1=ijk"));
    ASSERT_TRUE(tablet1.Query("kv", "key1", "12", "value1=12"));
    ASSERT_TRUE(tablet1.Query("kv", "key1", "14", "value1=Aijk"));
    ASSERT_TRUE(tablet1.Query("kv", "key1", "15", "value1=15"));

    ASSERT_TABLET_OK(tablet1.Merge(TableTestHelper::MergeOption::MergeAutoReadOption(true)));
    const auto& v1 = tablet1.GetCurrentVersion();
    ASSERT_EQ(v1.GetVersionId(), Version::PUBLIC_VERSION_ID_MASK | 1);
    ASSERT_EQ(v1.GetSegmentCount(), 2u);
    ASSERT_TRUE(v1.HasSegment(3));
    ASSERT_TRUE(v1.HasSegment(4));

    ASSERT_TRUE(tablet1.Query("kv", "key1", "1", "value1=ijk"));
    ASSERT_TRUE(tablet1.Query("kv", "key1", "2", ""));
    ASSERT_TRUE(tablet1.Query("kv", "key1", "3", "value1=Afgh"));
    ASSERT_TRUE(tablet1.Query("kv", "key1", "4", "value1=ijk"));
    ASSERT_TRUE(tablet1.Query("kv", "key1", "12", "value1=12"));
    ASSERT_TRUE(tablet1.Query("kv", "key1", "14", "value1=Aijk"));
    ASSERT_TRUE(tablet1.Query("kv", "key1", "15", "value1=15"));
}

TEST_F(KVTableBulkloadTest, testBulkloadMergedSegmentThenMerge)
{
    std::string root0 = PathUtil::JoinPath(GET_TEMP_DATA_PATH(), "0");
    KVTableTestHelper tablet0;
    ASSERT_TABLET_OK(tablet0.Open(framework::IndexRoot(root0, root0), _tabletSchema, _tabletOptions));
    ASSERT_TABLET_OK(tablet0.BuildSegment("cmd=add,key1=1,value1=abc,ts=1,locator=0:2;"
                                          "cmd=add,key1=2,value1=cde,ts=2,locator=0:3;"
                                          "cmd=add,key1=3,value1=fgh,ts=3,locator=0:4;"
                                          "cmd=add,key1=4,value1=ijk,ts=4,locator=0:5;"));

    ASSERT_TABLET_OK(tablet0.BuildSegment("cmd=add,key1=1,value1=ijk,ts=5,locator=0:6;"
                                          "cmd=add,key1=12,value1=Acde,ts=6,locator=0:7;"
                                          "cmd=add,key1=3,value1=Afgh,ts=7,locator=0:8;"
                                          "cmd=add,key1=14,value1=Aijk,ts=8,locator=0:9;"
                                          "cmd=add,key1=15,value1=15,ts=9,locator=0:10;"
                                          "cmd=delete,key1=15,ts=10,locator=0:11;"
                                          "cmd=delete,key1=2,ts=13,locator=0:12;"));

    ASSERT_TABLET_OK(tablet0.BuildSegment("cmd=add,key1=1,value1=ijk,ts=12,locator=0:13;"
                                          "cmd=add,key1=12,value1=12,ts=13,locator=0:14;"
                                          "cmd=add,key1=3,value1=Afgh,ts=14,locator=0:15;"
                                          "cmd=add,key1=15,value1=15,ts=15,locator=0:16;"));

    ASSERT_TABLET_OK(tablet0.Merge(TableTestHelper::MergeOption::MergeAutoReadOption(true)));
    const auto& v0 = tablet0.GetCurrentVersion();
    ASSERT_EQ(v0.GetVersionId(), Version::PUBLIC_VERSION_ID_MASK | 3);
    ASSERT_EQ(v0.GetSegmentCount(), 2u);
    ASSERT_TRUE(v0.HasSegment(0));
    ASSERT_TRUE(v0.HasSegment(1));

    ASSERT_TRUE(tablet0.Query("kv", "key1", "1", "value1=ijk"));
    ASSERT_TRUE(tablet0.Query("kv", "key1", "2", ""));
    ASSERT_TRUE(tablet0.Query("kv", "key1", "3", "value1=Afgh"));
    ASSERT_TRUE(tablet0.Query("kv", "key1", "4", "value1=ijk"));
    ASSERT_TRUE(tablet0.Query("kv", "key1", "12", "value1=12"));
    ASSERT_TRUE(tablet0.Query("kv", "key1", "14", "value1=Aijk"));
    ASSERT_TRUE(tablet0.Query("kv", "key1", "15", "value1=15"));

    std::string versionPath0 = PathUtil::JoinPath(root0, v0.GetVersionFileName());

    KVTableTestHelper tablet1;
    std::string root1 = GET_TEMP_DATA_PATH() + "/1";
    ASSERT_TABLET_OK(tablet1.Open(framework::IndexRoot(root1, root1), _tabletSchema, _tabletOptions));

    std::string bulkloadId = "bulkload_foo";
    std::vector<std::string> externalFiles = {versionPath0};
    auto options = std::make_shared<framework::ImportExternalFileOptions>();
    options->mode = framework::ImportExternalFileOptions::VERSION_MODE;

    ASSERT_TABLET_OK(tablet1.ImportExternalFiles(bulkloadId, externalFiles, options));

    framework::MergeTaskStatus taskStatus;
    ASSERT_TABLET_OK(tablet1.TriggerBulkloadTask(taskStatus));
    ASSERT_EQ(framework::MergeTaskStatus::DONE, taskStatus.code);

    framework::Version targetVersion;
    ASSERT_TABLET_OK(framework::VersionLoader::GetVersion(Directory::GetPhysicalDirectory(root1),
                                                          taskStatus.targetVersion.GetVersionId(), &targetVersion));
    ASSERT_EQ(targetVersion.GetVersionId(), 0);
    auto taskMeta = targetVersion.GetIndexTaskQueue()->Get(framework::BULKLOAD_TASK_TYPE, bulkloadId);
    ASSERT_TRUE(taskMeta);
    ASSERT_EQ(taskMeta->GetState(), framework::IndexTaskMeta::DONE);

    ASSERT_TABLET_OK(
        tablet1.Open(framework::IndexRoot(root1, root1), _tabletSchema, _tabletOptions, targetVersion.GetVersionId()));
    ASSERT_TRUE(tablet1.Query("kv", "key1", "1", "value1=ijk"));
    ASSERT_TRUE(tablet1.Query("kv", "key1", "2", ""));
    ASSERT_TRUE(tablet1.Query("kv", "key1", "3", "value1=Afgh"));
    ASSERT_TRUE(tablet1.Query("kv", "key1", "4", "value1=ijk"));
    ASSERT_TRUE(tablet1.Query("kv", "key1", "12", "value1=12"));
    ASSERT_TRUE(tablet1.Query("kv", "key1", "14", "value1=Aijk"));
    ASSERT_TRUE(tablet1.Query("kv", "key1", "15", "value1=15"));

    ASSERT_TABLET_OK(tablet1.Merge(TableTestHelper::MergeOption::MergeAutoReadOption(true)));
    const auto& v1 = tablet1.GetCurrentVersion();
    ASSERT_EQ(v1.GetVersionId(), Version::PUBLIC_VERSION_ID_MASK | 1);
    ASSERT_EQ(v1.GetSegmentCount(), 2u);
    ASSERT_TRUE(v1.HasSegment(2));
    ASSERT_TRUE(v1.HasSegment(3));

    ASSERT_TRUE(tablet1.Query("kv", "key1", "1", "value1=ijk"));
    ASSERT_TRUE(tablet1.Query("kv", "key1", "2", ""));
    ASSERT_TRUE(tablet1.Query("kv", "key1", "3", "value1=Afgh"));
    ASSERT_TRUE(tablet1.Query("kv", "key1", "4", "value1=ijk"));
    ASSERT_TRUE(tablet1.Query("kv", "key1", "12", "value1=12"));
    ASSERT_TRUE(tablet1.Query("kv", "key1", "14", "value1=Aijk"));
    ASSERT_TRUE(tablet1.Query("kv", "key1", "15", "value1=15"));
}

TEST_F(KVTableBulkloadTest, testIgnoreDuplicateSegment)
{
    std::string root0 = PathUtil::JoinPath(GET_TEMP_DATA_PATH(), "0");
    KVTableTestHelper tablet0;
    ASSERT_TABLET_OK(tablet0.Open(framework::IndexRoot(root0, root0), _tabletSchema, _tabletOptions));
    ASSERT_TABLET_OK(tablet0.BuildSegment("cmd=add,key1=foo,value1=old_bar,ts=1,locator=0:2;"));

    const auto& v0 = tablet0.GetCurrentVersion();
    ASSERT_EQ(v0.GetVersionId(), Version::PUBLIC_VERSION_ID_MASK | 0);
    ASSERT_EQ(1u, v0.GetSegmentCount());
    ASSERT_TRUE(v0.HasSegment(Segment::PUBLIC_SEGMENT_ID_MASK | 0));
    std::string versionPath0 = PathUtil::JoinPath(root0, v0.GetVersionFileName());

    ASSERT_TABLET_OK(tablet0.BuildSegment("cmd=add,key1=foo,value1=new_bar,ts=2,locator=0:3;"));

    const auto& v1 = tablet0.GetCurrentVersion();
    ASSERT_EQ(v1.GetVersionId(), Version::PUBLIC_VERSION_ID_MASK | 1);
    ASSERT_EQ(2u, v1.GetSegmentCount());
    ASSERT_TRUE(v1.HasSegment(Segment::PUBLIC_SEGMENT_ID_MASK | 0));
    ASSERT_TRUE(v1.HasSegment(Segment::PUBLIC_SEGMENT_ID_MASK | 1));
    std::string versionPath1 = PathUtil::JoinPath(root0, v1.GetVersionFileName());

    KVTableTestHelper tablet1;
    std::string root1 = PathUtil::JoinPath(GET_TEMP_DATA_PATH(), "1");
    ASSERT_TABLET_OK(tablet1.Open(framework::IndexRoot(root1, root1), _tabletSchema, _tabletOptions));
    std::string bulkloadId = "bulkload_foo";
    std::vector<std::string> externalFiles = {versionPath1, versionPath0};
    auto options = std::make_shared<framework::ImportExternalFileOptions>();
    options->mode = framework::ImportExternalFileOptions::VERSION_MODE;
    options->ignoreDuplicateFiles = true;

    ASSERT_TABLET_OK(tablet1.ImportExternalFiles(bulkloadId, externalFiles, options));

    framework::MergeTaskStatus taskStatus;
    ASSERT_TABLET_OK(tablet1.TriggerBulkloadTask(taskStatus));
    ASSERT_EQ(framework::MergeTaskStatus::DONE, taskStatus.code);
    Version targetVersion;
    ASSERT_TABLET_OK(framework::VersionLoader::GetVersion(Directory::GetPhysicalDirectory(root1),
                                                          taskStatus.targetVersion.GetVersionId(), &targetVersion));
    ASSERT_EQ(targetVersion.GetVersionId(), 0);
    ASSERT_EQ(targetVersion.GetSegmentCount(), 2u);
    auto taskMeta = targetVersion.GetIndexTaskQueue()->Get(framework::BULKLOAD_TASK_TYPE, bulkloadId);
    ASSERT_TRUE(taskMeta);
    ASSERT_EQ(taskMeta->GetState(), framework::IndexTaskMeta::DONE);
    KVTableTestHelper tablet2;
    ASSERT_TABLET_OK(
        tablet2.Open(framework::IndexRoot(root1, root1), _tabletSchema, _tabletOptions, targetVersion.GetVersionId()));
    ASSERT_TRUE(tablet2.Query("kv", "key1", "foo", "value1=new_bar"));
}

TEST_F(KVTableBulkloadTest, testNotIgnoreDuplicateSegment)
{
    std::string root0 = PathUtil::JoinPath(GET_TEMP_DATA_PATH(), "0");
    KVTableTestHelper tablet0;
    ASSERT_TABLET_OK(tablet0.Open(framework::IndexRoot(root0, root0), _tabletSchema, _tabletOptions));
    ASSERT_TABLET_OK(tablet0.BuildSegment("cmd=add,key1=foo,value1=old_bar,ts=1,locator=0:2;"));

    const auto& v0 = tablet0.GetCurrentVersion();
    ASSERT_EQ(v0.GetVersionId(), Version::PUBLIC_VERSION_ID_MASK | 0);
    ASSERT_EQ(1u, v0.GetSegmentCount());
    ASSERT_TRUE(v0.HasSegment(Segment::PUBLIC_SEGMENT_ID_MASK | 0));
    std::string versionPath0 = PathUtil::JoinPath(root0, v0.GetVersionFileName());

    ASSERT_TABLET_OK(tablet0.BuildSegment("cmd=add,key1=foo,value1=new_bar,ts=2,locator=0:3;"));

    const auto& v1 = tablet0.GetCurrentVersion();
    ASSERT_EQ(v1.GetVersionId(), Version::PUBLIC_VERSION_ID_MASK | 1);
    ASSERT_EQ(2u, v1.GetSegmentCount());
    ASSERT_TRUE(v1.HasSegment(Segment::PUBLIC_SEGMENT_ID_MASK | 0));
    ASSERT_TRUE(v1.HasSegment(Segment::PUBLIC_SEGMENT_ID_MASK | 1));
    std::string versionPath1 = PathUtil::JoinPath(root0, v1.GetVersionFileName());

    KVTableTestHelper tablet1;
    std::string root1 = PathUtil::JoinPath(GET_TEMP_DATA_PATH(), "1");
    ASSERT_TABLET_OK(tablet1.Open(framework::IndexRoot(root1, root1), _tabletSchema, _tabletOptions));
    std::string bulkloadId = "bulkload_foo";
    std::vector<std::string> externalFiles = {versionPath1, versionPath0};
    auto options = std::make_shared<framework::ImportExternalFileOptions>();
    options->mode = framework::ImportExternalFileOptions::VERSION_MODE;
    options->ignoreDuplicateFiles = false;

    ASSERT_TABLET_OK(tablet1.ImportExternalFiles(bulkloadId, externalFiles, options));

    framework::MergeTaskStatus taskStatus;
    ASSERT_TABLET_OK(tablet1.TriggerBulkloadTask(taskStatus));
    ASSERT_EQ(framework::MergeTaskStatus::DONE, taskStatus.code);
    Version targetVersion;
    ASSERT_TABLET_OK(framework::VersionLoader::GetVersion(Directory::GetPhysicalDirectory(root1),
                                                          taskStatus.targetVersion.GetVersionId(), &targetVersion));
    ASSERT_EQ(targetVersion.GetVersionId(), 0);
    ASSERT_EQ(targetVersion.GetSegmentCount(), 3u);
    auto taskMeta = targetVersion.GetIndexTaskQueue()->Get(framework::BULKLOAD_TASK_TYPE, bulkloadId);
    ASSERT_TRUE(taskMeta);
    ASSERT_EQ(taskMeta->GetState(), framework::IndexTaskMeta::DONE);
    KVTableTestHelper tablet2;
    ASSERT_TABLET_OK(
        tablet2.Open(framework::IndexRoot(root1, root1), _tabletSchema, _tabletOptions, targetVersion.GetVersionId()));
    ASSERT_TRUE(tablet2.Query("kv", "key1", "foo", "value1=old_bar"));
}

TEST_F(KVTableBulkloadTest, testRtWithBulkloadWithRt)
{
    // rt -> bulkload ->rt
    std::string root0 = PathUtil::JoinPath(GET_TEMP_DATA_PATH(), "0");
    KVTableTestHelper tablet0;
    ASSERT_TABLET_OK(tablet0.Open(framework::IndexRoot(root0, root0), _tabletSchema, _tabletOptions));
    ASSERT_TABLET_OK(tablet0.BuildSegment("cmd=add,key1=foo,value1=bulkload_value,ts=0,locator=0:1;"));

    const auto& v0 = tablet0.GetCurrentVersion();
    ASSERT_EQ(v0.GetVersionId(), Version::PUBLIC_VERSION_ID_MASK | 0);
    ASSERT_EQ(1u, v0.GetSegmentCount());
    ASSERT_TRUE(v0.HasSegment(Segment::PUBLIC_SEGMENT_ID_MASK | 0));
    std::string versionPath0 = PathUtil::JoinPath(root0, v0.GetVersionFileName());

    KVTableTestHelper tablet1;
    std::string root1 = PathUtil::JoinPath(GET_TEMP_DATA_PATH(), "1");
    ASSERT_TABLET_OK(tablet1.Open(framework::IndexRoot(root1, root1), _tabletSchema, _tabletOptions));
    ASSERT_TABLET_OK(tablet1.BuildSegment("cmd=add,key1=foo,value1=rt_value,ts=1,locator=0:2;"));

    std::string bulkloadId = "bulkload_foo";
    std::vector<std::string> externalFiles = {versionPath0};
    auto options = std::make_shared<framework::ImportExternalFileOptions>();
    options->mode = framework::ImportExternalFileOptions::VERSION_MODE;
    ASSERT_TABLET_OK(tablet1.ImportExternalFiles(bulkloadId, externalFiles, options));
    ASSERT_TABLET_OK(tablet1.BuildSegment("cmd=add,key1=foo,value1=rt_value2,ts=2,locator=0:3;"));
    ASSERT_TRUE(tablet1.Query("kv", "key1", "foo", "value1=rt_value2"));

    ASSERT_TABLET_OK(tablet1.Bulkload());
    Version targetVersion;
    ASSERT_TABLET_OK(framework::VersionLoader::GetVersion(Directory::GetPhysicalDirectory(root1), INVALID_VERSIONID,
                                                          &targetVersion));
    ASSERT_EQ(targetVersion.GetVersionId(), Version::PUBLIC_VERSION_ID_MASK | 3);
    auto taskMeta = targetVersion.GetIndexTaskQueue()->Get(framework::BULKLOAD_TASK_TYPE, bulkloadId);
    ASSERT_TRUE(taskMeta);
    ASSERT_EQ(taskMeta->GetState(), framework::IndexTaskMeta::DONE);
    ASSERT_TRUE(tablet1.Query("kv", "key1", "foo", "value1=rt_value2"));
}

TEST_F(KVTableBulkloadTest, testRtWithRtWithBulkload)
{
    // rt -> rt -> bulkload
    std::string root0 = PathUtil::JoinPath(GET_TEMP_DATA_PATH(), "0");
    KVTableTestHelper tablet0;
    ASSERT_TABLET_OK(tablet0.Open(framework::IndexRoot(root0, root0), _tabletSchema, _tabletOptions));
    ASSERT_TABLET_OK(tablet0.BuildSegment("cmd=add,key1=foo,value1=bulkload_value,ts=0,locator=0:1;"));

    const auto& v0 = tablet0.GetCurrentVersion();
    ASSERT_EQ(v0.GetVersionId(), Version::PUBLIC_VERSION_ID_MASK | 0);
    ASSERT_EQ(1u, v0.GetSegmentCount());
    ASSERT_TRUE(v0.HasSegment(Segment::PUBLIC_SEGMENT_ID_MASK | 0));
    std::string versionPath0 = PathUtil::JoinPath(root0, v0.GetVersionFileName());

    KVTableTestHelper tablet1;
    std::string root1 = PathUtil::JoinPath(GET_TEMP_DATA_PATH(), "1");
    ASSERT_TABLET_OK(tablet1.Open(framework::IndexRoot(root1, root1), _tabletSchema, _tabletOptions));
    ASSERT_TABLET_OK(tablet1.BuildSegment("cmd=add,key1=foo,value1=rt_value,ts=1,locator=0:2;"));
    ASSERT_TABLET_OK(tablet1.BuildSegment("cmd=add,key1=foo,value1=rt_value2,ts=2,locator=0:3;"));

    std::string bulkloadId = "bulkload_foo";
    std::vector<std::string> externalFiles = {versionPath0};
    auto options = std::make_shared<framework::ImportExternalFileOptions>();
    options->mode = framework::ImportExternalFileOptions::VERSION_MODE;
    ASSERT_TABLET_OK(tablet1.ImportExternalFiles(bulkloadId, externalFiles, options));
    ASSERT_TRUE(tablet1.Query("kv", "key1", "foo", "value1=rt_value2"));

    ASSERT_TABLET_OK(tablet1.Bulkload());
    Version targetVersion;
    ASSERT_TABLET_OK(framework::VersionLoader::GetVersion(Directory::GetPhysicalDirectory(root1), INVALID_VERSIONID,
                                                          &targetVersion));
    ASSERT_EQ(targetVersion.GetVersionId(), Version::PUBLIC_VERSION_ID_MASK | 3);
    auto taskMeta = targetVersion.GetIndexTaskQueue()->Get(framework::BULKLOAD_TASK_TYPE, bulkloadId);
    ASSERT_TRUE(taskMeta);
    ASSERT_EQ(taskMeta->GetState(), framework::IndexTaskMeta::DONE);
    ASSERT_TRUE(tablet1.Query("kv", "key1", "foo", "value1=bulkload_value"));
}

TEST_F(KVTableBulkloadTest, testInvalidImport)
{
    // prepare bulkload data with 2 shards
    std::string root0 = PathUtil::JoinPath(GET_TEMP_DATA_PATH(), "0");
    KVTableTestHelper tablet0;
    ASSERT_TABLET_OK(tablet0.Open(framework::IndexRoot(root0, root0), _tabletSchema, _tabletOptions));
    ASSERT_TABLET_OK(tablet0.BuildSegment("cmd=add,key1=1,value1=a,ts=1,locator=0:2;"));
    ASSERT_TABLET_OK(tablet0.BuildSegment("cmd=add,key1=2,value1=b,ts=2,locator=0:3;"));
    ASSERT_TABLET_OK(tablet0.BuildSegment("cmd=add,key1=3,value1=c,ts=3,locator=0:4;"));
    ASSERT_TABLET_OK(tablet0.Merge(TableTestHelper::MergeOption::MergeAutoReadOption(true)));

    const auto& v0 = tablet0.GetCurrentVersion();
    ASSERT_EQ(v0.GetVersionId(), Version::PUBLIC_VERSION_ID_MASK | 3);
    ASSERT_EQ(v0.GetSegmentCount(), 2u);
    ASSERT_TRUE(v0.HasSegment(0));
    ASSERT_TRUE(v0.HasSegment(1));

    ASSERT_TRUE(tablet0.Query("kv", "key1", "1", "value1=a"));
    ASSERT_TRUE(tablet0.Query("kv", "key1", "2", "value1=b"));
    ASSERT_TRUE(tablet0.Query("kv", "key1", "3", "value1=c"));

    std::string versionPath0 = PathUtil::JoinPath(root0, v0.GetVersionFileName());

    // prepare tablet with 4 shards
    KVTableTestHelper tablet1;
    std::string root1 = GET_TEMP_DATA_PATH() + "/1";
    std::string jsonStr = R"( {
    "online_index_config": {
        "build_config": {
            "sharding_column_num" : 4,
            "level_num" : 3
        }
    },
    "offline_index_config": {
        "build_config": {
            "sharding_column_num" : 4,
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
    ASSERT_TABLET_OK(tablet1.Open(framework::IndexRoot(root1, root1), _tabletSchema, tabletOptions));

    std::string bulkloadId = "bulkload_foo";
    std::vector<std::string> externalFiles = {versionPath0};
    auto options = std::make_shared<framework::ImportExternalFileOptions>();
    options->mode = framework::ImportExternalFileOptions::VERSION_MODE;

    ASSERT_TABLET_OK(tablet1.ImportExternalFiles(bulkloadId, externalFiles, options));
    ASSERT_FALSE(tablet1.Bulkload().IsOK());
}

} // namespace indexlibv2::table
