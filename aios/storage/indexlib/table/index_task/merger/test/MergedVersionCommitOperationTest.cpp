#include "indexlib/table/index_task/merger/MergedVersionCommitOperation.h"

#include "indexlib/config/MergeConfig.h"
#include "indexlib/config/OfflineConfig.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/MountOption.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/file_system/package/PackageFileTagConfigList.h"
#include "indexlib/framework/SegmentInfo.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/framework/Version.h"
#include "indexlib/framework/VersionLoader.h"
#include "indexlib/framework/index_task/IIndexTaskResourceCreator.h"
#include "indexlib/framework/index_task/IndexTaskResourceManager.h"
#include "indexlib/table/index_task/IndexTaskConstant.h"
#include "indexlib/table/index_task/merger/MergePlan.h"
#include "indexlib/table/normal_table/test/NormalTabletSchemaMaker.h"
#include "unittest/unittest.h"

namespace indexlibv2 { namespace table {

class FakeResourceCreator : public framework::IIndexTaskResourceCreator
{
public:
    FakeResourceCreator() : IIndexTaskResourceCreator() {}
    std::unique_ptr<framework::IndexTaskResource> CreateResource(const std::string& name,
                                                                 const framework::IndexTaskResourceType& type) override
    {
        assert(type == MERGE_PLAN);
        return std::make_unique<MergePlan>(name, type);
    }

private:
};

class FakeIndexTaskContext : public framework::IndexTaskContext
{
public:
    FakeIndexTaskContext()
    {
        _tabletOptions.reset(new config::TabletOptions);
        _tabletOptions->SetIsOnline(false);
    }
    void SetIndexRoot(std::shared_ptr<indexlib::file_system::Directory> indexRoot) { _indexRoot = indexRoot; }
    void AddFenceDir(framework::IndexOperationId id, std::shared_ptr<indexlib::file_system::Directory> fenceDir)
    {
        _fenceDirMap[id] = fenceDir->GetIDirectory();
    }
    void SetTabletData(std::shared_ptr<framework::TabletData> tabletData) { _tabletData = tabletData; }

private:
    Status _status;
    std::map<framework::IndexOperationId, std::shared_ptr<indexlib::file_system::IDirectory>> _fenceDirMap;
};

class MergedVersionCommitOperationTest : public TESTBASE
{
public:
    MergedVersionCommitOperationTest() {}
    ~MergedVersionCommitOperationTest() {}

public:
    void setUp() override { ::setenv("VALIDATE_MERGED_VERSION", "false", /*overwrite*/ 1); }
    void tearDown() override { ::unsetenv("VALIDATE_MERGED_VERSION"); }

private:
    static std::string GenerateTestString(size_t length, char c);
};

std::string MergedVersionCommitOperationTest::GenerateTestString(size_t length, char c)
{
    return std::string(length, c);
}

TEST_F(MergedVersionCommitOperationTest, TestCommitVersion)
{
    auto fsOptions = indexlib::file_system::FileSystemOptions::Offline();
    auto fileSystem =
        indexlib::file_system::FileSystemCreator::Create("test", GET_TEMP_DATA_PATH(), fsOptions).GetOrThrow();
    auto rootDir = indexlib::file_system::Directory::Get(fileSystem);
    auto pkDir = rootDir->MakeDirectory("segment_0_level_0/index/pk");
    pkDir->Store("data", "data_info");
    auto resourceDir = rootDir->MakeDirectory(ADAPTIVE_DICT_DIR_NAME);
    resourceDir->Store("resource", "resourceInfo");
    // test other dir are moved by other file system(when segment mv op failover)
    auto fileSystem1 =
        indexlib::file_system::FileSystemCreator::Create("test", GET_TEMP_DATA_PATH(), fsOptions).GetOrThrow();
    auto rootDir1 = indexlib::file_system::Directory::Get(fileSystem1);
    auto orcDir = rootDir1->MakeDirectory("segment_0_level_0/orc/orc_name");
    orcDir->Store("orc_data", "data_info");
    auto tabletData = std::make_shared<framework::TabletData>("test");
    // tabletData->TEST_SetOnDiskVersion(framework::Version(10000000));
    std::shared_ptr<framework::IndexTaskResourceManager> resourceManager(new framework::IndexTaskResourceManager);
    std::shared_ptr<framework::IIndexTaskResourceCreator> creator(new FakeResourceCreator);
    auto testRoot = GET_TEMP_DATA_PATH();
    auto fs = indexlib::file_system::FileSystemCreator::Create("test", testRoot).GetOrThrow();
    ASSERT_TRUE(resourceManager->Init(rootDir, "work", creator).IsOK());
    FakeIndexTaskContext context;
    context._resourceManager = resourceManager;
    context.SetTabletData(tabletData);
    context.SetIndexRoot(rootDir1);
    ASSERT_TRUE(context.SetDesignateTask("merge", "default_merge"));
    framework::Version version(0);
    version.AddSegment(0);
    framework::IndexOperationDescription desc(0, MergedVersionCommitOperation::OPERATION_TYPE);
    desc.AddParameter(PARAM_TARGET_VERSION, autil::legacy::ToJsonString(version));
    MergedVersionCommitOperation op(desc);
    auto status = op.Execute(context);
    ASSERT_TRUE(status.IsOK());
    auto fileSystem2 =
        indexlib::file_system::FileSystemCreator::Create("test", GET_TEMP_DATA_PATH(), fsOptions).GetOrThrow();
    auto mountType = indexlib::file_system::FSMT_READ_ONLY;
    status = toStatus(fileSystem2->MountVersion(GET_TEMP_DATA_PATH(), 0, "", mountType, nullptr));
    ASSERT_TRUE(status.IsOK());
    auto rootDir2 = indexlib::file_system::Directory::Get(fileSystem2);
    status = framework::VersionLoader::GetVersion(rootDir2, 0, &version);
    ASSERT_TRUE(status.IsOK());
    ASSERT_TRUE(version.GetCommitTime() > 0);
    ASSERT_TRUE(rootDir2->IsExist("segment_0_level_0/orc/orc_name/orc_data"));
    ASSERT_TRUE(rootDir2->IsExist("segment_0_level_0/index/pk/data"));
    ASSERT_TRUE(rootDir2->IsExist(std::string(ADAPTIVE_DICT_DIR_NAME) + "/resource"));
}

TEST_F(MergedVersionCommitOperationTest, TestUpdateSegmentDescriptions)
{
    auto testRoot = GET_TEMP_DATA_PATH();
    auto fs = indexlib::file_system::FileSystemCreator::Create("test", testRoot).GetOrThrow();
    auto resourceDir = indexlib::file_system::Directory::Get(fs);

    framework::Version version;
    version.AddSegment(1);
    version.AddSegment(2);
    std::shared_ptr<framework::IndexTaskResourceManager> resourceManager(new framework::IndexTaskResourceManager);
    framework::IndexOperationDescription desc;
    std::shared_ptr<framework::IIndexTaskResourceCreator> creator(new FakeResourceCreator);
    ASSERT_TRUE(resourceManager->Init(resourceDir, "work", creator).IsOK());
    MergedVersionCommitOperation commitOperation(desc);
    ASSERT_TRUE(commitOperation.UpdateSegmentDescriptions(resourceManager, &version).IsOK());

    std::shared_ptr<MergePlan> mergePlan;
    ASSERT_TRUE(resourceManager->LoadResource(MERGE_PLAN, MERGE_PLAN, mergePlan).IsNotFound());
    ASSERT_TRUE(resourceManager->CreateResource(MERGE_PLAN, MERGE_PLAN, mergePlan).IsOK());
    SegmentMergePlan segmentMergePlan1, segmentMergePlan2;
    framework::SegmentInfo segInfo1, segInfo2;
    framework::SegmentStatistics stat(2, {}, {{"key", "value"}});
    segInfo2.SetSegmentStatistics(stat);
    segmentMergePlan1.AddTargetSegment(1, segInfo1);
    segmentMergePlan1.AddTargetSegment(2, segInfo2);
    mergePlan->AddMergePlan(segmentMergePlan1);
    mergePlan->AddMergePlan(segmentMergePlan2);
    ASSERT_TRUE(resourceManager->CommitResource(MERGE_PLAN).IsOK());
    ASSERT_TRUE(commitOperation.UpdateSegmentDescriptions(resourceManager, &version).IsOK());
    auto stats = version.GetSegmentDescriptions()->GetSegmentStatisticsVector();
    ASSERT_EQ(1, stats.size());
    ASSERT_EQ(stat, stats[0]);
}

TEST_F(MergedVersionCommitOperationTest, TestMergeResourceDir)
{
    size_t PAGE_SIZE = getpagesize();
    auto fsOptions = indexlib::file_system::FileSystemOptions::Offline();
    auto fileSystem =
        indexlib::file_system::FileSystemCreator::Create("test", GET_TEMP_DATA_PATH(), fsOptions).GetOrThrow();
    auto rootDir = indexlib::file_system::Directory::Get(fileSystem);
    auto pkDir = rootDir->MakeDirectory("segment_0_level_0/index/pk");
    pkDir->Store("data", "01234567890123456789");
    auto adaptiveDir = rootDir->MakeDirectory(ADAPTIVE_DICT_DIR_NAME);
    adaptiveDir->Store("data1", GenerateTestString(20 * PAGE_SIZE, '0'));
    auto innerAdaptiveDir = adaptiveDir->MakeDirectory("inner1");
    innerAdaptiveDir->Store("data2", GenerateTestString(4 * PAGE_SIZE, '1'));
    innerAdaptiveDir->Store("data3", GenerateTestString(3 * PAGE_SIZE, '2'));
    innerAdaptiveDir = adaptiveDir->MakeDirectory("inner2");
    innerAdaptiveDir->Store("data4", GenerateTestString(2 * PAGE_SIZE, '3'));
    innerAdaptiveDir->Store("data5", GenerateTestString(5 * PAGE_SIZE, '4'));
    auto truncateDir = rootDir->MakeDirectory(TRUNCATE_META_DIR_NAME);
    truncateDir->Store("resource", GenerateTestString(7 * PAGE_SIZE, '5'));

    auto tabletData = std::make_shared<framework::TabletData>("test");
    std::shared_ptr<framework::IndexTaskResourceManager> resourceManager(new framework::IndexTaskResourceManager);
    std::shared_ptr<framework::IIndexTaskResourceCreator> creator(new FakeResourceCreator);
    auto testRoot = GET_TEMP_DATA_PATH();
    auto fs = indexlib::file_system::FileSystemCreator::Create("test", testRoot).GetOrThrow();

    ASSERT_TRUE(resourceManager->Init(rootDir, "work", creator).IsOK());
    FakeIndexTaskContext context;
    auto tabletSchema = NormalTabletSchemaMaker::Make(
        /*field=*/"string1:string", /*index=*/"pk:primarykey64:string1", /*attribute=*/"string1", /*summaryNames=*/"");
    context.TEST_SetTabletSchema(tabletSchema);
    context._resourceManager = resourceManager;
    context.SetTabletData(tabletData);
    context.SetIndexRoot(rootDir);
    auto fence0Dir = rootDir->MakeDirectory("0");
    context.TEST_SetFenceRoot(rootDir);
    context.AddFenceDir(0, fence0Dir);
    auto& mergeConfig = context.GetTabletOptions()->TEST_GetOfflineConfig().TEST_GetMergeConfig();
    mergeConfig.TEST_SetEnablePackageFile(true);
    mergeConfig.TEST_SetPackageFileSizeThresholdBytes(10 * PAGE_SIZE);
    indexlib::file_system::PackageFileTagConfigList packageFileTagConfigList;
    packageFileTagConfigList.TEST_PATCH();
    mergeConfig.TEST_SetPackageFileTagConfigList(packageFileTagConfigList);
    ASSERT_TRUE(context.SetDesignateTask("merge", "default_merge"));
    framework::Version version(0);
    version.AddSegment(0);
    framework::IndexOperationDescription desc(0, MergedVersionCommitOperation::OPERATION_TYPE);
    desc.AddParameter(PARAM_TARGET_VERSION, autil::legacy::ToJsonString(version));
    MergedVersionCommitOperation op(desc);
    auto status = op.Execute(context);
    ASSERT_TRUE(status.IsOK());

    auto outputFileSystem =
        indexlib::file_system::FileSystemCreator::Create("test", GET_TEMP_DATA_PATH(), fsOptions).GetOrThrow();
    auto mountType = indexlib::file_system::FSMT_READ_ONLY;
    status = toStatus(outputFileSystem->MountVersion(GET_TEMP_DATA_PATH(), 0, "", mountType, nullptr));
    ASSERT_TRUE(status.IsOK());
    auto outputRootDir = indexlib::file_system::Directory::Get(outputFileSystem);
    ASSERT_TRUE(toStatus(outputRootDir->GetFileSystem()->MountDir(
                             outputRootDir->GetPhysicalPath(""), std::string(ADAPTIVE_DICT_DIR_NAME),
                             std::string(ADAPTIVE_DICT_DIR_NAME), indexlib::file_system::FSMT_READ_WRITE, true))
                    .IsOK());
    EXPECT_TRUE(outputRootDir->IsExist("segment_0_level_0/index/pk/data"));
    EXPECT_TRUE(outputRootDir->IsExist(std::string(ADAPTIVE_DICT_DIR_NAME) + "/package_file.__meta__"));
    EXPECT_TRUE(outputRootDir->IsExist(std::string(ADAPTIVE_DICT_DIR_NAME) + "/package_file.__data__0"));
    EXPECT_TRUE(outputRootDir->IsExist(std::string(ADAPTIVE_DICT_DIR_NAME) + "/package_file.__data__1"));
    EXPECT_TRUE(outputRootDir->IsExist(std::string(ADAPTIVE_DICT_DIR_NAME) + "/package_file.__data__2"));
    EXPECT_FALSE(outputRootDir->IsExist(std::string(ADAPTIVE_DICT_DIR_NAME) + "/package_file.__data__3"));
    ASSERT_TRUE(toStatus(outputRootDir->GetFileSystem()->MountDir(
                             outputRootDir->GetPhysicalPath(""), std::string(TRUNCATE_META_DIR_NAME),
                             std::string(TRUNCATE_META_DIR_NAME), indexlib::file_system::FSMT_READ_WRITE, true))
                    .IsOK());
    EXPECT_TRUE(outputRootDir->IsExist(std::string(TRUNCATE_META_DIR_NAME) + "/package_file.__meta__"));
    EXPECT_TRUE(outputRootDir->IsExist(std::string(TRUNCATE_META_DIR_NAME) + "/package_file.__data__0"));
}

TEST_F(MergedVersionCommitOperationTest, TestMergeResourceDirFileAlign)
{
    size_t PAGE_SIZE = getpagesize();
    auto fsOptions = indexlib::file_system::FileSystemOptions::Offline();
    auto fileSystem =
        indexlib::file_system::FileSystemCreator::Create("test", GET_TEMP_DATA_PATH(), fsOptions).GetOrThrow();
    auto rootDir = indexlib::file_system::Directory::Get(fileSystem);
    auto pkDir = rootDir->MakeDirectory("segment_0_level_0/index/pk");
    pkDir->Store("data", "01234567890123456789");
    auto adaptiveDir = rootDir->MakeDirectory(ADAPTIVE_DICT_DIR_NAME);
    adaptiveDir->Store("data", GenerateTestString(20 * PAGE_SIZE, '0'));
    auto innerAdaptiveDir = adaptiveDir->MakeDirectory("inner1");
    innerAdaptiveDir->Store("data1", GenerateTestString(2 * PAGE_SIZE + 1, '1'));
    innerAdaptiveDir->Store("data2", GenerateTestString(3 * PAGE_SIZE + 1, '2'));
    innerAdaptiveDir = adaptiveDir->MakeDirectory("inner2");
    innerAdaptiveDir->Store("data", GenerateTestString(6 * PAGE_SIZE, '3'));
    innerAdaptiveDir = adaptiveDir->MakeDirectory("inner3");
    innerAdaptiveDir->Store("data", GenerateTestString(9 * PAGE_SIZE, '4'));
    innerAdaptiveDir = adaptiveDir->MakeDirectory("inner4");
    innerAdaptiveDir->Store("data", GenerateTestString(10 * PAGE_SIZE, '5'));

    auto tabletData = std::make_shared<framework::TabletData>("test");
    std::shared_ptr<framework::IndexTaskResourceManager> resourceManager(new framework::IndexTaskResourceManager);
    std::shared_ptr<framework::IIndexTaskResourceCreator> creator(new FakeResourceCreator);
    auto testRoot = GET_TEMP_DATA_PATH();
    auto fs = indexlib::file_system::FileSystemCreator::Create("test", testRoot).GetOrThrow();
    ASSERT_TRUE(resourceManager->Init(rootDir, "work", creator).IsOK());
    FakeIndexTaskContext context;
    context._resourceManager = resourceManager;
    context.SetTabletData(tabletData);
    context.SetIndexRoot(rootDir);
    auto fence0Dir = rootDir->MakeDirectory("0");
    context.TEST_SetFenceRoot(rootDir);
    context.AddFenceDir(0, fence0Dir);
    auto& mergeConfig = context.GetTabletOptions()->TEST_GetOfflineConfig().TEST_GetMergeConfig();
    mergeConfig.TEST_SetEnablePackageFile(true);
    mergeConfig.TEST_SetPackageFileSizeThresholdBytes(10 * PAGE_SIZE);
    indexlib::file_system::PackageFileTagConfigList packageFileTagConfigList;
    packageFileTagConfigList.TEST_PATCH();
    mergeConfig.TEST_SetPackageFileTagConfigList(packageFileTagConfigList);
    ASSERT_TRUE(context.SetDesignateTask("merge", "default_merge"));
    framework::Version version(0);
    version.AddSegment(0);
    framework::IndexOperationDescription desc(0, MergedVersionCommitOperation::OPERATION_TYPE);
    desc.AddParameter(PARAM_TARGET_VERSION, autil::legacy::ToJsonString(version));
    MergedVersionCommitOperation op(desc);
    auto status = op.Execute(context);
    ASSERT_TRUE(status.IsOK());

    auto outputFileSystem =
        indexlib::file_system::FileSystemCreator::Create("test", GET_TEMP_DATA_PATH(), fsOptions).GetOrThrow();
    auto mountType = indexlib::file_system::FSMT_READ_ONLY;
    status = toStatus(outputFileSystem->MountVersion(GET_TEMP_DATA_PATH(), 0, "", mountType, nullptr));
    ASSERT_TRUE(status.IsOK());
    auto outputRootDir = indexlib::file_system::Directory::Get(outputFileSystem);
    ASSERT_TRUE(outputRootDir->IsExist("segment_0_level_0/index/pk/data"));
    auto ec = outputRootDir->GetFileSystem()->MountDir(
        outputRootDir->GetPhysicalPath(""), std::string(ADAPTIVE_DICT_DIR_NAME), std::string(ADAPTIVE_DICT_DIR_NAME),
        indexlib::file_system::FSMT_READ_WRITE, true);
    ASSERT_EQ(ec, indexlib::file_system::FSEC_OK);
    EXPECT_TRUE(outputRootDir->IsExist(std::string(ADAPTIVE_DICT_DIR_NAME) + "/package_file.__meta__"));
    EXPECT_TRUE(outputRootDir->IsExist(std::string(ADAPTIVE_DICT_DIR_NAME) + "/package_file.__data__0"));
    EXPECT_TRUE(outputRootDir->IsExist(std::string(ADAPTIVE_DICT_DIR_NAME) + "/package_file.__data__1"));
    EXPECT_TRUE(outputRootDir->IsExist(std::string(ADAPTIVE_DICT_DIR_NAME) + "/package_file.__data__2"));
    EXPECT_TRUE(outputRootDir->IsExist(std::string(ADAPTIVE_DICT_DIR_NAME) + "/package_file.__data__3"));
    EXPECT_TRUE(outputRootDir->IsExist(std::string(ADAPTIVE_DICT_DIR_NAME) + "/package_file.__data__4"));
    EXPECT_FALSE(outputRootDir->IsExist(std::string(ADAPTIVE_DICT_DIR_NAME) + "/package_file.__data__5"));
}

TEST_F(MergedVersionCommitOperationTest, TestMergeResourceDirMultiThread)
{
    size_t PAGE_SIZE = getpagesize();
    auto fsOptions = indexlib::file_system::FileSystemOptions::Offline();
    auto fileSystem =
        indexlib::file_system::FileSystemCreator::Create("test", GET_TEMP_DATA_PATH(), fsOptions).GetOrThrow();
    auto rootDir = indexlib::file_system::Directory::Get(fileSystem);
    auto pkDir = rootDir->MakeDirectory("segment_0_level_0/index/pk");
    pkDir->Store("data", "01234567890123456789");
    auto adaptiveDir = rootDir->MakeDirectory(ADAPTIVE_DICT_DIR_NAME);
    adaptiveDir->Store("data1", GenerateTestString(20 * PAGE_SIZE, '0'));
    auto innerAdaptiveDir = adaptiveDir->MakeDirectory("inner1");
    innerAdaptiveDir->Store("data2", GenerateTestString(4 * PAGE_SIZE, '1'));
    innerAdaptiveDir->Store("data3", GenerateTestString(3 * PAGE_SIZE, '2'));
    innerAdaptiveDir = adaptiveDir->MakeDirectory("inner2");
    innerAdaptiveDir->Store("data4", GenerateTestString(2 * PAGE_SIZE, '3'));
    innerAdaptiveDir->Store("data5", GenerateTestString(5 * PAGE_SIZE, '4'));
    auto truncateDir = rootDir->MakeDirectory(TRUNCATE_META_DIR_NAME);
    truncateDir->Store("resource", GenerateTestString(7 * PAGE_SIZE, '5'));

    auto tabletData = std::make_shared<framework::TabletData>("test");
    std::shared_ptr<framework::IndexTaskResourceManager> resourceManager(new framework::IndexTaskResourceManager);
    std::shared_ptr<framework::IIndexTaskResourceCreator> creator(new FakeResourceCreator);
    auto testRoot = GET_TEMP_DATA_PATH();
    auto fs = indexlib::file_system::FileSystemCreator::Create("test", testRoot).GetOrThrow();

    ASSERT_TRUE(resourceManager->Init(rootDir, "work", creator).IsOK());
    FakeIndexTaskContext context;
    auto tabletSchema = NormalTabletSchemaMaker::Make(
        /*field=*/"string1:string", /*index=*/"pk:primarykey64:string1", /*attribute=*/"string1", /*summaryNames=*/"");
    context.TEST_SetTabletSchema(tabletSchema);
    context._resourceManager = resourceManager;
    context.SetTabletData(tabletData);
    context.SetIndexRoot(rootDir);
    auto fence0Dir = rootDir->MakeDirectory("0");
    context.TEST_SetFenceRoot(rootDir);
    context.AddFenceDir(0, fence0Dir);
    auto& mergeConfig = context.GetTabletOptions()->TEST_GetOfflineConfig().TEST_GetMergeConfig();
    mergeConfig.TEST_SetEnablePackageFile(true);
    mergeConfig.TEST_SetPackageFileSizeThresholdBytes(10 * PAGE_SIZE);
    mergeConfig.TEST_SetMergePackageThreadCount(3);
    indexlib::file_system::PackageFileTagConfigList packageFileTagConfigList;
    packageFileTagConfigList.TEST_PATCH();
    mergeConfig.TEST_SetPackageFileTagConfigList(packageFileTagConfigList);
    ASSERT_TRUE(context.SetDesignateTask("merge", "default_merge"));
    framework::Version version(0);
    version.AddSegment(0);
    framework::IndexOperationDescription desc(0, MergedVersionCommitOperation::OPERATION_TYPE);
    desc.AddParameter(PARAM_TARGET_VERSION, autil::legacy::ToJsonString(version));
    MergedVersionCommitOperation op(desc);
    auto status = op.Execute(context);
    ASSERT_TRUE(status.IsOK());

    auto outputFileSystem =
        indexlib::file_system::FileSystemCreator::Create("test", GET_TEMP_DATA_PATH(), fsOptions).GetOrThrow();
    auto mountType = indexlib::file_system::FSMT_READ_ONLY;
    status = toStatus(outputFileSystem->MountVersion(GET_TEMP_DATA_PATH(), 0, "", mountType, nullptr));
    ASSERT_TRUE(status.IsOK());
    auto outputRootDir = indexlib::file_system::Directory::Get(outputFileSystem);
    ASSERT_TRUE(toStatus(outputRootDir->GetFileSystem()->MountDir(
                             outputRootDir->GetPhysicalPath(""), std::string(ADAPTIVE_DICT_DIR_NAME),
                             std::string(ADAPTIVE_DICT_DIR_NAME), indexlib::file_system::FSMT_READ_WRITE, true))
                    .IsOK());
    EXPECT_TRUE(outputRootDir->IsExist("segment_0_level_0/index/pk/data"));
    EXPECT_TRUE(outputRootDir->IsExist(std::string(ADAPTIVE_DICT_DIR_NAME) + "/package_file.__meta__"));
    EXPECT_TRUE(outputRootDir->IsExist(std::string(ADAPTIVE_DICT_DIR_NAME) + "/package_file.__data__0"));
    EXPECT_TRUE(outputRootDir->IsExist(std::string(ADAPTIVE_DICT_DIR_NAME) + "/package_file.__data__1"));
    EXPECT_TRUE(outputRootDir->IsExist(std::string(ADAPTIVE_DICT_DIR_NAME) + "/package_file.__data__2"));
    EXPECT_FALSE(outputRootDir->IsExist(std::string(ADAPTIVE_DICT_DIR_NAME) + "/package_file.__data__3"));
    ASSERT_TRUE(toStatus(outputRootDir->GetFileSystem()->MountDir(
                             outputRootDir->GetPhysicalPath(""), std::string(TRUNCATE_META_DIR_NAME),
                             std::string(TRUNCATE_META_DIR_NAME), indexlib::file_system::FSMT_READ_WRITE, true))
                    .IsOK());
    EXPECT_TRUE(outputRootDir->IsExist(std::string(TRUNCATE_META_DIR_NAME) + "/package_file.__meta__"));
    EXPECT_TRUE(outputRootDir->IsExist(std::string(TRUNCATE_META_DIR_NAME) + "/package_file.__data__0"));
}

}} // namespace indexlibv2::table
