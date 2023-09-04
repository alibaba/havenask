#include "indexlib/merger/test/package_merge_file_system_unittest.h"

#include "autil/StringUtil.h"
#include "indexlib/config/impl/merge_config_impl.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/MergeDirsOption.h"
#include "indexlib/file_system/archive/ArchiveFile.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/file_system/package/PackageDiskStorage.h"
#include "indexlib/file_system/package/PackageFileTagConfigList.h"
#include "indexlib/index_base/branch_fs.h"
#include "indexlib/index_define.h"
#include "indexlib/util/PathUtil.h"

using namespace std;
using namespace indexlib::index_base;
using namespace indexlib::file_system;
using namespace indexlib::util;
namespace indexlib { namespace merger {
IE_LOG_SETUP(merger, PackageMergeFileSystemTest);

PackageMergeFileSystemTest::PackageMergeFileSystemTest() {}

PackageMergeFileSystemTest::~PackageMergeFileSystemTest() {}

void PackageMergeFileSystemTest::CaseSetUp() {}

void PackageMergeFileSystemTest::CaseTearDown() {}

void PackageMergeFileSystemTest::TestSimpleProcess()
{
    FileSystemOptions fsOptions;
    fsOptions.isOffline = true;
    fsOptions.outputStorage = FSST_PACKAGE_DISK;

    config::MergeConfig mergeConfig;
    mergeConfig.SetEnablePackageFile(true);
    mergeConfig.SetCheckpointInterval(10);
    auto logicalFs =
        FileSystemCreator::Create("", /*rootPath=*/GET_TEMP_DATA_PATH() + "/instance_0", fsOptions, nullptr, false)
            .GetOrThrow();
    PackageMergeFileSystemPtr packageMergeFileSystem = DYNAMIC_POINTER_CAST(
        PackageMergeFileSystem, MergeFileSystem::Create(/*rootPath=*/GET_TEMP_DATA_PATH() + "/instance_0", mergeConfig,
                                                        /*instanceId=*/0, logicalFs));
    ASSERT_NE(packageMergeFileSystem, nullptr);
    vector<string> targetSegmentPaths {"segment_1", "segment_2"};
    packageMergeFileSystem->Init(targetSegmentPaths);

    packageMergeFileSystem->MakeDirectory(/*relativePath=*/"segment_1/index");
    // Check it's ok to make directory with same name.
    packageMergeFileSystem->MakeDirectory(/*relativePath=*/"segment_1/index");
    packageMergeFileSystem->MakeDirectory(/*relativePath=*/"segment_2/index");
    EXPECT_NE(packageMergeFileSystem->GetDirectory(/*relativePath=*/"segment_1/index"), nullptr);
    EXPECT_NE(packageMergeFileSystem->GetDirectory(/*relativePath=*/"segment_2/index"), nullptr);

    packageMergeFileSystem->MakeCheckpoint("filename1");
    EXPECT_FALSE(packageMergeFileSystem->HasCheckpoint("filename1"));
    packageMergeFileSystem->Commit();
    packageMergeFileSystem->MakeCheckpoint("filename2");
    EXPECT_TRUE(packageMergeFileSystem->HasCheckpoint("filename1"));
    EXPECT_FALSE(packageMergeFileSystem->HasCheckpoint("filename2"));

    packageMergeFileSystem->Close();
}

void PackageMergeFileSystemTest::TestRecover()
{
    FileSystemOptions fsOptions;
    fsOptions.isOffline = true;
    fsOptions.outputStorage = FSST_PACKAGE_DISK;

    config::MergeConfig mergeConfig;
    mergeConfig.SetEnablePackageFile(true);
    mergeConfig.SetCheckpointInterval(0);

    auto logicalFs =
        FileSystemCreator::Create("", /*rootPath=*/GET_TEMP_DATA_PATH() + "/instance_0", fsOptions, nullptr, false)
            .GetOrThrow();
    PackageMergeFileSystemPtr packageMergeFileSystem = DYNAMIC_POINTER_CAST(
        PackageMergeFileSystem, MergeFileSystem::Create(/*rootPath=*/GET_TEMP_DATA_PATH() + "/instance_0", mergeConfig,
                                                        /*instanceId=*/0, logicalFs));
    ASSERT_NE(packageMergeFileSystem, nullptr);
    vector<string> targetSegmentPaths {"segment_1", "segment_2", "segment_3", "segment_4", "segment_5"};
    packageMergeFileSystem->Init(targetSegmentPaths);

    const string example = "1234567890";

    file_system::DirectoryPtr dir = packageMergeFileSystem->GetDirectory(/*relativePath=*/"segment_3");
    file_system::FileWriterPtr writer = dir->CreateFileWriter("data");
    writer->Write(example.data(), example.size()).GetOrThrow();
    ASSERT_EQ(FSEC_OK, writer->Close());
    packageMergeFileSystem->MakeCheckpoint("filename1");

    packageMergeFileSystem->Commit();

    dir = packageMergeFileSystem->GetDirectory(/*relativePath=*/"segment_4");
    writer = dir->CreateFileWriter("data");
    writer->Write(example.data(), example.size()).GetOrThrow();
    ASSERT_EQ(FSEC_OK, writer->Close());
    packageMergeFileSystem->MakeCheckpoint("filename2");
    packageMergeFileSystem->Close();

    packageMergeFileSystem = DYNAMIC_POINTER_CAST(
        PackageMergeFileSystem, MergeFileSystem::Create(/*rootPath=*/GET_TEMP_DATA_PATH() + "/instance_0", mergeConfig,
                                                        /*instanceId=*/0, logicalFs));
    ASSERT_NE(packageMergeFileSystem, nullptr);
    packageMergeFileSystem->Init(targetSegmentPaths);
    EXPECT_TRUE(packageMergeFileSystem->HasCheckpoint("filename1"));
    EXPECT_TRUE(packageMergeFileSystem->HasCheckpoint("filename2"));

    packageMergeFileSystem->MakeCheckpoint("filename3");

    EXPECT_NE(packageMergeFileSystem->GetDirectory(/*relativePath=*/"segment_1"), nullptr);
    EXPECT_NE(packageMergeFileSystem->GetDirectory(/*relativePath=*/"segment_2"), nullptr);
    dir = packageMergeFileSystem->GetDirectory(/*relativePath=*/"segment_3");
    ASSERT_NE(dir, nullptr);
    // ASSERT_TRUE(dir->IsExist("data"));

    dir = packageMergeFileSystem->GetDirectory(/*relativePath=*/"segment_4");
    ASSERT_NE(dir, nullptr);
    // EXPECT_TRUE(dir->IsExist("data"));

    EXPECT_NE(packageMergeFileSystem->GetDirectory(/*relativePath=*/"segment_5"), nullptr);
    EXPECT_TRUE(packageMergeFileSystem->HasCheckpoint("filename3"));

    packageMergeFileSystem->Close();
}

void PackageMergeFileSystemTest::TestRecoverWithBranchId()
{
    FileSystemOptions fsOptions;
    fsOptions.isOffline = true;
    fsOptions.outputStorage = FSST_PACKAGE_DISK;

    config::MergeConfig mergeConfig;
    mergeConfig.SetEnablePackageFile(true);
    mergeConfig.SetCheckpointInterval(0);

    auto logicalFs =
        FileSystemCreator::Create("", /*rootPath=*/GET_TEMP_DATA_PATH() + "/instance_0", fsOptions, nullptr, false)
            .GetOrThrow();

    PackageMergeFileSystemPtr packageMergeFileSystem = DYNAMIC_POINTER_CAST(
        PackageMergeFileSystem, MergeFileSystem::Create(/*rootPath=*/GET_TEMP_DATA_PATH() + "/instance_0", mergeConfig,
                                                        /*instanceId=*/0, logicalFs));
    ASSERT_NE(packageMergeFileSystem, nullptr);
    vector<string> targetSegmentPaths {"segment_3"};
    packageMergeFileSystem->Init(targetSegmentPaths);

    const string example = "1234567890";

    file_system::DirectoryPtr dir = packageMergeFileSystem->GetDirectory(/*relativePath=*/"segment_3");
    file_system::FileWriterPtr writer = dir->CreateFileWriter("data");
    writer->Write(example.data(), example.size()).GetOrThrow();
    ASSERT_EQ(FSEC_OK, writer->Close());
    packageMergeFileSystem->MakeCheckpoint("filename1");
    packageMergeFileSystem->Commit();
    ASSERT_TRUE(dir->IsExist("data"));
    file_system::FileWriterPtr writer2 = dir->CreateFileWriter("data2");
    writer2->Write(example.data(), example.size()).GetOrThrow();
    ASSERT_EQ(FSEC_OK, writer2->Close());
    ASSERT_FALSE(dir->IsExist("package_file.__data__.i0t0.1"));
    packageMergeFileSystem->Close();

    auto branchFs = BranchFS::Fork(/*rootPath=*/GET_TEMP_DATA_PATH() + "/instance_0/", /*srcBranch*/ "",
                                   BranchFS::TEST_GetBranchName(/*branch_1*/ 1), fsOptions, nullptr);
    logicalFs = branchFs->GetFileSystem();
    packageMergeFileSystem = DYNAMIC_POINTER_CAST(
        PackageMergeFileSystem, MergeFileSystem::Create(/*rootPath=*/GET_TEMP_DATA_PATH() + "/instance_0", mergeConfig,
                                                        /*instanceId=*/0, /*branch_1*/ logicalFs));
    ASSERT_NE(packageMergeFileSystem, nullptr);
    packageMergeFileSystem->Init(targetSegmentPaths);
    EXPECT_TRUE(packageMergeFileSystem->HasCheckpoint("filename1"));
    dir = packageMergeFileSystem->GetDirectory(/*relativePath=*/"segment_3");
    ASSERT_NE(dir, nullptr);
    ASSERT_TRUE(dir->IsExist("package_file.__data__.i0t0.0"));
    writer2 = dir->CreateFileWriter("data2");
    ASSERT_EQ(FSEC_OK, writer2->Close());
    packageMergeFileSystem->MakeCheckpoint("filename2");
    packageMergeFileSystem->Commit();
    packageMergeFileSystem->Close();

    // fake package_file.__meta__.i0t0.2 which has bigger meta number than checkpoint
    FslibWrapper::AtomicStoreE(
        PathUtil::JoinPath(GET_TEMP_DATA_PATH(), "instance_0/segment_3/package_file.__meta__.i0t0.2"), "nothing");

    branchFs = BranchFS::Fork(/*rootPath=*/GET_TEMP_DATA_PATH() + "/instance_0/", "",
                              BranchFS::TEST_GetBranchName(/*branch_1*/ 2), fsOptions, nullptr);
    logicalFs = branchFs->GetFileSystem();
    packageMergeFileSystem = DYNAMIC_POINTER_CAST(
        PackageMergeFileSystem, MergeFileSystem::Create(/*rootPath=*/GET_TEMP_DATA_PATH() + "/instance_0", mergeConfig,
                                                        /*instanceId=*/0, /*branch_2*/ logicalFs));
    ASSERT_NE(packageMergeFileSystem, nullptr);
    packageMergeFileSystem->Init(targetSegmentPaths);
    EXPECT_TRUE(packageMergeFileSystem->HasCheckpoint("filename1"));
    // EXPECT_TRUE(packageMergeFileSystem->HasCheckpoint("filename2"));
    ASSERT_TRUE(dir->IsExist("package_file.__data__.i0t0.0"));
    // ASSERT_TRUE(dir->IsExist("package_file.__data__.i0t0.1"));
    auto writer3 = dir->CreateFileWriter("data3");
    ASSERT_EQ(FSEC_OK, writer3->Close());
    packageMergeFileSystem->MakeCheckpoint("filename2");
    packageMergeFileSystem->Commit();
    packageMergeFileSystem->Close();
    // todo add more
}

file_system::ArchiveFolderPtr GetCheckpointArchiveFolder(const string& rootPath, int32_t instanceId)
{
    auto archiveFolder = std::make_shared<file_system::ArchiveFolder>(false);
    string checkpointDirPath = util::PathUtil::JoinPath(rootPath, MERGE_ITEM_CHECK_POINT_DIR_NAME);
    file_system::FileSystemOptions fsOptions;
    fsOptions.isOffline = true;

    auto fs = file_system::FileSystemCreator::Create("MergeCheckpoint", checkpointDirPath, fsOptions).GetOrThrow();
    archiveFolder->Open(file_system::IDirectory::Get(fs), autil::StringUtil::toString(instanceId)).GetOrThrow();
    return archiveFolder;
}

void PackageMergeFileSystemTest::TestCheckpointManager()
{
    auto cpm = std::make_shared<PackageMergeFileSystem::CheckpointManager>();
    auto folder = GetCheckpointArchiveFolder(GET_TEMP_DATA_PATH(), /*instanceId=*/0);
    cpm->Recover(/*folder=*/folder, /*threadCount=*/2);
    cpm->MakeCheckpoint("checkpoint1");
    EXPECT_TRUE(cpm->HasCheckpoint("checkpoint1"));
    cpm->Commit(folder, "desc", /*metaId=*/0);
    EXPECT_EQ(cpm->GetMetaId("desc"), 0);
    ASSERT_EQ(FSEC_OK, folder->Close());

    folder = GetCheckpointArchiveFolder(GET_TEMP_DATA_PATH(), /*instanceId=*/0);
    cpm = std::make_shared<PackageMergeFileSystem::CheckpointManager>();
    EXPECT_FALSE(cpm->HasCheckpoint("checkpoint1"));

    cpm->Recover(folder, /*threadCount=*/2);

    EXPECT_EQ(cpm->GetMetaId("desc"), 0);
    EXPECT_TRUE(cpm->HasCheckpoint("checkpoint1"));

    cpm->MakeCheckpoint("checkpoint2");
    EXPECT_TRUE(cpm->HasCheckpoint("checkpoint2"));
    ASSERT_EQ(FSEC_OK, folder->Close());
}

void PackageMergeFileSystemTest::TestTagFunction()
{
    config::MergeConfig mergeConfig;
    auto TagFunction = [mergeConfig](const string& relativeFilePath) {
        return mergeConfig.GetPackageFileTagConfigList().Match(relativeFilePath, "");
    };
    ASSERT_EQ("", TagFunction("segment_203_level_0/attribute/sku_pidvid/201_175.patch"));
    ASSERT_EQ("", TagFunction("segment_203_level_0/summary/data"));

    mergeConfig.mImpl->packageFileTagConfigList.TEST_PATCH();
    auto TagFunction1 = [mergeConfig](const string& relativeFilePath) {
        return mergeConfig.GetPackageFileTagConfigList().Match(relativeFilePath, "MERGE");
    };
    ASSERT_EQ("PATCH", TagFunction1("segment_203_level_0/attribute/sku_pidvid/201_175.patch"));
    ASSERT_EQ("PATCH", TagFunction1("segment_203_level_0/sub_segment/attribute/abc/201_175.patch"));
    ASSERT_EQ("PATCH", TagFunction1("segment_203_level_0/attribute/pack_attr/sku_pidvid/201_175.patch"));
    ASSERT_EQ("PATCH", TagFunction1("segment_203_level_0/index/sku_pidvid/201_175.patch"));
    ASSERT_EQ("PATCH", TagFunction1("segment_203_level_0/sub_segment/index/abc/201_175.patch"));
    ASSERT_EQ("MERGE", TagFunction1("segment_203_level_0/summary/201_175.patch"));
    ASSERT_EQ("MERGE", TagFunction1("segment_203_level_0/attribute/test/test/sku_pidvid/201_175.patch"));
    ASSERT_EQ("MERGE", TagFunction1("segment_203_level_0/index/title/posting"));
    ASSERT_EQ("MERGE", TagFunction1("segment_203_level_0/summary/data"));
    ASSERT_EQ("MERGE", TagFunction1("non-segment_203_level_0/attribute/sku_pidvid/201_175.patch"));
    ASSERT_EQ("MERGE", TagFunction1("segment_203_level_0/attribute/sku_pidvid/1.patch"));

    string jsonStr = R"(
    {
        "package_file_tag_config":
        [
            {"file_patterns": ["_PATCH_"], "tag": "PATCH" },
            {"file_patterns": ["_SUMMARY_DATA_"], "tag": "SUMMARY_DATA" },
            {"file_patterns": ["_SUMMARY_"], "tag": "SUMMARY" }
        ]
    }
    )";
    FromJsonString(mergeConfig.mImpl->packageFileTagConfigList, jsonStr);
    auto TagFunction2 = [mergeConfig](const string& relativeFilePath) {
        return mergeConfig.GetPackageFileTagConfigList().Match(relativeFilePath, "MERGE");
    };

    ASSERT_EQ("SUMMARY_DATA", TagFunction2("segment_203_level_0/summary/data"));
    ASSERT_EQ("SUMMARY", TagFunction2("segment_203_level_0/summary/offset"));
}

void PackageMergeFileSystemTest::TestRecoverPartialCommit()
{
    // const string example = "1234567890";
    // const int example_size = 10;

    // FileSystemOptions fsOptions;
    // fsOptions.outputStorage = FSST_PACKAGE_DISK;
    // fsOptions.packageCheckPointInterval = 0;
    // auto fs = FileSystemCreator::CreateForWrite("i1", mSubRootDir, fsOptions, /*metricProvider=*/nullptr,
    //                                                     /*isOverride=*/false);

    // DirectoryPtr packDir = Directory::Get(fs)->MakeDirectory("segment_0", true);
    // FileWriterPtr writer1 = packDir->CreateFileWriter("data");
    // writer1->Write(example.c_str(), 10);
    // writer1->Close();
    // int32_t checkpoint = fs->MakeCheckpoint();
    // EXPECT_GE(checkpoint, 0);

    // packDir = Directory::Get(fs)->MakeDirectory("segment_1", true);
    // writer1 = packDir->CreateFileWriter("data");
    // writer1->Write(example.c_str(), example_size);
    // writer1->Close();
    // // Checkpoint changes because packageCheckPointInterval is small and segment_1/data is written.
    // int32_t checkpoint2 = fs->MakeCheckpoint();
    // EXPECT_GT(checkpoint, checkpoint2);

    // // Use old checkpoint to recover, so changes to segment_1 is deleted.
    // fs = FileSystemCreator::CreateForWrite("i1", mSubRootDir, fsOptions, /*metricProvider=*/nullptr,
    //                                                /*isOverride=*/false);
    // fs->MakeDirectory("segment_1", true, true);
    // fs->TEST_Recover(checkpoint, "segment_1");

    // packDir = Directory::Get(fs)->GetDirectory("segment_0", /*throwExceptionIfNotExist=*/true);
    // std::cout << packDir->GetOutputPath() << std::endl;
    // ASSERT_TRUE(packDir->IsExist("data"));
    // EXPECT_EQ(packDir->CreateFileReader("data", file_system::FSOT_MEM)->TEST_Load(), example);

    // EXPECT_EQ(Directory::Get(fs)->GetDirectory("segment_1", /*throwExceptionIfNotExist=*/false), nullptr);
    // packDir = Directory::Get(fs)->GetDirectory("", /*throwExceptionIfNotExist=*/true);
    // EXPECT_FALSE(packDir->IsExist("segment_1/data"));
}

// The interval between checkpoints are smaller than threshold so the checkpoints are not actually made.
void PackageMergeFileSystemTest::TestRecoverTimeNotExpired()
{
    // string packDirName = "segment_0";
    // string checkpoint;
    // FileSystemOptions fsOptions;
    // fsOptions.outputStorage = FSST_PACKAGE_DISK;
    // fsOptions.packageCheckPointInterval = 3600; // 3600seconds should be long enough for this test to finish.
    // {
    //     auto fs = FileSystemCreator::CreateForWrite("i1", mSubRootDir, fsOptions, /*metricProvider=*/nullptr,
    //                                                         /*isOverride=*/false, checkpoint);
    //     DirectoryPtr packDir = Directory::Get(fs)->MakeDirectory(packDirName, DirectoryOption::Package());
    //     FileWriterPtr writer1 = packDir->CreateFileWriter("data1");
    //     writer1->Write("1234567890", 10);
    //     writer1->Close();
    //     packDir->Flush();
    //     checkpoint = fs->MakeCheckpoint();
    //     PackageDiskStorage::Checkpoint ckpStruct;
    //     autil::legacy::FromJsonString(ckpStruct, checkpoint);
    //     EXPECT_TRUE(ckpStruct.descriptionToMetaId.empty());
    // }

    // {
    //     auto fs = FileSystemCreator::CreateForWrite("i1", mSubRootDir, fsOptions, /*metricProvider=*/nullptr,
    //                                                         /*isOverride=*/false, checkpoint);
    //     DirectoryPtr packDir = Directory::Get(fs)->MakeDirectory(packDirName, DirectoryOption::Package());
    //     // Here the still no checkpoint is made.
    //     ASSERT_FALSE(packDir->IsExist("data1"));
    // }
}

void PackageMergeFileSystemTest::TestRecoverTimeExpired()
{
    // string packDirName = "segment_0";
    // string checkpoint;
    // FileSystemOptions fsOptions;
    // fsOptions.outputStorage = FSST_PACKAGE_DISK;
    // fsOptions.packageCheckPointInterval = 5; // 3600seconds should be long enough for this test to finish.
    // {
    //     auto fs = FileSystemCreator::CreateForWrite("i1", mSubRootDir, fsOptions, /*metricProvider=*/nullptr,
    //                                                         /*isOverride=*/false, checkpoint);
    //     DirectoryPtr packDir = Directory::Get(fs)->MakeDirectory(packDirName, DirectoryOption::Package());
    //     FileWriterPtr writer1 = packDir->CreateFileWriter("data1");
    //     writer1->Write("1234567890", 10);
    //     writer1->Close();
    //     packDir->Flush();
    //     checkpoint = fs->GetCheckpoint();
    //     PackageDiskStorage::Checkpoint ckpStruct;
    //     autil::legacy::FromJsonString(ckpStruct, checkpoint);
    //     EXPECT_TRUE(ckpStruct.descriptionToMetaId.empty());

    //     sleep(10);
    //     packDir->Flush();
    //     checkpoint = fs->MakeCheckpoint();
    //     PackageDiskStorage::Checkpoint ckpStruct2;
    //     autil::legacy::FromJsonString(ckpStruct2, checkpoint);
    //     EXPECT_FALSE(ckpStruct2.descriptionToMetaId.empty());
    //     EXPECT_FALSE(ckpStruct2.packageHintDirectoryPaths.empty());
    // }

    // {
    //     auto fs = FileSystemCreator::CreateForWrite("i1", mSubRootDir, fsOptions, /*metricProvider=*/nullptr,
    //                                                         /*isOverride=*/false, checkpoint);
    //     DirectoryPtr packDir = Directory::Get(fs)->MakeDirectory(packDirName, DirectoryOption::Package());
    //     // Here the one checkpoint is made that includes data1
    //     ASSERT_TRUE(packDir->IsExist("data1"));
    //     EXPECT_EQ(packDir->CreateFileReader("data1", FSOT_MEM)->TEST_Load(), "1234567890");
    // }
}

void PackageMergeFileSystemTest::TestMultiThread()
{
    // string rootDir = mRootDir;
    // string instanceRootDir = mRootDir + "/instance_1/";

    // FileSystemOptions fsOptions;
    // fsOptions.outputStorage = FSST_PACKAGE_DISK;
    // auto fs = FileSystemCreator::CreateForWrite("i1", instanceRootDir, fsOptions).GetOrThrow();

    // {
    //     vector<util::ThreadPtr> threads;
    //     for (size_t i = 0; i < 3; ++i) {
    //         autil::ThreadPtr thread = autil::Thread::createThread(
    //                 [&, i]() {
    //                     DirectoryPtr segDir = Directory::Get(fs)->MakeDirectory("segment_0",
    //                     DirectoryOption::Package());

    //                     string attrName = "attr_" + StringUtil::toString(i);
    //                     string content = "data" + StringUtil::toString(i);
    //                     segDir->MakeDirectory(attrName)->Store("data", content);
    //                     segDir->Sync(true);
    //                 });
    //         threads.push_back(thread);
    //     }
    // }
    // ASSERT_EQ(FSEC_OK, fs->TEST_Commit(3));

    // fslib::FileList fileList;
    // ASSERT_EQ(FSEC_OK, FslibWrapper::ListDir(instanceRootDir + "segment_0", fileList).Code());
    // EXPECT_EQ(set<string>({"package_file.__meta__.i1t0.0", "package_file.__meta__.i1t1.0",
    // "package_file.__meta__.i1t2.0",
    //                        "package_file.__data__.i1t0.0", "package_file.__data__.i1t1.0",
    //                        "package_file.__data__.i1t2.0", "package_file.__meta__.i1t0.1",
    //                        "package_file.__meta__.i1t1.1", "package_file.__meta__.i1t2.1"}),
    //           set<string>(fileList.begin(), fileList.end()));

    // EXPECT_TRUE(FslibWrapper::IsExist(instanceRootDir + "/segment_0/package_file.__meta__.i1t0.0").GetOrThrow());
    // EXPECT_TRUE(FslibWrapper::IsExist(instanceRootDir + "/segment_0/package_file.__meta__.i1t1.0").GetOrThrow());
    // EXPECT_TRUE(FslibWrapper::IsExist(instanceRootDir + "/segment_0/package_file.__meta__.i1t2.0").GetOrThrow());
    // EXPECT_TRUE(FslibWrapper::IsExist(instanceRootDir + "/segment_0/package_file.__data__.i1t0.0").GetOrThrow());
    // EXPECT_TRUE(FslibWrapper::IsExist(instanceRootDir + "/segment_0/package_file.__data__.i1t1.0").GetOrThrow());
    // EXPECT_TRUE(FslibWrapper::IsExist(instanceRootDir + "/segment_0/package_file.__data__.i1t2.0").GetOrThrow());

    // // end merge
    // fs = FileSystemCreator::CreateForRead("end_merge", rootDir, FileSystemOptions::Offline()).GetOrThrow();
    // fs->MergeDirs({instanceRootDir + "/segment_0"}, "segment_0");
    // ASSERT_EQ(FSEC_OK, fs->TEST_Commit(5));

    // for (int i = 0; i < 3; ++i)
    // {
    //     string attrName = "attr_" + StringUtil::toString(i);
    //     string content = "data" + StringUtil::toString(i);
    //     EXPECT_EQ(content,
    //               Directory::Get(fs)->CreateFileReader("segment_0/" + attrName + "/data", FSOT_MEM)->TEST_Load());
    // }

    // EXPECT_FALSE(FslibWrapper::IsExist(instanceRootDir + "/segment_0/package_file.__meta__.i1t0.0").GetOrThrow());
    // EXPECT_FALSE(FslibWrapper::IsExist(instanceRootDir + "/segment_0/package_file.__data__.i1t0.0").GetOrThrow());
    // EXPECT_FALSE(FslibWrapper::IsExist(instanceRootDir + "/segment_0/package_file.__data__.i1t1.0").GetOrThrow());
    // EXPECT_FALSE(FslibWrapper::IsExist(instanceRootDir + "/segment_0/package_file.__data__.i1t2.0").GetOrThrow());
    // EXPECT_TRUE(FslibWrapper::IsExist(rootDir + "/segment_0/package_file.__meta__").GetOrThrow());
    // EXPECT_TRUE(FslibWrapper::IsExist(rootDir + "/segment_0/package_file.__data__0").GetOrThrow());
    // EXPECT_TRUE(FslibWrapper::IsExist(rootDir + "/segment_0/package_file.__data__1").GetOrThrow());
    // EXPECT_TRUE(FslibWrapper::IsExist(rootDir + "/segment_0/package_file.__data__2").GetOrThrow());

    // // check read
    // fs = FileSystemCreator::CreateForRead("check_read", rootDir, FileSystemOptions::Offline()).GetOrThrow();
    // fs->MountVersion(rootDir, 5, "", FSMT_READ_ONLY);

    // for (int i = 0; i < 3; ++i)
    // {
    //     string attrName = "attr_" + StringUtil::toString(i);
    //     string content = "data" + StringUtil::toString(i);
    //     EXPECT_EQ(content,
    //               Directory::Get(fs)->CreateFileReader("segment_0/" + attrName + "/data", FSOT_MEM)->TEST_Load());
    // }
}

void PackageMergeFileSystemTest::TestOneThreadMultipleSegment()
{
    // string rootDir = mRootDir;
    // string instanceRootDir = mRootDir + "/instance_1/";

    // FileSystemOptions fsOptions;
    // fsOptions.outputStorage = FSST_PACKAGE_DISK;
    // auto fs = FileSystemCreator::CreateForWrite("i1", instanceRootDir, fsOptions).GetOrThrow();

    // {
    //     vector<util::ThreadPtr> threads;
    //     for (size_t i = 0; i < 3; ++i) {
    //         autil::ThreadPtr thread = autil::Thread::createThread(
    //                 [&, i]() {
    //                     for (auto segDirName : { "segment_1", "segment_2" })
    //                     {
    //                         DirectoryPtr segDir = Directory::Get(fs)->MakeDirectory(segDirName,
    //                         DirectoryOption::Package()); string attrName = "attr_" + StringUtil::toString(i); string
    //                         content = "data" + StringUtil::toString(i);
    //                         segDir->MakeDirectory(attrName)->Store("data", content);
    //                         segDir->Flush();
    //                     }
    //                     fs->Sync(true);
    //                 });
    //     }
    // }

    // ASSERT_EQ(FSEC_OK, fs->TEST_Commit(3));

    // fslib::FileList fileList;
    // ASSERT_EQ(FSEC_OK, FslibWrapper::ListDir(instanceRootDir + "segment_1", fileList).Code());
    // EXPECT_EQ(set<string>({"package_file.__meta__.i1t0.0", "package_file.__meta__.i1t1.0",
    // "package_file.__meta__.i1t2.0",
    //                        "package_file.__data__.i1t0.0", "package_file.__data__.i1t1.0",
    //                        "package_file.__data__.i1t2.0", "package_file.__meta__.i1t0.1",
    //                        "package_file.__meta__.i1t1.1", "package_file.__meta__.i1t2.1",
    //                        "package_file.__meta__.i1t0.2", "package_file.__meta__.i1t1.2",
    //                        "package_file.__meta__.i1t2.2"}),
    //           set<string>(fileList.begin(), fileList.end()));

    // EXPECT_TRUE(FslibWrapper::IsExist(instanceRootDir + "/segment_1/package_file.__meta__.i1t0.0").GetOrThrow());
    // EXPECT_TRUE(FslibWrapper::IsExist(instanceRootDir + "/segment_1/package_file.__meta__.i1t1.0").GetOrThrow());
    // EXPECT_TRUE(FslibWrapper::IsExist(instanceRootDir + "/segment_1/package_file.__meta__.i1t2.0").GetOrThrow());
    // EXPECT_TRUE(FslibWrapper::IsExist(instanceRootDir + "/segment_1/package_file.__data__.i1t0.0").GetOrThrow());
    // EXPECT_TRUE(FslibWrapper::IsExist(instanceRootDir + "/segment_1/package_file.__data__.i1t1.0").GetOrThrow());
    // EXPECT_TRUE(FslibWrapper::IsExist(instanceRootDir + "/segment_1/package_file.__data__.i1t2.0").GetOrThrow());

    // // end merge
    // fs = FileSystemCreator::CreateForRead("end_merge", rootDir, FileSystemOptions::Offline()).GetOrThrow();
    // fs->MergeDirs({instanceRootDir + "/segment_1"}, "segment_1");
    // fs->MergeDirs({instanceRootDir + "/segment_2"}, "segment_2");
    // ASSERT_EQ(FSEC_OK, fs->TEST_Commit(5));

    // for (int i = 0; i < 3; ++i)
    // {
    //     string attrName = "attr_" + StringUtil::toString(i);
    //     string content = "data" + StringUtil::toString(i);
    //     EXPECT_EQ(content,
    //               Directory::Get(fs)->CreateFileReader("segment_1/" + attrName + "/data", FSOT_MEM)->TEST_Load());
    //     EXPECT_EQ(content,
    //               Directory::Get(fs)->CreateFileReader("segment_2/" + attrName + "/data", FSOT_MEM)->TEST_Load());
    // }

    // // check read
    // fs = FileSystemCreator::CreateForRead("check_read", rootDir, FileSystemOptions::Offline()).GetOrThrow();
    // fs->MountVersion(rootDir, 5, "", FSMT_READ_ONLY);

    // for (int i = 0; i < 3; ++i)
    // {
    //     string attrName = "attr_" + StringUtil::toString(i);
    //     string content = "data" + StringUtil::toString(i);
    //     EXPECT_EQ(content,
    //               Directory::Get(fs)->CreateFileReader("segment_1/" + attrName + "/data", FSOT_MEM)->TEST_Load());
    //     EXPECT_EQ(content,
    //               Directory::Get(fs)->CreateFileReader("segment_2/" + attrName + "/data", FSOT_MEM)->TEST_Load());
    // }
}
}} // namespace indexlib::merger
