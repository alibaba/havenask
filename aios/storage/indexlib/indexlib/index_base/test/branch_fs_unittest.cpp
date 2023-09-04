#include "autil/EnvUtil.h"
#include "autil/StringUtil.h"
#include "indexlib/file_system/LogicalFileSystem.h"
#include "indexlib/file_system/MergeDirsOption.h"
#include "indexlib/file_system/MountOption.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index_base/branch_fs.h"
#include "indexlib/index_base/common_branch_hinter.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/memory_control/MemoryQuotaControllerCreator.h"

using namespace std;

using namespace indexlib::util;
using namespace indexlib::file_system;
namespace indexlib { namespace index_base {

class FakeCommonBranchHinter : public index_base::CommonBranchHinter
{
public:
    FakeCommonBranchHinter(const index_base::CommonBranchHinterOption& option) : CommonBranchHinter(option) {}
    bool SelectFastestBranch(const std::string& rootPath, std::string& branchName) { return false; }

protected:
    void GetClearFile(const std::string& rootPath, const std::string& branchName, std::vector<std::string>& fileList) {}
};
class BranchFsTest : public INDEXLIB_TESTBASE
{
public:
    BranchFsTest();
    ~BranchFsTest();

    DECLARE_CLASS_NAME(BranchFsTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    std::string GetTestPath(const std::string& path) const
    {
        return PathUtil::NormalizePath(PathUtil::JoinPath(GET_TEMP_DATA_PATH(), path));
    }

    std::string GetEpochId(const string& branchName)
    {
        return branchName.substr(string(BRANCH_DIR_NAME_PREFIX).size());
    }

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP(index_base, BranchFsTest);

BranchFsTest::BranchFsTest() {}

BranchFsTest::~BranchFsTest() {}

void BranchFsTest::CaseSetUp() {}

void BranchFsTest::CaseTearDown() {}

TEST_F(BranchFsTest, TestMakeMultiBranch)
{
    FileSystemOptions fsOptions;
    fsOptions.isOffline = true;
    fsOptions.outputStorage = FSST_DISK;
    fsOptions.memoryQuotaController = MemoryQuotaControllerCreator::CreatePartitionMemoryController();
    auto fs = BranchFS::Create(GetTestPath("TestMakeMultiBranch"), "main");
    fs->Init(nullptr, fsOptions);
    auto directory = fs->GetRootDirectory();
    auto writer = directory->CreateFileWriter("test1");
    ASSERT_EQ(FSEC_OK, writer->Close());
    {
        auto fsBack = BranchFS::Fork(GetTestPath("TestMakeMultiBranch"), /*src branch*/ "main", "backup1", fsOptions);
        auto directoryNew = fsBack->GetRootDirectory();
        ASSERT_TRUE(directoryNew->IsExist("test1"));
        auto writerNew = directoryNew->CreateFileWriter("test2");
        ASSERT_EQ(FSEC_OK, writerNew->Close());
    }
    // legacy
    {
        auto fsBack = BranchFS::Fork(GetTestPath("TestMakeMultiBranch"), "main", "backup1", fsOptions);
        auto directoryNew = fsBack->GetRootDirectory();
        ASSERT_TRUE(directoryNew->IsExist("test2"));
        ASSERT_TRUE(directoryNew->IsExist("test1"));
    }
    ASSERT_FALSE(directory->IsExist("test2"));
    {
        auto fsBack = BranchFS::Fork(GetTestPath("TestMakeMultiBranch"), "backup1", "backup2", fsOptions);
        auto directoryNew = fsBack->GetRootDirectory();
        ASSERT_TRUE(directoryNew->IsExist("test2"));
        ASSERT_TRUE(directoryNew->IsExist("test1"));
    }
    writer = directory->CreateFileWriter("test3");
    ASSERT_EQ(FSEC_OK, writer->Close());
    {
        auto fsBack = BranchFS::Fork(GetTestPath("TestMakeMultiBranch"), "main", "backup3", fsOptions);
        auto directoryNew = fsBack->GetRootDirectory();
        ASSERT_FALSE(directoryNew->IsExist("test2"));
        ASSERT_TRUE(directoryNew->IsExist("test1"));
        ASSERT_TRUE(directoryNew->IsExist("test3"));
    }
    /*
       main -> backup1 -> backup2
            -> backup3
    */
}

TEST_F(BranchFsTest, TestOverlappedCase)
{
    FileSystemOptions fsOptions;
    fsOptions.isOffline = true;
    fsOptions.outputStorage = FSST_DISK;
    fsOptions.memoryQuotaController = MemoryQuotaControllerCreator::CreatePartitionMemoryController();
    auto fs = BranchFS::Create(GetTestPath("TestOverlappedCase"), "main");
    fs->Init(nullptr, fsOptions);
    auto dir = fs->GetRootDirectory();
    auto subDir = dir->MakeDirectory("segment_0");
    auto writer = subDir->CreateFileWriter("file0");
    ASSERT_EQ(FSEC_OK, writer->Close());
    {
        auto fsBack = BranchFS::Fork(GetTestPath("TestOverlappedCase"), /*src branch*/ "main",
                                     /*dst branch*/ "backup1", fsOptions);
        auto dir = fsBack->GetRootDirectory();
        auto subDir = dir->GetDirectory("segment_0", false);
        ASSERT_TRUE(subDir->IsExist("file0"));
        auto writer = subDir->CreateFileWriter("file1");
        ASSERT_EQ(FSEC_OK, writer->Close());
    }
    {
        auto fsBack = BranchFS::Fork(GetTestPath("TestOverlappedCase"), /*src branch*/ "main",
                                     /*dst branch*/ "backup2", fsOptions);
        auto dir = fsBack->GetRootDirectory();
        auto subDir = dir->GetDirectory("segment_0", false);
        ASSERT_TRUE(subDir->IsExist("file0"));
        ASSERT_FALSE(subDir->IsExist("file1"));
    }
    {
        auto fsBack = BranchFS::Fork(GetTestPath("TestOverlappedCase"), /*src branch*/ "backup1",
                                     /*dst branch*/ "backup3", fsOptions);
        auto dir = fsBack->GetRootDirectory();
        auto subDir = dir->GetDirectory("segment_0", false);
        ASSERT_TRUE(subDir->IsExist("file0"));
        ASSERT_TRUE(subDir->IsExist("file1"));
        auto subSubDir = subDir->MakeDirectory("sub_segment_0");
        auto writer = subSubDir->CreateFileWriter("file2");
        ASSERT_EQ(FSEC_OK, writer->Close());
    }
    {
        auto fsBack = BranchFS::Fork(GetTestPath("TestOverlappedCase"), /*src branch*/ "backup3",
                                     /*dst branch*/ "backup4", fsOptions);
        auto dir = fsBack->GetRootDirectory();
        auto subDir = dir->GetDirectory("segment_0", false);
        auto subSubDir = subDir->GetDirectory("sub_segment_0", false);
        ASSERT_TRUE(subSubDir->IsExist("file2"));
        auto writer = subSubDir->CreateFileWriter("file3");
        ASSERT_EQ(FSEC_OK, writer->Close());
    }
    {
        auto fsBack = BranchFS::Fork(GetTestPath("TestOverlappedCase"), /*src branch*/ "backup3",
                                     /*dst branch*/ "backup5", fsOptions);
        auto dir = fsBack->GetRootDirectory();
        auto subDir = dir->GetDirectory("segment_0", false);
        ASSERT_TRUE(subDir->IsExist("file0"));
        ASSERT_TRUE(subDir->IsExist("file1"));
        auto subSubDir = subDir->GetDirectory("sub_segment_0", false);
        ASSERT_TRUE(subSubDir->IsExist("file2"));
        ASSERT_FALSE(subSubDir->IsExist("file3"));
    }
}

TEST_F(BranchFsTest, TestCreateBranchWithLegacy)
{
    FileSystemOptions fsOptions;
    fsOptions.isOffline = true;
    fsOptions.outputStorage = FSST_DISK;
    fsOptions.memoryQuotaController = MemoryQuotaControllerCreator::CreatePartitionMemoryController();
    auto fs = BranchFS::Create(GetTestPath("TestCreateBranchWithLegacy"), "main");
    fs->Init(nullptr, fsOptions);
    auto fs2 = BranchFS::Fork(GetTestPath("TestCreateBranchWithLegacy"), "main", "backup1", fsOptions);
    auto directoryNew = fs2->GetRootDirectory();
    auto writerNew = directoryNew->CreateFileWriter("test");
    writerNew->WriteVUInt32(2).GetOrThrow();
    ASSERT_EQ(FSEC_OK, writerNew->Close());
    auto directory = fs->GetRootDirectory();
    auto writer = directory->CreateFileWriter("test");
    writer->WriteVUInt32(1).GetOrThrow();
    ASSERT_EQ(FSEC_OK, writer->Close());
    {
        auto fs2 = BranchFS::Fork(GetTestPath("TestCreateBranchWithLegacy"), "main", "backup1", fsOptions);
        auto directoryNew = fs2->GetRootDirectory();
        auto fileReader = directoryNew->CreateFileReader("test", FSOT_MEM);
        ASSERT_EQ(2, fileReader->ReadVUInt32().GetOrThrow());
    }
    auto fileReader = directory->CreateFileReader("test", FSOT_MEM);
    ASSERT_EQ(1, fileReader->ReadVUInt32().GetOrThrow());
}

TEST_F(BranchFsTest, TestCommitChangeDefaultBranch)
{
    FileSystemOptions fsOptions;
    fsOptions.isOffline = true;
    fsOptions.outputStorage = FSST_DISK;
    fsOptions.memoryQuotaController = MemoryQuotaControllerCreator::CreatePartitionMemoryController();
    auto fs = BranchFS::Create(GetTestPath("TestCommitChangeDefaultBranch"), string(BRANCH_DIR_NAME_PREFIX) + "main");
    fs->Init(nullptr, fsOptions);
    auto directory = fs->GetRootDirectory();
    ASSERT_EQ(BranchFS::GetDefaultBranch(GetTestPath("TestCommitChangeDefaultBranch"), nullptr), "");
    fs->CommitToDefaultBranch();
    ASSERT_EQ(BranchFS::GetDefaultBranch(GetTestPath("TestCommitChangeDefaultBranch"), nullptr),
              string(BRANCH_DIR_NAME_PREFIX) + "main");
    auto fs2 = BranchFS::Fork(GetTestPath("TestCommitChangeDefaultBranch"), string(BRANCH_DIR_NAME_PREFIX) + "main",
                              string(BRANCH_DIR_NAME_PREFIX) + "backup1", fsOptions);
    fs2->CommitToDefaultBranch();
    ASSERT_EQ(BranchFS::GetDefaultBranch(GetTestPath("TestCommitChangeDefaultBranch"), nullptr),
              string(BRANCH_DIR_NAME_PREFIX) + "main");
}

TEST_F(BranchFsTest, TestListDir)
{
    FileSystemOptions fsOptions;
    fsOptions.isOffline = true;
    fsOptions.outputStorage = FSST_DISK;
    fsOptions.memoryQuotaController = MemoryQuotaControllerCreator::CreatePartitionMemoryController();
    auto fs = BranchFS::Create(GetTestPath("TestListDir"));
    fs->Init(nullptr, fsOptions);
    auto rootDir = fs->GetRootDirectory();
    auto segmentDir = rootDir->MakeDirectory("segment_0_level_0");
    auto subSegmentDir = segmentDir->MakeDirectory("sub_segment");
    auto writer1 = subSegmentDir->CreateFileWriter("test1");
    ASSERT_EQ(FSEC_OK, writer1->Close());

    auto fs2 = BranchFS::Fork(GetTestPath("TestListDir"), BranchFS::TEST_GetBranchName(0),
                              BranchFS::TEST_GetBranchName(1), fsOptions);
    rootDir = fs2->GetRootDirectory();
    segmentDir = rootDir->GetDirectory("segment_0_level_0", true);
    subSegmentDir = segmentDir->GetDirectory("sub_segment", true);
    auto writer2 = subSegmentDir->CreateFileWriter("test2");
    ASSERT_EQ(FSEC_OK, writer2->Close());

    auto fs3 = BranchFS::Fork(GetTestPath("TestListDir"), BranchFS::TEST_GetBranchName(1),
                              BranchFS::TEST_GetBranchName(2), fsOptions);

    rootDir = fs3->GetRootDirectory();
    segmentDir = rootDir->GetDirectory("segment_0_level_0", true);
    subSegmentDir = segmentDir->GetDirectory("sub_segment", true);
    auto writer3 = subSegmentDir->CreateFileWriter("test3");
    ASSERT_EQ(FSEC_OK, writer3->Close());

    FileList fileList;
    subSegmentDir->ListDir("", fileList, true);
    ASSERT_THAT(fileList, ElementsAre("test1", "test2", "test3"));

    subSegmentDir->RemoveFile("test3");

    // mock failOver
    fs3 = BranchFS::Fork(GetTestPath("TestListDir"), BranchFS::TEST_GetBranchName(1), BranchFS::TEST_GetBranchName(2),
                         fsOptions);

    segmentDir = rootDir->GetDirectory("segment_0_level_0", true);
    subSegmentDir = segmentDir->GetDirectory("sub_segment", false);
    fileList.clear();
    subSegmentDir->ListDir("", fileList, true);
    ASSERT_THAT(fileList, ElementsAre("test1", "test2"));
}

TEST_F(BranchFsTest, TestUpdatePreloadDependence)
{
    FileSystemOptions fsOptions;
    fsOptions.isOffline = true;
    fsOptions.outputStorage = FSST_DISK;
    auto fs = BranchFS::Create(GetTestPath("TestUpdatePreloadDependence"));
    fs->Init(nullptr, fsOptions);
    auto dir = fs->GetRootDirectory();
    auto writer1 = dir->CreateFileWriter("test1");
    ASSERT_EQ(FSEC_OK, writer1->Close());
    auto fs2 = BranchFS::Fork(GetTestPath("TestUpdatePreloadDependence"), BranchFS::TEST_GetBranchName(0),
                              BranchFS::TEST_GetBranchName(1), fsOptions);
    dir = fs2->GetRootDirectory();
    dir->RemoveFile("test1");
    writer1 = dir->CreateFileWriter("test1");
    ASSERT_EQ(FSEC_OK, writer1->Close());
    dir->RemoveFile("test1");
    ASSERT_FALSE(dir->IsExist("test1"));
    {
        auto fs3 = BranchFS::Fork(GetTestPath("TestUpdatePreloadDependence"), BranchFS::TEST_GetBranchName(0),
                                  BranchFS::TEST_GetBranchName(1), fsOptions);
        auto dir = fs3->GetRootDirectory();
        // the test1 here is actually the test1 of master branch
        ASSERT_TRUE(dir->IsExist("test1"));
    }
    fs2->UpdatePreloadDependence();
    {
        auto fs3 = BranchFS::Fork(GetTestPath("TestUpdatePreloadDependence"), BranchFS::TEST_GetBranchName(0),
                                  BranchFS::TEST_GetBranchName(1), fsOptions);
        auto dir = fs3->GetRootDirectory();
        // the test1 is not in new preloaded entryTable because we update the preloaded entryTable
        ASSERT_FALSE(dir->IsExist("test1"));
    }
    // test failOver when updatePreloadDependence failed
    dir->RemoveFile(ENTRY_TABLE_PRELOAD_FILE_NAME);
    {
        auto fs3 = BranchFS::Fork(GetTestPath("TestUpdatePreloadDependence"), BranchFS::TEST_GetBranchName(0),
                                  BranchFS::TEST_GetBranchName(1), fsOptions);
        auto dir = fs3->GetRootDirectory();
        // the test1 here is actually the test1 of master branch
        ASSERT_TRUE(dir->IsExist("test1"));
    }
}

TEST_F(BranchFsTest, TestBugMayCauseMergeDirsFailed)
{
    FileSystemOptions fsOptions;
    fsOptions.isOffline = true;
    fsOptions.outputStorage = FSST_DISK;
    {
        auto fs = BranchFS::Create(GetTestPath("TestBugMayCauseMergeDirsFailed"));
        fs->Init(nullptr, fsOptions);
        auto dir = fs->GetRootDirectory();
        auto segDir = dir->MakeDirectory("segment_0");
        auto subDir = segDir->MakeDirectory("index");
        auto writer1 = subDir->CreateFileWriter("test1");
        ASSERT_EQ(FSEC_OK, writer1->Close());
    }
    {
        auto fs2 = BranchFS::Fork(GetTestPath("TestBugMayCauseMergeDirsFailed"), BranchFS::TEST_GetBranchName(0),
                                  BranchFS::TEST_GetBranchName(1), fsOptions);
        auto dir = fs2->GetRootDirectory();
        auto segDir = dir->GetDirectory("segment_0", false);
        auto subDir = segDir->GetDirectory("index", false);
        auto writer2 = subDir->CreateFileWriter("test2");
        ASSERT_EQ(FSEC_OK, writer2->Close());
    }
    {
        auto fs2 = BranchFS::Fork(GetTestPath("TestBugMayCauseMergeDirsFailed"), BranchFS::TEST_GetBranchName(0),
                                  BranchFS::TEST_GetBranchName(1), fsOptions);
        auto dir = fs2->GetRootDirectory();
        auto segDir = dir->GetDirectory("segment_0", false);

        auto fsNew = BranchFS::Create(GetTestPath("TestBugMayCauseMergeDirsFailed"), "another");
        fsNew->Init(nullptr, fsOptions);
        auto logicalFS = fsNew->GetFileSystem();
        ASSERT_EQ(FSEC_OK, logicalFS->MergeDirs({segDir->GetPhysicalPath("index")}, "segment_0/index/",
                                                MergeDirsOption::NoMergePackage()));
        ASSERT_TRUE(logicalFS->IsExist("segment_0/index/test2").GetOrThrow());
        ASSERT_FALSE(logicalFS->IsExist("segment_0/index/test1").GetOrThrow());
    }
}

TEST_F(BranchFsTest, TestGetNewBranchName)
{
    // more than max branch
    {
        autil::EnvUtil::setEnv("MAX_BRANCH_COUNT", "1");
        file_system::FileSystemOptions option = FileSystemOptions::Offline();
        FakeCommonBranchHinter hinter1(CommonBranchHinterOption::Test());
        auto branch = BranchFS::CreateWithAutoFork(GET_TEMP_DATA_PATH(), option, NULL, &hinter1);
        string branchName = branch->GetBranchName();
        string epochId1 = GetEpochId(branchName);

        FakeCommonBranchHinter hinter2(CommonBranchHinterOption::Test());
        auto branch2 = BranchFS::CreateWithAutoFork(GET_TEMP_DATA_PATH(), option, NULL, &hinter2);
        string branchName2 = branch2->GetBranchName();
        string epochId2 = GetEpochId(branchName2);

        ASSERT_LT(autil::StringUtil::numberFromString<int64_t>(epochId1),
                  autil::StringUtil::numberFromString<int64_t>(epochId2));

        FakeCommonBranchHinter hinter3(CommonBranchHinterOption::Test());
        auto branch3 = BranchFS::CreateWithAutoFork(GET_TEMP_DATA_PATH(), option, NULL, &hinter3);
        string branchName3 = branch3->GetBranchName();
        ASSERT_EQ(branchName2, branchName3);

        autil::EnvUtil::unsetEnv("MAX_BRANCH_COUNT");
    }
}

TEST_F(BranchFsTest, TestForkWithoutPreload)
{
    // bug: xxxx://invalid/issue/46055810
    FileSystemOptions fsOptions;
    fsOptions.isOffline = true;
    fsOptions.outputStorage = FSST_DISK;
    fsOptions.memoryQuotaController = MemoryQuotaControllerCreator::CreatePartitionMemoryController();
    auto fs = BranchFS::Create(GetTestPath("TestOverlappedCase"), "main");
    fs->Init(nullptr, fsOptions);
    auto dir = fs->GetRootDirectory();
    auto subDir = dir->MakeDirectory("segment_0");
    auto writer = subDir->CreateFileWriter("file0");
    ASSERT_EQ(FSEC_OK, writer->Close());
    {
        auto fsBack = BranchFS::Fork(GetTestPath("TestOverlappedCase"), /*src branch*/ "main",
                                     /*dst branch*/ "backup1", fsOptions);
        auto dir = fsBack->GetRootDirectory();
        auto subDir = dir->GetDirectory("segment_0", false);
        ASSERT_TRUE(subDir->IsExist("file0"));
        auto writer = subDir->CreateFileWriter("file1");
        ASSERT_EQ(FSEC_OK, writer->Close());
        fsBack->UpdatePreloadDependence();
        ASSERT_EQ(FSEC_OK, FslibWrapper::DeleteFile(GetTestPath("TestOverlappedCase/backup1/entry_table.preload"),
                                                    DeleteOption::NoFence(false)));
    }
    {
        auto fsBack = BranchFS::Fork(GetTestPath("TestOverlappedCase"), /*src branch*/ "backup1",
                                     /*dst branch*/ "backup2", fsOptions);
        auto dir = fsBack->GetRootDirectory();
        auto subDir = dir->GetDirectory("segment_0", false);
        ASSERT_TRUE(subDir->IsExist("file0"));
        ASSERT_TRUE(subDir->IsExist("file1"));
    }
}

TEST_F(BranchFsTest, DISABLED_EstimateFastestBranchId)
{
    FileSystemOptions fsOptions;
    fsOptions.isOffline = true;
    fsOptions.outputStorage = FSST_DISK;
    auto fs = BranchFS::Create(GetTestPath("TestEstimateBranch"));
    fs->Init(nullptr, fsOptions);
    auto directory = fs->GetRootDirectory();
    auto writer1 = directory->CreateFileWriter("test1");
    ASSERT_EQ(FSEC_OK, writer1->Close());
    {
        auto fs2 = BranchFS::Fork(GetTestPath("TestEstimateBranch"), BranchFS::TEST_GetBranchName(0),
                                  BranchFS::TEST_GetBranchName(1), fsOptions);
        auto writer2 = fs2->GetRootDirectory()->CreateFileWriter("test2");
        ASSERT_EQ(FSEC_OK, writer2->Close());
    }
    ASSERT_EQ(BranchFS::TEST_EstimateFastestBranchName(GetTestPath("TestEstimateBranch")),
              string(BRANCH_DIR_NAME_PREFIX) + "1");
    auto writer3 = directory->CreateFileWriter("test3");
    auto writer4 = directory->CreateFileWriter("test4");
    ASSERT_EQ(FSEC_OK, writer3->Close());
    ASSERT_EQ(FSEC_OK, writer4->Close());
    ASSERT_EQ(BranchFS::TEST_EstimateFastestBranchName(GetTestPath("TestEstimateBranch")),
              string(BRANCH_DIR_NAME_PREFIX) + "0");
    {
        auto fs3 = BranchFS::Fork(GetTestPath("TestEstimateBranch"), BranchFS::TEST_GetBranchName(1),
                                  BranchFS::TEST_GetBranchName(2), fsOptions);
        auto subDir = fs3->GetRootDirectory()->MakeDirectory("tmp");
        auto writer5 = subDir->CreateFileWriter("test5");
        auto writer6 = subDir->CreateFileWriter("test6");
        auto writer7 = subDir->CreateFileWriter("test7");
        ASSERT_EQ(FSEC_OK, writer5->Close());
        ASSERT_EQ(FSEC_OK, writer6->Close());
        ASSERT_EQ(FSEC_OK, writer7->Close());
    }
    ASSERT_EQ(BranchFS::TEST_EstimateFastestBranchName(GetTestPath("TestEstimateBranch")),
              string(BRANCH_DIR_NAME_PREFIX) + "2");
}
}} // namespace indexlib::index_base
