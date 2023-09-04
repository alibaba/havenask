#include "indexlib/file_system/package/PackageDiskStorage.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include <iostream>
#include <memory>
#include <vector>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "autil/StringUtil.h"
#include "autil/legacy/jsonizable.h"
#include "fslib/common/common_type.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/EntryMeta.h"
#include "indexlib/file_system/EntryTable.h"
#include "indexlib/file_system/EntryTableBuilder.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/file_system/LogicalFileSystem.h"
#include "indexlib/file_system/MergeDirsOption.h"
#include "indexlib/file_system/MountOption.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/file_system/package/BufferedPackageFileWriter.h"
#include "indexlib/file_system/package/DirectoryMerger.h"
#include "indexlib/file_system/package/InnerFileMeta.h"
#include "indexlib/file_system/package/PackageFileMeta.h"
#include "indexlib/file_system/package/PackageFileTagConfigList.h"
#include "indexlib/file_system/package/VersionedPackageFileMeta.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/byte_slice_list/ByteSliceList.h"
#include "indexlib/util/memory_control/MemoryQuotaControllerCreator.h"
#include "indexlib/util/memory_control/PartitionMemoryQuotaController.h"
#include "indexlib/util/metrics/MetricProvider.h"
#include "indexlib/util/testutil/unittest.h"

using namespace std;
using namespace autil;

using namespace indexlib::util;

namespace indexlib { namespace file_system {

class PackageDiskStorageTest : public INDEXLIB_TESTBASE
{
public:
    PackageDiskStorageTest();
    ~PackageDiskStorageTest();

    DECLARE_CLASS_NAME(PackageDiskStorageTest);
    void CaseSetUp() override;
    void CaseTearDown() override;

private:
    void AssertInnerFileMeta(const InnerFileMeta& meta, const std::string& expectPath, bool expectIsDir,
                             size_t expectOffset = 0, size_t expectLength = 0, uint32_t expectFileIdx = 0);
    VersionedPackageFileMeta GetPackageMeta(const std::string& root, const std::string& description,
                                            int32_t metaVersionId);
    void PrintPackageMeta(const std::string& root, const std::string& description, int32_t metaVersionId);
    std::shared_ptr<LogicalFileSystem> CreateFS(const std::string& name);
    PackageDiskStoragePtr CreateStorage(int32_t recoverMetaId);

public:
    void TestSimpleProcess();
    void TestProcessMultipleFiles();
    void TestPackageFileMeta();
    void TestCommitMultiTimes();
    void TestRecover();
    void TestRecoverAfterCommitMoreThen10Times();
    void TestRecoverEraseDataIfNoCheckpoint();
    void TestTags();
    void TestDirectoryOnly();
    void TestEntryTable();

private:
    std::string _rootDir;
    std::string _subRootDir;
    std::shared_ptr<IFileSystem> _fs;

private:
    AUTIL_LOG_DECLARE();
};
AUTIL_LOG_SETUP(indexlib.file_system, PackageDiskStorageTest);

INDEXLIB_UNIT_TEST_CASE(PackageDiskStorageTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(PackageDiskStorageTest, TestProcessMultipleFiles);
INDEXLIB_UNIT_TEST_CASE(PackageDiskStorageTest, TestPackageFileMeta);
INDEXLIB_UNIT_TEST_CASE(PackageDiskStorageTest, TestCommitMultiTimes);
INDEXLIB_UNIT_TEST_CASE(PackageDiskStorageTest, TestRecover);
INDEXLIB_UNIT_TEST_CASE(PackageDiskStorageTest, TestRecoverAfterCommitMoreThen10Times);
INDEXLIB_UNIT_TEST_CASE(PackageDiskStorageTest, TestRecoverEraseDataIfNoCheckpoint);
INDEXLIB_UNIT_TEST_CASE(PackageDiskStorageTest, TestDirectoryOnly);
INDEXLIB_UNIT_TEST_CASE(PackageDiskStorageTest, TestTags);
INDEXLIB_UNIT_TEST_CASE(PackageDiskStorageTest, TestEntryTable);
//////////////////////////////////////////////////////////////////////

PackageDiskStorageTest::PackageDiskStorageTest() {}

PackageDiskStorageTest::~PackageDiskStorageTest() {}

void PackageDiskStorageTest::CaseSetUp()
{
    _rootDir = PathUtil::NormalizeDir(GET_TEMP_DATA_PATH());
    _subRootDir = PathUtil::JoinPath(_rootDir, "root");
    FileSystemOptions fsOptions;
    fsOptions.outputStorage = FSST_PACKAGE_DISK;
    _fs = FileSystemCreator::CreateForWrite("i1t0", _subRootDir, fsOptions).GetOrThrow();
}

void PackageDiskStorageTest::CaseTearDown() {}

std::shared_ptr<LogicalFileSystem> PackageDiskStorageTest::CreateFS(const std::string& name)
{
    FileSystemOptions fsOptions;
    fsOptions.isOffline = true;
    fsOptions.memoryQuotaController = MemoryQuotaControllerCreator::CreatePartitionMemoryController();
    fsOptions.outputStorage = FSST_PACKAGE_DISK;
    std::shared_ptr<LogicalFileSystem> fs(new LogicalFileSystem(name, _rootDir, nullptr));
    THROW_IF_FS_ERROR(fs->Init(fsOptions), "");
    return fs;
}

VersionedPackageFileMeta PackageDiskStorageTest::GetPackageMeta(const string& root, const string& description,
                                                                int32_t metaVersionId)
{
    string path = VersionedPackageFileMeta::GetPackageMetaFileName(description, metaVersionId);
    VersionedPackageFileMeta packageMeta;
    auto ec = packageMeta.Load(PathUtil::JoinPath(root, path)).ec;
    INDEXLIB_FATAL_ERROR_IF(ec != FSEC_OK, FileIO, "load package meta failed: %s", path.c_str());
    return packageMeta;
}

void PackageDiskStorageTest::PrintPackageMeta(const string& root, const string& description, int32_t metaVersionId)
{
    const VersionedPackageFileMeta& packageMeta = GetPackageMeta(root, description, metaVersionId);
    cout << "=== "
         << PathUtil::JoinPath(root, VersionedPackageFileMeta::GetPackageMetaFileName(description, metaVersionId))
         << " ===" << endl;
    cout << "FileCount: " << packageMeta.GetInnerFileCount() << " AlignSize: " << packageMeta.GetFileAlignSize()
         << endl;
    for (auto iter = packageMeta.Begin(); iter != packageMeta.End(); iter++) {
        cout << iter->GetFilePath() << endl;
        cout << "IsDir[" << iter->IsDir() << "]";
        if (!iter->IsDir()) {
            cout << "  Offset[" << iter->GetOffset() << "]"
                 << "  Length[" << iter->GetLength() << "]"
                 << "  FileIdx[" << iter->GetDataFileIdx() << "]";
        }
        cout << endl;
    }
    cout << "============================================" << endl;
}

void PrintPackageMetaDebug(const InnerFileMeta& meta)
{
    cout << "meta: " << meta.GetFilePath() << endl;
    cout << "offset: " << meta.GetOffset() << " length: " << meta.GetLength()
         << " file index: " << meta.GetDataFileIdx();
    cout << endl;
}

void PackageDiskStorageTest::AssertInnerFileMeta(const InnerFileMeta& meta, const string& expectPath, bool expectIsDir,
                                                 size_t expectOffset, size_t expectLength, uint32_t expectFileIdx)
{
    // cout << "actual: " << endl;
    // PrintPackageMetaDebug(meta);
    InnerFileMeta expectMeta(expectPath, expectIsDir);
    if (!expectIsDir) {
        expectMeta.Init(expectOffset, expectLength, expectFileIdx);
    }
    // cout << "expected: " << endl;
    // PrintPackageMetaDebug(expectMeta);
    ASSERT_EQ(expectMeta, meta) << expectMeta.GetFilePath() << ":" << meta.GetFilePath();
}

void PackageDiskStorageTest::TestSimpleProcess()
{
    std::shared_ptr<Directory> rootDir = Directory::Get(_fs);
    std::shared_ptr<Directory> segDir = rootDir->MakeDirectory("segment_0", DirectoryOption::Package());
    std::shared_ptr<Directory> indexDir = segDir->MakeDirectory("index");

    std::shared_ptr<FileWriter> writer = indexDir->CreateFileWriter("data1");
    ASSERT_NE(std::dynamic_pointer_cast<BufferedPackageFileWriter>(writer), nullptr);
    ASSERT_EQ(FSEC_OK, writer->Write("1234567890ABCDEF", 16).Code());
    ASSERT_EQ(FSEC_OK, writer->Close());

    _fs->CommitPackage().GetOrThrow();

    string physicalMetaFilePath = PathUtil::JoinPath(_subRootDir, "segment_0", "package_file.__meta__.i1t0.0");
    ASSERT_TRUE(FslibWrapper::IsExist(physicalMetaFilePath).GetOrThrow());
    string metaContent;
    ASSERT_EQ(FSEC_OK, FslibWrapper::AtomicLoad(physicalMetaFilePath, metaContent).Code());

    string physicalDataFilePath = PathUtil::JoinPath(_subRootDir, "segment_0", "package_file.__data__.i1t0.0");
    ASSERT_TRUE(FslibWrapper::IsExist(physicalDataFilePath).GetOrThrow());
    string dataContent;
    ASSERT_EQ(FSEC_OK, FslibWrapper::AtomicLoad(physicalDataFilePath, dataContent).Code());
    EXPECT_EQ(dataContent, "1234567890ABCDEF");
}

void PackageDiskStorageTest::TestProcessMultipleFiles()
{
    std::shared_ptr<Directory> rootDir = Directory::Get(_fs);
    std::shared_ptr<Directory> segDir = rootDir->MakeDirectory("segment_0", DirectoryOption::Package());
    std::shared_ptr<Directory> indexDir = segDir->MakeDirectory("index");

    std::shared_ptr<FileWriter> writer = indexDir->CreateFileWriter("data1");
    ASSERT_NE(std::dynamic_pointer_cast<BufferedPackageFileWriter>(writer), nullptr);
    ASSERT_EQ(FSEC_OK, writer->Write("1234567890ABCDEF", 16).Code());
    ASSERT_EQ(FSEC_OK, writer->Close());

    writer = indexDir->CreateFileWriter("data2");
    ASSERT_NE(std::dynamic_pointer_cast<BufferedPackageFileWriter>(writer), nullptr);
    ASSERT_EQ(FSEC_OK, writer->Write("1234567890", 10).Code());
    ASSERT_EQ(FSEC_OK, writer->Close());

    _fs->CommitPackage().GetOrThrow();
    ASSERT_EQ(FSEC_OK, _fs->TEST_Commit(0));

    string physicalMetaFilePath = PathUtil::JoinPath(_subRootDir, "segment_0", "package_file.__meta__.i1t0.0");
    ASSERT_TRUE(FslibWrapper::IsExist(physicalMetaFilePath).GetOrThrow());
    string metaContent;
    ASSERT_EQ(FSEC_OK, FslibWrapper::AtomicLoad(physicalMetaFilePath, metaContent).Code());

    string physicalDataFilePath = PathUtil::JoinPath(_subRootDir, "segment_0", "package_file.__data__.i1t0.0");
    ASSERT_TRUE(FslibWrapper::IsExist(physicalDataFilePath).GetOrThrow());
    string dataContent;
    ASSERT_EQ(FSEC_OK, FslibWrapper::AtomicLoad(physicalDataFilePath, dataContent).Code());
    // EXPECT_EQ(dataContent, "1234567890ABCDEF");
    EXPECT_EQ(dataContent.size(), 4096 + 10);
}

void PackageDiskStorageTest::TestPackageFileMeta()
{
    string packageRoot = PathUtil::JoinPath(_subRootDir, "segment_0");
    std::shared_ptr<Directory> rootDir = Directory::Get(_fs);
    std::shared_ptr<Directory> segDir = rootDir->MakeDirectory("segment_0", DirectoryOption::Package());
    std::shared_ptr<FileWriter> writer1 = segDir->CreateFileWriter("data1");
    std::shared_ptr<FileWriter> writer2 = segDir->CreateFileWriter("data2");
    ASSERT_EQ(FSEC_OK, writer1->Write("1234567890ABCDEF", 16).Code());
    ASSERT_EQ(FSEC_OK, writer2->Write("", 0).Code());
    ASSERT_EQ(FSEC_OK, writer1->Close());
    ASSERT_EQ(FSEC_OK, writer2->Close());

    std::shared_ptr<Directory> subDirectory = segDir->MakeDirectory("dir1/dir2");
    std::shared_ptr<FileWriter> writer3 = subDirectory->CreateFileWriter("data3");
    ASSERT_EQ(FSEC_OK, writer3->Write("1234567890", 10).Code());
    ASSERT_EQ(FSEC_OK, writer3->Close());

    segDir->MakeDirectory("dir3", DirectoryOption());
    segDir->MakeDirectory("dir3/dir4", DirectoryOption());
    _fs->CommitPackage().GetOrThrow();
    ASSERT_EQ(FSEC_OK, _fs->TEST_Commit(0));

    fslib::FileList fileList;
    ASSERT_EQ(FSEC_OK, FslibWrapper::ListDir(packageRoot, fileList).Code());
    EXPECT_EQ(2, fileList.size());

    const VersionedPackageFileMeta& meta = GetPackageMeta(packageRoot, "i1t0", 0);
    EXPECT_EQ(4096, meta.GetFileAlignSize());
    ASSERT_EQ(7, meta.GetInnerFileCount());

    const auto& metas = meta._fileMetaVec;
    AssertInnerFileMeta(metas[0], "dir1", /*expectIsDir=*/true);
    AssertInnerFileMeta(metas[1], "dir1/dir2", /*expectIsDir=*/true);
    AssertInnerFileMeta(metas[2], "dir3", /*expectIsDir=*/true);
    AssertInnerFileMeta(metas[3], "dir3/dir4", /*expectIsDir=*/true);
    AssertInnerFileMeta(metas[4], "data1", /*expectIsDir=*/false, 0, 16, 0);
    AssertInnerFileMeta(metas[5], "data2", /*expectIsDir=*/false, 4096, 0, 0);
    AssertInnerFileMeta(metas[6], "dir1/dir2/data3", /*expectIsDir=*/false, 4096, 10, 0);
}

void PackageDiskStorageTest::TestCommitMultiTimes()
{
    string packageRoot = PathUtil::JoinPath(_subRootDir, "segment_0");
    std::shared_ptr<Directory> rootDir = Directory::Get(_fs);
    std::shared_ptr<Directory> segDir = rootDir->MakeDirectory("segment_0", DirectoryOption::Package());

    std::shared_ptr<FileWriter> writer1 = segDir->CreateFileWriter("data1");
    ASSERT_EQ(FSEC_OK, writer1->Write("1234567890", 10).Code());
    ASSERT_EQ(FSEC_OK, writer1->Close());
    ASSERT_EQ(0, _fs->CommitPackage().GetOrThrow());
    PackageDiskStoragePtr storage = std::dynamic_pointer_cast<PackageDiskStorage>(_fs->TEST_GetOutputStorage());
    ASSERT_NE(storage, nullptr);

    std::shared_ptr<FileWriter> writer2 = segDir->CreateFileWriter("data2");
    ASSERT_EQ(FSEC_OK, writer2->Write("ABCDEFG", 7).Code());
    ASSERT_EQ(FSEC_OK, writer2->Close());
    ASSERT_EQ(1, _fs->CommitPackage().GetOrThrow());
    ASSERT_EQ(2, _fs->CommitPackage().GetOrThrow());

    fslib::FileList fileList;
    ASSERT_EQ(FSEC_OK, FslibWrapper::ListDir(packageRoot, fileList).Code());
    EXPECT_THAT(fileList, testing::UnorderedElementsAre("package_file.__data__.i1t0.0", "package_file.__meta__.i1t0.0",
                                                        "package_file.__data__.i1t0.1", "package_file.__meta__.i1t0.1",
                                                        "package_file.__meta__.i1t0.2"));

    // PrintPackageMeta(packageRoot, "i1t0", 0);
    // PrintPackageMeta(packageRoot, "i1t0", 1);

    const VersionedPackageFileMeta& meta = GetPackageMeta(packageRoot, "i1t0", /*metaVersionId=*/1);
    ASSERT_EQ(2, meta.GetInnerFileCount());
    const auto& metas = meta._fileMetaVec;
    AssertInnerFileMeta(metas[0], "data1", /*expectIsDir=*/false, 0, 10, 0);
    AssertInnerFileMeta(metas[1], "data2", /*expectIsDir=*/false, 0, 7, 1);
}

void PackageDiskStorageTest::TestRecover()
{
    string packDirName = "segment_0";
    int32_t checkpoint = -1;
    {
        std::shared_ptr<Directory> packDir =
            Directory::Get(_fs)->MakeDirectory(packDirName, DirectoryOption::Package());
        std::shared_ptr<FileWriter> writer1 = packDir->CreateFileWriter("data1");
        ASSERT_EQ(FSEC_OK, writer1->Write("1234567890", 10).Code());
        ASSERT_EQ(FSEC_OK, writer1->Close());
        checkpoint = _fs->CommitPackage().GetOrThrow();
        EXPECT_GE(checkpoint, 0);
        std::shared_ptr<FileWriter> writer2 = packDir->CreateFileWriter("data2");
        ASSERT_EQ(FSEC_OK, writer2->Write("ABCDEFG", 7).Code());
        ASSERT_EQ(FSEC_OK, writer2->Close());
    }

    {
        FileSystemOptions fsOptions;
        fsOptions.outputStorage = FSST_PACKAGE_DISK;
        fsOptions.isOffline = true;
        auto fs = FileSystemCreator::CreateForWrite("i1t0", _subRootDir, fsOptions, /*metricProvider=*/nullptr,
                                                    /*isOverride=*/false)
                      .GetOrThrow();
        std::shared_ptr<Directory> packDir = Directory::Get(fs)->MakeDirectory(packDirName, DirectoryOption::Package());
        ASSERT_EQ(FSEC_OK, fs->TEST_Recover(checkpoint, packDirName));
        // ASSERT_TRUE(packDir->IsExist("data1"));
        // ASSERT_FALSE(packDir->IsExist("data2"));

        std::shared_ptr<FileWriter> writer3 = packDir->CreateFileWriter("data3");
        ASSERT_EQ(FSEC_OK, writer3->Write("abc", 3).Code());
        ASSERT_EQ(FSEC_OK, writer3->Close());
        ASSERT_EQ(1, fs->CommitPackage().GetOrThrow());

        const VersionedPackageFileMeta& meta = GetPackageMeta(PathUtil::JoinPath(_subRootDir, packDirName), "i1t0", 1);
        ASSERT_EQ(2, meta.GetInnerFileCount());
        const auto& metas = meta._fileMetaVec;
        // AssertInnerFileMeta(metas[0], "data1", /*expectIsDir=*/false, 0, 10, 0);
        AssertInnerFileMeta(metas[1], "data3", /*expectIsDir=*/false, 0, 3, 1);
    }
}

void PackageDiskStorageTest::TestRecoverAfterCommitMoreThen10Times()
{
    string packDirName = "segment_0";
    int successCommitTime = 12;
    int32_t checkpoint = -1;
    {
        std::shared_ptr<Directory> packDir =
            Directory::Get(_fs)->MakeDirectory(packDirName, DirectoryOption::Package());
        for (int i = 0; i < successCommitTime; ++i) {
            string fileName = "data_" + StringUtil::toString(i);
            string content = "success commit file: " + StringUtil::toString(i);

            std::shared_ptr<FileWriter> writer = packDir->CreateFileWriter(fileName);
            ASSERT_EQ(FSEC_OK, writer->Write(content.c_str(), content.size()).Code());
            ASSERT_EQ(FSEC_OK, writer->Close());
            checkpoint = _fs->CommitPackage().GetOrThrow();
        }
        EXPECT_GE(checkpoint, 0);
        std::shared_ptr<FileWriter> writer = packDir->CreateFileWriter("data");
        ASSERT_EQ(FSEC_OK, writer->Write("ABCDEFG", 7).Code());
        ASSERT_EQ(FSEC_OK, writer->Close());
    }

    {
        FileSystemOptions fsOptions;
        fsOptions.isOffline = true;
        fsOptions.outputStorage = FSST_PACKAGE_DISK;
        auto fs = FileSystemCreator::CreateForWrite("i1t0", _subRootDir, fsOptions, /*metricProvider=*/nullptr,
                                                    /*isOverride=*/false)
                      .GetOrThrow();
        std::shared_ptr<Directory> packDir = Directory::Get(fs)->MakeDirectory(packDirName, DirectoryOption::Package());
        ASSERT_EQ(FSEC_OK, fs->TEST_Recover(checkpoint, packDirName));
        // for (int i = 0; i < successCommitTime; ++i)
        // {
        //     ..string fileName = "data_" + StringUtil::toString(i);
        //     string expectContent = "success commit file: " + StringUtil::toString(i);

        //     EXPECT_TRUE(packDir->IsExist(fileName)) << "file [" << fileName << "] not exist";
        //     string actualContent;
        //     ASSERT_NO_THROW(packDir->Load(fileName, actualContent));
        //     EXPECT_EQ(expectContent, actualContent);
        // }
        // ASSERT_FALSE(packDir->IsExist("data"));
    }
}

void PackageDiskStorageTest::TestRecoverEraseDataIfNoCheckpoint()
{
    string packDirName = "segment_0";
    string checkpoint;

    std::shared_ptr<Directory> packDir = Directory::Get(_fs)->MakeDirectory(packDirName, DirectoryOption::Package());
    std::shared_ptr<FileWriter> writer = packDir->CreateFileWriter("data1");
    ASSERT_EQ(FSEC_OK, writer->Write("1234567890", 10).Code());
    ASSERT_EQ(FSEC_OK, writer->Close());
    _fs->CommitPackage().GetOrThrow();

    FileSystemOptions fsOptions;
    fsOptions.outputStorage = FSST_PACKAGE_DISK;
    auto fs = FileSystemCreator::CreateForWrite("i1t0", _subRootDir, fsOptions, /*metricProvider=*/nullptr,
                                                /*isOverride=*/false)
                  .GetOrThrow();
    packDir = Directory::Get(fs)->MakeDirectory(packDirName, DirectoryOption::Package());
    ASSERT_FALSE(packDir->IsExist("data1"));
    writer = packDir->CreateFileWriter("data1");
    ASSERT_EQ(FSEC_OK, writer->Write("abc", 3).Code());
    ASSERT_EQ(FSEC_OK, writer->Close());
    ASSERT_TRUE(fs->CommitPackage().OK());
}

void PackageDiskStorageTest::TestDirectoryOnly()
{
    string packDirName = "segment_0";
    string packageRoot = PathUtil::JoinPath(_subRootDir, packDirName);
    std::shared_ptr<Directory> packDir = Directory::Get(_fs)->MakeDirectory(packDirName, DirectoryOption::Package());
    packDir->MakeDirectory("index");
    _fs->CommitPackage().GetOrThrow();
    FSResult<bool> mergePackageFilesRet = DirectoryMerger::MergePackageFiles(packageRoot);
    ASSERT_EQ(mergePackageFilesRet.ec, FSEC_OK);
    ASSERT_TRUE(mergePackageFilesRet.result);
    ASSERT_TRUE(FslibWrapper::IsExist(PathUtil::JoinPath(packageRoot, "package_file.__data__0")).GetOrThrow());
    ASSERT_TRUE(FslibWrapper::IsExist(PathUtil::JoinPath(packageRoot, "package_file.__meta__")).GetOrThrow());
    PackageFileMeta packageFileMeta;
    ASSERT_EQ(packageFileMeta.Load(PathUtil::JoinPath(packageRoot, "package_file.__meta__")), FSEC_OK);
    ASSERT_EQ(1, packageFileMeta.GetPhysicalFileNames().size());
    ASSERT_EQ(1, packageFileMeta.GetPhysicalFileTags().size());
    ASSERT_EQ(1, packageFileMeta.GetPhysicalFileLengths().size());
    ASSERT_EQ("package_file.__data__0", packageFileMeta.GetPhysicalFileName(0));
    ASSERT_EQ("", packageFileMeta.GetPhysicalFileTag(0));
    ASSERT_EQ(0, packageFileMeta.GetPhysicalFileLength(0));
}

void PackageDiskStorageTest::TestTags()
{
    FileSystemOptions fsOptions;
    fsOptions.isOffline = true;
    fsOptions.outputStorage = FSST_PACKAGE_DISK;
    fsOptions.packageFileTagConfigList.reset(new PackageFileTagConfigList);

    string jsonStr = R"(
    {
        "package_file_tag_config":
        [
            {"file_patterns": ["_PATCH_"], "tag": "PATCH" },
            {"file_patterns": [".*"], "tag": "MERGE" }
        ]
    }
    )";
    autil::legacy::FromJsonString(*fsOptions.packageFileTagConfigList, jsonStr);
    auto fs = FileSystemCreator::CreateForWrite("i1t0", _subRootDir, fsOptions).GetOrThrow();

    std::shared_ptr<Directory> rootDir = Directory::Get(fs);
    std::shared_ptr<Directory> segDir = rootDir->MakeDirectory("segment_0_level_1", DirectoryOption::Package());
    segDir->MakeDirectory("attribute/price")->Store("201_175.patch", "1");
    segDir->MakeDirectory("attribute/pack_attr/name")->Store("201_175.patch", "2");
    segDir->MakeDirectory("index/title")->Store("posting", "3");
    segDir->MakeDirectory("summary")->Store("data", "4");

    ASSERT_TRUE(fs->CommitPackage().OK());
    ASSERT_EQ(FSEC_OK, fs->TEST_Commit(0));

    ASSERT_TRUE(FslibWrapper::IsExist(_subRootDir + "/segment_0_level_1/package_file.__meta__.i1t0.0").GetOrThrow());
    ASSERT_TRUE(FslibWrapper::IsExist(_subRootDir + "/segment_0_level_1/package_file.__data__.i1t0.0").GetOrThrow());
    ASSERT_TRUE(FslibWrapper::IsExist(_subRootDir + "/segment_0_level_1/package_file.__data__.i1t0.1").GetOrThrow());
    ASSERT_FALSE(FslibWrapper::IsExist(_subRootDir + "/segment_0_level_1/package_file.__data__.i1t0.2").GetOrThrow());

    // end merge
    fs = FileSystemCreator::CreateForWrite("end_merge", _rootDir, FileSystemOptions::Offline()).GetOrThrow();
    ASSERT_EQ(FSEC_OK, fs->MergeDirs({_subRootDir + "/segment_0_level_1"}, "segment_0_level_1",
                                     MergeDirsOption::MergePackage()));
    ASSERT_EQ(FSEC_OK, fs->TEST_Commit(1));

    ASSERT_TRUE(FslibWrapper::IsExist(_rootDir + "/segment_0_level_1/package_file.__meta__").GetOrThrow());
    // The order/FileId(.0, .1) of package_file.__data__.PATCH.0 and package_file.__data__.MERGE.1 might change
    // depending on which file is added/written first.
    ASSERT_TRUE(FslibWrapper::IsExist(_rootDir + "/segment_0_level_1/package_file.__data__.PATCH.0").GetOrThrow());
    ASSERT_TRUE(FslibWrapper::IsExist(_rootDir + "/segment_0_level_1/package_file.__data__.MERGE.1").GetOrThrow());
    ASSERT_FALSE(
        FslibWrapper::IsExist(_rootDir + "/segment_0_level_1/package_file.__data__.PATCH.i1t0.1").GetOrThrow());
    ASSERT_FALSE(
        FslibWrapper::IsExist(_rootDir + "/segment_0_level_1/package_file.__data__.MERGE.i1t0.1").GetOrThrow());
}

void PackageDiskStorageTest::TestEntryTable()
{
    string packDirName = "segment_0";
    std::shared_ptr<Directory> packDir = Directory::Get(_fs)->MakeDirectory(packDirName, DirectoryOption::Package());
    std::shared_ptr<FileWriter> writer1 = packDir->CreateFileWriter("data1");
    ASSERT_EQ(FSEC_OK, writer1->Write("1234567890", 10).Code());
    ASSERT_EQ(FSEC_OK, writer1->Close());
    ASSERT_EQ(0, _fs->CommitPackage().GetOrThrow());
    std::shared_ptr<FileWriter> writer2 = packDir->CreateFileWriter("data2");
    ASSERT_EQ(FSEC_OK, writer2->Write("ABCDEFG", 7).Code());
    ASSERT_EQ(FSEC_OK, writer2->Close());
    ASSERT_EQ(1, _fs->CommitPackage().GetOrThrow());
    ASSERT_EQ(FSEC_OK, _fs->TEST_Commit(1));

    EntryTableBuilder entryTableBuilder;
    std::shared_ptr<EntryTable> entryTable = entryTableBuilder.CreateEntryTable(
        "", _subRootDir, make_shared<FileSystemOptions>(FileSystemOptions::Offline()));
    ASSERT_EQ(FSEC_OK, entryTableBuilder.MountVersion(_subRootDir, 1, "", FSMT_READ_ONLY));

    const EntryMeta& packDirMeta = entryTable->GetEntryMeta(packDirName).result;
    ASSERT_TRUE(packDirMeta.IsDir());
    const EntryMeta& file1Meta = entryTable->GetEntryMeta(PathUtil::JoinPath(packDirName, "data1")).result;
    ASSERT_EQ(10, file1Meta.GetLength());
    ASSERT_EQ(0, file1Meta.GetOffset());
    ASSERT_EQ("segment_0/package_file.__data__.i1t0.0", file1Meta.GetPhysicalPath());
    const EntryMeta& file2Meta = entryTable->GetEntryMeta(PathUtil::JoinPath(packDirName, "data2")).result;
    ASSERT_EQ(7, file2Meta.GetLength());
    ASSERT_EQ(0, file2Meta.GetOffset());
    ASSERT_EQ("segment_0/package_file.__data__.i1t0.1", file2Meta.GetPhysicalPath());
}
}} // namespace indexlib::file_system
