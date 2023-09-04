#include "indexlib/file_system/Directory.h"

#include "gtest/gtest.h"
#include <assert.h>
#include <iosfwd>
#include <memory>
#include <string.h>
#include <vector>

#include "fslib/common/common_type.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/FileInfo.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/PhysicalDirectory.h"
#include "indexlib/file_system/file/CompressFileReader.h"
#include "indexlib/file_system/file/CompressFileWriter.h"
#include "indexlib/file_system/file/DecompressCachedCompressFileReader.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/file/IntegratedCompressFileReader.h"
#include "indexlib/file_system/file/NormalCompressFileReader.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/file_system/test/PackageFileUtil.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/testutil/unittest.h"

using namespace std;

namespace indexlib { namespace file_system {

class DirectoryTest : public INDEXLIB_TESTBASE
{
public:
    DirectoryTest();
    ~DirectoryTest();

    DECLARE_CLASS_NAME(DirectoryTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestStoreAndLoad();
    void TestCreateIntegratedFileReader();
    void TestCreateIntegratedFileReaderNoReplace();
    void TestCreateCompressFileReader();
    void TestCreateCompressFileWithExcludePattern();
    void TestProhibitInMemDump();
    void TestAddSolidPathFileInfos();
    void TestRename();
    void TestCreateArchiveFolder();
    void TestListPhysicalFile();
    void TestListPhysicalFilePackage();
    void TestGetDirectorySize();
    void TestGetDirectorySizePackage();

private:
    enum CompressReaderType {
        COMP_READER_INTE,
        COMP_READER_NORMAL,
        COMP_READER_CACHE,
        COMP_READER_NO_COMPRESS,
    };

private:
    void DoTestCreateIntegratedFileReader(std::string loadType, FSOpenType expectOpenType);

    // config::IndexPartitionOptions CreateOptionWithLoadConfig(std::string loadType);
    // config::IndexPartitionOptions
    // CreateOptionWithLoadConfig(const std::vector<std::pair<std::string, LoadStrategy*>>& configs);

    void MakeCompressFile(const std::string& fileName, size_t bufferSize, const std::string& excludePattern = "");

    void CheckReaderType(const std::shared_ptr<Directory>& dir, const std::string& fileName,
                         CompressReaderType readerType);
    void MakePackageFile(const std::string& dirName, const std::string& packageFileInfoStr,
                         const std::string& packageDirInfoStr);

    void CheckListPhysicalFile(Directory* dir, const std::string& relativePath,
                               const std::vector<std::string>& expectedPaths);
    void SETUP_PACKAGE_FILESYSTEM();

private:
    std::shared_ptr<Directory> _directory;
    std::shared_ptr<IFileSystem> _fileSystem;

private:
    AUTIL_LOG_DECLARE();
};
AUTIL_LOG_SETUP(indexlib.file_system, DirectoryTest);

INDEXLIB_UNIT_TEST_CASE(DirectoryTest, TestStoreAndLoad);
INDEXLIB_UNIT_TEST_CASE(DirectoryTest, TestCreateIntegratedFileReader);
INDEXLIB_UNIT_TEST_CASE(DirectoryTest, TestCreateIntegratedFileReaderNoReplace);
INDEXLIB_UNIT_TEST_CASE(DirectoryTest, TestCreateCompressFileReader);
INDEXLIB_UNIT_TEST_CASE(DirectoryTest, TestCreateCompressFileWithExcludePattern);
INDEXLIB_UNIT_TEST_CASE(DirectoryTest, TestProhibitInMemDump);
INDEXLIB_UNIT_TEST_CASE(DirectoryTest, TestAddSolidPathFileInfos);
INDEXLIB_UNIT_TEST_CASE(DirectoryTest, TestRename);
INDEXLIB_UNIT_TEST_CASE(DirectoryTest, TestCreateArchiveFolder);
INDEXLIB_UNIT_TEST_CASE(DirectoryTest, TestListPhysicalFile);
INDEXLIB_UNIT_TEST_CASE(DirectoryTest, TestListPhysicalFilePackage);
INDEXLIB_UNIT_TEST_CASE(DirectoryTest, TestGetDirectorySize);
INDEXLIB_UNIT_TEST_CASE(DirectoryTest, TestGetDirectorySizePackage);

//////////////////////////////////////////////////////////////////////

DirectoryTest::DirectoryTest() {}

DirectoryTest::~DirectoryTest() {}

void DirectoryTest::CaseSetUp()
{
    _fileSystem = FileSystemCreator::Create("", GET_TEMP_DATA_PATH()).GetOrThrow();
    _directory = Directory::Get(_fileSystem);
}

void DirectoryTest::CaseTearDown() {}

void DirectoryTest::SETUP_PACKAGE_FILESYSTEM()
{
    FileSystemOptions options;
    options.outputStorage = FSST_PACKAGE_MEM;
    _fileSystem = FileSystemCreator::Create("DirectoryTest", GET_TEMP_DATA_PATH(), options).GetOrThrow();
    _directory = Directory::Get(_fileSystem);
}

void DirectoryTest::TestStoreAndLoad()
{
    string testStr = "hello, directory";
    string fileName = "testdir.temp";
    ASSERT_NO_THROW(_directory->Store(fileName, testStr));
    string compStr;

    ASSERT_NO_THROW(_directory->Load(fileName, compStr));
    ASSERT_EQ(testStr, compStr);
}

void DirectoryTest::TestCreateIntegratedFileReader()
{
    DoTestCreateIntegratedFileReader("mmap", FSOT_LOAD_CONFIG);
    DoTestCreateIntegratedFileReader("cache", FSOT_MEM);
}

void DirectoryTest::TestCreateCompressFileReader()
{
    MakeCompressFile("mmap_compress_file", 4096);
    MakeCompressFile("cache_decompress_file", 8192);
    MakeCompressFile("cache_decompress_file1", 4096);
    MakeCompressFile("cache_compress_file", 4096);

    auto& rootDirectory = _directory;

    CheckReaderType(rootDirectory, "mmap_compress_file", COMP_READER_INTE);
    CheckReaderType(rootDirectory, "cache_decompress_file", COMP_READER_INTE);
    CheckReaderType(rootDirectory, "cache_decompress_file1", COMP_READER_INTE);
    CheckReaderType(rootDirectory, "cache_compress_file", COMP_READER_INTE);
}

void DirectoryTest::TestProhibitInMemDump()
{
    auto& rootDirectory = _directory;
    std::shared_ptr<Directory> inMemDir = rootDirectory->MakeDirectory("in_mem_dir", DirectoryOption::Mem());
    inMemDir->Store("file", "abc", WriterOption::AtomicDump());

    ASSERT_TRUE(FslibWrapper::IsExist(GET_TEMP_DATA_PATH() + "/in_mem_dir/file").GetOrThrow());

    /*
    FileStat fileStat;
    inMemDir->TEST_GetFileStat("file", fileStat);
    ASSERT_FALSE(fileStat.inCache);
    */
}

void DirectoryTest::DoTestCreateIntegratedFileReader(string loadType, FSOpenType expectOpenType)
{
    auto ifs = _fileSystem;
    string caseDir = loadType;
    std::shared_ptr<Directory> directory = Directory::Get(ifs)->MakeDirectory(caseDir);
    directory->Store("file_name", "file_content");
    std::shared_ptr<FileReader> fileReader = directory->CreateFileReader("file_name", expectOpenType);
    ASSERT_EQ(expectOpenType, fileReader->GetOpenType());
    char buffer[1024];
    auto sz = fileReader->Read(buffer, 12).GetOrThrow();
    ASSERT_EQ(12, sz);
    ASSERT_EQ(0, memcmp("file_content", buffer, 12));
    ASSERT_NE(nullptr, fileReader->GetBaseAddress());
}

void DirectoryTest::TestCreateIntegratedFileReaderNoReplace()
{
    string loadType = "cache";
    string caseDir = loadType;
    //  FslibWrapper::MkDirE(caseDir);
    // config::IndexPartitionOptions option = CreateOptionWithLoadConfig(loadType);
    // std::shared_ptr<IFileSystem> fileSystem = FileSystemFactory::Create(caseDir, "", option, NULL);
    // std::shared_ptr<Directory> directory = DirectoryCreator::Get(fileSystem, caseDir, true);
    auto directory = _directory->MakeDirectory(caseDir);
    directory->Store("file_name", "file_content");
    directory->CreateFileReader("file_name", FSOT_MEM_ACCESS);
    directory->CreateFileReader("file_name", FSOT_MEM_ACCESS);

    /*
    FileSystemMetrics metrics = _fileSystem->GetFileSystemMetrics();
    const StorageMetrics& localStorageMetrics = metrics.GetLocalStorageMetrics();
    const FileCacheMetrics& fileCacheMetrics = localStorageMetrics.GetFileCacheMetrics();
    ASSERT_EQ(0, fileCacheMetrics.replaceCount.getValue());
    ASSERT_EQ(1, fileCacheMetrics.hitCount.getValue());
    ASSERT_EQ(1, fileCacheMetrics.missCount.getValue());
    ASSERT_EQ(0, fileCacheMetrics.removeCount.getValue());
    */
}

void DirectoryTest::TestAddSolidPathFileInfos()
{
    string caseDir = "solidPath";
    /*
    FslibWrapper::MkDirE(caseDir);
    config::IndexPartitionOptions option = CreateOptionWithLoadConfig("mmap");
    option.SetIsOnline(false);

    std::shared_ptr<IFileSystem> fileSystem =
        FileSystemFactory::Create(caseDir, "", option, NULL);
    std::shared_ptr<Directory> directory = DirectoryCreator::Get(fileSystem, caseDir, true);
    */
    auto directory = _directory->MakeDirectory(caseDir);

    directory->MakeDirectory("dir");
    directory->MakeDirectory("dir/sub_dir");
    directory->Store("dir/file1", "abc");
    // this will trigger add file2 to metaCache
    directory->Store("dir/file2", "efg");
    directory->Store("dir/sub_dir/file3", "efg");

    vector<FileInfo> fileInfos = {FileInfo("dir/"), FileInfo("dir/file1", 3), FileInfo("dir/sub_dir/"),
                                  FileInfo("dir/sub_dir/file3", 3)};

    // test exist when not add solid path
    ASSERT_TRUE(directory->IsExist("dir/file1"));
    ASSERT_TRUE(directory->IsExist("dir/file2"));
    ASSERT_TRUE(directory->IsExist("dir/sub_dir/file3"));

    // test exist after add solid path
    // directory->AddSolidPathFileInfos(fileInfos.begin(), fileInfos.end());
    ASSERT_TRUE(directory->IsExist("dir/file1"));
    ASSERT_TRUE(directory->IsExist("dir/file2"));
    ASSERT_FALSE(directory->IsExist("dir/not-exist-file"));
    ASSERT_TRUE(directory->IsExist("dir/sub_dir/file3"));

    // test exist when solid path change
    // FslibWrapper::DeleteFileE(util::PathUtil::JoinPath(caseDir, "dir/file1"));
    // ASSERT_TRUE(directory->IsExist("dir/file1"));

    FileList files;
    directory->ListDir("dir", files, true);
    ASSERT_EQ(4, files.size());

    // test remove data in solid path
    directory->RemoveDirectory("dir/sub_dir");
    ASSERT_TRUE(directory->IsExist("dir/file1"));
    ASSERT_FALSE(directory->IsExist("dir/sub_dir/file3"));

    // test remove solid path
    directory->RemoveDirectory("dir");
    ASSERT_FALSE(directory->IsExist("dir/file1"));
}

// config::IndexPartitionOptions DirectoryTest::CreateOptionWithLoadConfig(
//         string loadType)
// {
//     std::pair<std::string, LoadStrategy*> strategy(".*", nullptr);
//     if (loadType == "mmap")
//     {
//         strategy.second = new MmapLoadStrategy;
//     }
//     else
//     {
//         strategy.second = new CacheLoadStrategy;
//     }
//     return CreateOptionWithLoadConfig({strategy});
// }

// config::IndexPartitionOptions
// DirectoryTest::CreateOptionWithLoadConfig(const std::vector<std::pair<std::string, LoadStrategy*>>& configs)
// {
//     LoadConfigList loadConfigList;
//     for (auto& p : configs)
//     {
//         LoadConfig::FilePatternStringVector pattern;
//         pattern.push_back(p.first);
//         LoadConfig loadConfig;
//         loadConfig.SetFilePatternString(pattern);
//         LoadStrategyPtr loadStrategy(p.second);
//         loadConfig.SetLoadStrategyPtr(loadStrategy);
//         loadConfigList.PushBack(loadConfig);
//     }
//     config::IndexPartitionOptions option;
//     option.GetOnlineConfig().loadConfigList = loadConfigList;
//     return option;
// }

void DirectoryTest::MakeCompressFile(const string& fileName, size_t bufferSize, const string& excludePattern)
{
    string testStr = "hello, directory";
    std::shared_ptr<FileWriter> compWriter =
        _directory->CreateFileWriter(fileName, WriterOption::Compress("zstd", bufferSize, excludePattern));
    ASSERT_EQ(FSEC_OK, compWriter->Write(testStr.c_str(), testStr.size()).Code());
    ASSERT_EQ(FSEC_OK, compWriter->Close());
}

void DirectoryTest::TestCreateCompressFileWithExcludePattern()
{
    string excludePattern = ".*(offset|data)$";
    MakeCompressFile("offset", 4096, excludePattern);
    CheckReaderType(_directory, "offset", COMP_READER_NO_COMPRESS);

    MakeCompressFile("data", 4096, excludePattern);
    CheckReaderType(_directory, "data", COMP_READER_NO_COMPRESS);

    MakeCompressFile("no_match", 4096, excludePattern);
    CheckReaderType(_directory, "no_match", COMP_READER_INTE);
}

void DirectoryTest::CheckReaderType(const std::shared_ptr<Directory>& dir, const string& fileName,
                                    CompressReaderType readerType)
{
    auto fileReader = dir->CreateFileReader(fileName, ReaderOption::SupportCompress(FSOT_LOAD_CONFIG));
    CompressFileReaderPtr compReader = std::dynamic_pointer_cast<CompressFileReader>(fileReader);
    if (readerType == COMP_READER_NO_COMPRESS) {
        ASSERT_TRUE(compReader.get() == nullptr);
        return;
    }

    ASSERT_TRUE(compReader);
    switch (readerType) {
    case COMP_READER_NORMAL: {
        NormalCompressFileReaderPtr typedReader = std::dynamic_pointer_cast<NormalCompressFileReader>(compReader);
        ASSERT_TRUE(typedReader != nullptr);
        break;
    }
    case COMP_READER_INTE: {
        IntegratedCompressFileReaderPtr typedReader =
            std::dynamic_pointer_cast<IntegratedCompressFileReader>(compReader);
        ASSERT_TRUE(typedReader != nullptr);
        break;
    }
    default: {
        assert(readerType == COMP_READER_CACHE);
        DecompressCachedCompressFileReaderPtr typedReader =
            std::dynamic_pointer_cast<DecompressCachedCompressFileReader>(compReader);
        ASSERT_TRUE(typedReader != nullptr);
    }
    }
}

void DirectoryTest::TestRename()
{
    /*
    IndexPartitionOptions options;
    std::shared_ptr<IFileSystem> fileSystem = FileSystemFactory::Create(
            GET_TEMP_DATA_PATH(), "", options, NULL);
    std::shared_ptr<Directory> rootDirectory = DirectoryCreator::Get(fileSystem, GET_TEMP_DATA_PATH(), true);
    */
    auto& rootDirectory = _directory;
    rootDirectory->Store("f1", "abc", WriterOption::AtomicDump());
    ASSERT_TRUE(FslibWrapper::IsExist(GET_TEMP_DATA_PATH() + "/f1").GetOrThrow());
    std::shared_ptr<Directory> localDir1 = rootDirectory->MakeDirectory("local1");
    std::shared_ptr<Directory> localDir2 = rootDirectory->MakeDirectory("local2");

    rootDirectory->Rename("f1", localDir1);
    ASSERT_FALSE(rootDirectory->IsExist("file"));
    ASSERT_FALSE(rootDirectory->IsExist("f1"));
    ASSERT_TRUE(rootDirectory->IsExist("local1/f1"));

    localDir1->Rename("f1", localDir2, "f2.tmp");
    ASSERT_FALSE(rootDirectory->IsExist("local1/f1"));
    ASSERT_TRUE(rootDirectory->IsExist("local2/f2.tmp"));

    // std::shared_ptr<Directory> inMemDir = rootDirectory->MakeDirectory("in_mem_dir", DirectoryOption::Mem());
    // inMemDir->Store("file", "abc", WriterOption::AtomicDump());
    // ASSERT_THROW(localDir2->Rename("f2.tmp", inMemDir), util::UnSupportedException);
}

void DirectoryTest::TestCreateArchiveFolder()
{
    // TODO(xingwo): Modify ArchiveFolder to use Directory, not physical path
    /*
    const string content = "abcdef";
    auto PrepareArchiveFolder = [&content](const string& path) {
        auto ifs = _fileSystem;
        if (!ifs->IsExist(path))
        {
            ifs->MakeDirectory(path);
        }
        ASSERT_EQ(FSEC_OK, ArchiveFolder archiveFolderWrite(false).Code());
        archiveFolderWrite.Open(PathUtil::JoinPath(GET_TEMP_DATA_PATH(), path), "i");
        auto archiveFile = archiveFolderWrite.CreateFileWriter("test1");

        ASSERT_EQ(FSEC_OK, archiveFile->Write(content.c_str(), content.length()).Code());
        ASSERT_EQ(FSEC_OK,archiveFile->Close());
        ASSERT_EQ(FSEC_OK,archiveFolderWrite.Close());
    };
    auto CheckArchiveFolder = [&content](const ArchiveFolderPtr& folder) {
        auto archiveFileToRead = folder->CreateFileReader("test1");
        char buffer[10];
        archiveFileToRead->Read(buffer, content.length());
        string result(buffer, content.length());
        ASSERT_EQ(content, result);
        ASSERT_EQ(FSEC_OK,archiveFileToRead->Close());
        ASSERT_EQ(FSEC_OK,folder->Close());
    };

    /// IndexPartitionOptions options;
    // options.SetIsOnline(true);

    {
        string primaryRoot = _directory->MakeDirectory("pri")->GetLogicalPath();
        string secondaryRoot = _directory->MakeDirectory("sec")->GetLogicalPath();
        PrepareArchiveFolder(primaryRoot + "/" + ADAPTIVE_DICT_DIR_NAME);
        auto dir = _directory->Get(primaryRoot + "/" + ADAPTIVE_DICT_DIR_NAME, true);
        CheckArchiveFolder(dir->CreateArchiveFolder(false, ""));
    }
    tearDown();
    setUp();
    {
        string primaryRoot = _directory->MakeDirectory("pri")->GetLogicalPath();
        string secondaryRoot = _directory->MakeDirectory("sec")->GetLogicalPath();

        string jsonStr = R"( {
            "need_read_remote_index" : true,
            "load_config" : [
                { "file_patterns" : [".*"], "remote" : true, "deploy": false }
            ]
        } )";
        OnlineConfig onlineConfig;
        FromJsonString(onlineConfig, jsonStr);
        options.SetOnlineConfig(onlineConfig);

        PrepareArchiveFolder(secondaryRoot + "/" + ADAPTIVE_DICT_DIR_NAME);
        std::shared_ptr<IFileSystem> fs = FileSystemFactory::Create(primaryRoot, secondaryRoot, options, NULL);
        auto dir = DirectoryCreator::Get(fs, primaryRoot + "/" + ADAPTIVE_DICT_DIR_NAME, true);
        CheckArchiveFolder(dir->CreateArchiveFolder(false, ""));
    }
    */
}

void DirectoryTest::MakePackageFile(const std::string& dirName, const std::string& packageFileInfoStr,
                                    const std::string& packageDirInfoStr)
{
    std::shared_ptr<Directory> packageDirectory = _directory->MakeDirectory(dirName, DirectoryOption::Package());
    PackageFileUtil::CreatePackageFile(packageDirectory, packageFileInfoStr, packageDirInfoStr);
    packageDirectory->FlushPackage();
    packageDirectory->Sync(/*waitFinish=*/true);
}

void DirectoryTest::CheckListPhysicalFile(Directory* dir, const std::string& relativePath,
                                          const std::vector<std::string>& expectedPaths)
{
    std::vector<FileInfo> fileInfos;
    dir->ListPhysicalFile(relativePath, fileInfos, /*recursive=*/true);
    std::vector<std::string> paths;
    for (const auto& fileInfo : fileInfos) {
        paths.push_back(fileInfo.filePath);
    }
    ASSERT_EQ(expectedPaths.size(), fileInfos.size());
    EXPECT_THAT(paths, testing::UnorderedElementsAreArray(expectedPaths));
}

void DirectoryTest::TestListPhysicalFile()
{
    std::shared_ptr<Directory> rootDirectory = _directory;
    rootDirectory->Store(/*fileName=*/
                         "f1", /*fileContent=*/"abc", WriterOption::AtomicDump());
    std::shared_ptr<Directory> localDir1 = rootDirectory->MakeDirectory("local1");
    std::shared_ptr<Directory> localDir2 = rootDirectory->MakeDirectory("local2/local_inner");
    localDir1->Store(/*fileName=*/
                     "l1", /*fileContent=*/"abcdef", WriterOption::AtomicDump());
    localDir2->Store(/*fileName=*/"l2", /*fileContent=*/"1234567890", WriterOption::AtomicDump());

    CheckListPhysicalFile(
        rootDirectory.get(), /*relativePath=*/"",
        /*expectedPaths=*/ {"f1", "local1/", "local2/", "local1/l1", "local2/local_inner/", "local2/local_inner/l2"});
    CheckListPhysicalFile(rootDirectory.get(), /*relativePath=*/"local1",
                          /*expectedPaths=*/ {"l1"});
    CheckListPhysicalFile(rootDirectory.get(), /*relativePath=*/"local2",
                          /*expectedPaths=*/ {"local_inner/", "local_inner/l2"});
}

void DirectoryTest::TestListPhysicalFilePackage()
{
    SETUP_PACKAGE_FILESYSTEM();
    std::shared_ptr<Directory> rootDirectory = _directory;
    MakePackageFile(/*dirName=*/"segment_0", /*packageFileInfoStr=*/"file1:abcd#index/file2:12345",
                    /*packageDirInfoStr=*/"index:deletion_map:attribute:random_dir");

    // Duplicate package data files because LogicalFileSystem does not dedup logical files within the same package data
    // file.
    // TODO: It's ok to change UT expected behavior if file system is changed.
    CheckListPhysicalFile(rootDirectory.get(), /*relativePath=*/"",
                          /*expectedPaths=*/
                          {
                              "segment_0/",
                              "segment_0/package_file.__data__0",
                              "segment_0/package_file.__data__0",
                              "segment_0/package_file.__meta__",
                          });
    CheckListPhysicalFile(rootDirectory.get(), /*relativePath=*/"segment_0",
                          /*expectedPaths=*/
                          {
                              "package_file.__data__0",
                              "package_file.__data__0",
                              "package_file.__meta__",
                          });
}

void DirectoryTest::TestGetDirectorySize()
{
    const std::string f1_fileContent = "abc";
    std::shared_ptr<Directory> rootDirectory = _directory;
    rootDirectory->Store(/*fileName=*/
                         "f1", f1_fileContent, WriterOption::AtomicDump());
    std::shared_ptr<Directory> localDir1 = rootDirectory->MakeDirectory("local1");
    std::shared_ptr<Directory> localDir2 = rootDirectory->MakeDirectory("local2/local_inner");
    const std::string l1_fileContent = "abcdef";
    const std::string l2_fileContent = "1234567890";
    localDir1->Store(/*fileName=*/"l1", l1_fileContent, WriterOption::AtomicDump());
    localDir2->Store(/*fileName=*/"l2", l2_fileContent, WriterOption::AtomicDump());

    EXPECT_EQ(f1_fileContent.size() + l1_fileContent.size() + l2_fileContent.size(),
              rootDirectory->GetDirectorySize(/*path=*/""));
    EXPECT_EQ(l1_fileContent.size(), rootDirectory->GetDirectorySize(/*path=*/"local1"));
    EXPECT_EQ(l2_fileContent.size(), rootDirectory->GetDirectorySize(/*path=*/"local2"));
    EXPECT_EQ(l2_fileContent.size(), rootDirectory->GetDirectorySize(/*path=*/"local2/local_inner"));

    EXPECT_THROW(rootDirectory->GetDirectorySize(/*path=*/"not-existed"), autil::legacy::ExceptionBase);
    EXPECT_THROW(rootDirectory->GetDirectorySize(/*path=*/"local1/l1"), autil::legacy::ExceptionBase);
    EXPECT_THROW(rootDirectory->GetDirectorySize(/*path=*/"local2/local_inner/l2"), autil::legacy::ExceptionBase);
}

void DirectoryTest::TestGetDirectorySizePackage()
{
    SETUP_PACKAGE_FILESYSTEM();
    std::shared_ptr<Directory> rootDirectory = _directory;
    MakePackageFile(
        /*dirName=*/"segment_0", /*packageFileInfoStr=*/"file1:abcd#index/file2:12345",
        /*packageDirInfoStr=*/"index:deletion_map:attribute:random_dir");

    int64_t packageDataFileLength;
    ASSERT_EQ(FSEC_OK, FslibWrapper::GetFileLength(
                           util::PathUtil::JoinPath(GET_TEMP_DATA_PATH(), "segment_0/package_file.__data__0"),
                           packageDataFileLength));
    int64_t packageMetaFileLength;
    ASSERT_EQ(FSEC_OK, FslibWrapper::GetFileLength(
                           util::PathUtil::JoinPath(GET_TEMP_DATA_PATH(), "segment_0/package_file.__meta__"),
                           packageMetaFileLength));

    EXPECT_EQ(packageDataFileLength + packageMetaFileLength, rootDirectory->GetDirectorySize(/*path=*/""));
    EXPECT_EQ(packageDataFileLength + packageMetaFileLength, rootDirectory->GetDirectorySize(/*path=*/"segment_0"));
    EXPECT_THROW(rootDirectory->GetDirectorySize(/*path=*/"not-existed"), autil::legacy::ExceptionBase);
    EXPECT_THROW(rootDirectory->GetDirectorySize(/*path=*/"segment_0/not-existed"), autil::legacy::ExceptionBase);
}

}} // namespace indexlib::file_system
