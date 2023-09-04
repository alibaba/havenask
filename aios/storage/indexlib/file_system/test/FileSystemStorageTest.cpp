
#include "autil/StringUtil.h"
#include "fslib/fslib.h"
#include "indexlib/file_system/DirectoryOption.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/file_system/test/FileSystemTestUtil.h"
#include "indexlib/file_system/test/TestFileCreator.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/testutil/unittest.h"

using namespace std;
using namespace autil;

using namespace indexlib::util;

namespace indexlib { namespace file_system {
typedef std::tuple<FSStorageType, bool> FileSystemStorageType;

class FileSystemStorageTest : public INDEXLIB_TESTBASE_WITH_PARAM<FileSystemStorageType>
{
public:
    FileSystemStorageTest();
    ~FileSystemStorageTest();

    DECLARE_CLASS_NAME(FileSystemStorageTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestStorageSync();
    void TestStorageCleanCache();
    void TestMakeDirectory();
    void TestMakeDirectoryRecursive();
    void TestMakeDirectoryException();
    void TestMakeDirectoryRecursiveException();

private:
    std::shared_ptr<IFileSystem> CreateFileSystem();
    void InnerMakeDirectoryExistException(bool recursive, int lineNo);
    void InnerMakeDirectoryBadParameterException(bool recursive, int lineNo);
    void InnerMakeDirectoryParentNonExistException(bool recursive, int lineNo);
    std::string GetSubDirAbsPath(std::string path) const;
    void CheckDirectoryExist(std::shared_ptr<IFileSystem> fileSystem, std::string expectExistDirsStr, int lineNo) const;
    void CheckFileStat(std::shared_ptr<IFileSystem> fileSystem, std::string filePath, bool isOnDisk, bool isInCache,
                       int lineNo) const;

private:
    std::string _rootDir;
    std::string _subDir;
    std::shared_ptr<IFileSystem> _fileSystem;

private:
    AUTIL_LOG_DECLARE();
};
AUTIL_LOG_SETUP(indexlib.file_system, FileSystemStorageTest);

INSTANTIATE_TEST_CASE_P(InMem, FileSystemStorageTest,
                        testing::Combine(testing::Values(FSST_MEM, FSST_DISK), testing::Values(true, false)));
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(FileSystemStorageTest, TestStorageSync);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(FileSystemStorageTest, TestStorageCleanCache);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(FileSystemStorageTest, TestMakeDirectory);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(FileSystemStorageTest, TestMakeDirectoryRecursive);
// INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(FileSystemStorageTest, TestMakeDirectoryException);
// INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(FileSystemStorageTest, TestMakeDirectoryRecursiveException);

//////////////////////////////////////////////////////////////////////

#define MAKE_DIRECTORY_EXIST_EXCEPTION(recursive) InnerMakeDirectoryExistException((recursive), __LINE__)
#define MAKE_DIRECTORY_PARENT_NONEXIST_EXCEPTION(recursive)                                                            \
    InnerMakeDirectoryParentNonExistException((recursive), __LINE__)
#define MAKE_DIRECTORY_BAD_PARAMETER_EXCEPTION(recursive) InnerMakeDirectoryBadParameterException((recursive), __LINE__)
#define CHECK_DIRECTORY_EXIST(fileSystem, expectExistDirsStr)                                                          \
    CheckDirectoryExist((fileSystem), (expectExistDirsStr), __LINE__)
#define CHECK_FILE_STAT(fileSystem, filePath, isOnDisk, isInCache)                                                     \
    CheckFileStat((fileSystem), (filePath), (isOnDisk), (isInCache), __LINE__)
FileSystemStorageTest::FileSystemStorageTest() {}

FileSystemStorageTest::~FileSystemStorageTest() {}

void FileSystemStorageTest::CaseSetUp()
{
    _rootDir = GET_TEMP_DATA_PATH();
    _subDir = "dir";
}

void FileSystemStorageTest::CaseTearDown() {}

void FileSystemStorageTest::TestStorageSync()
{
    FSStorageType storageType = GET_PARAM_VALUE(0);
    bool needFlush = GET_PARAM_VALUE(1);
    std::shared_ptr<IFileSystem> fileSystem = CreateFileSystem();

    string filePath = PathUtil::JoinPath(_subDir, "sync_file");

    WriterOption option;
    option.atomicDump = true;
    std::shared_ptr<FileWriter> fileWriter = TestFileCreator::CreateWriter(
        fileSystem, storageType, (storageType == FSST_MEM) ? FSOT_MEM : FSOT_BUFFERED, "hello", filePath, option);

    // file not in cache and can't sync when file is not closed
    bool isExist = false;
    ASSERT_EQ(FslibWrapper::IsExist(filePath, isExist), FSEC_OK);
    ASSERT_FALSE(isExist);
    fileSystem->Sync(true).GetOrThrow();
    ASSERT_EQ(FslibWrapper::IsExist(filePath, isExist), FSEC_OK);
    ASSERT_FALSE(isExist);
    ASSERT_EQ(FSEC_OK, fileWriter->Close());

    // buffered file not in cache when close
    CHECK_FILE_STAT(fileSystem, filePath, (storageType != FSST_MEM), (storageType == FSST_MEM));

    // it's on disk when storageType is FSST_DISK or needFlush is true
    fileSystem->Sync(true).GetOrThrow();
    CHECK_FILE_STAT(fileSystem, filePath, ((storageType == FSST_DISK) || needFlush),
                    ((storageType == FSST_MEM) && !needFlush));

    fileSystem.reset();
}

void FileSystemStorageTest::TestStorageCleanCache()
{
    FSStorageType storageType = GET_PARAM_VALUE(0);
    std::shared_ptr<IFileSystem> fileSystem = CreateFileSystem();

    FSOpenType writeType = (storageType == FSST_MEM) ? FSOT_MEM : FSOT_BUFFERED;
    string dirPath = "clean_cache_dir_path";
    string filePath = PathUtil::JoinPath(dirPath, "clean_cache_file");
    std::shared_ptr<FileWriter> fileWriter =
        TestFileCreator::CreateWriter(fileSystem, storageType, writeType, "hello", filePath);
    ASSERT_EQ(FSEC_OK, fileWriter->Close());

    // can't clean, because file dirty and reference > 1
    fileSystem->CleanCache();
    FileStat fileStat;
    ASSERT_EQ(FSEC_OK, fileSystem->TEST_GetFileStat(filePath, fileStat));
    // ASSERT_EQ(GET_PARAM_VALUE(1), fileStat.inCache);
    // ASSERT_EQ((storageType != FSST_MEM), fileStat.onDisk);

    fileSystem->Sync(true).GetOrThrow();
    // can't clean, reference > 1
    fileSystem->CleanCache();
    ASSERT_EQ(FSEC_OK, fileSystem->TEST_GetFileStat(filePath, fileStat));
    // ASSERT_TRUE(fileStat.inCache);
    // ASSERT_EQ(((storageType == FSST_DISK) || GET_PARAM_VALUE(1)), fileStat.onDisk);

    fileWriter.reset();
    fileSystem->CleanCache();
    ASSERT_EQ(FSEC_OK, fileSystem->TEST_GetFileStat(filePath, fileStat));
    ASSERT_EQ(((storageType == FSST_MEM) && !GET_PARAM_VALUE(1)), fileStat.inCache);
    // ASSERT_EQ(((storageType == FSST_DISK) || GET_PARAM_VALUE(1)), fileStat.onDisk);
}

void FileSystemStorageTest::TestMakeDirectory()
{
    std::shared_ptr<IFileSystem> ifs = CreateFileSystem();

    ASSERT_EQ(FSEC_OK, ifs->MakeDirectory(GetSubDirAbsPath("normaldir"), DirectoryOption::Local(false, false)));
    CHECK_DIRECTORY_EXIST(ifs, "normaldir");

    ASSERT_EQ(FSEC_OK, ifs->MakeDirectory(GetSubDirAbsPath("normaldir////dir"), DirectoryOption::Local(false, false)));
    CHECK_DIRECTORY_EXIST(ifs, "normaldir;normaldir/dir");
}

void FileSystemStorageTest::TestMakeDirectoryRecursive()
{
    std::shared_ptr<IFileSystem> ifs = CreateFileSystem();

    ASSERT_EQ(FSEC_OK, ifs->MakeDirectory(GetSubDirAbsPath("normaldir/subdir"), DirectoryOption()));
    CHECK_DIRECTORY_EXIST(ifs, "normaldir;normaldir/subdir");

    ASSERT_EQ(FSEC_OK, ifs->MakeDirectory(GetSubDirAbsPath("normaldir////subdir1"), DirectoryOption()));
    CHECK_DIRECTORY_EXIST(ifs, "normaldir;normaldir/subdir;normaldir/subdir1");
}

void FileSystemStorageTest::TestMakeDirectoryException()
{
    MAKE_DIRECTORY_EXIST_EXCEPTION(false);
    MAKE_DIRECTORY_BAD_PARAMETER_EXCEPTION(false);
    MAKE_DIRECTORY_PARENT_NONEXIST_EXCEPTION(false);
}

void FileSystemStorageTest::TestMakeDirectoryRecursiveException()
{
    MAKE_DIRECTORY_EXIST_EXCEPTION(true);
    MAKE_DIRECTORY_BAD_PARAMETER_EXCEPTION(true);
    MAKE_DIRECTORY_PARENT_NONEXIST_EXCEPTION(true);
}

void FileSystemStorageTest::InnerMakeDirectoryExistException(bool recursive, int lineNo)
{
    SCOPED_TRACE(string("line number[") + StringUtil::toString(lineNo) + string("]"));

    std::shared_ptr<IFileSystem> ifs = CreateFileSystem();

    if (recursive) {
        // exist in file system
        ASSERT_EQ(FSEC_OK, ifs->MakeDirectory("dir", DirectoryOption()));
        // exist on disk
        ASSERT_EQ(FslibWrapper::MkDir("dirOnDisk").Code(), FSEC_OK);
        ASSERT_EQ(FSEC_OK, ifs->MakeDirectory("dirOnDisk", DirectoryOption()));
    } else {
        // exist in file system
        // ASSERT_THROW(ifs->MakeDirectory("dir", false), ExistException);
        // exist on disk
        ASSERT_EQ(FslibWrapper::MkDir("dirOnDisk").Code(), FSEC_OK);
        ASSERT_EQ(FSEC_EXIST, ifs->MakeDirectory("dirOnDisk", DirectoryOption::Local(false, false)));
    }
    ASSERT_EQ(FslibWrapper::DeleteDir("dirOnDisk", DeleteOption::NoFence(false)).Code(), FSEC_OK);
}

void FileSystemStorageTest::InnerMakeDirectoryBadParameterException(bool recursive, int lineNo)
{
    SCOPED_TRACE(string("line number[") + StringUtil::toString(lineNo) + string("]"));

    std::shared_ptr<IFileSystem> ifs = CreateFileSystem();

    // outof root
    // ASSERT_THROW(ifs->MakeDirectory("", recursive), BadParameterException);
    ASSERT_EQ(FSEC_BADARGS, ifs->MakeDirectory("/outof/root/dir", DirectoryOption::Local(recursive, false)));

    // error protocol
    ASSERT_EQ(FSEC_OK, ifs->MakeDirectory("error://errorpath", DirectoryOption::Local(recursive, false)));
}

void FileSystemStorageTest::InnerMakeDirectoryParentNonExistException(bool recursive, int lineNo)
{
    SCOPED_TRACE(string("line number[") + StringUtil::toString(lineNo) + string("]"));

    std::shared_ptr<IFileSystem> ifs = CreateFileSystem();

    // parent not exist
    ASSERT_EQ(FSEC_NOENT, ifs->MakeDirectory("parentnotexist/dir", DirectoryOption::Local(false, false)));
}

std::shared_ptr<IFileSystem> FileSystemStorageTest::CreateFileSystem()
{
    FSStorageType type = GET_PARAM_VALUE(0);
    bool needFlush = GET_PARAM_VALUE(1);

    _fileSystem.reset();
    CaseTearDown();
    CaseSetUp();

    FileSystemOptions fsOptions;
    fsOptions.needFlush = needFlush;
    fsOptions.outputStorage = type;
    _fileSystem = FileSystemCreator::Create("FileSystemStorageTest", _rootDir, fsOptions).GetOrThrow();
    EXPECT_EQ(FSEC_OK, _fileSystem->MakeDirectory(_subDir, DirectoryOption()));
    return _fileSystem;
}

std::string FileSystemStorageTest::GetSubDirAbsPath(std::string path) const
{
    return PathUtil::JoinPath(_subDir, path);
}

void FileSystemStorageTest::CheckDirectoryExist(std::shared_ptr<IFileSystem> fileSystem, string expectExistDirsStr,
                                                int lineNo) const
{
    SCOPED_TRACE(string("line number[") + StringUtil::toString(lineNo) + string("]"));

    vector<string> existDirs;
    StringUtil::fromString(expectExistDirsStr, existDirs, ";");
    for (size_t i = 0; i < existDirs.size(); ++i) {
        string path = GetSubDirAbsPath(existDirs[i]);
        ASSERT_TRUE(fileSystem->IsExist(path).GetOrThrow()) << "index:" << i;
    }
}

void FileSystemStorageTest::CheckFileStat(std::shared_ptr<IFileSystem> fileSystem, string filePath, bool isOnDisk,
                                          bool isInCache, int lineNo) const
{
    SCOPED_TRACE(string("line number[") + StringUtil::toString(lineNo) + string("]"));
    FileStat fileStat;
    ASSERT_EQ(FSEC_OK, fileSystem->TEST_GetFileStat(filePath, fileStat));
    // ASSERT_EQ(isOnDisk, fileStat.onDisk);
    ASSERT_EQ(isInCache, fileStat.inCache);
}
}} // namespace indexlib::file_system
