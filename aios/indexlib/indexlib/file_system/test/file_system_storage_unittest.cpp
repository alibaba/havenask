#include <fslib/fslib.h>
#include <autil/StringUtil.h>
#include "indexlib/misc/exception.h"
#include "indexlib/util/path_util.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/file_system/test/file_system_storage_unittest.h"
#include "indexlib/file_system/test/file_system_creator.h"
#include "indexlib/file_system/test/test_file_creator.h"
#include "indexlib/file_system/test/file_system_test_util.h"

using namespace std;
using namespace fslib;
using namespace autil;

IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(storage);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, FileSystemStorageTest);

#define MAKE_DIRECTORY_EXIST_EXCEPTION(recursive)               \
    InnerMakeDirectoryExistException((recursive), __LINE__)
#define MAKE_DIRECTORY_PARENT_NONEXIST_EXCEPTION(recursive)             \
    InnerMakeDirectoryParentNonExistException((recursive), __LINE__)
#define MAKE_DIRECTORY_BAD_PARAMETER_EXCEPTION(recursive)               \
    InnerMakeDirectoryBadParameterException((recursive), __LINE__)
#define CHECK_DIRECTORY_EXIST(fileSystem, expectExistDirsStr)           \
    CheckDirectoryExist((fileSystem), (expectExistDirsStr), __LINE__)
#define CHECK_FILE_STAT(fileSystem, filePath, isOnDisk, isInCache)      \
    CheckFileStat((fileSystem), (filePath), (isOnDisk), (isInCache), __LINE__)
FileSystemStorageTest::FileSystemStorageTest()
{
}

FileSystemStorageTest::~FileSystemStorageTest()
{
}

void FileSystemStorageTest::CaseSetUp()
{
    mRootDir = GET_TEST_DATA_PATH();
    mSubDir = PathUtil::JoinPath(mRootDir, "dir");
}

void FileSystemStorageTest::CaseTearDown()
{
}

void FileSystemStorageTest::TestStorageSync()
{
    FSStorageType storageType = GET_PARAM_VALUE(0);
    bool needFlush = GET_PARAM_VALUE(1);
    IndexlibFileSystemPtr fileSystem = CreateFileSystem();

    string filePath = PathUtil::JoinPath(mSubDir, "sync_file");

    FileWriterPtr fileWriter = TestFileCreator::CreateWriter(
            fileSystem, storageType, 
            (storageType==FSST_IN_MEM)?FSOT_IN_MEM:FSOT_BUFFERED, 
            "hello", filePath,
            FSWriterParam(true));

    // file not in cache and can't sync when file is not closed
    ASSERT_FALSE(FileSystemWrapper::IsExist(filePath));
    fileSystem->Sync(true);
    ASSERT_FALSE(FileSystemWrapper::IsExist(filePath));
    fileWriter->Close();

    // buffered file not in cache when close
    CHECK_FILE_STAT(fileSystem, filePath, (storageType != FSST_IN_MEM),
                    (storageType == FSST_IN_MEM));

    // it's on disk when storageType is FSST_LOCAL or needFlush is true
    fileSystem->Sync(true);
    CHECK_FILE_STAT(fileSystem, filePath, 
                    ((storageType == FSST_LOCAL) || needFlush),
                    ((storageType == FSST_IN_MEM) && !needFlush));

    fileSystem.reset();
}

void FileSystemStorageTest::TestStorageCleanCache()
{
    FSStorageType storageType = GET_PARAM_VALUE(0);
    IndexlibFileSystemPtr fileSystem = CreateFileSystem();

    FSOpenType writeType = (storageType==FSST_IN_MEM)?FSOT_IN_MEM:FSOT_BUFFERED;
    string dirPath = PathUtil::JoinPath(fileSystem->GetRootPath(), 
            "clean_cache_dir_path");
    string filePath = PathUtil::JoinPath(dirPath, "clean_cache_file");
    FileWriterPtr fileWriter = TestFileCreator::CreateWriter(
            fileSystem, storageType, writeType, "hello", filePath);
    fileWriter->Close();

    // can't clean, because file dirty and reference > 1
    fileSystem->CleanCache();
    FileStat fileStat;
    fileSystem->GetFileStat(filePath, fileStat);
    //ASSERT_EQ(GET_PARAM_VALUE(1), fileStat.inCache);
    ASSERT_EQ((storageType != FSST_IN_MEM), fileStat.onDisk);

    fileSystem->Sync(true);
    // can't clean, reference > 1
    fileSystem->CleanCache();
    fileSystem->GetFileStat(filePath, fileStat);
    // ASSERT_TRUE(fileStat.inCache);
    ASSERT_EQ(((storageType == FSST_LOCAL) || GET_PARAM_VALUE(1)), fileStat.onDisk);

    fileWriter.reset();
    fileSystem->CleanCache();
    fileSystem->GetFileStat(filePath, fileStat);
    ASSERT_EQ(((storageType == FSST_IN_MEM) && !GET_PARAM_VALUE(1)), fileStat.inCache);
    ASSERT_EQ(((storageType == FSST_LOCAL) || GET_PARAM_VALUE(1)), fileStat.onDisk);
}

void FileSystemStorageTest::TestMakeDirectory()
{
    IndexlibFileSystemPtr ifs = CreateFileSystem();
    
    ifs->MakeDirectory(GetSubDirAbsPath("normaldir"), false);
    CHECK_DIRECTORY_EXIST(ifs, "normaldir");

    ifs->MakeDirectory(GetSubDirAbsPath("normaldir////dir"), false);
    CHECK_DIRECTORY_EXIST(ifs, "normaldir;normaldir/dir");
}

void FileSystemStorageTest::TestMakeDirectoryRecursive()
{
    IndexlibFileSystemPtr ifs = CreateFileSystem();
    
    ifs->MakeDirectory(GetSubDirAbsPath("normaldir/subdir"), true);
    CHECK_DIRECTORY_EXIST(ifs, "normaldir;normaldir/subdir");

    ifs->MakeDirectory(GetSubDirAbsPath("normaldir////subdir1"), true);
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

void FileSystemStorageTest::InnerMakeDirectoryExistException(
        bool recursive, int lineNo)
{
    SCOPED_TRACE(string("line number[") 
                 + StringUtil::toString(lineNo) + string("]"));
    
    IndexlibFileSystemPtr ifs = CreateFileSystem();
    
    if (recursive)
    {
        // exist in file system
        ASSERT_NO_THROW(ifs->MakeDirectory(GET_ABS_PATH("dir"), true));
        // exist on disk
        FileSystemWrapper::MkDir(GET_ABS_PATH("dirOnDisk"));
        ASSERT_NO_THROW(ifs->MakeDirectory(GET_ABS_PATH("dirOnDisk"), true));
    }
    else
    {
        // exist in file system
        ASSERT_THROW(ifs->MakeDirectory(GET_ABS_PATH("dir"), false),
                     ExistException);
        // exist on disk
        FileSystemWrapper::MkDir(GET_ABS_PATH("dirOnDisk"));
        ASSERT_THROW(ifs->MakeDirectory(GET_ABS_PATH("dirOnDisk"), false),
                     ExistException);
    }
    FileSystemWrapper::DeleteDir(GET_ABS_PATH("dirOnDisk"));
}

void FileSystemStorageTest::InnerMakeDirectoryBadParameterException(
        bool recursive, int lineNo)
{
    SCOPED_TRACE(string("line number[") 
                 + StringUtil::toString(lineNo) + string("]"));

    IndexlibFileSystemPtr ifs = CreateFileSystem();
    
    // outof root
    ASSERT_THROW(ifs->MakeDirectory("", recursive), BadParameterException);
    ASSERT_THROW(ifs->MakeDirectory("/outof/root/dir", recursive), 
                 BadParameterException);

    // error protocol
    ASSERT_THROW(ifs->MakeDirectory("error://errorpath", recursive), 
                 BadParameterException);
}

void FileSystemStorageTest::InnerMakeDirectoryParentNonExistException(
        bool recursive, int lineNo)
{
    SCOPED_TRACE(string("line number[") 
                 + StringUtil::toString(lineNo) + string("]"));

    IndexlibFileSystemPtr ifs = CreateFileSystem();
    
    // parent not exist
    ASSERT_THROW(ifs->MakeDirectory(GET_ABS_PATH("parentnotexist/dir"), 
                                    false), NonExistException);
}

void FileSystemStorageTest::TestMakeDirecotryForSync()
{
    IndexlibFileSystemPtr fileSystem = FileSystemCreator::CreateFileSystem(
            mRootDir, "", LoadConfigList(), true);
 
    string filePath = GET_ABS_PATH("inmem/file");
    TestFileCreator::CreateFiles(fileSystem, FSST_IN_MEM, FSOT_IN_MEM,
                                 "hello", filePath);
    ASSERT_FALSE(FileSystemTestUtil::IsOnDisk(fileSystem, filePath));

    fileSystem->MakeDirectory(GET_ABS_PATH("disk"));
    ASSERT_TRUE(FileSystemTestUtil::IsOnDisk(fileSystem, filePath));
}

IndexlibFileSystemPtr FileSystemStorageTest::CreateFileSystem()
{
    FSStorageType type = GET_PARAM_VALUE(0);
    bool needFlush = GET_PARAM_VALUE(1);

    mFileSystem.reset();
    TestDataPathTearDown();
    TestDataPathSetup();

    mFileSystem = FileSystemCreator::CreateFileSystem(
            mRootDir, "", LoadConfigList(), needFlush);
    if (type == FSST_IN_MEM)
    {
        mFileSystem->MountInMemStorage(mSubDir);
    }
    else
    {
        mFileSystem->MakeDirectory(mSubDir);
    }

    return mFileSystem;
}

std::string FileSystemStorageTest::GetSubDirAbsPath(std::string path) const
{
    return PathUtil::JoinPath(mSubDir, path);
}

void FileSystemStorageTest::CheckDirectoryExist(
        IndexlibFileSystemPtr fileSystem, string expectExistDirsStr, 
        int lineNo) const
{
    SCOPED_TRACE(string("line number[") 
                 + StringUtil::toString(lineNo) + string("]"));

    vector<string> existDirs;
    StringUtil::fromString(expectExistDirsStr, existDirs, ";");
    for (size_t i = 0; i < existDirs.size(); ++i)
    {
        string path = GetSubDirAbsPath(existDirs[i]);
        ASSERT_TRUE(fileSystem->IsExist(path)) << "index:" << i;
    }
}

void FileSystemStorageTest::CheckFileStat(IndexlibFileSystemPtr fileSystem,
        string filePath, bool isOnDisk, bool isInCache, int lineNo) const
{
    SCOPED_TRACE(string("line number[") 
                 + StringUtil::toString(lineNo) + string("]"));
    FileStat fileStat;
    fileSystem->GetFileStat(filePath, fileStat);
    ASSERT_EQ(isOnDisk, fileStat.onDisk);
    ASSERT_EQ(isInCache, fileStat.inCache);
}

IE_NAMESPACE_END(file_system);

