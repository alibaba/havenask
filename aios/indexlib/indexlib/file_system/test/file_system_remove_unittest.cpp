#include <autil/StringUtil.h>
#include <fslib/fslib.h>
#include "indexlib/util/path_util.h"
#include "indexlib/misc/exception.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/file_system/test/file_system_remove_unittest.h"
#include "indexlib/file_system/test/file_system_creator.h"
#include "indexlib/file_system/test/test_file_creator.h"
#include "indexlib/file_system/test/file_system_test_util.h"

using namespace std;
using namespace autil;
using namespace fslib;
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(storage);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, FileSystemRemoveTest);

FileSystemRemoveTest::FileSystemRemoveTest()
{
}

FileSystemRemoveTest::~FileSystemRemoveTest()
{
}

void FileSystemRemoveTest::CaseSetUp()
{
    mRootDir= PathUtil::JoinPath(GET_TEST_DATA_PATH(), "root");
}

void FileSystemRemoveTest::CaseTearDown()
{
    mFileSystem.reset();
}

#define CHECK_REMOVE(path, expectStr)                   \
    CheckRemovePath((path), (expectStr), __LINE__)

void FileSystemRemoveTest::TestRemoveFile()
{
    CHECK_REMOVE(GetAbsPath("/file"),
                 "slice,"
                 "local/,local/file,local/local_0/,local/slice,"
                 "inmem/,inmem/file,inmem/inmem_0/,inmem/slice");
    CHECK_REMOVE(GetAbsPath("/local/file"),
                 "file,slice,"
                 "local/,local/local_0/,local/slice,"
                 "inmem/,inmem/file,inmem/inmem_0/,inmem/slice");
    CHECK_REMOVE(GetAbsPath("/inmem/file"),
                 "file,slice,"
                 "local/,local/file,local/local_0/,local/slice,"
                 "inmem/,inmem/inmem_0/,inmem/slice");

    // remove slice file
    CHECK_REMOVE(GetAbsPath("/slice"),
                 "file,"
                 "local/,local/file,local/local_0/,local/slice,"
                 "inmem/,inmem/file,inmem/inmem_0/,inmem/slice");
    CHECK_REMOVE(GetAbsPath("/local/slice"),
                 "file,slice,"
                 "local/,local/local_0/,local/file,"
                 "inmem/,inmem/file,inmem/inmem_0/,inmem/slice");
    CHECK_REMOVE(GetAbsPath("/inmem/slice"),
                 "file,slice,"
                 "local/,local/file,local/local_0/,local/slice,"
                 "inmem/,inmem/inmem_0/,inmem/file");
}

void FileSystemRemoveTest::TestRemoveDirectory()
{
    CHECK_REMOVE(GetAbsPath("/local"),
                 "file,slice,inmem/,inmem/file,inmem/inmem_0/,inmem/slice");
    CHECK_REMOVE(GetAbsPath("/inmem"),
                 "file,slice,local/,local/file,local/local_0/,local/slice");
    CHECK_REMOVE(GetAbsPath("/local/local_0"),
                 "file,slice,local/,local/file,local/slice,"
                 "inmem/,inmem/file,inmem/slice,inmem/inmem_0/");
    CHECK_REMOVE(GetAbsPath("/inmem/inmem_0"), 
                 "file,slice,local/,local/file,local/slice,local/local_0/,"
                 "inmem/,inmem/file,inmem/slice");
    // remove mount path
}

#define CHECK_UNMOUNT_PATH(treeStr, unMountPath, subDir)        \
    CheckUnmountPath((treeStr), (unMountPath), (subDir), __LINE__)
void FileSystemRemoveTest::TestUnMount()
{
    string treeStr = "#:multi;"
                     "#/multi:local,inmem^I,file^F;"
                     "#/multi/local:local_0,file^F;"
                     "#/multi/inmem:inmem_0,file^F";    

    string subDir = GetAbsPath("multi/inmem/subDir");
    // unmount inmem storage path
    CHECK_UNMOUNT_PATH(treeStr, GetAbsPath("multi/inmem"), subDir);
    // unmount multi storage path
    CHECK_UNMOUNT_PATH(treeStr, GetAbsPath("multi"), subDir);
    // unmount local storage path
    CHECK_UNMOUNT_PATH(treeStr, GetAbsPath("multi/local"),
                       GetAbsPath("multi/local/dir"));
}

void FileSystemRemoveTest::TestRemoveFileException()
{
    IndexlibFileSystemPtr ifs = PrepareData(mTreeStr);

    // remove out of root
    ASSERT_THROW(ifs->RemoveFile(""), BadParameterException);
    ASSERT_THROW(ifs->RemoveFile("/outofroot"), BadParameterException);

    // remove nonexist
    ASSERT_THROW(ifs->RemoveFile(GetAbsPath("/inmem/nonexist")), 
                 NonExistException);
    ASSERT_THROW(ifs->RemoveFile(GetAbsPath("/local/nonexist")), 
                 NonExistException);

    // remove dir
    FileSystemWrapper::MkDir(GetAbsPath("/inmem/ondisk"));
    ASSERT_THROW(ifs->RemoveFile(GetAbsPath("/inmem/ondisk")), 
                 MemFileIOException);
    ASSERT_THROW(ifs->RemoveFile(GetAbsPath("/inmem/inmem_0")), 
                 MemFileIOException);
    ASSERT_THROW(ifs->RemoveFile(GetAbsPath("/local/local_0")), 
                 MemFileIOException);

    // use count
    FileReaderPtr fileReader = ifs->CreateFileReader(GetAbsPath("/inmem/file"),
            FSOT_IN_MEM);
    ASSERT_THROW(ifs->RemoveFile(GetAbsPath("/inmem/file")), 
                 MemFileIOException);
    fileReader = ifs->CreateFileReader(GetAbsPath("/local/file"),
            FSOT_IN_MEM);
    ASSERT_THROW(ifs->RemoveFile(GetAbsPath("/local/file")), 
                 MemFileIOException);
}

void FileSystemRemoveTest::TestRemoveDirectoryException()
{
    IndexlibFileSystemPtr ifs = PrepareData(mTreeStr);

    // remove out of root
    ASSERT_THROW(ifs->RemoveDirectory(""), BadParameterException);
    ASSERT_THROW(ifs->RemoveDirectory("/outofroot"), BadParameterException);

    // remove nonexist
    ASSERT_THROW(ifs->RemoveDirectory(GetAbsPath("/inmem/nonexist")), 
                 NonExistException);
    ASSERT_THROW(ifs->RemoveDirectory(GetAbsPath("/local/nonexist")), 
                 NonExistException);

    // remove file
    ASSERT_THROW(ifs->RemoveDirectory(GetAbsPath("/inmem/file")), 
                 MemFileIOException);
    ASSERT_THROW(ifs->RemoveDirectory(GetAbsPath("/local/file")), 
                 MemFileIOException);
    
    // use count
    FileReaderPtr fileReader = ifs->CreateFileReader(GetAbsPath("/inmem/file"),
            FSOT_IN_MEM);
    ASSERT_THROW(ifs->RemoveDirectory(GetAbsPath("/inmem")), 
                 MemFileIOException);
    fileReader = ifs->CreateFileReader(GetAbsPath("/local/file"),
            FSOT_IN_MEM);
    ASSERT_THROW(ifs->RemoveDirectory(GetAbsPath("/local")), 
                 MemFileIOException);

    // remove root
    ASSERT_THROW(ifs->RemoveDirectory(ifs->GetRootPath()), 
                 BadParameterException);
}

IndexlibFileSystemPtr FileSystemRemoveTest::PrepareData(string treeStr)
{
    if (mFileSystem)
    {
        mFileSystem->Sync(true);
    }
    TestDataPathTearDown();
    TestDataPathSetup();

    FileSystemWrapper::MkDir(mRootDir);    
    mFileSystem = FileSystemCreator::CreateFileSystem(mRootDir);
    if (treeStr.empty())
    {
        treeStr = "#:local,inmem^I,file^F,slice^S;"
                  "#/local:local_0,file^F,slice^S;"
                  "#/inmem:inmem_0,file^F,slice^S";
    }
    TestFileCreator::CreateFileTree(mFileSystem, treeStr);
    return mFileSystem;
}

void FileSystemRemoveTest::CheckRemovePath(string path, string expectStr,
        int lineNo)
{
    SCOPED_TRACE(string("line number[") 
                 + StringUtil::toString(lineNo) + string("]"));

    PrepareData(mTreeStr);

    FileStat fileStat;
    mFileSystem->GetFileStat(path, fileStat);
    if (fileStat.isDirectory)
    {
        mFileSystem->RemoveDirectory(path);
    }
    else
    {
        mFileSystem->RemoveFile(path);
    }
    ASSERT_FALSE(mFileSystem->IsExist(path));
    ASSERT_TRUE(FileSystemTestUtil::CheckListFile(mFileSystem, 
                    mFileSystem->GetRootPath(), true, expectStr)); 
}

void FileSystemRemoveTest::CheckUnmountPath(string treeStr, string unMountDir, 
        string subDir, int lineNo)
{
    SCOPED_TRACE(string("line number[") 
                 + StringUtil::toString(lineNo) + string("]"));

    IndexlibFileSystemPtr ifs = PrepareData(treeStr);
    ifs->RemoveDirectory(unMountDir);
    ASSERT_FALSE(ifs->IsExist(unMountDir));
    ifs->MakeDirectory(subDir, true);
    FileStat fileStat;
    mFileSystem->GetFileStat(subDir, fileStat);
    ASSERT_EQ(FSST_LOCAL, fileStat.storageType);
    ASSERT_FALSE(fileStat.inCache);
    ASSERT_TRUE(fileStat.onDisk);
    ASSERT_TRUE(fileStat.isDirectory);
}

string FileSystemRemoveTest::GetAbsPath(string path)
{
    string rootPath = GET_ABS_PATH("root");
    return PathUtil::JoinPath(rootPath, path);
}


IE_NAMESPACE_END(file_system);

