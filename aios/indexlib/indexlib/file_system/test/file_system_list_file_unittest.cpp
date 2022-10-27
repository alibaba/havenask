#include <autil/StringUtil.h>
#include <fslib/fslib.h>
#include "indexlib/util/path_util.h"
#include "indexlib/misc/exception.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/file_system/test/file_system_creator.h"
#include "indexlib/file_system/test/test_file_creator.h"
#include "indexlib/file_system/test/file_system_test_util.h"
#include "indexlib/file_system/test/file_system_list_file_unittest.h"

using namespace std;
using namespace autil;
using namespace fslib;

IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(storage);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, FileSystemListFileTest);

#define CHECK_LIST_FILE(ifs, path, recursive, expectStr)                \
    CheckListFile((ifs), (path), (recursive), (expectStr), __LINE__)

#define CHECK_FOR_TEMP_FILE(isLocal, isFile)                    \
    CheckListFileForTempFile((isLocal), (isFile), __LINE__)

FileSystemListFileTest::FileSystemListFileTest()
{
}

FileSystemListFileTest::~FileSystemListFileTest()
{
}

void FileSystemListFileTest::CaseSetUp()
{
    mRootDir = GET_ABS_PATH("root");
}

void FileSystemListFileTest::CaseTearDown()
{
    mFileSystem.reset();
}

void FileSystemListFileTest::TestListFile()
{
    IndexlibFileSystemPtr fileSystem = PrepareData();
    
    string memDirOnDisk = GetAbsPath("/inmem/ondisk");
    FileSystemWrapper::MkDir(memDirOnDisk,true);
    CHECK_LIST_FILE(fileSystem, fileSystem->GetRootPath(), false,
                    "file,inmem,local,slice");
    CHECK_LIST_FILE(fileSystem, GetAbsPath("/local"), false, 
                    "file,local_0,slice");
    CHECK_LIST_FILE(fileSystem, GetAbsPath("/inmem"), false, 
                    "file,inmem_0,slice,ondisk");
    CHECK_LIST_FILE(fileSystem, GetAbsPath("/local/local_0"), false, "");
    CHECK_LIST_FILE(fileSystem, GetAbsPath("/inmem/inmem_0"), false, "");
}

void FileSystemListFileTest::TestForTempFile()
{
    bool isLocal = (GET_PARAM_VALUE(0) == FSST_LOCAL);
    // test file name
    CHECK_FOR_TEMP_FILE(isLocal, true);

    // dir name not support include TEMP_FILE_SUFFIX
    // else the result is not stable
}

void FileSystemListFileTest::TestListFileRecursive()
{
    IndexlibFileSystemPtr fileSystem = PrepareData();

    CHECK_LIST_FILE(fileSystem, fileSystem->GetRootPath(), true,
                    "file,inmem/,local/,slice,"
                    "local/local_0/,local/file,local/slice,"
                    "inmem/inmem_0/,inmem/file,inmem/slice");
    CHECK_LIST_FILE(fileSystem, GetAbsPath("/local"), true,
                    "file,local_0/,slice");
    CHECK_LIST_FILE(fileSystem, GetAbsPath("/inmem"), true, 
                    "file,inmem_0/,slice");
    CHECK_LIST_FILE(fileSystem, GetAbsPath("/local/local_0"), true, "");
    CHECK_LIST_FILE(fileSystem, GetAbsPath("/inmem/inmem_0"), true, "");

    fileSystem->Sync(true);
    CHECK_LIST_FILE(fileSystem, fileSystem->GetRootPath(), true,
                    "file,slice,local/,local/local_0/,local/file,local/slice,"
                    "inmem/,inmem/inmem_0/,inmem/file,inmem/slice");

    fileSystem->CleanCache();
    CHECK_LIST_FILE(fileSystem, fileSystem->GetRootPath(), true,
                    "file,local/,local/local_0/,local/file,"
                    "inmem/,inmem/inmem_0/,inmem/file");
}

void FileSystemListFileTest::TestListFileForMultiInMemStorage()
{
    string treeStr = "#:local1,local2,inmem1^I,inmem2^I,file^F;"
                     "#/local1:local,inmem11^I,inmem12^I,slice^S;"
                     "#/inmem1:inmem,slice^S;"
                     "#/local1/inmem11:inmem,file^F";

    IndexlibFileSystemPtr fileSystem = PrepareData(treeStr);
    string rootPath = fileSystem->GetRootPath();
    string inMemDir1 = PathUtil::JoinPath(rootPath, "inmem1");
    string inMemDir2 = PathUtil::JoinPath(rootPath, "local1/inmem11");

    // test not recursive list file
    CHECK_LIST_FILE(fileSystem, rootPath, false,
                    "local1,local2,inmem1,inmem2,file");
    CHECK_LIST_FILE(fileSystem, inMemDir1, false, "inmem,slice");
    CHECK_LIST_FILE(fileSystem, inMemDir2, false, "inmem,file");

    // test list file recursive 
    CHECK_LIST_FILE(fileSystem, rootPath, true,
                    "local1/,local2/,inmem1/,inmem2/,file,"
                    "local1/local/,local1/inmem11/,local1/inmem12/,local1/slice,"
                    "inmem1/inmem/,inmem1/slice,"
                    "local1/inmem11/inmem/,local1/inmem11/file");

    fileSystem->Sync(true);
    // test list file recursive 
    CHECK_LIST_FILE(fileSystem, rootPath, true,
                    "local1/,local2/,inmem1/,inmem2/,file,"
                    "local1/local/,local1/inmem11/,local1/inmem12/,local1/slice,"
                    "inmem1/inmem/,inmem1/slice,"
                    "local1/inmem11/inmem/,local1/inmem11/file");

    fileSystem->CleanCache();
    // test list file recursive 
    CHECK_LIST_FILE(fileSystem, rootPath, true,
                    "local1/,local2/,inmem1/,inmem2/,file,"
                    "local1/local/,local1/inmem11/,local1/inmem12/,"
                    "inmem1/inmem/,"
                    "local1/inmem11/inmem/,local1/inmem11/file");
}

void FileSystemListFileTest::TestEmptyFileTree()
{
    IndexlibFileSystemPtr fileSystem = PrepareData("#:");
    CHECK_LIST_FILE(fileSystem, fileSystem->GetRootPath(), false, "");
}

void FileSystemListFileTest::TestStorageListFile()
{
    string treeStr = (GET_PARAM_VALUE(0) == FSST_IN_MEM)?
                     "#:dir,file^F;#/dir:subdir,subfile^F":
                     "#:dir^I,file^F;#/dir:subdir^I,subfile^F";    
    IndexlibFileSystemPtr fileSystem = PrepareData(treeStr);
    bool recursive = GET_PARAM_VALUE(1);
    CHECK_LIST_FILE(fileSystem, fileSystem->GetRootPath(), recursive,
                    recursive?"dir/,file,dir/subdir/,dir/subfile":"dir,file");
    CHECK_LIST_FILE(fileSystem, GetAbsPath("dir"), recursive, 
                    recursive?"subdir/,subfile":"subdir,subfile");
    CHECK_LIST_FILE(fileSystem, GetAbsPath("dir/subdir"), recursive, "");
}

void FileSystemListFileTest::TestListRecursiveWithoutCache()
{
    RESET_FILE_SYSTEM(LoadConfigList(), false, false);
    IndexlibFileSystemPtr fileSystem = GET_FILE_SYSTEM();
    TestFileCreator::CreateFileTree(
            fileSystem, "#:dir,file^F;#/dir:subdir,subfile^F");

    CHECK_LIST_FILE(fileSystem, fileSystem->GetRootPath(), true,
                    "file,"
                    "dir/,dir/subdir/,dir/subfile");

    CHECK_LIST_FILE(fileSystem, GET_ABS_PATH("dir"), true, 
                    "subdir/,"
                    "subfile");
    CHECK_LIST_FILE(fileSystem, GET_ABS_PATH("dir/subdir"), true, "");
}

void FileSystemListFileTest::TestListFileException()
{
    IndexlibFileSystemPtr fileSystem = PrepareData();
    
    bool recursive = GET_PARAM_VALUE(1);
    FileList fileList;

    // list empty path string
    ASSERT_THROW(fileSystem->ListFile("",
                    fileList, recursive), BadParameterException);
    // list error path string
    ASSERT_THROW(fileSystem->ListFile("__ERROR_PROTOCOL__://",
                    fileList, recursive), BadParameterException);
    // list file
    ASSERT_THROW(fileSystem->ListFile(GetAbsPath("file"), 
                    fileList, recursive), InconsistentStateException);
    ASSERT_THROW(fileSystem->ListFile(GetAbsPath("local/file"), 
                    fileList, recursive), InconsistentStateException);
    ASSERT_THROW(fileSystem->ListFile(GetAbsPath("inmem/file"),
                    fileList, recursive), InconsistentStateException);
    // non exist
    ASSERT_THROW(fileSystem->ListFile(GetAbsPath("nonexist"), 
                    fileList, recursive), NonExistException);
    ASSERT_THROW(fileSystem->ListFile(GetAbsPath("local/nonexist"), 
                    fileList, recursive), NonExistException);
    ASSERT_THROW(fileSystem->ListFile(GetAbsPath("inmem/nonexist"),
                    fileList, recursive), NonExistException);
}

IndexlibFileSystemPtr FileSystemListFileTest::PrepareData(string treeStr)
{
    if (mFileSystem)
    {
        mFileSystem->Sync(true);
    }
    mFileSystem.reset();
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

void FileSystemListFileTest::CheckListFile(IndexlibFileSystemPtr ifs, 
        string path, bool recursive, string expectStr, int lineNo)
{
    SCOPED_TRACE(string("line number[") 
                 + StringUtil::toString(lineNo) + string("]"));
    ASSERT_TRUE(FileSystemTestUtil::CheckListFile(ifs, path, recursive, 
                    expectStr)); 
}

string FileSystemListFileTest::GetAbsPath(string path)
{
    return PathUtil::JoinPath(mRootDir, path);
}

string FileSystemListFileTest::GetTempTree(string rootDirName,
        bool isLocal, bool isFile, bool recursive, string& expectStr)
{
    string name = (isFile?"file":"dir");
    string type = (isFile?"^F":"");
    string tempName = TEMP_FILE_SUFFIX;
    string dirSuffix = ((!isFile && recursive)?"/":"");

    stringstream ssTreeStr;
    ssTreeStr << "#:" << rootDirName << (isLocal?";":"^I;")
              << "#/" << rootDirName << ":" 
              << name << "1" << TEMP_FILE_SUFFIX << type
              << "," << name << "2" << TEMP_FILE_SUFFIX << "end" << type
              << "," << TEMP_FILE_SUFFIX << name << "3" << type
              << ",repeat1" << TEMP_FILE_SUFFIX << type
              << ",repeat2" << TEMP_FILE_SUFFIX << type;
    string treeStr = ssTreeStr.str();

    stringstream ssExpectStr;
    ssExpectStr << name << "2" << TEMP_FILE_SUFFIX << "end" << dirSuffix
                << "," << TEMP_FILE_SUFFIX << name << "3" << dirSuffix;

    expectStr = ssExpectStr.str();

    return treeStr;
}

void FileSystemListFileTest::CheckListFileForTempFile(bool isLocal, 
        bool isFile, int lineNo)
{
    SCOPED_TRACE(string("line number[") 
                 + StringUtil::toString(lineNo) + string("]"));

    string expectStr;
    bool recursive = GET_PARAM_VALUE(1);
    string treeStr = GetTempTree("root", isLocal, isFile, recursive, expectStr);
    IndexlibFileSystemPtr fileSystem = PrepareData(treeStr);
    string rootPath = PathUtil::JoinPath(fileSystem->GetRootPath(), "root");
    CHECK_LIST_FILE(fileSystem, rootPath, recursive, expectStr);
}
IE_NAMESPACE_END(file_system);

