#include "gtest/gtest.h"
#include <memory>
#include <ostream>
#include <string>

#include "autil/CommonMacros.h"
#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "fslib/common/common_type.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/file_system/ListOption.h"
#include "indexlib/file_system/load_config/LoadConfigList.h"
#include "indexlib/file_system/test/FileSystemTestUtil.h"
#include "indexlib/file_system/test/TestFileCreator.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/testutil/unittest.h"

namespace indexlib { namespace util {
class InconsistentStateException;
class NonExistException;
}} // namespace indexlib::util

using namespace std;
using namespace autil;
using namespace indexlib::util;

namespace indexlib { namespace file_system {

typedef bool TestRecursiveType;
typedef std::tuple<FSStorageType, TestRecursiveType> TestListFileParam;

class FileSystemListFileTest : public INDEXLIB_TESTBASE_WITH_PARAM<TestListFileParam>
{
public:
    FileSystemListFileTest();
    ~FileSystemListFileTest();

    DECLARE_CLASS_NAME(FileSystemListFileTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;

public:
    void TestListFile();
    void TestForTempFile();
    void TestListFileRecursive();
    void TestListFileException();
    void TestEmptyFileTree();
    void TestStorageListFile();
    void TestListFileForMultiInMemStorage();
    void TestListRecursiveWithoutCache();

private:
    std::shared_ptr<IFileSystem> PrepareData(std::string treeStr = "");
    void CheckListFile(std::shared_ptr<IFileSystem> ifs, std::string path, bool recursive, std::string expectStr,
                       int lineNo);
    std::string GetAbsPath(std::string path);
    std::string GetTempTree(std::string rootDirName, bool isLocal, bool isFile, bool recursive, std::string& expectStr);
    void CheckListFileForTempFile(bool isLocal, bool isFile, int lineNo);

private:
    std::shared_ptr<IFileSystem> _fileSystem;

private:
    AUTIL_LOG_DECLARE();
};
AUTIL_LOG_SETUP(indexlib.file_system, FileSystemListFileTest);

INDEXLIB_UNIT_TEST_CASE(FileSystemListFileTest, TestListFile);
INDEXLIB_UNIT_TEST_CASE(FileSystemListFileTest, TestListFileRecursive);
INDEXLIB_UNIT_TEST_CASE(FileSystemListFileTest, TestEmptyFileTree);
INDEXLIB_UNIT_TEST_CASE(FileSystemListFileTest, TestListFileForMultiInMemStorage);
INDEXLIB_UNIT_TEST_CASE(FileSystemListFileTest, TestListRecursiveWithoutCache);

INDEXLIB_INSTANTIATE_TEST_CASE_ONE_P(FileSystemListFileTestExceptionTest, FileSystemListFileTest,
                                     testing::Combine(testing::Values(FSST_UNKNOWN), testing::Values(true, false)));

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(FileSystemListFileTestExceptionTest, TestListFileException);

INDEXLIB_INSTANTIATE_TEST_CASE_ONE_P(FileSystemListFileTestStorageListFile, FileSystemListFileTest,
                                     testing::Combine(testing::Values(FSST_MEM, FSST_DISK),
                                                      testing::Values(true, false)));

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(FileSystemListFileTestStorageListFile, TestStorageListFile);

INDEXLIB_INSTANTIATE_TEST_CASE_ONE_P(FileSystemListFileTestTempFile, FileSystemListFileTest,
                                     testing::Combine(testing::Values(FSST_MEM, FSST_DISK),
                                                      testing::Values(true, false)));

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(FileSystemListFileTestTempFile, TestForTempFile);

//////////////////////////////////////////////////////////////////////

#define CHECK_LIST_FILE(ifs, path, recursive, expectStr)                                                               \
    CheckListFile((ifs), (path), (recursive), (expectStr), __LINE__)

#define CHECK_FOR_TEMP_FILE(isLocal, isFile) CheckListFileForTempFile((isLocal), (isFile), __LINE__)

FileSystemListFileTest::FileSystemListFileTest() {}

FileSystemListFileTest::~FileSystemListFileTest() {}

void FileSystemListFileTest::CaseSetUp()
{
    _fileSystem = FileSystemCreator::Create("", GET_TEMP_DATA_PATH()).GetOrThrow();
}

void FileSystemListFileTest::CaseTearDown() { _fileSystem.reset(); }

void FileSystemListFileTest::TestListFile()
{
    std::shared_ptr<IFileSystem> fileSystem = PrepareData();

    string memDirOnDisk = "inmem/ondisk";
    ASSERT_EQ(FSEC_OK, fileSystem->MakeDirectory(memDirOnDisk, DirectoryOption()));
    CHECK_LIST_FILE(fileSystem, "", false, "file,inmem,local,slice");
    CHECK_LIST_FILE(fileSystem, "local", false, "file,local_0,slice");
    CHECK_LIST_FILE(fileSystem, "inmem", false, "file,inmem_0,slice,ondisk");
    CHECK_LIST_FILE(fileSystem, "local/local_0", false, "");
    CHECK_LIST_FILE(fileSystem, "inmem/inmem_0", false, "");
}

void FileSystemListFileTest::TestForTempFile()
{
    bool isLocal = (GET_PARAM_VALUE(0) == FSST_DISK);
    // test file name
    CHECK_FOR_TEMP_FILE(isLocal, true);

    // dir name not support include TEMP_FILE_SUFFIX
    // else the result is not stable
}

void FileSystemListFileTest::TestListFileRecursive()
{
    std::shared_ptr<IFileSystem> fileSystem = PrepareData();

    CHECK_LIST_FILE(fileSystem, "", true,
                    "file,inmem/,local/,slice,"
                    "local/local_0/,local/file,local/slice,"
                    "inmem/inmem_0/,inmem/file,inmem/slice");
    CHECK_LIST_FILE(fileSystem, GetAbsPath("/local"), true, "file,local_0/,slice");
    CHECK_LIST_FILE(fileSystem, GetAbsPath("/inmem"), true, "file,inmem_0/,slice");
    CHECK_LIST_FILE(fileSystem, GetAbsPath("/local/local_0"), true, "");
    CHECK_LIST_FILE(fileSystem, GetAbsPath("/inmem/inmem_0"), true, "");

    fileSystem->Sync(true).GetOrThrow();
    CHECK_LIST_FILE(fileSystem, "", true,
                    "file,slice,local/,local/local_0/,local/file,local/slice,"
                    "inmem/,inmem/inmem_0/,inmem/file,inmem/slice");

    fileSystem->CleanCache();
    CHECK_LIST_FILE(fileSystem, "", true,
                    "file,local/,local/local_0/,local/file,"
                    "inmem/,inmem/inmem_0/,inmem/file");
}

void FileSystemListFileTest::TestListFileForMultiInMemStorage()
{
    string treeStr = "#:local1,local2,inmem1^I,inmem2^I,file^F;"
                     "#/local1:local,inmem11^I,inmem12^I,slice^S;"
                     "#/inmem1:inmem,slice^S;"
                     "#/local1/inmem11:inmem,file^F";

    std::shared_ptr<IFileSystem> fileSystem = PrepareData(treeStr);
    string rootPath = "";
    string inMemDir1 = PathUtil::JoinPath(rootPath, "inmem1");
    string inMemDir2 = PathUtil::JoinPath(rootPath, "local1/inmem11");

    // test not recursive list file
    CHECK_LIST_FILE(fileSystem, rootPath, false, "local1,local2,inmem1,inmem2,file");
    CHECK_LIST_FILE(fileSystem, inMemDir1, false, "inmem,slice");
    CHECK_LIST_FILE(fileSystem, inMemDir2, false, "inmem,file");

    // test list file recursive
    CHECK_LIST_FILE(fileSystem, rootPath, true,
                    "local1/,local2/,inmem1/,inmem2/,file,"
                    "local1/local/,local1/inmem11/,local1/inmem12/,local1/slice,"
                    "inmem1/inmem/,inmem1/slice,"
                    "local1/inmem11/inmem/,local1/inmem11/file");

    fileSystem->Sync(true).GetOrThrow();
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
    std::shared_ptr<IFileSystem> fileSystem = PrepareData("#:");
    CHECK_LIST_FILE(fileSystem, "", false, "");
}

void FileSystemListFileTest::TestStorageListFile()
{
    string treeStr = (GET_PARAM_VALUE(0) == FSST_MEM) ? "#:dir,file^F;#/dir:subdir,subfile^F"
                                                      : "#:dir^I,file^F;#/dir:subdir^I,subfile^F";
    std::shared_ptr<IFileSystem> fileSystem = PrepareData(treeStr);
    bool recursive = GET_PARAM_VALUE(1);
    CHECK_LIST_FILE(fileSystem, "", recursive, recursive ? "dir/,file,dir/subdir/,dir/subfile" : "dir,file");
    CHECK_LIST_FILE(fileSystem, GetAbsPath("dir"), recursive, recursive ? "subdir/,subfile" : "subdir,subfile");
    CHECK_LIST_FILE(fileSystem, GetAbsPath("dir/subdir"), recursive, "");
}

void FileSystemListFileTest::TestListRecursiveWithoutCache()
{
    FileSystemOptions options;
    options.enableAsyncFlush = false;
    options.useCache = false;
    _fileSystem = FileSystemCreator::Create("ut", GET_TEMP_DATA_PATH(), options).GetOrThrow();
    std::shared_ptr<IFileSystem> fileSystem = _fileSystem;
    TestFileCreator::CreateFileTree(fileSystem, "#:dir,file^F;#/dir:subdir,subfile^F");

    CHECK_LIST_FILE(fileSystem, "", true,
                    "file,"
                    "dir/,dir/subdir/,dir/subfile");

    CHECK_LIST_FILE(fileSystem, "dir", true,
                    "subdir/,"
                    "subfile");
    CHECK_LIST_FILE(fileSystem, "dir/subdir", true, "");
}

void FileSystemListFileTest::TestListFileException()
{
    std::shared_ptr<IFileSystem> fileSystem = PrepareData();

    bool recursive = GET_PARAM_VALUE(1);
    FileList fileList;

    // list empty path string
    ASSERT_EQ(FSEC_OK, fileSystem->ListDir("", ListOption::Recursive(recursive), fileList));

    // list error path string
    // ASSERT_THROW(fileSystem->ListDir("__ERROR_PROTOCOL__://", fileList, recursive), BadParameterException);

    // list file
    ASSERT_EQ(FSEC_NOTDIR, fileSystem->ListDir(GetAbsPath("file"), ListOption::Recursive(recursive), fileList));
    ASSERT_EQ(FSEC_NOTDIR, fileSystem->ListDir(GetAbsPath("local/file"), ListOption::Recursive(recursive), fileList));
    ASSERT_EQ(FSEC_NOTDIR, fileSystem->ListDir(GetAbsPath("inmem/file"), ListOption::Recursive(recursive), fileList));
    // non exist
    ASSERT_EQ(FSEC_NOENT, fileSystem->ListDir(GetAbsPath("nonexist"), ListOption::Recursive(recursive), fileList));
    ASSERT_EQ(FSEC_NOENT,
              fileSystem->ListDir(GetAbsPath("local/nonexist"), ListOption::Recursive(recursive), fileList));
    ASSERT_EQ(FSEC_NOENT,
              fileSystem->ListDir(GetAbsPath("inmem/nonexist"), ListOption::Recursive(recursive), fileList));
}

std::shared_ptr<IFileSystem> FileSystemListFileTest::PrepareData(string treeStr)
{
    if (_fileSystem) {
        _fileSystem->Sync(true).GetOrThrow();
    }
    _fileSystem.reset();
    CaseTearDown();
    CaseSetUp();

    if (treeStr.empty()) {
        treeStr = "#:local,inmem^I,file^F,slice^S;"
                  "#/local:local_0,file^F,slice^S;"
                  "#/inmem:inmem_0,file^F,slice^S";
    }
    TestFileCreator::CreateFileTree(_fileSystem, treeStr);

    return _fileSystem;
}

void FileSystemListFileTest::CheckListFile(std::shared_ptr<IFileSystem> ifs, string path, bool recursive,
                                           string expectStr, int lineNo)
{
    SCOPED_TRACE(string("line number[") + StringUtil::toString(lineNo) + string("]"));
    ASSERT_TRUE(FileSystemTestUtil::CheckListFile(ifs, path, recursive, expectStr));
}

string FileSystemListFileTest::GetAbsPath(string path)
{
    if (!path.empty() && path[0] == '/') {
        path = path.substr(1);
    }
    return path;
}

string FileSystemListFileTest::GetTempTree(string rootDirName, bool isLocal, bool isFile, bool recursive,
                                           string& expectStr)
{
    string name = (isFile ? "file" : "dir");
    string type = (isFile ? "^F" : "");
    string tempName = TEMP_FILE_SUFFIX;
    string dirSuffix = ((!isFile && recursive) ? "/" : "");

    stringstream ssTreeStr;
    ssTreeStr << "#:" << rootDirName << (isLocal ? ";" : "^I;") << "#/" << rootDirName << ":" << name << "1"
              << TEMP_FILE_SUFFIX << type << "," << name << "2" << TEMP_FILE_SUFFIX << "end" << type << ","
              << TEMP_FILE_SUFFIX << name << "3" << type << ",repeat1" << TEMP_FILE_SUFFIX << type << ",repeat2"
              << TEMP_FILE_SUFFIX << type;
    string treeStr = ssTreeStr.str();

    stringstream ssExpectStr;
    ssExpectStr << name << "2" << TEMP_FILE_SUFFIX << "end" << dirSuffix << "," << TEMP_FILE_SUFFIX << name << "3"
                << dirSuffix;

    expectStr = ssExpectStr.str();

    return treeStr;
}

void FileSystemListFileTest::CheckListFileForTempFile(bool isLocal, bool isFile, int lineNo)
{
    SCOPED_TRACE(string("line number[") + StringUtil::toString(lineNo) + string("]"));

    string expectStr;
    bool recursive = GET_PARAM_VALUE(1);
    string treeStr = GetTempTree("", isLocal, isFile, recursive, expectStr);
    std::shared_ptr<IFileSystem> fileSystem = PrepareData(treeStr);
    string rootPath = "";
    CHECK_LIST_FILE(fileSystem, rootPath, recursive, expectStr);
}
}} // namespace indexlib::file_system
