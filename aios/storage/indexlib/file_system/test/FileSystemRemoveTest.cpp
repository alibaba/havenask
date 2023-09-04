#include "gtest/gtest.h"
#include <iosfwd>
#include <memory>
#include <string>

#include "autil/CommonMacros.h"
#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "fslib/common/common_type.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/file_system/RemoveOption.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/file_system/test/FileSystemTestUtil.h"
#include "indexlib/file_system/test/TestFileCreator.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/testutil/unittest.h"

namespace indexlib { namespace util {
class BadParameterException;
class FileIOException;
class NonExistException;
}} // namespace indexlib::util

using namespace std;
using namespace autil;
using namespace indexlib::util;

namespace indexlib { namespace file_system {

typedef bool TestNeedFlushType;
typedef std::tuple<FSStorageType, TestNeedFlushType> TestRemoveParam;

class FileSystemRemoveTest : public INDEXLIB_TESTBASE_WITH_PARAM<TestRemoveParam>
{
public:
    FileSystemRemoveTest();
    ~FileSystemRemoveTest();

    DECLARE_CLASS_NAME(FileSystemRemoveTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;

public:
    void TestRemoveFile();
    void TestRemoveDirectory();
    void TestUnMount();

    void TestRemoveFileException();
    void TestRemoveDirectoryException();

private:
    std::shared_ptr<IFileSystem> PrepareData(std::string treeStr = "");
    void CheckRemovePath(std::string path, std::string expectStr, int lineNo);
    void CheckUnmountPath(std::string treeStr, std::string unMountDir, std::string subDir, int lineNo);
    std::string GetAbsPath(std::string path);

private:
    std::string _rootDir;
    std::string _treeStr;
    std::shared_ptr<IFileSystem> _fileSystem;

private:
    AUTIL_LOG_DECLARE();
};

INDEXLIB_INSTANTIATE_TEST_CASE_ONE_P(FileSystemRemoveTestNeedFlush, FileSystemRemoveTest,
                                     testing::Combine(testing::Values(FSST_UNKNOWN), testing::Values(true, false)));
AUTIL_LOG_SETUP(indexlib.file_system, FileSystemRemoveTest);

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(FileSystemRemoveTestNeedFlush, TestRemoveFile);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(FileSystemRemoveTestNeedFlush, TestRemoveDirectory);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(FileSystemRemoveTestNeedFlush, TestUnMount);

INDEXLIB_UNIT_TEST_CASE(FileSystemRemoveTest, TestRemoveFileException);
INDEXLIB_UNIT_TEST_CASE(FileSystemRemoveTest, TestRemoveDirectoryException);

//////////////////////////////////////////////////////////////////////

FileSystemRemoveTest::FileSystemRemoveTest() {}

FileSystemRemoveTest::~FileSystemRemoveTest() {}

void FileSystemRemoveTest::CaseSetUp() { _rootDir = PathUtil::JoinPath(GET_TEMP_DATA_PATH(), "root"); }

void FileSystemRemoveTest::CaseTearDown() { _fileSystem.reset(); }

#define CHECK_REMOVE(path, expectStr) CheckRemovePath((path), (expectStr), __LINE__)

void FileSystemRemoveTest::TestRemoveFile()
{
    CHECK_REMOVE(GetAbsPath("/file"), "slice,"
                                      "local/,local/file,local/local_0/,local/slice,"
                                      "inmem/,inmem/file,inmem/inmem_0/,inmem/slice");
    CHECK_REMOVE(GetAbsPath("/local/file"), "file,slice,"
                                            "local/,local/local_0/,local/slice,"
                                            "inmem/,inmem/file,inmem/inmem_0/,inmem/slice");
    CHECK_REMOVE(GetAbsPath("/inmem/file"), "file,slice,"
                                            "local/,local/file,local/local_0/,local/slice,"
                                            "inmem/,inmem/inmem_0/,inmem/slice");
    // remove slice file
    CHECK_REMOVE(GetAbsPath("/slice"), "file,"
                                       "local/,local/file,local/local_0/,local/slice,"
                                       "inmem/,inmem/file,inmem/inmem_0/,inmem/slice");
    CHECK_REMOVE(GetAbsPath("/local/slice"), "file,slice,"
                                             "local/,local/local_0/,local/file,"
                                             "inmem/,inmem/file,inmem/inmem_0/,inmem/slice");
    CHECK_REMOVE(GetAbsPath("/inmem/slice"), "file,slice,"
                                             "local/,local/file,local/local_0/,local/slice,"
                                             "inmem/,inmem/inmem_0/,inmem/file");
}

void FileSystemRemoveTest::TestRemoveDirectory()
{
    CHECK_REMOVE(GetAbsPath("/local"), "file,slice,inmem/,inmem/file,inmem/inmem_0/,inmem/slice");
    CHECK_REMOVE(GetAbsPath("/inmem"), "file,slice,local/,local/file,local/local_0/,local/slice");
    CHECK_REMOVE(GetAbsPath("/local/local_0"), "file,slice,local/,local/file,local/slice,"
                                               "inmem/,inmem/file,inmem/slice,inmem/inmem_0/");
    CHECK_REMOVE(GetAbsPath("/inmem/inmem_0"), "file,slice,local/,local/file,local/slice,local/local_0/,"
                                               "inmem/,inmem/file,inmem/slice");
    // remove mount path
}

#define CHECK_UNMOUNT_PATH(treeStr, unMountPath, subDir) CheckUnmountPath((treeStr), (unMountPath), (subDir), __LINE__)
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
    CHECK_UNMOUNT_PATH(treeStr, GetAbsPath("multi/local"), GetAbsPath("multi/local/dir"));
}

void FileSystemRemoveTest::TestRemoveFileException()
{
    std::shared_ptr<IFileSystem> ifs = PrepareData(_treeStr);

    // remove out of root
    ASSERT_EQ(FSEC_BADARGS, ifs->RemoveFile("", RemoveOption()));
    ASSERT_EQ(FSEC_NOENT, ifs->RemoveFile("outofroot", RemoveOption()));

    // remove nonexist
    ASSERT_EQ(FSEC_NOENT, ifs->RemoveFile(GetAbsPath("/inmem/nonexist"), RemoveOption()));
    ASSERT_EQ(FSEC_NOENT, ifs->RemoveFile(GetAbsPath("/local/nonexist"), RemoveOption()));

    // remove dir
    ASSERT_EQ(FSEC_OK, ifs->MakeDirectory(GetAbsPath("/inmem/ondisk"), DirectoryOption()));
    ASSERT_EQ(FSEC_ISDIR, ifs->RemoveFile(GetAbsPath("/inmem/ondisk"), RemoveOption()));
    ASSERT_EQ(FSEC_ISDIR, ifs->RemoveFile(GetAbsPath("/inmem/inmem_0"), RemoveOption()));
    ASSERT_EQ(FSEC_ISDIR, ifs->RemoveFile(GetAbsPath("/local/local_0"), RemoveOption()));

    // use count
    std::shared_ptr<FileReader> fileReader = ifs->CreateFileReader(GetAbsPath("/inmem/file"), FSOT_MEM).GetOrThrow();
    ASSERT_EQ(FSEC_ERROR, ifs->RemoveFile(GetAbsPath("/inmem/file"), RemoveOption()));
    fileReader = ifs->CreateFileReader(GetAbsPath("/local/file"), FSOT_MEM).GetOrThrow();
    ASSERT_EQ(FSEC_ERROR, ifs->RemoveFile(GetAbsPath("/local/file"), RemoveOption()));
}

void FileSystemRemoveTest::TestRemoveDirectoryException()
{
    std::shared_ptr<IFileSystem> ifs = PrepareData(_treeStr);

    // remove out of root
    ASSERT_EQ(FSEC_BADARGS, ifs->RemoveDirectory("", RemoveOption()));
    ASSERT_EQ(FSEC_NOENT, ifs->RemoveDirectory("outofroot", RemoveOption()));

    // remove nonexist
    ASSERT_EQ(FSEC_NOENT, ifs->RemoveDirectory(GetAbsPath("/inmem/nonexist"), RemoveOption()));
    ASSERT_EQ(FSEC_NOENT, ifs->RemoveDirectory(GetAbsPath("/local/nonexist"), RemoveOption()));

    // remove file
    ASSERT_EQ(FSEC_NOTDIR, ifs->RemoveDirectory(GetAbsPath("/inmem/file"), RemoveOption()));
    ASSERT_EQ(FSEC_NOTDIR, ifs->RemoveDirectory(GetAbsPath("/local/file"), RemoveOption()));

    // TODO
    // use count
    // TODO(xiuchen) use_count has been commented now
    // std::shared_ptr<FileReader> fileReader = ifs->CreateFileReader(GetAbsPath("/inmem/file"), FSOT_MEM);
    // ASSERT_THROW(ifs->RemoveDirectory(GetAbsPath("/inmem")), FileIOException);
    // fileReader = ifs->CreateFileReader(GetAbsPath("/local/file"), FSOT_MEM);
    // ASSERT_THROW(ifs->RemoveDirectory(GetAbsPath("/local")), FileIOException);

    // remove root
    ASSERT_EQ(FSEC_BADARGS, ifs->RemoveDirectory("", RemoveOption()));
}

std::shared_ptr<IFileSystem> FileSystemRemoveTest::PrepareData(string treeStr)
{
    if (_fileSystem) {
        _fileSystem->Sync(true).GetOrThrow();
    }
    testDataPathTearDown();
    testDataPathSetup();

    EXPECT_EQ(FSEC_OK, FslibWrapper::MkDir(_rootDir).Code());
    FileSystemOptions fsOptions;
    fsOptions.isOffline = true;
    _fileSystem = FileSystemCreator::Create("FileSystemRemoveTest", _rootDir, fsOptions).GetOrThrow();
    if (treeStr.empty()) {
        treeStr = "#:local,inmem^I,file^F,slice^S;"
                  "#/local:local_0,file^F,slice^S;"
                  "#/inmem:inmem_0,file^F,slice^S";
    }
    TestFileCreator::CreateFileTree(_fileSystem, treeStr);
    return _fileSystem;
}

void FileSystemRemoveTest::CheckRemovePath(string path, string expectStr, int lineNo)
{
    SCOPED_TRACE(string("line number[") + StringUtil::toString(lineNo) + string("]"));

    PrepareData(_treeStr);

    bool isDir = _fileSystem->IsDir(path).GetOrThrow();
    if (isDir) {
        ASSERT_EQ(FSEC_OK, _fileSystem->RemoveDirectory(path, RemoveOption()));
    } else {
        ASSERT_EQ(FSEC_OK, _fileSystem->RemoveFile(path, RemoveOption()));
    }
    ASSERT_FALSE(_fileSystem->IsExist(path).GetOrThrow());
    ASSERT_TRUE(FileSystemTestUtil::CheckListFile(_fileSystem, "", true, expectStr));
}

void FileSystemRemoveTest::CheckUnmountPath(string treeStr, string unMountDir, string subDir, int lineNo)
{
    SCOPED_TRACE(string("line number[") + StringUtil::toString(lineNo) + string("]"));

    std::shared_ptr<IFileSystem> ifs = PrepareData(treeStr);
    ASSERT_EQ(FSEC_OK, ifs->RemoveDirectory(unMountDir, RemoveOption()));
    ASSERT_FALSE(ifs->IsExist(unMountDir).GetOrThrow());
    ASSERT_EQ(FSEC_OK, ifs->MakeDirectory(subDir, DirectoryOption()));
    ASSERT_TRUE(ifs->IsExist(subDir).GetOrThrow());
}

string FileSystemRemoveTest::GetAbsPath(string path)
{
    if (path[0] == '/') {
        path = path.substr(1);
    }
    return path;
}
}} // namespace indexlib::file_system
