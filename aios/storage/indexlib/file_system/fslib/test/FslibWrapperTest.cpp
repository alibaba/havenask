#include "indexlib/file_system/fslib/FslibWrapper.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include <cstddef>
#include <stdint.h>
#include <string>
#include <sys/mman.h>

#include "autil/EnvUtil.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/File.h"
#include "fslib/fs/FileSystem.h"
#include "fslib/fs/MMapFile.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/fslib/FslibCommonFileWrapper.h"
#include "indexlib/file_system/fslib/FslibFileWrapper.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/testutil/unittest.h"

using namespace std;

namespace indexlib { namespace file_system {

class FslibWrapperTest : public INDEXLIB_TESTBASE
{
public:
    FslibWrapperTest() {}
    ~FslibWrapperTest() {}
    DECLARE_CLASS_NAME(FslibWrapperTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestCaseForOpen4Read();
    void TestCaseForOpen4Write();
    void TestCaseForOpen4Append();
    void TestCaseForOpen4WriteExist();
    void TestCaseForOpenFail();
    void TestCaseForLoadAndStore();
    void TestCaseForRemoveNotExistFile();
    void TestCaseForIsExist();
    void TestCaseForListDir();
    void TestCaseForListDirRecursive();
    void TestCaseForDeleteDir();
    void TestCaseForDeleteDirFail();
    void TestCaseForCopy();
    void TestCaseForRename();
    void TestCaseForMkDir();
    void TestCaseForGetFileMeta();
    void TestCaseForGetFileLength();
    void TestCaseForIsDir();
    void TestCaseForDeleteIfExist();
    void TestCaseForNeedMkParentDirBeforeOpen();
    void TestCaseForMkDirIfNotExist();

    void TestCaseForSimpleMmapFile();
    void TestCaseForLinkWithInvalidInput();
    void TestCaseForLocalLink();
    void TestCaseForSymLink();

    void TestCreateFenceContext();
    void TestCaseForDeleteFence();
    void TestCaseForRenameFence();
    void TestCaseForAtomicStoreFence();
    void TestListDirEndsWithSlash();

private:
    void CreateFile(const std::string& dir, const std::string& fileName);

private:
    std::string _rootDir;
    std::string _existFile;
    std::string _notExistFile;
};
INDEXLIB_UNIT_TEST_CASE(FslibWrapperTest, TestCaseForOpen4Read);
INDEXLIB_UNIT_TEST_CASE(FslibWrapperTest, TestCaseForOpen4Write);
INDEXLIB_UNIT_TEST_CASE(FslibWrapperTest, TestCaseForOpen4Append);
INDEXLIB_UNIT_TEST_CASE(FslibWrapperTest, TestCaseForOpen4WriteExist);
INDEXLIB_UNIT_TEST_CASE(FslibWrapperTest, TestCaseForOpenFail);
INDEXLIB_UNIT_TEST_CASE(FslibWrapperTest, TestCaseForLoadAndStore);
INDEXLIB_UNIT_TEST_CASE(FslibWrapperTest, TestCaseForRemoveNotExistFile);
INDEXLIB_UNIT_TEST_CASE(FslibWrapperTest, TestCaseForIsExist);
INDEXLIB_UNIT_TEST_CASE(FslibWrapperTest, TestCaseForListDir);
INDEXLIB_UNIT_TEST_CASE(FslibWrapperTest, TestCaseForListDirRecursive);
INDEXLIB_UNIT_TEST_CASE(FslibWrapperTest, TestListDirEndsWithSlash);
INDEXLIB_UNIT_TEST_CASE(FslibWrapperTest, TestCaseForDeleteDir);
INDEXLIB_UNIT_TEST_CASE(FslibWrapperTest, TestCaseForDeleteDirFail);
INDEXLIB_UNIT_TEST_CASE(FslibWrapperTest, TestCaseForCopy);
INDEXLIB_UNIT_TEST_CASE(FslibWrapperTest, TestCaseForRename);
INDEXLIB_UNIT_TEST_CASE(FslibWrapperTest, TestCaseForMkDir);
INDEXLIB_UNIT_TEST_CASE(FslibWrapperTest, TestCaseForGetFileMeta);
INDEXLIB_UNIT_TEST_CASE(FslibWrapperTest, TestCaseForGetFileLength);
INDEXLIB_UNIT_TEST_CASE(FslibWrapperTest, TestCaseForIsDir);
INDEXLIB_UNIT_TEST_CASE(FslibWrapperTest, TestCaseForDeleteIfExist);
INDEXLIB_UNIT_TEST_CASE(FslibWrapperTest, TestCaseForNeedMkParentDirBeforeOpen);
INDEXLIB_UNIT_TEST_CASE(FslibWrapperTest, TestCaseForMkDirIfNotExist);
INDEXLIB_UNIT_TEST_CASE(FslibWrapperTest, TestCaseForSimpleMmapFile);
INDEXLIB_UNIT_TEST_CASE(FslibWrapperTest, TestCaseForLinkWithInvalidInput);
INDEXLIB_UNIT_TEST_CASE(FslibWrapperTest, TestCaseForLocalLink);
INDEXLIB_UNIT_TEST_CASE(FslibWrapperTest, TestCaseForSymLink);

INDEXLIB_UNIT_TEST_CASE(FslibWrapperTest, TestCreateFenceContext);
INDEXLIB_UNIT_TEST_CASE(FslibWrapperTest, TestCaseForDeleteFence);
INDEXLIB_UNIT_TEST_CASE(FslibWrapperTest, TestCaseForRenameFence);
INDEXLIB_UNIT_TEST_CASE(FslibWrapperTest, TestCaseForAtomicStoreFence);

void FslibWrapperTest::CaseSetUp()
{
    FslibWrapper::Reset();
    _rootDir = GET_TEMP_DATA_PATH() + "/root/";

    _existFile = _rootDir + "exist_file";
    fslib::fs::File* file = fslib::fs::FileSystem::openFile(_existFile, fslib::WRITE);
    delete file;
    _notExistFile = _rootDir + "not_exist_file";
}

void FslibWrapperTest::CaseTearDown() { FslibWrapper::Reset(); }

void FslibWrapperTest::TestCaseForOpen4Read()
{
    auto [ec, file] = FslibWrapper::OpenFile(_existFile, fslib::READ);
    ASSERT_EQ(FSEC_OK, ec);
    ASSERT_TRUE(file != NULL);
    FslibCommonFileWrapper* commonFileWrapper = dynamic_cast<FslibCommonFileWrapper*>(file.get());
    ASSERT_TRUE(commonFileWrapper != NULL);
    ASSERT_EQ(FSEC_OK, file->Close());
}

void FslibWrapperTest::TestCaseForOpen4Write()
{
    auto [ec, file] = FslibWrapper::OpenFile(_existFile, fslib::WRITE);
    ASSERT_EQ(FSEC_OK, ec);
    ASSERT_TRUE(file != NULL);
    FslibCommonFileWrapper* commonFileWrapper = dynamic_cast<FslibCommonFileWrapper*>(file.get());
    ASSERT_TRUE(commonFileWrapper != NULL);
    ASSERT_EQ(FSEC_OK, file->Close());
}

void FslibWrapperTest::TestCaseForOpen4Append()
{
    auto [ec, file] = FslibWrapper::OpenFile(_notExistFile, fslib::APPEND);
    ASSERT_EQ(FSEC_OK, ec);
    ASSERT_TRUE(file != NULL);
    FslibCommonFileWrapper* commonFileWrapper = dynamic_cast<FslibCommonFileWrapper*>(file.get());
    ASSERT_TRUE(commonFileWrapper != NULL);
    ASSERT_EQ(FSEC_OK, file->Close());
}

void FslibWrapperTest::TestCaseForOpen4WriteExist()
{
    auto [ec, file] = FslibWrapper::OpenFile(_existFile, fslib::WRITE);
    ASSERT_EQ(FSEC_OK, ec);
    ASSERT_TRUE(file != NULL);
    ASSERT_EQ(FSEC_OK, file->Close());
}

void FslibWrapperTest::TestCaseForOpenFail()
{
    auto [ec, file] = FslibWrapper::OpenFile(_notExistFile, fslib::READ);
    ASSERT_EQ(FSEC_NOENT, ec) << _notExistFile;
}

void FslibWrapperTest::TestCaseForLoadAndStore()
{
    string filePath = "./file_system_wrapper_atomic";
    string fileContent = "content test";

    bool ret = false;
    ASSERT_EQ(FSEC_OK, FslibWrapper::IsExist(filePath, ret));
    if (ret) {
        ASSERT_EQ(FSEC_OK, FslibWrapper::DeleteFile(filePath, DeleteOption::NoFence(false)).Code());
    }
    ASSERT_EQ(FSEC_OK, FslibWrapper::AtomicStore(filePath, fileContent).Code());

    string loadedFileCont;
    ASSERT_EQ(FSEC_OK, FslibWrapper::AtomicLoad(filePath, loadedFileCont).Code());
    ASSERT_EQ(fileContent, loadedFileCont);

    ASSERT_EQ(FSEC_OK, FslibWrapper::DeleteFile(filePath, DeleteOption::NoFence(false)).Code());
    ASSERT_EQ(FSEC_OK, FslibWrapper::IsExist(filePath, ret));
    ASSERT_TRUE(!ret);
    size_t len = 193000;
    char buff[len];
    for (size_t i = 0; i < len; i++) {
        buff[i] = (char)(i % 126 + 1);
    }

    ASSERT_EQ(FSEC_OK, FslibWrapper::AtomicStore(filePath, string(buff, len)).Code());
    ASSERT_EQ(FSEC_OK, FslibWrapper::AtomicLoad(filePath, loadedFileCont).Code());
    ASSERT_EQ(string(buff, len), loadedFileCont);

    ASSERT_EQ(FSEC_OK, FslibWrapper::DeleteFile(filePath, DeleteOption::NoFence(false)).Code());
    ASSERT_EQ(FSEC_OK, FslibWrapper::IsExist(filePath, ret));
    ASSERT_TRUE(!ret);
}

void FslibWrapperTest::TestCaseForRemoveNotExistFile()
{
    ASSERT_NE(FSEC_OK, FslibWrapper::DeleteFile(_notExistFile, DeleteOption::NoFence(false)).Code());
}

void FslibWrapperTest::TestCaseForIsExist()
{
    bool ret = false;
    ASSERT_EQ(FSEC_OK, FslibWrapper::IsExist(_existFile, ret));
    ASSERT_TRUE(ret) << _existFile;
    ASSERT_EQ(FSEC_OK, FslibWrapper::IsExist(_notExistFile, ret));
    ASSERT_FALSE(ret) << _notExistFile;
}

void FslibWrapperTest::TestCaseForListDir()
{
    fslib::FileList fileList;
    ASSERT_EQ(FSEC_OK, FslibWrapper::ListDir(_rootDir, fileList).Code());
    ASSERT_EQ((size_t)1, fileList.size());
    ASSERT_EQ(_existFile, _rootDir + fileList[0]);
}

void FslibWrapperTest::TestCaseForListDirRecursive()
{
    {
        string dir = _rootDir + "testListDirRecursive";
        ASSERT_EQ(FSEC_OK, FslibWrapper::MkDir(dir).Code());
        string subDir1 = util::PathUtil::JoinPath(dir, "subDir1");
        ASSERT_EQ(FSEC_OK, FslibWrapper::MkDir(subDir1).Code());
        string subDir2 = util::PathUtil::JoinPath(subDir1, "subDir2");
        ASSERT_EQ(FSEC_OK, FslibWrapper::MkDir(subDir2).Code());
        string subFile = util::PathUtil::JoinPath(subDir2, "subFile");
        ASSERT_EQ(FSEC_OK, FslibWrapper::AtomicStore(subFile, "").Code());
        string tempFile = util::PathUtil::JoinPath(dir, "file.__tmp__");
        ASSERT_EQ(FSEC_OK, FslibWrapper::AtomicStore(tempFile, "").Code());
        fslib::FileList fileList;
        ASSERT_EQ(FSEC_OK, FslibWrapper::ListDirRecursive(dir, fileList).Code());
        ASSERT_EQ((size_t)3, fileList.size());
        ASSERT_EQ(string("subDir1/"), fileList[0]);
        ASSERT_EQ(string("subDir1/subDir2/"), fileList[1]);
        ASSERT_EQ(string("subDir1/subDir2/subFile"), fileList[2]);
        ASSERT_EQ(FSEC_OK, FslibWrapper::DeleteDir(dir, DeleteOption::NoFence(false)).Code());
    }
    {
        fslib::FileList fileList;
        ASSERT_EQ(FSEC_NOENT, FslibWrapper::ListDirRecursive(_notExistFile, fileList).Code());
    }
    {
        string dir = _rootDir + "testListDirRecursive";
        ASSERT_EQ(FSEC_OK, FslibWrapper::MkDir(dir).Code());
        CreateFile(dir, "1.__tmp__");
        CreateFile(dir, "2.__tmp__.");
        CreateFile(dir, "3");
        CreateFile(dir, "__tmp__");
        CreateFile(dir, ".__tmp__");

        fslib::FileList fileList;
        ASSERT_EQ(FSEC_OK, FslibWrapper::ListDirRecursive(dir, fileList).Code());

        EXPECT_THAT(fileList, testing::UnorderedElementsAre(string("2.__tmp__."), string("__tmp__"), string("3")));
        ASSERT_EQ(FSEC_OK, FslibWrapper::DeleteDir(dir, DeleteOption::NoFence(false)).Code());
    }
}

void FslibWrapperTest::TestListDirEndsWithSlash()
{
    string dir = _rootDir + "testListDirRecursive";
    ASSERT_EQ(FSEC_OK, FslibWrapper::MkDir(dir).Code());
    string subDir1 = util::PathUtil::JoinPath(dir, "subDir1");
    ASSERT_EQ(FSEC_OK, FslibWrapper::MkDir(subDir1).Code());

    fslib::FileList fileList;
    ASSERT_EQ(FSEC_OK, FslibWrapper::ListDirRecursive(dir, fileList).Code());
    ASSERT_EQ('/', fileList[0].back());
}

void FslibWrapperTest::CreateFile(const string& dir, const string& fileName)
{
    string filePath = util::PathUtil::JoinPath(dir, fileName);
    ASSERT_EQ(FSEC_OK, FslibWrapper::AtomicStore(filePath, "").Code());
}

void FslibWrapperTest::TestCaseForDeleteDir()
{
    bool ret = false;
    string dir = _rootDir + "testdir";
    ASSERT_EQ(FSEC_OK, FslibWrapper::MkDir(dir).Code());
    ASSERT_EQ(FSEC_OK, FslibWrapper::DeleteDir(dir, DeleteOption::NoFence(false)).Code());
    ASSERT_EQ(FSEC_OK, FslibWrapper::IsExist(dir, ret));
    ASSERT_TRUE(!ret);
}

void FslibWrapperTest::TestCaseForDeleteDirFail()
{
    ASSERT_EQ(FSEC_NOENT, FslibWrapper::DeleteDir(_notExistFile + "/", DeleteOption::NoFence(false)).Code());
}

void FslibWrapperTest::TestCaseForCopy()
{
    ASSERT_EQ(FSEC_NOENT, FslibWrapper::Copy(_notExistFile, _existFile).Code());

    ASSERT_EQ(FSEC_ERROR, FslibWrapper::Copy(_existFile, _existFile).Code());
    ASSERT_EQ(FSEC_OK, FslibWrapper::Copy(_existFile, _notExistFile).Code());
}

void FslibWrapperTest::TestCaseForRename()
{
    ASSERT_EQ(FSEC_NOENT, FslibWrapper::Rename(_notExistFile, _existFile).Code());

    ASSERT_EQ(FSEC_OK, FslibWrapper::Rename(_existFile, _existFile).Code());

    ASSERT_EQ(FSEC_OK, FslibWrapper::Rename(_existFile, _notExistFile).Code());
}

void FslibWrapperTest::TestCaseForMkDir() { ASSERT_EQ(FSEC_EXIST, FslibWrapper::MkDir(_existFile).Code()); }

void FslibWrapperTest::TestCaseForGetFileMeta()
{
    fslib::FileMeta fileMeta;
    ASSERT_EQ(FSEC_NOENT, FslibWrapper::GetFileMeta(_notExistFile, fileMeta));

    fslib::FileMeta fileMeta2;
    ASSERT_EQ(FSEC_OK, FslibWrapper::GetFileMeta(_existFile, fileMeta2));
    ASSERT_TRUE(fileMeta2.fileLength == 0);
}

void FslibWrapperTest::TestCaseForGetFileLength()
{
    int64_t fileLength = -1;
    ASSERT_EQ(FSEC_NOENT, FslibWrapper::GetFileLength(_notExistFile, fileLength));

    ASSERT_EQ(FSEC_OK, FslibWrapper::GetFileLength(_existFile, fileLength));
    ASSERT_EQ(0L, fileLength);
}

void FslibWrapperTest::TestCaseForIsDir()
{
    ASSERT_TRUE(FslibWrapper::IsDir(_rootDir).GetOrThrow());
    ASSERT_FALSE(FslibWrapper::IsDir(_existFile).GetOrThrow());
    ASSERT_FALSE(FslibWrapper::IsDir(_notExistFile).GetOrThrow());
}

void FslibWrapperTest::TestCaseForDeleteIfExist()
{
    ASSERT_EQ(FSEC_OK, FslibWrapper::DeleteFile(_notExistFile, DeleteOption::NoFence(true)).Code());
    ASSERT_EQ(FSEC_OK, FslibWrapper::DeleteFile(_existFile, DeleteOption::NoFence(true)).Code());
    bool ret = true;
    ASSERT_EQ(FSEC_OK, FslibWrapper::IsExist(_existFile, ret));
    ASSERT_FALSE(ret);
}

void FslibWrapperTest::TestCaseForNeedMkParentDirBeforeOpen()
{
    EXPECT_FALSE(FslibWrapper::NeedMkParentDirBeforeOpen("pangu://abc"));
    EXPECT_FALSE(FslibWrapper::NeedMkParentDirBeforeOpen("hdfs://abc"));
    EXPECT_FALSE(FslibWrapper::NeedMkParentDirBeforeOpen("unknown://abc"));
    EXPECT_TRUE(FslibWrapper::NeedMkParentDirBeforeOpen("//abc"));
    EXPECT_TRUE(FslibWrapper::NeedMkParentDirBeforeOpen("./abc"));
}

void FslibWrapperTest::TestCaseForMkDirIfNotExist()
{
    string notExistDir = GET_TEMP_DATA_PATH() + "/not/exist/dir";
    ASSERT_EQ(FSEC_OK, FslibWrapper::MkDirIfNotExist(notExistDir).Code());
    bool ret = false;
    ASSERT_EQ(FSEC_OK, FslibWrapper::IsExist(notExistDir, ret));
    EXPECT_TRUE(ret);
    ASSERT_EQ(FSEC_OK, FslibWrapper::MkDirIfNotExist(notExistDir).Code());
    ASSERT_EQ(FSEC_OK, FslibWrapper::IsExist(notExistDir, ret));
    EXPECT_TRUE(ret);
    ASSERT_EQ(FSEC_OK, FslibWrapper::MkDirIfNotExist(_existFile).Code());
}

void FslibWrapperTest::TestCaseForSimpleMmapFile()
{
    string simpleFilePath = _rootDir + "simple_mmap_file";
    string content = "hello mmap file";
    ASSERT_EQ(FSEC_OK, FslibWrapper::AtomicStore(simpleFilePath, content).Code());
    auto [ec, mmapFile] =
        FslibWrapper::MmapFile(simpleFilePath, fslib::READ, NULL, content.size(), PROT_READ, MAP_PRIVATE, 0, -1);
    ASSERT_EQ(FSEC_OK, ec);
    ASSERT_TRUE(mmapFile);
    ASSERT_EQ((int64_t)content.size(), mmapFile->getLength());
    char* base = mmapFile->getBaseAddress();
    ASSERT_EQ(string(base, mmapFile->getLength()), content);
}

void FslibWrapperTest::TestCaseForLinkWithInvalidInput()
{
    {
        string srcFilePath = _rootDir + "file1";
        string dstFilePath = "pangu://xxx/file2";
        ASSERT_EQ(FSEC_NOTSUP, FslibWrapper::Link(srcFilePath, dstFilePath));
    }
    {
        string srcFilePath = "pangu://xxxfile1";
        string dstFilePath = _rootDir + "file2";
        ASSERT_EQ(FSEC_NOTSUP, FslibWrapper::Link(srcFilePath, dstFilePath));
    }
    {
        string srcFilePath = "pangu://xxxfile1";
        string dstFilePath = "dfs://xxxfile1";
        ASSERT_EQ(FSEC_NOTSUP, FslibWrapper::Link(srcFilePath, dstFilePath));
    }
    {
        string srcFilePath = "dfs://xxxfile1";
        string dstFilePath = "pangu://xxxfile1";
        ASSERT_EQ(FSEC_NOTSUP, FslibWrapper::Link(srcFilePath, dstFilePath));
    }
}

void FslibWrapperTest::TestCaseForLocalLink()
{
    string srcFilePath = _rootDir + "file1";
    string dstFilePath = _rootDir + "file2";
    ASSERT_EQ(FSEC_NOENT, FslibWrapper::Link(srcFilePath, dstFilePath));
    string content = "hello file";
    ASSERT_EQ(FSEC_OK, FslibWrapper::AtomicStore(srcFilePath, content).Code());
    ASSERT_EQ(FSEC_OK, FslibWrapper::Link(srcFilePath, dstFilePath));
    string tmp;
    ASSERT_EQ(FSEC_OK, FslibWrapper::AtomicLoad(dstFilePath, tmp).Code());
    ASSERT_EQ(tmp, content);

    ASSERT_EQ(FSEC_EXIST, FslibWrapper::Link(srcFilePath, _existFile));

    fslib::FileList fileList;
    ASSERT_EQ(FSEC_OK, FslibWrapper::ListDir(_rootDir, fileList).Code());
    ASSERT_EQ(3, fileList.size());

    ASSERT_EQ(FSEC_OK, FslibWrapper::DeleteFile(srcFilePath, DeleteOption::NoFence(false)).Code());
    bool ret = false;
    ASSERT_EQ(FSEC_OK, FslibWrapper::IsExist(dstFilePath, ret));
    string tmp2;
    ASSERT_EQ(FSEC_OK, FslibWrapper::AtomicLoad(dstFilePath, tmp2).Code());
    ASSERT_EQ(tmp2, content);
}

void FslibWrapperTest::TestCaseForSymLink()
{
    string symlinkPath = _rootDir + "/link_path";

    ASSERT_EQ(FSEC_NOTSUP, FslibWrapper::SymLink("hdfs://abc", symlinkPath));
    ASSERT_EQ(FSEC_OK, FslibWrapper::SymLink(_rootDir, symlinkPath));
    bool ret = false;
    ASSERT_EQ(FSEC_OK, FslibWrapper::IsExist(symlinkPath, ret));
    ASSERT_TRUE(ret);
    string filePath = _rootDir + "/hello_file";
    string symFilePath = symlinkPath + "/hello_file";
    string content = "hello file";
    ASSERT_EQ(FSEC_OK, FslibWrapper::AtomicStore(filePath, content).Code());

    string tmp;
    ASSERT_EQ(FSEC_OK, FslibWrapper::AtomicLoad(symFilePath, tmp).Code());
    ASSERT_EQ(tmp, content);

    // list link
    fslib::FileList fileList;
    ASSERT_EQ(FSEC_OK, FslibWrapper::ListDir(_rootDir, fileList).Code());
    ASSERT_EQ(3, fileList.size());

    // remove link
    ASSERT_EQ(FSEC_OK, FslibWrapper::DeleteFile(symlinkPath, DeleteOption::NoFence(false)).Code());
    ret = false;
    ASSERT_EQ(FSEC_OK, FslibWrapper::IsExist(symlinkPath, ret));
    ASSERT_FALSE(ret);
    ASSERT_EQ(FSEC_OK, FslibWrapper::IsExist(symFilePath, ret));
    ASSERT_FALSE(ret);
    ASSERT_EQ(FSEC_OK, FslibWrapper::IsExist(filePath, ret));
    ASSERT_TRUE(ret);
}

void FslibWrapperTest::TestCreateFenceContext()
{
    // autil::EnvGuard envGuard("INDEXLIB_TEST_MOCK_PANGU_INLINE_FILE", "");
    {
        std::shared_ptr<FenceContext> fenceContext(FslibWrapper::CreateFenceContext("pangu://abc", "123"));
        ASSERT_TRUE(fenceContext);
        ASSERT_TRUE(fenceContext->usePangu);
        ASSERT_EQ("123", fenceContext->epochId);
        ASSERT_EQ("pangu://abc", fenceContext->fenceHintPath);
    }

    // {
    //     std::shared_ptr<FenceContext> fenceContext = FslibWrapper::CreateFenceContext("hdfs://abc", "123");
    //     ASSERT_FALSE(fenceContext);
    //     ASSERT_FALSE(fenceContext->usePangu);
    // }
}

void FslibWrapperTest::TestCaseForDeleteFence()
{
    std::shared_ptr<FenceContext> fenceContext(FslibWrapper::CreateFenceContext(GET_TEMP_DATA_PATH(), "123"));
    ASSERT_TRUE(fenceContext);
    string filePath = GET_TEMP_DATA_PATH() + "/file";
    ASSERT_EQ(FSEC_OK, FslibWrapper::AtomicStore(filePath, string("hello"), true, fenceContext.get()).Code());

    std::shared_ptr<FenceContext> oldFenceContext(FslibWrapper::CreateFenceContext(GET_TEMP_DATA_PATH(), "110"));
    ASSERT_EQ(FSEC_ERROR, FslibWrapper::DeleteFile(filePath, DeleteOption::Fence(oldFenceContext.get(), false)).Code());
    ASSERT_EQ(FSEC_OK, FslibWrapper::DeleteFile(filePath, DeleteOption::Fence(fenceContext.get(), false)).Code());
}

void FslibWrapperTest::TestCaseForRenameFence()
{
    std::shared_ptr<FenceContext> fenceContext(FslibWrapper::CreateFenceContext(GET_TEMP_DATA_PATH(), "123"));
    ASSERT_TRUE(fenceContext);
    string filePath = GET_TEMP_DATA_PATH() + "/file";
    ASSERT_EQ(FSEC_OK,
              FslibWrapper::AtomicStore(filePath + ".__tmp__", string("hello"), true, fenceContext.get()).Code());

    std::shared_ptr<FenceContext> oldFenceContext(FslibWrapper::CreateFenceContext(GET_TEMP_DATA_PATH(), "110"));
    ASSERT_EQ(
        FSEC_ERROR,
        FslibWrapper::RenameWithFenceContext(filePath + ".__tmp__", filePath + ".dest", oldFenceContext.get()).Code());

    ASSERT_EQ(FSEC_OK,
              FslibWrapper::AtomicStore(filePath + ".__tmp__", string("hello"), true, fenceContext.get()).Code());
    ASSERT_EQ(
        FSEC_OK,
        FslibWrapper::RenameWithFenceContext(filePath + ".__tmp__", filePath + ".dest", fenceContext.get()).Code());

    // rename not-temp file
    ASSERT_EQ(FSEC_OK, FslibWrapper::AtomicStore(filePath, string("hello"), true, fenceContext.get()).Code());
    ASSERT_EQ(FSEC_ERROR,
              FslibWrapper::RenameWithFenceContext(filePath, filePath + ".dest", fenceContext.get()).Code());
}

void FslibWrapperTest::TestCaseForAtomicStoreFence()
{
    std::shared_ptr<FenceContext> fenceContext(FslibWrapper::CreateFenceContext(GET_TEMP_DATA_PATH(), "123"));
    ASSERT_TRUE(fenceContext);
    string filePath = GET_TEMP_DATA_PATH() + "/file";
    ASSERT_EQ(FSEC_OK, FslibWrapper::AtomicStore(filePath, string("hello"), true, fenceContext.get()).Code());

    // old worker fence, new worker can't rm
    std::shared_ptr<FenceContext> oldFenceContext(FslibWrapper::CreateFenceContext(GET_TEMP_DATA_PATH(), "110"));
    ASSERT_EQ(FSEC_ERROR, FslibWrapper::AtomicStore(filePath, string("hello"), true, oldFenceContext.get()).Code());
    ASSERT_EQ(FSEC_OK, FslibWrapper::AtomicStore(filePath, string("hello"), true, fenceContext.get()).Code());
}

}} // namespace indexlib::file_system
