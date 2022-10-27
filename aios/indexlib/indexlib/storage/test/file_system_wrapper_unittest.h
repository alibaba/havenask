#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

IE_NAMESPACE_BEGIN(storage);

class FileSystemWrapperTest : public INDEXLIB_TESTBASE
{
public:
    FileSystemWrapperTest() {}
    ~FileSystemWrapperTest() {}
    DECLARE_CLASS_NAME(FileSystemWrapperTest);

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
    void TestCaseForSymLink();

private:
    void CreateFile(const std::string& dir, const std::string& fileName);

private:
    std::string mRootDir;
    std::string mExistFile;
    std::string mNotExistFile;
};

INDEXLIB_UNIT_TEST_CASE(FileSystemWrapperTest, TestCaseForOpen4Read);
INDEXLIB_UNIT_TEST_CASE(FileSystemWrapperTest, TestCaseForOpen4Write);
INDEXLIB_UNIT_TEST_CASE(FileSystemWrapperTest, TestCaseForOpen4Append);
INDEXLIB_UNIT_TEST_CASE(FileSystemWrapperTest, TestCaseForOpen4WriteExist);
INDEXLIB_UNIT_TEST_CASE(FileSystemWrapperTest, TestCaseForOpenFail);
INDEXLIB_UNIT_TEST_CASE(FileSystemWrapperTest, TestCaseForLoadAndStore);
INDEXLIB_UNIT_TEST_CASE(FileSystemWrapperTest, TestCaseForRemoveNotExistFile);
INDEXLIB_UNIT_TEST_CASE(FileSystemWrapperTest, TestCaseForIsExist);
INDEXLIB_UNIT_TEST_CASE(FileSystemWrapperTest, TestCaseForListDir);
INDEXLIB_UNIT_TEST_CASE(FileSystemWrapperTest, TestCaseForListDirRecursive);
INDEXLIB_UNIT_TEST_CASE(FileSystemWrapperTest, TestCaseForDeleteDir);
INDEXLIB_UNIT_TEST_CASE(FileSystemWrapperTest, TestCaseForDeleteDirFail);
INDEXLIB_UNIT_TEST_CASE(FileSystemWrapperTest, TestCaseForCopy);
INDEXLIB_UNIT_TEST_CASE(FileSystemWrapperTest, TestCaseForRename);
INDEXLIB_UNIT_TEST_CASE(FileSystemWrapperTest, TestCaseForMkDir);
INDEXLIB_UNIT_TEST_CASE(FileSystemWrapperTest, TestCaseForGetFileMeta);
INDEXLIB_UNIT_TEST_CASE(FileSystemWrapperTest, TestCaseForGetFileLength);
INDEXLIB_UNIT_TEST_CASE(FileSystemWrapperTest, TestCaseForIsDir);
INDEXLIB_UNIT_TEST_CASE(FileSystemWrapperTest, TestCaseForDeleteIfExist);
INDEXLIB_UNIT_TEST_CASE(FileSystemWrapperTest, TestCaseForNeedMkParentDirBeforeOpen);
INDEXLIB_UNIT_TEST_CASE(FileSystemWrapperTest, TestCaseForMkDirIfNotExist);
INDEXLIB_UNIT_TEST_CASE(FileSystemWrapperTest, TestCaseForSimpleMmapFile);
INDEXLIB_UNIT_TEST_CASE(FileSystemWrapperTest, TestCaseForSymLink);

IE_NAMESPACE_END(storage);
