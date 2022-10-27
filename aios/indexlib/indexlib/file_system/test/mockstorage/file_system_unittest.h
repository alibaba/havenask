#ifndef __INDEXLIB_FILESYSTEMMOCKSTORAGETEST_H
#define __INDEXLIB_FILESYSTEMMOCKSTORAGETEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

IE_NAMESPACE_BEGIN(file_system);

class FileSystemTest : public INDEXLIB_TESTBASE
{
public:
    FileSystemTest();
    ~FileSystemTest();

    DECLARE_CLASS_NAME(FileSystemTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestAtomicStoreForLocalStorage();
    void TestAtomicStoreForInMemStorage();

private:
    std::string mRootDir;
    IndexlibFileSystemPtr mFileSystem;
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(FileSystemTest, TestAtomicStoreForLocalStorage);
INDEXLIB_UNIT_TEST_CASE(FileSystemTest, TestAtomicStoreForInMemStorage);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_FILESYSTEMMOCKSTORAGETEST_H
