#ifndef __INDEXLIB_PACKAGEMERGEFILESYSTEMTEST_H
#define __INDEXLIB_PACKAGEMERGEFILESYSTEMTEST_H

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/merger/package_merge_file_system.h"

IE_NAMESPACE_BEGIN(merger);

class PackageMergeFileSystemTest : public INDEXLIB_TESTBASE
{
public:
    PackageMergeFileSystemTest();
    ~PackageMergeFileSystemTest();

    DECLARE_CLASS_NAME(PackageMergeFileSystemTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestCheckpointManager();
    void TestTagFunction();
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(PackageMergeFileSystemTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(PackageMergeFileSystemTest, TestCheckpointManager);
INDEXLIB_UNIT_TEST_CASE(PackageMergeFileSystemTest, TestTagFunction);

IE_NAMESPACE_END(merger);

#endif //__INDEXLIB_PACKAGEMERGEFILESYSTEMTEST_H
