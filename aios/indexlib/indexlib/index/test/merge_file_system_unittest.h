#ifndef __INDEXLIB_MERGEFILESYSTEMTEST_H
#define __INDEXLIB_MERGEFILESYSTEMTEST_H

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/merge_file_system.h"

IE_NAMESPACE_BEGIN(index);

class MergeFileSystemTest : public INDEXLIB_TESTBASE
{
public:
    MergeFileSystemTest();
    ~MergeFileSystemTest();

    DECLARE_CLASS_NAME(MergeFileSystemTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(MergeFileSystemTest, TestSimpleProcess);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_MERGEFILESYSTEMTEST_H
