#ifndef __INDEXLIB_DIROPERATIONCACHETEST_H
#define __INDEXLIB_DIROPERATIONCACHETEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/file_system/dir_operation_cache.h"

IE_NAMESPACE_BEGIN(file_system);

class DirOperationCacheTest : public INDEXLIB_TESTBASE
{
public:
    DirOperationCacheTest();
    ~DirOperationCacheTest();

    DECLARE_CLASS_NAME(DirOperationCacheTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestGetParent();
    void TestNoNeedMkParentDir();
    void TestMakeDirWithFlushCache();
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(DirOperationCacheTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(DirOperationCacheTest, TestGetParent);
INDEXLIB_UNIT_TEST_CASE(DirOperationCacheTest, TestNoNeedMkParentDir);
INDEXLIB_UNIT_TEST_CASE(DirOperationCacheTest, TestMakeDirWithFlushCache);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_DIROPERATIONCACHETEST_H
