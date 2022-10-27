#ifndef __INDEXLIB_LIFECYCLETABLETEST_H
#define __INDEXLIB_LIFECYCLETABLETEST_H

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/file_system/lifecycle_table.h"

IE_NAMESPACE_BEGIN(file_system);

class LifecycleTableTest : public INDEXLIB_TESTBASE
{
public:
    LifecycleTableTest();
    ~LifecycleTableTest();

    DECLARE_CLASS_NAME(LifecycleTableTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(LifecycleTableTest, TestSimpleProcess);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_LIFECYCLETABLETEST_H
