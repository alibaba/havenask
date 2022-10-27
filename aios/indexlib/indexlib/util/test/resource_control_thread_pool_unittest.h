#ifndef __INDEXLIB_RESOURCECONTROLTHREADPOOLTEST_H
#define __INDEXLIB_RESOURCECONTROLTHREADPOOLTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/resource_control_thread_pool.h"

IE_NAMESPACE_BEGIN(util);

class ResourceControlThreadPoolTest : public INDEXLIB_TESTBASE
{
public:
    ResourceControlThreadPoolTest();
    ~ResourceControlThreadPoolTest();

    DECLARE_CLASS_NAME(ResourceControlThreadPoolTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestResourceNotEnough();
    void TestInit();
    void TestThrowException();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(ResourceControlThreadPoolTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(ResourceControlThreadPoolTest, TestResourceNotEnough);
INDEXLIB_UNIT_TEST_CASE(ResourceControlThreadPoolTest, TestInit);
INDEXLIB_UNIT_TEST_CASE(ResourceControlThreadPoolTest, TestThrowException);

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_RESOURCECONTROLTHREADPOOLTEST_H
