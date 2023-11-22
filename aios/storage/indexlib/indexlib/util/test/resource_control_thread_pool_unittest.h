#pragma once

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/resource_control_thread_pool.h"

namespace indexlib { namespace util {

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
}} // namespace indexlib::util
