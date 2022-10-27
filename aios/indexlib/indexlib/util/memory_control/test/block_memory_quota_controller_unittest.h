#ifndef __INDEXLIB_BLOCKMEMORYQUOTACONTROLLERTEST_H
#define __INDEXLIB_BLOCKMEMORYQUOTACONTROLLERTEST_H

#include "indexlib/common_define.h"
#include <autil/Lock.h>

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/memory_control/block_memory_quota_controller.h"
#include "indexlib/test/multi_thread_test_base.h"

IE_NAMESPACE_BEGIN(util);

class BlockMemoryQuotaControllerTest : public test::MultiThreadTestBase
{
public:
    BlockMemoryQuotaControllerTest();
    ~BlockMemoryQuotaControllerTest();

    DECLARE_CLASS_NAME(BlockMemoryQuotaControllerTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestAllocAndFree();
    void TestMultiThreadAllocAndFree();
    void TestReserve();
    void TestShrink();

private:
    void DoWrite() override;
    void DoRead(int* status) override;

    void FillAllocSizeVector(std::vector<int64_t>& vec, size_t size, int64_t total);

private:
    int64_t mTotalMemory;
    MemoryQuotaControllerPtr mMc;
    PartitionMemoryQuotaControllerPtr mPmc;
    BlockMemoryQuotaControllerPtr mBmc;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(BlockMemoryQuotaControllerTest, TestAllocAndFree);
INDEXLIB_UNIT_TEST_CASE(BlockMemoryQuotaControllerTest, TestMultiThreadAllocAndFree);
INDEXLIB_UNIT_TEST_CASE(BlockMemoryQuotaControllerTest, TestReserve);
INDEXLIB_UNIT_TEST_CASE(BlockMemoryQuotaControllerTest, TestShrink);

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_BLOCKMEMORYQUOTACONTROLLERTEST_H
