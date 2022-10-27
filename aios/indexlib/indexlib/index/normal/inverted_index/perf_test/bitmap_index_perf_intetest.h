#ifndef __INDEXLIB_BITMAPINDEXPERFTEST_H
#define __INDEXLIB_BITMAPINDEXPERFTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/multi_thread_test_base.h"

IE_NAMESPACE_BEGIN(index);

class BitmapIndexPerfTest : public test::MultiThreadTestBase
{
public:
    BitmapIndexPerfTest();
    ~BitmapIndexPerfTest();

    DECLARE_CLASS_NAME(BitmapIndexPerfTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestSimpleProcess();

private:
    void DoWrite() override;
    void DoRead(int* errCode) override;

private:
    test::PartitionStateMachinePtr mPsm;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(BitmapIndexPerfTest, TestSimpleProcess);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_BITMAPINDEXPERFTEST_H
