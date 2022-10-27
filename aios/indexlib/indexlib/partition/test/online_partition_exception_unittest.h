#ifndef __INDEXLIB_ONLINEPARTITIONEXCEPTIONTEST_H
#define __INDEXLIB_ONLINEPARTITIONEXCEPTIONTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/partition/test/fake_partition.h"

IE_NAMESPACE_BEGIN(partition);

class OnlinePartitionExceptionTest : public INDEXLIB_TESTBASE
{
public:
    OnlinePartitionExceptionTest();
    ~OnlinePartitionExceptionTest();

    DECLARE_CLASS_NAME(OnlinePartitionExceptionTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestOpenException();
    void TestReopenException();
    void TestReopenFail();
    void TestSummaryReader();
    void TestExceptionInRTBuild();

private:
    void InnerTestReopenFail(int32_t executorFalseIdx,
                             int32_t executorExceptionIdx,
                             int32_t dropExceptionIdx, 
                             IndexPartition::OpenStatus expectOpenStatus);
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(OnlinePartitionExceptionTest, TestOpenException);
INDEXLIB_UNIT_TEST_CASE(OnlinePartitionExceptionTest, TestReopenException);
INDEXLIB_UNIT_TEST_CASE(OnlinePartitionExceptionTest, TestSummaryReader);
INDEXLIB_UNIT_TEST_CASE(OnlinePartitionExceptionTest, TestExceptionInRTBuild);
INDEXLIB_UNIT_TEST_CASE(OnlinePartitionExceptionTest, TestReopenFail);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_ONLINEPARTITIONEXCEPTIONTEST_H
