#pragma once

#include "indexlib/common_define.h"
#include "indexlib/partition/test/online_partition_reopen_unittest.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace partition {

class OnlinePartitionReopenWithExceptionTest : public OnlinePartitionReopenTest
{
public:
    OnlinePartitionReopenWithExceptionTest();
    ~OnlinePartitionReopenWithExceptionTest();

    DECLARE_CLASS_NAME(OnlinePartitionReopenWithExceptionTest);

public:
    void CaseSetUp() override { OnlinePartitionReopenTest::CaseSetUp(); }
    void CaseTearDown() override { OnlinePartitionReopenTest::CaseSetUp(); }

public:
    void TestReopenWithException();
    void TestForceReopenWithException();
    void TestAsyncDumpReopenFail();
    void TestForceOpenWithException();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(OnlinePartitionReopenWithExceptionTest, TestReopenWithException);
INDEXLIB_UNIT_TEST_CASE(OnlinePartitionReopenWithExceptionTest, TestForceReopenWithException);
INDEXLIB_UNIT_TEST_CASE(OnlinePartitionReopenWithExceptionTest, TestAsyncDumpReopenFail);
INDEXLIB_UNIT_TEST_CASE(OnlinePartitionReopenWithExceptionTest, TestForceOpenWithException);
}} // namespace indexlib::partition
