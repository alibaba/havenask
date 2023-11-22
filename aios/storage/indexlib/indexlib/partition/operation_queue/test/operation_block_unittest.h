#pragma once

#include "indexlib/common_define.h"
#include "indexlib/partition/operation_queue/operation_block.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace partition {

class OperationBlockTest : public INDEXLIB_TESTBASE
{
public:
    OperationBlockTest();
    ~OperationBlockTest();

    DECLARE_CLASS_NAME(OperationBlockTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestGetTotalMemoryUse();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(OperationBlockTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(OperationBlockTest, TestGetTotalMemoryUse);
}} // namespace indexlib::partition
