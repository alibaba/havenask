#pragma once

#include "indexlib/common_define.h"
#include "indexlib/partition/operation_queue/sub_doc_operation.h"
#include "indexlib/partition/operation_queue/test/mock_operation.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace partition {

class SubDocOperationTest : public INDEXLIB_TESTBASE
{
public:
    SubDocOperationTest();
    ~SubDocOperationTest();

    DECLARE_CLASS_NAME(SubDocOperationTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestGetMemoryUse();
    void TestClone();
    void TestSerialize();

private:
    MockOperation* MakeMockOperation(int64_t ts, autil::mem_pool::Pool* pool);

private:
    autil::mem_pool::Pool mPool;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(SubDocOperationTest, TestGetMemoryUse);
INDEXLIB_UNIT_TEST_CASE(SubDocOperationTest, TestClone);
INDEXLIB_UNIT_TEST_CASE(SubDocOperationTest, TestSerialize);
}} // namespace indexlib::partition
