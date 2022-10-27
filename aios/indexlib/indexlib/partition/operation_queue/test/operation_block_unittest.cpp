#include "indexlib/partition/operation_queue/test/operation_block_unittest.h"
#include "indexlib/partition/operation_queue/test/mock_operation.h"

using namespace std;

IE_NAMESPACE_BEGIN(partition);
IE_LOG_SETUP(partition, OperationBlockTest);

OperationBlockTest::OperationBlockTest()
{
}

OperationBlockTest::~OperationBlockTest()
{
}

void OperationBlockTest::CaseSetUp()
{
}

void OperationBlockTest::CaseTearDown()
{
}

void OperationBlockTest::TestSimpleProcess()
{
    OperationBlock opBlock(10);
    ASSERT_TRUE(opBlock.GetPool());

    opBlock.AddOperation(MockOperationMaker::MakeOperation(
                    10, opBlock.GetPool()));
    ASSERT_EQ((int64_t)10, opBlock.GetMinTimestamp());
    ASSERT_EQ((int64_t)10, opBlock.GetMaxTimestamp());
    ASSERT_EQ((size_t)1, opBlock.Size());

    opBlock.AddOperation(MockOperationMaker::MakeOperation(
                    9, opBlock.GetPool()));
    ASSERT_EQ((int64_t)9, opBlock.GetMinTimestamp());
    ASSERT_EQ((int64_t)10, opBlock.GetMaxTimestamp());
    ASSERT_EQ((size_t)2, opBlock.Size());

    opBlock.AddOperation(MockOperationMaker::MakeOperation(
                    20, opBlock.GetPool()));
    ASSERT_EQ((int64_t)9, opBlock.GetMinTimestamp());
    ASSERT_EQ((int64_t)20, opBlock.GetMaxTimestamp());
    ASSERT_EQ((size_t)3, opBlock.Size());
}

void OperationBlockTest::TestGetTotalMemoryUse()
{
    OperationBlock opBlock(10);
    opBlock.AddOperation(MockOperationMaker::MakeOperation(
                    10, opBlock.GetPool()));
    opBlock.AddOperation(MockOperationMaker::MakeOperation(
                    10, opBlock.GetPool()));
    //16: two opertions pointer size
    ASSERT_EQ((size_t)16 + opBlock.GetPool()->getUsedBytes(),
              opBlock.GetTotalMemoryUse());
}

IE_NAMESPACE_END(partition);

