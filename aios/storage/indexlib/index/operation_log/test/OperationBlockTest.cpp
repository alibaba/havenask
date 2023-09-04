#include "indexlib/index/operation_log/OperationBlock.h"

#include "indexlib/index/operation_log/test/MockOperation.h"
#include "indexlib/util/testutil/unittest.h"

namespace indexlib::index {

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
    AUTIL_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(OperationBlockTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(OperationBlockTest, TestGetTotalMemoryUse);
AUTIL_LOG_SETUP(indexlib.index, OperationBlockTest);

OperationBlockTest::OperationBlockTest() {}

OperationBlockTest::~OperationBlockTest() {}

void OperationBlockTest::CaseSetUp() {}

void OperationBlockTest::CaseTearDown() {}

void OperationBlockTest::TestSimpleProcess()
{
    OperationBlock opBlock(10);
    ASSERT_TRUE(opBlock.GetPool());

    opBlock.AddOperation(MockOperationMaker::MakeOperation(10, opBlock.GetPool()));
    ASSERT_EQ((int64_t)10, opBlock.GetMinOffset().first);
    ASSERT_EQ((int64_t)10, opBlock.GetMaxOffset().first);
    ASSERT_EQ((size_t)1, opBlock.Size());

    opBlock.AddOperation(MockOperationMaker::MakeOperation(9, opBlock.GetPool()));
    ASSERT_EQ((int64_t)9, opBlock.GetMinOffset().first);
    ASSERT_EQ((int64_t)10, opBlock.GetMaxOffset().first);
    ASSERT_EQ((size_t)2, opBlock.Size());

    opBlock.AddOperation(MockOperationMaker::MakeOperation(20, opBlock.GetPool()));
    ASSERT_EQ((int64_t)9, opBlock.GetMinOffset().first);
    ASSERT_EQ((int64_t)20, opBlock.GetMaxOffset().first);
    ASSERT_EQ((size_t)3, opBlock.Size());
}

void OperationBlockTest::TestGetTotalMemoryUse()
{
    OperationBlock opBlock(10);
    opBlock.AddOperation(MockOperationMaker::MakeOperation(10, opBlock.GetPool()));
    opBlock.AddOperation(MockOperationMaker::MakeOperation(10, opBlock.GetPool()));
    // 16: two opertions pointer size
    ASSERT_EQ((size_t)16 + opBlock.GetPool()->getUsedBytes(), opBlock.GetTotalMemoryUse());
}

} // namespace indexlib::index
