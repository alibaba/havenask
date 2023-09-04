#ifndef __INDEXLIB_PARTITIONSTATEMACHINETEST_H
#define __INDEXLIB_PARTITIONSTATEMACHINETEST_H

#include "indexlib/common_define.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace test {

class PartitionStateMachineTest : public INDEXLIB_TESTBASE_WITH_PARAM<int>
{
public:
    PartitionStateMachineTest();
    ~PartitionStateMachineTest();

    DECLARE_CLASS_NAME(PartitionStateMachineTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestBuildWithBinaryString();
    void TestInvertedIndex();
    void TestDocIdQuery();
    void TestCustomizedDocumentFactory();

private:
    IE_LOG_DECLARE();
};

INSTANTIATE_TEST_CASE_P(BuildMode, PartitionStateMachineTest, testing::Values(0, 1, 2));

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(PartitionStateMachineTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(PartitionStateMachineTest, TestInvertedIndex);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(PartitionStateMachineTest, TestDocIdQuery);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(PartitionStateMachineTest, TestBuildWithBinaryString);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(PartitionStateMachineTest, TestCustomizedDocumentFactory);
}} // namespace indexlib::test

#endif //__INDEXLIB_PARTITIONSTATEMACHINETEST_H
