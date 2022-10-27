#ifndef __INDEXLIB_PARTITIONSTATEMACHINETEST_H
#define __INDEXLIB_PARTITIONSTATEMACHINETEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/test/partition_state_machine.h"

IE_NAMESPACE_BEGIN(test);

class PartitionStateMachineTest : public INDEXLIB_TESTBASE
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

INDEXLIB_UNIT_TEST_CASE(PartitionStateMachineTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(PartitionStateMachineTest, TestInvertedIndex);
INDEXLIB_UNIT_TEST_CASE(PartitionStateMachineTest, TestDocIdQuery);
INDEXLIB_UNIT_TEST_CASE(PartitionStateMachineTest, TestBuildWithBinaryString);
INDEXLIB_UNIT_TEST_CASE(PartitionStateMachineTest, TestCustomizedDocumentFactory);

IE_NAMESPACE_END(test);

#endif //__INDEXLIB_PARTITIONSTATEMACHINETEST_H
