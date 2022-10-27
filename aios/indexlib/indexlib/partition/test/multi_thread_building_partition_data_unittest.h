#ifndef __INDEXLIB_MULTITHREADBUILDINGPARTITIONDATATEST_H
#define __INDEXLIB_MULTITHREADBUILDINGPARTITIONDATATEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/test/multi_thread_test_base.h"
#include "indexlib/partition/building_partition_data.h"

IE_NAMESPACE_BEGIN(partition);

class MultiThreadBuildingPartitionDataTest : public test::MultiThreadTestBase
{
public:
    MultiThreadBuildingPartitionDataTest();
    ~MultiThreadBuildingPartitionDataTest();

    DECLARE_CLASS_NAME(MultiThreadBuildingPartitionDataTest);
    
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

protected:
    void DoWrite() override;
    void DoRead(int* status) override;

private:
    BuildingPartitionDataPtr mPartitionData;
    
private:
    IE_LOG_DECLARE();
};

//TODO:
//INDEXLIB_UNIT_TEST_CASE(MultiThreadBuildingPartitionDataTest, TestSimpleProcess);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_MULTITHREADBUILDINGPARTITIONDATATEST_H
