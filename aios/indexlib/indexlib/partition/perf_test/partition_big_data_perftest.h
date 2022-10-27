#ifndef __INDEXLIB_PARTITIONBIGDATAPERFTEST_H
#define __INDEXLIB_PARTITIONBIGDATAPERFTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/test/partition_state_machine.h"

IE_NAMESPACE_BEGIN(partition);

class PartitionBigDataPerfTest : public INDEXLIB_TESTBASE
{
public:
    PartitionBigDataPerfTest();
    ~PartitionBigDataPerfTest();

    DECLARE_CLASS_NAME(PartitionBigDataPerfTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestSummaryInBuildingSegmentWithBigData();

private:
    void MakeData(size_t docSize, size_t docNum, std::string& docString);
    void CheckSummary(test::PartitionStateMachine& psm, size_t docSize, size_t docNum);
    std::string MakeSummaryStr(size_t docSize, size_t docId);

private:
    std::string mRootDir;
    config::IndexPartitionOptions mOptions;
    config::IndexPartitionSchemaPtr mSchema;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(PartitionBigDataPerfTest, TestSummaryInBuildingSegmentWithBigData);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_PARTITIONBIGDATAPERFTEST_H
