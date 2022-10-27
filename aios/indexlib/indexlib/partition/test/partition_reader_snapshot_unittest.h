#ifndef __INDEXLIB_PARTITIONREADERSNAPSHOTTEST_H
#define __INDEXLIB_PARTITIONREADERSNAPSHOTTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/partition/partition_reader_snapshot.h"

IE_NAMESPACE_BEGIN(partition);

class PartitionReaderSnapshotTest : public INDEXLIB_TESTBASE
{
public:
    PartitionReaderSnapshotTest();
    ~PartitionReaderSnapshotTest();

    DECLARE_CLASS_NAME(PartitionReaderSnapshotTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestQueryAuxTableByHashIndexTable();
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(PartitionReaderSnapshotTest, TestQueryAuxTableByHashIndexTable);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_PARTITIONREADERSNAPSHOTTEST_H
