#ifndef __INDEXLIB_PARTITIONREADERSNAPSHOTTEST_H
#define __INDEXLIB_PARTITIONREADERSNAPSHOTTEST_H

#include "indexlib/common_define.h"
#include "indexlib/partition/partition_reader_snapshot.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace partition {

class PartitionReaderSnapshotTest : public INDEXLIB_TESTBASE
{
public:
    PartitionReaderSnapshotTest();
    ~PartitionReaderSnapshotTest();

    DECLARE_CLASS_NAME(PartitionReaderSnapshotTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestCreateAuxTableFromSingleValueKVTable();
    void TestCreateAuxTableFromMultiValueKVTable();
    void TestQueryAuxTableByHashKVTable();
    void TestQueryAuxTableByHashIndexTable();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(PartitionReaderSnapshotTest, TestCreateAuxTableFromSingleValueKVTable);
INDEXLIB_UNIT_TEST_CASE(PartitionReaderSnapshotTest, TestCreateAuxTableFromMultiValueKVTable);
INDEXLIB_UNIT_TEST_CASE(PartitionReaderSnapshotTest, TestQueryAuxTableByHashKVTable);
INDEXLIB_UNIT_TEST_CASE(PartitionReaderSnapshotTest, TestQueryAuxTableByHashIndexTable);
}} // namespace indexlib::partition

#endif //__INDEXLIB_PARTITIONREADERSNAPSHOTTEST_H
