#ifndef __INDEXLIB_READERSNAPSHOTTEST_H
#define __INDEXLIB_READERSNAPSHOTTEST_H

#include "indexlib/common_define.h"
#include "indexlib/partition/partition_reader_snapshot.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace partition {

class ReaderSnapshotTest : public INDEXLIB_TESTBASE
{
public:
    ReaderSnapshotTest();
    ~ReaderSnapshotTest();

    DECLARE_CLASS_NAME(ReaderSnapshotTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;

public:
    void TestSimpleProcess();
    void TestLeadingTable();
    void TestLeadingTableWithJoinTable();

private:
    void AddSub();

private:
    config::IndexPartitionSchemaPtr schemaA;
    config::IndexPartitionSchemaPtr schemaB;
    IndexPartitionReaderPtr readerA;
    IndexPartitionReaderPtr readerB;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(ReaderSnapshotTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(ReaderSnapshotTest, TestLeadingTable);
INDEXLIB_UNIT_TEST_CASE(ReaderSnapshotTest, TestLeadingTableWithJoinTable);
}} // namespace indexlib::partition

#endif //__INDEXLIB_READERSNAPSHOTTEST_H
