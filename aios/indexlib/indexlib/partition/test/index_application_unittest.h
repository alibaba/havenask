#ifndef __INDEXLIB_INDEXAPPLICATIONTEST_H
#define __INDEXLIB_INDEXAPPLICATIONTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/partition/index_application.h"
#include "indexlib/index/test/partition_schema_maker.h"
#include "indexlib/partition/test/fake_index_partition_reader_base.h"


IE_NAMESPACE_BEGIN(partition);

class IndexApplicationTest : public INDEXLIB_TESTBASE
{
public:
    IndexApplicationTest();
    ~IndexApplicationTest();

    DECLARE_CLASS_NAME(IndexApplicationTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void AddSub();
    void TestAddIndexPartitions();
    void TestAddIndexPartitionsWithSubDoc();
    void TestParseJoinRelations();
    void TestCreateSnapshot();
    void TestIsLatestSnapshotReader();
    void TestCreateSnapshotForCustomOnlinePartition();

private:
    config::IndexPartitionSchemaPtr schemaA;
    config::IndexPartitionSchemaPtr schemaB;
    config::IndexPartitionSchemaPtr schemaC;
    config::IndexPartitionSchemaPtr schemaD;
    config::IndexPartitionSchemaPtr schemaE;
    IndexPartitionReaderPtr readerA;
    IndexPartitionReaderPtr readerB;
    IndexPartitionReaderPtr readerC;
    IndexPartitionReaderPtr readerD;
    IndexPartitionReaderPtr readerE;
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(IndexApplicationTest, TestAddIndexPartitions);
INDEXLIB_UNIT_TEST_CASE(IndexApplicationTest, TestAddIndexPartitionsWithSubDoc);
INDEXLIB_UNIT_TEST_CASE(IndexApplicationTest, TestParseJoinRelations);
INDEXLIB_UNIT_TEST_CASE(IndexApplicationTest, TestCreateSnapshot);
INDEXLIB_UNIT_TEST_CASE(IndexApplicationTest, TestIsLatestSnapshotReader);
INDEXLIB_UNIT_TEST_CASE(IndexApplicationTest, TestCreateSnapshotForCustomOnlinePartition);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_INDEXAPPLICATIONTEST_H
