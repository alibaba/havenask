#ifndef __INDEXLIB_OFFLINEPARTITIONWRITERTEST_H
#define __INDEXLIB_OFFLINEPARTITIONWRITERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/partition/offline_partition_writer.h"
#include "indexlib/partition/segment/single_segment_writer.h"
#include "indexlib/index_base/partition_data.h"

IE_NAMESPACE_BEGIN(partition);

class OfflinePartitionWriterTest : public INDEXLIB_TESTBASE
{
public:
    OfflinePartitionWriterTest();
    ~OfflinePartitionWriterTest();

    DECLARE_CLASS_NAME(OfflinePartitionWriterTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestSimpleProcess();
    void TestOpen();
    void TestNeedDump();
    void TestGetFileSystemMemoryUse();
    void TestEmptyData();
    void TestDumpSegment();
    void TestGetLastFlushedLocator();
    void TestCleanResourceBeforeDumpSegment();
    void TestDumpSegmentWithOnlyDeleteDoc();
    void TestDumpSegmentWithOnlyUpdateDoc();

    void TestAddDocument();
    void TestBuildWithOperationWriter();

    void TestUpdateDocumentInBuildingSegment();
    void TestUpdateDocumentInBuiltSegment();
    void TestUpdateDocumentNotExist();
    void TestUpdateDocumentNoPkSchema();

    void TestDeleteDocumentInBuildingSegment();
    void TestDeleteDocumentInBuiltSegment();
    void TestDeleteDocumentNotExist();
    void TestDeleteDocumentNoPkSchema();

private:
    void InnerTestOpen(const config::IndexPartitionSchemaPtr& Schema,
                       const config::IndexPartitionOptions& options);

    file_system::FileReaderPtr CreateDataFileReader(
            const file_system::DirectoryPtr& directory, int64_t dataSize);

    void OpenWriter(OfflinePartitionWriter& writer,
                    const config::IndexPartitionSchemaPtr& schema,
                    const index_base::PartitionDataPtr& partitionData);
    void ReOpenNewSegment(OfflinePartitionWriter& writer,
                          const config::IndexPartitionSchemaPtr& schema,
                          const index_base::PartitionDataPtr& partitionData);
private:
    std::string mRootDir;
    config::IndexPartitionSchemaPtr mSchema;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(OfflinePartitionWriterTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(OfflinePartitionWriterTest, TestOpen);
INDEXLIB_UNIT_TEST_CASE(OfflinePartitionWriterTest, TestNeedDump);
INDEXLIB_UNIT_TEST_CASE(OfflinePartitionWriterTest, TestGetFileSystemMemoryUse);
INDEXLIB_UNIT_TEST_CASE(OfflinePartitionWriterTest, TestEmptyData);
INDEXLIB_UNIT_TEST_CASE(OfflinePartitionWriterTest, TestDumpSegment);
INDEXLIB_UNIT_TEST_CASE(OfflinePartitionWriterTest, TestGetLastFlushedLocator);
INDEXLIB_UNIT_TEST_CASE(OfflinePartitionWriterTest, TestCleanResourceBeforeDumpSegment);
INDEXLIB_UNIT_TEST_CASE(OfflinePartitionWriterTest, TestDumpSegmentWithOnlyDeleteDoc);
INDEXLIB_UNIT_TEST_CASE(OfflinePartitionWriterTest, TestDumpSegmentWithOnlyUpdateDoc);
INDEXLIB_UNIT_TEST_CASE(OfflinePartitionWriterTest, TestAddDocument);
INDEXLIB_UNIT_TEST_CASE(OfflinePartitionWriterTest, TestBuildWithOperationWriter);

INDEXLIB_UNIT_TEST_CASE(OfflinePartitionWriterTest, TestUpdateDocumentInBuildingSegment);
INDEXLIB_UNIT_TEST_CASE(OfflinePartitionWriterTest, TestUpdateDocumentInBuiltSegment);
INDEXLIB_UNIT_TEST_CASE(OfflinePartitionWriterTest, TestUpdateDocumentNotExist);
INDEXLIB_UNIT_TEST_CASE(OfflinePartitionWriterTest, TestUpdateDocumentNoPkSchema);

INDEXLIB_UNIT_TEST_CASE(OfflinePartitionWriterTest, TestDeleteDocumentInBuildingSegment);
INDEXLIB_UNIT_TEST_CASE(OfflinePartitionWriterTest, TestDeleteDocumentInBuiltSegment);
INDEXLIB_UNIT_TEST_CASE(OfflinePartitionWriterTest, TestDeleteDocumentNotExist);
INDEXLIB_UNIT_TEST_CASE(OfflinePartitionWriterTest, TestDeleteDocumentNoPkSchema);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_OFFLINEPARTITIONWRITERTEST_H
