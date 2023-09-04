#ifndef __INDEXLIB_OFFLINEPARTITIONWRITERTEST_H
#define __INDEXLIB_OFFLINEPARTITIONWRITERTEST_H

#include "indexlib/common_define.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/partition/offline_partition_writer.h"
#include "indexlib/partition/segment/single_segment_writer.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace partition {

class OfflinePartitionWriterTest : public INDEXLIB_TESTBASE_WITH_PARAM<int>
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
    void TestFailOverWithTemperature();

    void TestDeleteDocumentInBuildingSegment();
    void TestDeleteDocumentInBuiltSegment();
    void TestDeleteDocumentNotExist();
    void TestDeleteDocumentNoPkSchema();

    void TestKVTable();

private:
    void InnerTestOpen(const config::IndexPartitionSchemaPtr& Schema, const config::IndexPartitionOptions& options);

    file_system::FileReaderPtr CreateDataFileReader(const file_system::DirectoryPtr& directory, int64_t dataSize);

    void OpenWriter(OfflinePartitionWriter& writer, const config::IndexPartitionSchemaPtr& schema,
                    const index_base::PartitionDataPtr& partitionData);
    void ReOpenNewSegment(OfflinePartitionWriter& writer, const config::IndexPartitionSchemaPtr& schema,
                          const index_base::PartitionDataPtr& partitionData);
    std::vector<document::DocumentPtr> CreateDocuments(std::vector<std::tuple<DocOperateType, std::string, int>> input);
    bool TestEqual(const std::vector<document::DocumentPtr>& docs,
                   const std::vector<document::DocumentPtr>& expectedDocs);

private:
    std::string mRootDir;
    config::IndexPartitionSchemaPtr mSchema;

private:
    IE_LOG_DECLARE();
};

INSTANTIATE_TEST_CASE_P(BuildMode, OfflinePartitionWriterTest, testing::Values(0, 1, 2));

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OfflinePartitionWriterTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OfflinePartitionWriterTest, TestOpen);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OfflinePartitionWriterTest, TestNeedDump);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OfflinePartitionWriterTest, TestGetFileSystemMemoryUse);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OfflinePartitionWriterTest, TestEmptyData);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OfflinePartitionWriterTest, TestDumpSegment);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OfflinePartitionWriterTest, TestGetLastFlushedLocator);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OfflinePartitionWriterTest, TestCleanResourceBeforeDumpSegment);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OfflinePartitionWriterTest, TestDumpSegmentWithOnlyDeleteDoc);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OfflinePartitionWriterTest, TestDumpSegmentWithOnlyUpdateDoc);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OfflinePartitionWriterTest, TestAddDocument);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OfflinePartitionWriterTest, TestBuildWithOperationWriter);

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OfflinePartitionWriterTest, TestUpdateDocumentInBuildingSegment);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OfflinePartitionWriterTest, TestUpdateDocumentInBuiltSegment);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OfflinePartitionWriterTest, TestUpdateDocumentNotExist);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OfflinePartitionWriterTest, TestUpdateDocumentNoPkSchema);

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OfflinePartitionWriterTest, TestDeleteDocumentInBuildingSegment);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OfflinePartitionWriterTest, TestDeleteDocumentInBuiltSegment);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OfflinePartitionWriterTest, TestDeleteDocumentNotExist);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OfflinePartitionWriterTest, TestDeleteDocumentNoPkSchema);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OfflinePartitionWriterTest, TestKVTable);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OfflinePartitionWriterTest, TestFailOverWithTemperature);
}} // namespace indexlib::partition

#endif //__INDEXLIB_OFFLINEPARTITIONWRITERTEST_H
