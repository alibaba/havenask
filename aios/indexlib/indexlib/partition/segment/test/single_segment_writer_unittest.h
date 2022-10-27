#ifndef __INDEXLIB_SINGLESEGMENTWRITERTEST_H
#define __INDEXLIB_SINGLESEGMENTWRITERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/partition/segment/single_segment_writer.h"
#include "indexlib/index_base/index_meta/segment_metrics.h"

IE_NAMESPACE_BEGIN(partition);

class SingleSegmentWriterTest : public INDEXLIB_TESTBASE
{
public:
    SingleSegmentWriterTest();
    ~SingleSegmentWriterTest();

    DECLARE_CLASS_NAME(SingleSegmentWriterTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestSimpleProcess();
    void TestSimpleProcessForCustomizedIndex();    
    void TestInit();
    void TestAddDocumentNormal();
    void TestAddDocumentWithVirtualAttribute();
    void TestAddDocumentWithoutAttributeSchema();
    void TestAddDocumentWithoutPkSchema();
    void TestAddDocumentWithoutSummarySchema();
    void TestAddDocumentWithoutAttributeDoc();
    void TestAddDocumentWithoutPkDoc();
    void TestAddDocumentWithoutSummaryDoc();
    void TestCaseForInitSegmentMetrics();
    void TestIsValidDocumentWithNoPkDocWhenSchemaHasPk();
    void TestGetTotalMemoryUse();
    void TestCreateInMemSegmentReader();
    void TestIsDirty();
    void TestDump();
    void TestDumpEmptySegment();
    void TestDumpSegmentWithCopyOnDumpAttribute();
    void TestEstimateAttrDumpTemporaryMemoryUse();
    void TestIsValidDocumentWithNumPk();
    void TestEstimateInitMemUse();
    void TestEstimateInitMemUseWithShardingIndex();
    
private:
    config::IndexPartitionSchemaPtr CreateSchemaWithVirtualAttribute();
    void CheckInMemorySegmentReader(
            const config::IndexPartitionSchemaPtr& schema, 
            const index::InMemorySegmentReaderPtr inMemSegReader);

    void InnerTestDumpSegmentWithCopyOnDumpAttribute(
        const config::IndexPartitionOptions& options);
    
private:
    config::IndexPartitionSchemaPtr mSchema;
    config::IndexPartitionOptions mOptions;
    std::string mRootDir;
    index_base::SegmentMetricsPtr mSegmentMetrics;
    std::string mDocStr;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(SingleSegmentWriterTest, TestSimpleProcess);
//INDEXLIB_UNIT_TEST_CASE(SingleSegmentWriterTest, TestSimpleProcessForCustomizedIndex);
INDEXLIB_UNIT_TEST_CASE(SingleSegmentWriterTest, TestIsValidDocumentWithNoPkDocWhenSchemaHasPk);
INDEXLIB_UNIT_TEST_CASE(SingleSegmentWriterTest, TestAddDocumentNormal);
INDEXLIB_UNIT_TEST_CASE(SingleSegmentWriterTest, TestAddDocumentWithVirtualAttribute);
INDEXLIB_UNIT_TEST_CASE(SingleSegmentWriterTest, TestAddDocumentWithoutAttributeSchema);
INDEXLIB_UNIT_TEST_CASE(SingleSegmentWriterTest, TestAddDocumentWithoutPkSchema);
INDEXLIB_UNIT_TEST_CASE(SingleSegmentWriterTest, TestAddDocumentWithoutSummarySchema);
INDEXLIB_UNIT_TEST_CASE(SingleSegmentWriterTest, TestAddDocumentWithoutAttributeDoc);
INDEXLIB_UNIT_TEST_CASE(SingleSegmentWriterTest, TestAddDocumentWithoutPkDoc);
INDEXLIB_UNIT_TEST_CASE(SingleSegmentWriterTest, TestAddDocumentWithoutSummaryDoc);
INDEXLIB_UNIT_TEST_CASE(SingleSegmentWriterTest, TestGetTotalMemoryUse);
INDEXLIB_UNIT_TEST_CASE(SingleSegmentWriterTest, TestCaseForInitSegmentMetrics);
INDEXLIB_UNIT_TEST_CASE(SingleSegmentWriterTest, TestInit);
INDEXLIB_UNIT_TEST_CASE(SingleSegmentWriterTest, TestCreateInMemSegmentReader);
INDEXLIB_UNIT_TEST_CASE(SingleSegmentWriterTest, TestIsDirty);
INDEXLIB_UNIT_TEST_CASE(SingleSegmentWriterTest, TestDump);
INDEXLIB_UNIT_TEST_CASE(SingleSegmentWriterTest, TestDumpEmptySegment);
INDEXLIB_UNIT_TEST_CASE(SingleSegmentWriterTest, TestDumpSegmentWithCopyOnDumpAttribute);
INDEXLIB_UNIT_TEST_CASE(SingleSegmentWriterTest, TestEstimateAttrDumpTemporaryMemoryUse);
INDEXLIB_UNIT_TEST_CASE(SingleSegmentWriterTest, TestIsValidDocumentWithNumPk);
INDEXLIB_UNIT_TEST_CASE(SingleSegmentWriterTest, TestEstimateInitMemUse);
INDEXLIB_UNIT_TEST_CASE(SingleSegmentWriterTest, TestEstimateInitMemUseWithShardingIndex);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_SINGLESEGMENTWRITERTEST_H
