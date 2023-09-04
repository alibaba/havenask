#ifndef __INDEXLIB_SINGLESEGMENTWRITERTEST_H
#define __INDEXLIB_SINGLESEGMENTWRITERTEST_H

#include "indexlib/common_define.h"
#include "indexlib/framework/SegmentMetrics.h"
#include "indexlib/partition/segment/single_segment_writer.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/SimplePool.h"

namespace indexlib { namespace partition {

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
    void TestGetTotalMemoryUse();
    void TestCreateInMemSegmentReader();
    void TestIsDirty();
    void TestDump();
    void TestDumpEmptySegment();
    void TestDumpSegmentWithCopyOnDumpAttribute();
    void TestEstimateAttrDumpTemporaryMemoryUse();
    void TestEstimateInitMemUse();
    void TestEstimateInitMemUseWithShardingIndex();
    void TestCloneInPrivateMode();

private:
    config::IndexPartitionSchemaPtr CreateSchemaWithVirtualAttribute(bool enableSrc = false);
    void CheckInMemorySegmentReader(const config::IndexPartitionSchemaPtr& schema,
                                    const index_base::InMemorySegmentReaderPtr inMemSegReader);

    void InnerTestDumpSegmentWithCopyOnDumpAttribute(const config::IndexPartitionOptions& options);

private:
    config::IndexPartitionSchemaPtr mSchema;
    config::IndexPartitionOptions mOptions;
    std::string mRootDir;
    std::shared_ptr<framework::SegmentMetrics> mSegmentMetrics;
    std::string mDocStr;
    util::SimplePool mPool;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(SingleSegmentWriterTest, TestSimpleProcess);
// INDEXLIB_UNIT_TEST_CASE(SingleSegmentWriterTest, TestSimpleProcessForCustomizedIndex);
INDEXLIB_UNIT_TEST_CASE(SingleSegmentWriterTest, TestGetTotalMemoryUse);
INDEXLIB_UNIT_TEST_CASE(SingleSegmentWriterTest, TestCaseForInitSegmentMetrics);
INDEXLIB_UNIT_TEST_CASE(SingleSegmentWriterTest, TestInit);
INDEXLIB_UNIT_TEST_CASE(SingleSegmentWriterTest, TestCreateInMemSegmentReader);
INDEXLIB_UNIT_TEST_CASE(SingleSegmentWriterTest, TestIsDirty);
INDEXLIB_UNIT_TEST_CASE(SingleSegmentWriterTest, TestDump);
INDEXLIB_UNIT_TEST_CASE(SingleSegmentWriterTest, TestDumpEmptySegment);
INDEXLIB_UNIT_TEST_CASE(SingleSegmentWriterTest, TestDumpSegmentWithCopyOnDumpAttribute);
INDEXLIB_UNIT_TEST_CASE(SingleSegmentWriterTest, TestEstimateAttrDumpTemporaryMemoryUse);
INDEXLIB_UNIT_TEST_CASE(SingleSegmentWriterTest, TestEstimateInitMemUse);
INDEXLIB_UNIT_TEST_CASE(SingleSegmentWriterTest, TestEstimateInitMemUseWithShardingIndex);
INDEXLIB_UNIT_TEST_CASE(SingleSegmentWriterTest, TestCloneInPrivateMode);
}} // namespace indexlib::partition

#endif //__INDEXLIB_SINGLESEGMENTWRITERTEST_H
