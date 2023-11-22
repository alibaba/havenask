#pragma once

#include "indexlib/common_define.h"
#include "indexlib/partition/segment/in_memory_segment_creator.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

DECLARE_REFERENCE_CLASS(util, CounterMap);
namespace indexlib { namespace partition {

class InMemorySegmentCreatorTest : public INDEXLIB_TESTBASE
{
public:
    InMemorySegmentCreatorTest();
    ~InMemorySegmentCreatorTest();

    DECLARE_CLASS_NAME(InMemorySegmentCreatorTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestExtractNonTruncateIndexNames();
    void TestInit();
    void TestCreateWithoutSubDoc();
    void TestCreateWithSubDoc();
    void TestCreateWithOperationWriter();
    void TestCreateSegmentWriterOutOfMem();
    void TestCreateSubWriterOOM();

private:
    config::IndexPartitionSchemaPtr CreateSchema(bool hasSub);

    void CheckSegmentMetrics(const std::shared_ptr<framework::SegmentMetrics>& segmentMetrics,
                             const std::vector<std::string>& indexNames, size_t expectTermCount);
    void CheckSegmentWriter(index_base::SegmentWriterPtr segWriter, bool isSubDocWriter);

private:
    util::CounterMapPtr mCounterMap;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(InMemorySegmentCreatorTest, TestExtractNonTruncateIndexNames);
INDEXLIB_UNIT_TEST_CASE(InMemorySegmentCreatorTest, TestInit);
INDEXLIB_UNIT_TEST_CASE(InMemorySegmentCreatorTest, TestCreateWithoutSubDoc);
INDEXLIB_UNIT_TEST_CASE(InMemorySegmentCreatorTest, TestCreateWithSubDoc);
INDEXLIB_UNIT_TEST_CASE(InMemorySegmentCreatorTest, TestCreateWithOperationWriter);
INDEXLIB_UNIT_TEST_CASE(InMemorySegmentCreatorTest, TestCreateSegmentWriterOutOfMem);
INDEXLIB_UNIT_TEST_CASE(InMemorySegmentCreatorTest, TestCreateSubWriterOOM);
}} // namespace indexlib::partition
