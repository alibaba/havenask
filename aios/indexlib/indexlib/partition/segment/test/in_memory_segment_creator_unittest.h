#ifndef __INDEXLIB_INMEMORYSEGMENTCREATORTEST_H
#define __INDEXLIB_INMEMORYSEGMENTCREATORTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/partition/segment/in_memory_segment_creator.h"

DECLARE_REFERENCE_CLASS(util, CounterMap);
IE_NAMESPACE_BEGIN(partition);

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

    void CheckSegmentMetrics(const index_base::SegmentMetricsPtr& segmentMetrics, 
                              const std::vector<std::string>& indexNames, 
                             size_t expectTermCount);
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

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_INMEMORYSEGMENTCREATORTEST_H
