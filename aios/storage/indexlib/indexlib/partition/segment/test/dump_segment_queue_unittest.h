#ifndef __INDEXLIB_DUMPSEGMENTCONTAINERTEST_H
#define __INDEXLIB_DUMPSEGMENTCONTAINERTEST_H

#include "indexlib/common_define.h"
#include "indexlib/partition/custom_partition_data.h"
#include "indexlib/partition/segment/dump_segment_queue.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace partition {
DECLARE_REFERENCE_CLASS(partition, PartitionData);
DECLARE_REFERENCE_CLASS(partition, CustomPartitionData);
DECLARE_REFERENCE_CLASS(file_system, IFileSystem);
DECLARE_REFERENCE_CLASS(file_system, Directory);
DECLARE_REFERENCE_CLASS(util, MemoryReserver);
DECLARE_REFERENCE_CLASS(table, TableWriter);
DECLARE_REFERENCE_CLASS(partition, NewSegmentMeta);
DECLARE_REFERENCE_CLASS(index_base, SegmentInfo);

class DumpSegmentQueueTest : public INDEXLIB_TESTBASE
{
public:
    DumpSegmentQueueTest();
    ~DumpSegmentQueueTest();

    DECLARE_CLASS_NAME(DumpSegmentQueueTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestBase();
    void TestGetLastSegment();
    void TestClone();
    void TestPopDumpedItems();
    void TestProcessOneItem();
    void TestGetEstimateDumpSize();
    void TestGetDumpingSegmentsSize();
    void TestReclaimSegments();
    void TestGetBuildingSegmentReaders();

private:
    std::string mRootDir;
    config::IndexPartitionSchemaPtr mSchema;
    config::IndexPartitionOptions mOptions;
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(DumpSegmentQueueTest, TestBase);
INDEXLIB_UNIT_TEST_CASE(DumpSegmentQueueTest, TestGetLastSegment);
INDEXLIB_UNIT_TEST_CASE(DumpSegmentQueueTest, TestClone);
INDEXLIB_UNIT_TEST_CASE(DumpSegmentQueueTest, TestPopDumpedItems);
INDEXLIB_UNIT_TEST_CASE(DumpSegmentQueueTest, TestProcessOneItem);
INDEXLIB_UNIT_TEST_CASE(DumpSegmentQueueTest, TestGetEstimateDumpSize);
INDEXLIB_UNIT_TEST_CASE(DumpSegmentQueueTest, TestGetDumpingSegmentsSize);
INDEXLIB_UNIT_TEST_CASE(DumpSegmentQueueTest, TestReclaimSegments);
INDEXLIB_UNIT_TEST_CASE(DumpSegmentQueueTest, TestGetBuildingSegmentReaders);
}} // namespace indexlib::partition

#endif //__INDEXLIB_DUMPSEGMENTCONTAINERTEST_H
