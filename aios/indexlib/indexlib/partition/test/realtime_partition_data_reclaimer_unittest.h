#ifndef __INDEXLIB_REALTIMEPARTITIONDATARECLAIMERTEST_H 
#define __INDEXLIB_REALTIMEPARTITIONDATARECLAIMERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/file_system/indexlib_file_system.h"
#include "indexlib/partition/realtime_partition_data_reclaimer.h"

IE_NAMESPACE_BEGIN(partition);

class RealtimePartitionDataReclaimerTest : public INDEXLIB_TESTBASE
{
private:
    typedef RealtimePartitionDataReclaimer::SimpleSegmentMetas SimpleSegmentMetas;
    typedef RealtimePartitionDataReclaimer::SimpleSegmentMeta SimpleSegmentMeta;

public:
    RealtimePartitionDataReclaimerTest();
    ~RealtimePartitionDataReclaimerTest();

    DECLARE_CLASS_NAME(RealtimePartitionDataReclaimerTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestExtractSegmentsToReclaim();
    void TestExtractSegmentsToReclaimForBreakInTime();
    void TestExtractSegmentsToReclaimForAllDelSegs();
    void TestExtractSegmentsToReclaimForObsoleteTimestamp();
    void TestExtractSegmentsToReclaimForSegmentWithOperationQueue();

private:
    void MakeSegmentMetas(const std::string& segments, 
                          SimpleSegmentMetas& segmentMetas);
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(RealtimePartitionDataReclaimerTest,
                        TestExtractSegmentsToReclaim);
INDEXLIB_UNIT_TEST_CASE(RealtimePartitionDataReclaimerTest,
                        TestExtractSegmentsToReclaimForBreakInTime);
INDEXLIB_UNIT_TEST_CASE(RealtimePartitionDataReclaimerTest,
                        TestExtractSegmentsToReclaimForAllDelSegs);
INDEXLIB_UNIT_TEST_CASE(RealtimePartitionDataReclaimerTest,
                        TestExtractSegmentsToReclaimForObsoleteTimestamp);
INDEXLIB_UNIT_TEST_CASE(RealtimePartitionDataReclaimerTest,
                        TestExtractSegmentsToReclaimForSegmentWithOperationQueue);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_REALTIMEPARTITIONDATARECLAIMERTEST_H
