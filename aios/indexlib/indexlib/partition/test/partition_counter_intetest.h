#ifndef __INDEXLIB_PARTITIONCOUNTERTEST_H
#define __INDEXLIB_PARTITIONCOUNTERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_partition_schema.h"

DECLARE_REFERENCE_CLASS(util, CounterMap);

IE_NAMESPACE_BEGIN(partition);

class PartitionCounterTest : public INDEXLIB_TESTBASE
{
public:
    PartitionCounterTest();
    ~PartitionCounterTest();

    DECLARE_CLASS_NAME(PartitionCounterTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestUpdateCounters();
    void TestOpenPartition();
    void TestOpenOfflinePartitionWithNoCounterFile();    
    void TestReOpenPartition();
    void TestSinlgePartitionCounterMerge();
    void TestPartitionMergeWithNoCounterFile();    
    void TestUpdateFieldCounters();
    void TestMergeTimeCounters();
    void TestOfflineRecover();
    void TestSizeCounters();
    void TestSizeCounterSkipOnlineSegments();
    void TestPartitionDocCounter();
private:
    void OverWriteCounterFile(segmentid_t segId, int64_t val1, int64_t val2, bool removeCounterFile=false);
    void CheckCounterValues(segmentid_t segId, const std::string& prefix,
                            const std::string& expectValues);
    void CheckCounterMap(const util::CounterMapPtr& counterMap,
                         const std::string& prefix,
                         const std::string& expectValues);

    void MakeOneByteFile(const std::string& dir, const std::string& fileName);
    void PrepareSegmentData(const config::IndexPartitionSchemaPtr& schema,
                            const std::string& segmentDir, int64_t docCount = 0);
    void CheckSizeCounters(const util::CounterMapPtr& counterMap,
                           const std::map<std::string, int64_t>& expectedSize);
private:
    std::string mRootDir;
    config::IndexPartitionOptions mOptions;
    config::IndexPartitionSchemaPtr mSchema;
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(PartitionCounterTest, TestUpdateCounters);
INDEXLIB_UNIT_TEST_CASE(PartitionCounterTest, TestOpenPartition);
INDEXLIB_UNIT_TEST_CASE(PartitionCounterTest, TestOpenOfflinePartitionWithNoCounterFile);
INDEXLIB_UNIT_TEST_CASE(PartitionCounterTest, TestReOpenPartition);
INDEXLIB_UNIT_TEST_CASE(PartitionCounterTest, TestSinlgePartitionCounterMerge);
INDEXLIB_UNIT_TEST_CASE(PartitionCounterTest, TestPartitionMergeWithNoCounterFile);
INDEXLIB_UNIT_TEST_CASE(PartitionCounterTest, TestUpdateFieldCounters);
INDEXLIB_UNIT_TEST_CASE(PartitionCounterTest, TestMergeTimeCounters);
INDEXLIB_UNIT_TEST_CASE(PartitionCounterTest, TestOfflineRecover);
INDEXLIB_UNIT_TEST_CASE(PartitionCounterTest, TestSizeCounters);
INDEXLIB_UNIT_TEST_CASE(PartitionCounterTest, TestSizeCounterSkipOnlineSegments);
INDEXLIB_UNIT_TEST_CASE(PartitionCounterTest, TestPartitionDocCounter);
IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_PARTITIONCOUNTERTEST_H
