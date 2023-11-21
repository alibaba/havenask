#pragma once

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

DECLARE_REFERENCE_CLASS(util, CounterMap);

namespace indexlib { namespace partition {

class PartitionCounterTest : public INDEXLIB_TESTBASE_WITH_PARAM<int>
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
    void OverWriteCounterFile(segmentid_t segId, int64_t val1, int64_t val2, bool removeCounterFile = false,
                              const file_system::DirectoryPtr& rootDir = nullptr);
    void CheckCounterValues(const file_system::DirectoryPtr& rootDir, segmentid_t segId, const std::string& prefix,
                            const std::string& expectValues);
    void CheckCounterMap(const util::CounterMapPtr& counterMap, const std::string& prefix,
                         const std::string& expectValues);

    void MakeOneByteFile(const file_system::DirectoryPtr& dir, const std::string& fileName);
    void PrepareSegmentData(const config::IndexPartitionSchemaPtr& schema, const std::string& segmentDir,
                            int64_t docCount = 0);
    void CheckSizeCounters(const util::CounterMapPtr& counterMap, const std::map<std::string, int64_t>& expectedSize);

    void RemoveEntryTable();

private:
    std::string mRootDir;
    config::IndexPartitionOptions mOptions;
    config::IndexPartitionSchemaPtr mSchema;

private:
    IE_LOG_DECLARE();
};

INSTANTIATE_TEST_CASE_P(BuildMode, PartitionCounterTest, testing::Values(0, 1, 2));

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(PartitionCounterTest, TestUpdateCounters);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(PartitionCounterTest, TestOpenPartition);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(PartitionCounterTest, TestOpenOfflinePartitionWithNoCounterFile);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(PartitionCounterTest, TestReOpenPartition);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(PartitionCounterTest, TestSinlgePartitionCounterMerge);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(PartitionCounterTest, TestPartitionMergeWithNoCounterFile);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(PartitionCounterTest, TestUpdateFieldCounters);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(PartitionCounterTest, TestMergeTimeCounters);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(PartitionCounterTest, TestOfflineRecover);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(PartitionCounterTest, TestSizeCounters);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(PartitionCounterTest, TestSizeCounterSkipOnlineSegments);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(PartitionCounterTest, TestPartitionDocCounter);
}} // namespace indexlib::partition
