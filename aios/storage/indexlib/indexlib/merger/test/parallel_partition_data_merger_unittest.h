#ifndef __INDEXLIB_PARALLELPARTITIONDATAMERGERTEST_H
#define __INDEXLIB_PARALLELPARTITIONDATAMERGERTEST_H

#include "indexlib/common_define.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/merger/parallel_partition_data_merger.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace merger {

class ParallelPartitionDataMergerTest : public INDEXLIB_TESTBASE
{
public:
    ParallelPartitionDataMergerTest();
    ~ParallelPartitionDataMergerTest();

    DECLARE_CLASS_NAME(ParallelPartitionDataMergerTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestLocatorAndTimestamp();
    void TestNoNeedMerge();
    void TestMergeEmptyInstanceDir();
    void TestMergeVersionWithSegmentMetric();
    void TestMergeVersionWithSegmentStatistics();
    void TestOldWorkerCannotMoveNewDir();
    void TestMultiWorkerToMoveData();

private:
    void SetLastSegmentInfo(const indexlib::file_system::DirectoryPtr& rootDir, int src, int offset, int64_t ts,
                            int64_t maxTTL);
    int64_t GetLastSegmentLocator(const indexlib::file_system::DirectoryPtr& rootDir);

    // info : segId,segId,segId;....
    std::vector<std::pair<index_base::Version, size_t>> MakeVersionPair(const std::string& versionInfos,
                                                                        versionid_t baseVersion);

private:
    config::IndexPartitionSchemaPtr mSchema;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(ParallelPartitionDataMergerTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(ParallelPartitionDataMergerTest, TestLocatorAndTimestamp);
INDEXLIB_UNIT_TEST_CASE(ParallelPartitionDataMergerTest, TestNoNeedMerge);
INDEXLIB_UNIT_TEST_CASE(ParallelPartitionDataMergerTest, TestMergeEmptyInstanceDir);
INDEXLIB_UNIT_TEST_CASE(ParallelPartitionDataMergerTest, TestMergeVersionWithSegmentMetric);
INDEXLIB_UNIT_TEST_CASE(ParallelPartitionDataMergerTest, TestMergeVersionWithSegmentStatistics);
INDEXLIB_UNIT_TEST_CASE(ParallelPartitionDataMergerTest, TestOldWorkerCannotMoveNewDir);
INDEXLIB_UNIT_TEST_CASE(ParallelPartitionDataMergerTest, TestMultiWorkerToMoveData);
}} // namespace indexlib::merger

#endif //__INDEXLIB_PARALLELPARTITIONDATAMERGERTEST_H
