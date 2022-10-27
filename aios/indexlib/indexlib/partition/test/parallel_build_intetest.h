#ifndef __INDEXLIB_PARALLELBUILDINTETEST_H
#define __INDEXLIB_PARALLELBUILDINTETEST_H

#include "indexlib/common_define.h"

#include "indexlib/util/counter/counter_map.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/config/index_partition_options.h"

IE_NAMESPACE_BEGIN(partition);

class ParallelBuildInteTest : public INDEXLIB_TESTBASE
{
public:
    ParallelBuildInteTest();
    ~ParallelBuildInteTest();

    DECLARE_CLASS_NAME(ParallelBuildInteTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestBeginMergeRestartForUpdateConfig();
    void TestEndMergeRestartForUpdateConfig();
    void TestCustomizedTable();
    void TestRtBuildNotInParallelPath();
    void TestDeletionMapMerged();
    void TestDeletionMapMergedForSubDoc();
    void TestCounterMerge();
    void TestBuildWithPackageFileInParentPath();
    void TestRecoverBuildVersionForIncInstance();
    void TestBuildEmptyDataForIncInstance();
    void TestBuildFromEmptyForIncInstance();
    void TestBuildWithConflictSegmentInParentPath();
    void TestMergeRecoveredBuildSegment();
    void TestRecoverableSegmentNotInInstanceVersion();

private:
    void PrepareFullData(bool needMerge = true);
    util::CounterMapPtr LoadCounterMap(const std::string& rootPath);
    std::vector<std::string> MakeLostSegments(
            const std::string& rootPath, segmentid_t startSegId, size_t segCount);

    void CheckPathExist(const std::vector<std::string>& pathVec, bool exist);
    void CheckMetaFile(const std::string& rootPath, bool checkPartitionPatch = true);

private:
    config::IndexPartitionSchemaPtr mSchema;
    config::IndexPartitionOptions mOptions;
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(ParallelBuildInteTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(ParallelBuildInteTest, TestBeginMergeRestartForUpdateConfig);
INDEXLIB_UNIT_TEST_CASE(ParallelBuildInteTest, TestEndMergeRestartForUpdateConfig);
INDEXLIB_UNIT_TEST_CASE(ParallelBuildInteTest, TestCustomizedTable);
INDEXLIB_UNIT_TEST_CASE(ParallelBuildInteTest, TestRtBuildNotInParallelPath);
INDEXLIB_UNIT_TEST_CASE(ParallelBuildInteTest, TestDeletionMapMerged);
INDEXLIB_UNIT_TEST_CASE(ParallelBuildInteTest, TestDeletionMapMergedForSubDoc);
INDEXLIB_UNIT_TEST_CASE(ParallelBuildInteTest, TestCounterMerge);
INDEXLIB_UNIT_TEST_CASE(ParallelBuildInteTest, TestBuildWithPackageFileInParentPath);
INDEXLIB_UNIT_TEST_CASE(ParallelBuildInteTest, TestBuildEmptyDataForIncInstance);
INDEXLIB_UNIT_TEST_CASE(ParallelBuildInteTest, TestRecoverBuildVersionForIncInstance);
INDEXLIB_UNIT_TEST_CASE(ParallelBuildInteTest, TestBuildFromEmptyForIncInstance);
INDEXLIB_UNIT_TEST_CASE(ParallelBuildInteTest, TestBuildWithConflictSegmentInParentPath);
INDEXLIB_UNIT_TEST_CASE(ParallelBuildInteTest, TestMergeRecoveredBuildSegment);
INDEXLIB_UNIT_TEST_CASE(ParallelBuildInteTest, TestRecoverableSegmentNotInInstanceVersion);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_PARALLELBUILDINTETEST_H
