#pragma once

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/counter/CounterMap.h"

namespace indexlib { namespace partition {

class ParallelBuildInteTest : public INDEXLIB_TESTBASE_WITH_PARAM<int>
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
    void TestKeyValueMerge();
    void TestKKVMerge();
    void TestBuildWithPackageFileInParentPath();
    void TestRecoverBuildVersionForIncInstance();
    void TestBuildEmptyDataForIncInstance();
    void TestBuildFromEmptyForIncInstance();
    void TestBuildWithConflictSegmentInParentPath();
    void TestMergeRecoveredBuildSegment();
    void TestRecoverableSegmentNotInInstanceVersion();
    void TestKVMerge_bugfix_aone_40233622();
    void TestKKVMerge_bugfix_aone_40233622();

private:
    void PrepareFullData(bool needMerge = true);
    util::CounterMapPtr LoadCounterMap(const file_system::DirectoryPtr& rootDir);
    std::vector<std::string> MakeLostSegments(const file_system::DirectoryPtr& rootDir, segmentid_t startSegId,
                                              size_t segCount);

    void CheckPathExist(const std::vector<std::string>& pathVec, bool exist);
    void CheckMetaFile(const file_system::DirectoryPtr& rootDir, bool checkPartitionPatch = true);

private:
    config::IndexPartitionSchemaPtr mSchema;
    config::IndexPartitionOptions mOptions;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(ParallelBuildInteTest, TestSimpleProcess);
// INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(ParallelBuildInteTest, TestBeginMergeRestartForUpdateConfig);  // TODO: (qingran),
// not a good ut
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(ParallelBuildInteTest, TestEndMergeRestartForUpdateConfig);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(ParallelBuildInteTest, TestCustomizedTable);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(ParallelBuildInteTest, TestRtBuildNotInParallelPath);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(ParallelBuildInteTest, TestDeletionMapMerged);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(ParallelBuildInteTest, TestDeletionMapMergedForSubDoc);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(ParallelBuildInteTest, TestCounterMerge);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(ParallelBuildInteTest, TestKeyValueMerge);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(ParallelBuildInteTest, TestKKVMerge);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(ParallelBuildInteTest, TestBuildWithPackageFileInParentPath);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(ParallelBuildInteTest, TestBuildEmptyDataForIncInstance);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(ParallelBuildInteTest, TestRecoverBuildVersionForIncInstance);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(ParallelBuildInteTest, TestBuildFromEmptyForIncInstance);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(ParallelBuildInteTest, TestBuildWithConflictSegmentInParentPath);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(ParallelBuildInteTest, TestMergeRecoveredBuildSegment);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(ParallelBuildInteTest, TestRecoverableSegmentNotInInstanceVersion);

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(ParallelBuildInteTest, TestKVMerge_bugfix_aone_40233622);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(ParallelBuildInteTest, TestKKVMerge_bugfix_aone_40233622);

INSTANTIATE_TEST_CASE_P(BuildMode, ParallelBuildInteTest, testing::Values(0, 1, 2));
}} // namespace indexlib::partition
