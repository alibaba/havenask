#pragma once

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/partition/in_memory_index_cleaner.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace partition {

class InMemoryIndexCleanerTest : public INDEXLIB_TESTBASE_WITH_PARAM<int>
{
public:
    InMemoryIndexCleanerTest();
    ~InMemoryIndexCleanerTest();

    DECLARE_CLASS_NAME(InMemoryIndexCleanerTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestClean();
    void TestCleanWithNoSegmentRemoved();
    void TestCleanVersions();
    void TestCleanWithEmptyDirectory();
    void TestCleanWithSegmentNotInVersion();

private:
    void MakeVersions(versionid_t incVersionId, const std::string& incSegmentIds, versionid_t rtVersionId,
                      const std::string& rtSegmentIds, versionid_t joinVersionId, const std::string& joinSegmentIds);

    void MakeVersionInDrectory(versionid_t versionId, const std::string& segmentIds,
                               const file_system::DirectoryPtr& directory);

    bool IsExistRtSegment(segmentid_t segmentId);
    bool IsExistJoinSegment(segmentid_t segmentId);

private:
    file_system::DirectoryPtr mRootDir;
    file_system::DirectoryPtr mRtPartitionDir;
    file_system::DirectoryPtr mJoinPartitionDir;
    file_system::IFileSystemPtr mFileSystem;
    config::IndexPartitionOptions mOptions;

private:
    IE_LOG_DECLARE();
};

INSTANTIATE_TEST_CASE_P(BuildMode, InMemoryIndexCleanerTest, testing::Values(0, 1, 2));

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(InMemoryIndexCleanerTest, TestClean);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(InMemoryIndexCleanerTest, TestCleanVersions);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(InMemoryIndexCleanerTest, TestCleanWithEmptyDirectory);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(InMemoryIndexCleanerTest, TestCleanWithSegmentNotInVersion);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(InMemoryIndexCleanerTest, TestCleanWithNoSegmentRemoved);
}} // namespace indexlib::partition
