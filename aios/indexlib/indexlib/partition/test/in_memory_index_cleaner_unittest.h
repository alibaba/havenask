#ifndef __INDEXLIB_INMEMORYINDEXCLEANERTEST_H
#define __INDEXLIB_INMEMORYINDEXCLEANERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/partition/in_memory_index_cleaner.h"
#include "indexlib/file_system/directory.h"
#include "indexlib/file_system/indexlib_file_system.h"
#include "indexlib/config/index_partition_options.h"

IE_NAMESPACE_BEGIN(partition);

class InMemoryIndexCleanerTest : public INDEXLIB_TESTBASE
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
    void MakeVersions(versionid_t incVersionId, const std::string& incSegmentIds,
                      versionid_t rtVersionId, const std::string& rtSegmentIds,
                      versionid_t joinVersionId, const std::string& joinSegmentIds);

    void MakeVersionInDrectory(versionid_t versionId,
                               const std::string& segmentIds,
                               const file_system::DirectoryPtr& directory);

    bool IsExistRtSegment(segmentid_t segmentId);
    bool IsExistJoinSegment(segmentid_t segmentId);

private:
    file_system::DirectoryPtr mRootDir;
    file_system::DirectoryPtr mRtPartitionDir;
    file_system::DirectoryPtr mJoinPartitionDir;
    file_system::IndexlibFileSystemPtr mFileSystem;
    config::IndexPartitionOptions mOptions;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(InMemoryIndexCleanerTest, TestClean);
INDEXLIB_UNIT_TEST_CASE(InMemoryIndexCleanerTest, TestCleanVersions);
INDEXLIB_UNIT_TEST_CASE(InMemoryIndexCleanerTest, TestCleanWithEmptyDirectory);
INDEXLIB_UNIT_TEST_CASE(InMemoryIndexCleanerTest, TestCleanWithSegmentNotInVersion);
INDEXLIB_UNIT_TEST_CASE(InMemoryIndexCleanerTest, TestCleanWithNoSegmentRemoved);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_INMEMORYINDEXCLEANERTEST_H
