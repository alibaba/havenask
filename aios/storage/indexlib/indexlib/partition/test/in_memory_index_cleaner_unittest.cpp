#include "indexlib/partition/test/in_memory_index_cleaner_unittest.h"

#include "autil/StringUtil.h"
#include "indexlib/index_base/segment/join_segment_directory.h"
#include "indexlib/index_base/segment/realtime_segment_directory.h"
#include "indexlib/index_define.h"
#include "indexlib/partition/reader_container.h"
#include "indexlib/test/version_maker.h"
#include "indexlib/util/test/build_test_util.h"

using namespace std;
using namespace autil;
using namespace indexlib::index;
using namespace indexlib::config;
using namespace indexlib::index_base;

using namespace indexlib::file_system;

namespace indexlib { namespace partition {
IE_LOG_SETUP(partition, InMemoryIndexCleanerTest);

InMemoryIndexCleanerTest::InMemoryIndexCleanerTest() {}

InMemoryIndexCleanerTest::~InMemoryIndexCleanerTest() {}

void InMemoryIndexCleanerTest::CaseSetUp()
{
    mFileSystem = GET_FILE_SYSTEM();
    mRootDir = GET_PARTITION_DIRECTORY();
    mRtPartitionDir = mRootDir->MakeDirectory(RT_INDEX_PARTITION_DIR_NAME);
    mJoinPartitionDir = mRootDir->MakeDirectory(JOIN_INDEX_PARTITION_DIR_NAME);

    mOptions = IndexPartitionOptions();
    util::BuildTestUtil::SetupIndexPartitionOptionsForBuildMode(/*buildMode=*/GetParam(), &mOptions);
}

void InMemoryIndexCleanerTest::CaseTearDown() {}

void InMemoryIndexCleanerTest::TestClean()
{
    MakeVersions(1, "1", 1, "1", 1, "1");
    MakeVersions(0, "0", 0, "0", 0, "0");

    ReaderContainerPtr container(new ReaderContainer);
    InMemoryIndexCleaner cleaner(container, mRootDir);
    cleaner.Execute();

    ASSERT_TRUE(mRootDir->IsExist("segment_0_level_0"));
    ASSERT_TRUE(mRootDir->IsExist("segment_1_level_0"));
    ASSERT_TRUE(!IsExistRtSegment(0));
    ASSERT_TRUE(IsExistRtSegment(1));
    ASSERT_TRUE(!IsExistJoinSegment(0));
    ASSERT_TRUE(IsExistJoinSegment(1));
}

void InMemoryIndexCleanerTest::TestCleanWithNoSegmentRemoved()
{
    MakeVersions(1, "0,1", 1, "0,1", 1, "0,1");
    MakeVersions(0, "0", 0, "0", 0, "0");

    ReaderContainerPtr container(new ReaderContainer);
    InMemoryIndexCleaner cleaner(container, mRootDir);
    cleaner.Execute();

    ASSERT_TRUE(mRootDir->IsExist("segment_0_level_0"));
    ASSERT_TRUE(mRootDir->IsExist("segment_1_level_0"));
    ASSERT_TRUE(IsExistRtSegment(0));
    ASSERT_TRUE(IsExistRtSegment(1));
    ASSERT_TRUE(IsExistJoinSegment(0));
    ASSERT_TRUE(IsExistJoinSegment(1));
}

void InMemoryIndexCleanerTest::TestCleanVersions()
{
    ReaderContainerPtr container(new ReaderContainer);
    InMemoryIndexCleaner cleaner(container, mRootDir);

    // only one version
    MakeVersions(INVALID_VERSIONID, "", 2, "", 2, "");
    cleaner.Execute();
    ASSERT_TRUE(mRtPartitionDir->IsExist("version.2"));

    // multiple versions, version.2 already exists
    MakeVersions(INVALID_VERSIONID, "", 3, "", 3, "");
    MakeVersions(INVALID_VERSIONID, "", 4, "", 4, "");
    cleaner.Execute();
    ASSERT_TRUE(!mRtPartitionDir->IsExist("version.2"));
    ASSERT_TRUE(!mRtPartitionDir->IsExist("version.3"));
    ASSERT_TRUE(mRtPartitionDir->IsExist("version.4"));
    ASSERT_TRUE(!mJoinPartitionDir->IsExist("version.2"));
    ASSERT_TRUE(!mJoinPartitionDir->IsExist("version.3"));
    ASSERT_TRUE(mJoinPartitionDir->IsExist("version.4"));
}

void InMemoryIndexCleanerTest::TestCleanWithEmptyDirectory()
{
    DirectoryPtr directory = mRootDir->MakeDirectory("empty_dir");
    ReaderContainerPtr container(new ReaderContainer);
    InMemoryIndexCleaner cleaner(container, directory);
    ASSERT_NO_THROW(cleaner.Execute());
}

void InMemoryIndexCleanerTest::TestCleanWithSegmentNotInVersion()
{
    MakeVersions(2, "2", 2, "2", 2, "2");
    MakeVersions(0, "0", 0, "0", 0, "0");

    mRtPartitionDir->MakeDirectory("segment_1_level_0");
    mJoinPartitionDir->MakeDirectory("segment_1_level_0");

    string rtSegPath =
        string("segment_") + StringUtil::toString(RealtimeSegmentDirectory::ConvertToRtSegmentId(3)) + "_level_0";
    mRtPartitionDir->MakeDirectory(rtSegPath);
    string joinSegPath =
        string("segment_") + StringUtil::toString(JoinSegmentDirectory::ConvertToJoinSegmentId(3)) + "_level_0";
    mJoinPartitionDir->MakeDirectory(joinSegPath);

    ReaderContainerPtr container(new ReaderContainer);
    InMemoryIndexCleaner cleaner(container, mRootDir);
    cleaner.Execute();

    ASSERT_TRUE(mRootDir->IsExist("segment_0_level_0"));
    ASSERT_TRUE(mRootDir->IsExist("segment_2_level_0"));
    ASSERT_TRUE(!IsExistRtSegment(0));
    ASSERT_TRUE(!IsExistRtSegment(1));
    ASSERT_TRUE(IsExistRtSegment(2));
    ASSERT_TRUE(IsExistRtSegment(3));

    ASSERT_TRUE(!IsExistJoinSegment(0));
    ASSERT_TRUE(!IsExistJoinSegment(1));
    ASSERT_TRUE(IsExistJoinSegment(2));
    ASSERT_TRUE(IsExistJoinSegment(3));
}

void InMemoryIndexCleanerTest::MakeVersions(versionid_t incVersionId, const string& incSegmentIds,
                                            versionid_t rtVersionId, const string& rtSegmentIds,
                                            versionid_t joinVersionId, const string& joinSegmentIds)
{
    MakeVersionInDrectory(incVersionId, incSegmentIds, mRootDir);

    vector<segmentid_t> formatedRtSegIds;
    StringUtil::fromString(rtSegmentIds, formatedRtSegIds, ",");
    for (size_t i = 0; i < formatedRtSegIds.size(); ++i) {
        formatedRtSegIds[i] = RealtimeSegmentDirectory::ConvertToRtSegmentId(formatedRtSegIds[i]);
    }
    string formatedRtSegIdsStr = StringUtil::toString(formatedRtSegIds, ",");
    MakeVersionInDrectory(rtVersionId, formatedRtSegIdsStr, mRtPartitionDir);

    vector<segmentid_t> formatedJoinSegIds;
    StringUtil::fromString(joinSegmentIds, formatedJoinSegIds, ",");
    for (size_t i = 0; i < formatedJoinSegIds.size(); ++i) {
        formatedJoinSegIds[i] = JoinSegmentDirectory::ConvertToJoinSegmentId(formatedJoinSegIds[i]);
    }
    string formatedJoinSegIdsStr = StringUtil::toString(formatedJoinSegIds, ",");
    MakeVersionInDrectory(joinVersionId, formatedJoinSegIdsStr, mJoinPartitionDir);
}

void InMemoryIndexCleanerTest::MakeVersionInDrectory(versionid_t versionId, const std::string& segmentIds,
                                                     const file_system::DirectoryPtr& directory)
{
    Version version = VersionMaker::Make(versionId, segmentIds);
    if (versionId != INVALID_VERSIONID) {
        version.Store(directory, false);
    }
    for (size_t i = 0; i < version.GetSegmentCount(); ++i) {
        string segPath = version.GetSegmentDirName(version[i]);
        if (!directory->IsExist(segPath)) {
            directory->MakeDirectory(segPath);
        }
    }
}

bool InMemoryIndexCleanerTest::IsExistRtSegment(segmentid_t segmentId)
{
    segmentid_t rtSegmentId = RealtimeSegmentDirectory::ConvertToRtSegmentId(segmentId);
    stringstream ss;
    ss << SEGMENT_FILE_NAME_PREFIX << "_" << rtSegmentId << "_level_0";
    return mRtPartitionDir->IsExist(ss.str());
}

bool InMemoryIndexCleanerTest::IsExistJoinSegment(segmentid_t segmentId)
{
    segmentid_t joinSegmentId = JoinSegmentDirectory::ConvertToJoinSegmentId(segmentId);
    stringstream ss;
    ss << SEGMENT_FILE_NAME_PREFIX << "_" << joinSegmentId << "_level_0";
    return mJoinPartitionDir->IsExist(ss.str());
}
}} // namespace indexlib::partition
