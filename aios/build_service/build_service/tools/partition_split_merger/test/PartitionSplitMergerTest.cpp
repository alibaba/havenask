#include "build_service/test/unittest.h"
#include "build_service/tools/partition_split_merger/PartitionSplitMerger.h"
#include "build_service/util/FileUtil.h"
#include "build_service/util/RangeUtil.h"
#include <indexlib/index_base/version_loader.h>
#include <autil/StringUtil.h>
#include <sys/wait.h>

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(index_base);

using namespace build_service::util;
using namespace build_service::proto;

namespace build_service {
namespace tools {

class PartitionSplitMergerTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp() {
        string src = TEST_DATA_PATH + string("/partition_split_merger_test/test_partition_split_merge_index");
        _targetDir = GET_TEST_DATA_PATH() + "/test_partition_split_merge_index";
        int ret = system(string("tar xzf " + src + ".tgz -C " + GET_TEST_DATA_PATH()).c_str());
        ASSERT_TRUE(ret != -1);
    }
    void tearDown() {
    }
    void createVersion(const string& segIdsStr, int64_t timestamp, Version &version);
    void checkVersion(const VersionPtr& version, const Version& expectVersion);
public:
    string _targetDir;
};

TEST_F(PartitionSplitMergerTest, testInit) {
    {
        PartitionSplitMerger::Param param;
        param.generationDir = _targetDir;
        param.buildPartIdxFrom = 0;
        param.buildPartCount = 6;
        param.globalPartCount = 5;
        param.partSplitNum = 2;
        param.buildParallelNum = 1;
        PartitionSplitMerger merger;
        EXPECT_FALSE(merger.init(param));
    }
    {
        PartitionSplitMerger::Param param;
        param.generationDir = _targetDir;
        param.buildPartIdxFrom = 0;
        param.buildPartCount = 5;
        param.globalPartCount = 5;
        param.partSplitNum = 2;
        param.buildParallelNum = 1;

        PartitionSplitMerger merger;
        ASSERT_TRUE(merger.init(param));
        EXPECT_EQ((size_t)5, merger._splitMergeInfos.size());
        vector<Range> rangeVec = RangeUtil::splitRange(0, 65535, 5);
        for (size_t i = 0; i < 5; ++i) {
            string expectMergePath = _targetDir + "/" +
                                     PartitionSplitMerger::getPartitionName(rangeVec[i]);
            EXPECT_EQ(merger._splitMergeInfos[i].mergePartDir, expectMergePath);
        }
    }

}

TEST_F(PartitionSplitMergerTest, testIsSegmentOrVersion) {
    {
        string entry = "nothing";
        EXPECT_TRUE(!PartitionSplitMerger::isSegmentOrVersion(entry));
    }

    {
        string entry = "segment_a";
        EXPECT_TRUE(!PartitionSplitMerger::isSegmentOrVersion(entry));
    }

    {
        string entry = "version.b";
        EXPECT_TRUE(!PartitionSplitMerger::isSegmentOrVersion(entry));
    }

    {
        string entry = "version.1";
        EXPECT_TRUE(PartitionSplitMerger::isSegmentOrVersion(entry));
    }


    {
        string entry = "segment_1";
        EXPECT_TRUE(PartitionSplitMerger::isSegmentOrVersion(entry));
    }

    {
        string entry = "segment_1////";
        EXPECT_TRUE(PartitionSplitMerger::isSegmentOrVersion(entry));
    }

}

TEST_F(PartitionSplitMergerTest, testMergeVersion)
{
    VersionPtr versionPtr(new Version(0));

    Version splitVersion(1);
    createVersion("1,2,3", 1, splitVersion);
    PartitionSplitMerger::mergeVersion(splitVersion, versionPtr);
    checkVersion(versionPtr, splitVersion);

    Version newSplitVersion(2);
    createVersion("1,2,3,8", 4, newSplitVersion);
    PartitionSplitMerger::mergeVersion(newSplitVersion, versionPtr);
    Version expectVersion(0);
    createVersion("1,2,3,5,6,7,12", 4, expectVersion);
    checkVersion(versionPtr, expectVersion);
}

TEST_F(PartitionSplitMergerTest, testMoveSegmentData)
{
    string splitDir = _targetDir + "/partition_0_6553";
    string mergedDir = _targetDir + "/testMoveSegment";
    FileUtil::mkDir(mergedDir);

    Version splitVersion;
    VersionLoader::GetVersion(splitDir, splitVersion, INVALID_VERSION);
    PartitionSplitMerger::moveSegmentData(splitDir, splitVersion, mergedDir, 4);

    vector<string> entryVec;
    FileUtil::listDir(mergedDir, entryVec);
    sort(entryVec.begin(), entryVec.end());

    EXPECT_EQ((size_t)2, entryVec.size());
    EXPECT_EQ(string("segment_5"), entryVec[0]);
    EXPECT_EQ(string("segment_6"), entryVec[1]);
}

TEST_F(PartitionSplitMergerTest, testMoveDataExceptSegmentAndVersion)
{
    string splitDir = _targetDir + "/partition_0_6553";
    string mergedDir = _targetDir + "/testMoveDataExceptSegmentAndVersion";
    FileUtil::mkDir(mergedDir);

    PartitionSplitMerger::moveDataExceptSegmentAndVersion(splitDir, mergedDir);
    vector<string> entryVec;
    FileUtil::listDir(mergedDir, entryVec);
    sort(entryVec.begin(), entryVec.end());

    EXPECT_EQ((size_t)4, entryVec.size());
    EXPECT_EQ(string("index_format_version"), entryVec[0]);
    EXPECT_EQ(string("index_partition_meta"), entryVec[1]);
    EXPECT_EQ(string("schema.json"), entryVec[2]);
    EXPECT_EQ(string("start_locator.info"), entryVec[3]);
}

TEST_F(PartitionSplitMergerTest, testMerge) {
    PartitionSplitMerger::Param param;
    param.generationDir = _targetDir;
    param.buildPartIdxFrom = 0;
    param.buildPartCount = 5;
    param.globalPartCount = 5;
    param.partSplitNum = 2;
    param.buildParallelNum = 1;

    PartitionSplitMerger merger;
    ASSERT_TRUE(merger.init(param));
    merger.merge();

    vector<string> entryVec;
    FileUtil::listDir(_targetDir, entryVec);
    EXPECT_EQ((size_t)5, entryVec.size());

    vector<Range> rangeVec = RangeUtil::splitRange(0, 65535, param.globalPartCount);
    for (size_t i = 0; i < rangeVec.size(); ++i) {
        string expectMergePath = _targetDir + "/" +
                                 PartitionSplitMerger::getPartitionName(rangeVec[i]);
        vector<string> entryVec;
        FileUtil::listDir(expectMergePath, entryVec);
        EXPECT_THAT(entryVec, testing::UnorderedElementsAre("index_format_version", "schema.json",
                        "segment_0", "segment_1", "segment_2", "segment_3",
                        "start_locator.info", "version.0", "index_partition_meta",
                        "deploy_meta.0"));
    }
}

TEST_F(PartitionSplitMergerTest, testMergePartFrom) {
    PartitionSplitMerger::Param param;
    param.generationDir = _targetDir;
    param.buildPartIdxFrom = 2;
    param.buildPartCount = 2;
    param.globalPartCount = 5;
    param.partSplitNum = 2;
    param.buildParallelNum = 1;

    PartitionSplitMerger merger;
    ASSERT_TRUE(merger.init(param));
    merger.merge();

    vector<string> entryVec;
    FileUtil::listDir(_targetDir, entryVec);

    vector<Range> rangeVec = RangeUtil::splitRange(0, 65535, param.globalPartCount);
    EXPECT_THAT(entryVec, testing::Contains(PartitionSplitMerger::getPartitionName(rangeVec[2])));
    EXPECT_THAT(entryVec, testing::Contains(PartitionSplitMerger::getPartitionName(rangeVec[3])));
    for (size_t i = param.buildPartIdxFrom; i < param.buildPartIdxFrom + param.buildPartCount; ++i) {
        string expectMergePath = _targetDir + "/" +
                                 PartitionSplitMerger::getPartitionName(rangeVec[i]);
        vector<string> entryVec;
        FileUtil::listDir(expectMergePath, entryVec);
        EXPECT_THAT(entryVec, testing::UnorderedElementsAre("index_format_version", "schema.json",
                        "segment_0", "segment_1", "segment_2", "segment_3",
                        "start_locator.info", "version.0", "index_partition_meta",
                        "deploy_meta.0"));
    }
}

// segIdsStr : 0,1,2
void PartitionSplitMergerTest::createVersion(
        const string& segIdsStr, int64_t timestamp, Version &version)
{
    vector<segmentid_t> segIds;
    StringUtil::fromString(segIdsStr, segIds, ",");
    for (size_t i = 0; i < segIds.size(); ++i) {
        version.AddSegment(segIds[i]);
    }
    version.SetTimestamp(timestamp);
}

void PartitionSplitMergerTest::checkVersion(
        const VersionPtr& version,
        const Version& expectVersion)
{
    EXPECT_EQ(expectVersion.GetSegmentCount(), version->GetSegmentCount());

    for (size_t i = 0; i < expectVersion.GetSegmentCount(); ++i) {
        EXPECT_EQ(expectVersion[i], (*version)[i]);
    }

    EXPECT_EQ(expectVersion.GetTimestamp(),
              version->GetTimestamp());
}

}
}
