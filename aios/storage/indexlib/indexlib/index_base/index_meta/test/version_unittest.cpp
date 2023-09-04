#include "indexlib/index_base/index_meta/test/version_unittest.h"

#include "autil/StringUtil.h"
#include "indexlib/test/version_maker.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/PathUtil.h"

using namespace std;
using namespace autil;
using namespace indexlib::util;

namespace indexlib { namespace index_base {

void VersionTest::TestCaseForFromString()
{
    string str = "{\"segments\":[1,3,12],\"versionid\":10}";
    Version v1;
    v1.FromString(str);

    Version v2;
    v2.AddSegment(1);
    v2.AddSegment(3);
    v2.AddSegment(12);
    v2.SetVersionId(10);

    CheckEqual(v1, v2);
}

void VersionTest::TestCaseForFromStringWithTimestamp()
{
    string str = "{\"segments\":[1,3,12],\"versionid\":10,\"timestamp\":123}";
    Version v1;
    v1.FromString(str);

    Version v2;
    v2.AddSegment(1);
    v2.AddSegment(3);
    v2.AddSegment(12);
    v2.SetVersionId(10);
    v2.SetTimestamp(123);

    CheckEqual(v1, v2);
}

void VersionTest::TestCaseForFromStringWithBadStr()
{
    string str = "{\"segments\":[1,3,12] \"versionid\":10}";
    Version v1;
    ASSERT_THROW(v1.FromString(str), autil::legacy::ParameterInvalidException);
}

void VersionTest::TestCaseForToString()
{
    Version v1;
    v1.AddSegment(1);
    v1.AddSegment(3);
    v1.AddSegment(12);
    v1.SetVersionId(10);
    v1.AddDescription("batchId", "123");
    v1.AddDescription("batchId", "456");

    string buf = v1.ToString();

    Version v2;
    v2.FromString(buf);

    CheckEqual(v1, v2);
    string batchId;
    ASSERT_TRUE(v2.GetDescription("batchId", batchId));
    ASSERT_EQ("456", batchId);

    map<string, string> newDesc;
    newDesc["batchId"] = "789";
    newDesc["ts"] = "abc";
    v2.SetDescriptions(newDesc);
    ASSERT_EQ("789", v2.mDesc["batchId"]);
    ASSERT_EQ("abc", v2.mDesc["ts"]);
}

void VersionTest::CheckEqual(const Version& v1, const Version& v2)
{
    INDEXLIB_TEST_EQUAL(v1.GetVersionId(), v2.GetVersionId());
    INDEXLIB_TEST_EQUAL(v1.GetSegmentCount(), v2.GetSegmentCount());
    INDEXLIB_TEST_EQUAL(v1.GetTimestamp(), v2.GetTimestamp());
    INDEXLIB_TEST_EQUAL(v1.GetLevelInfo(), v2.GetLevelInfo());
    Version::Iterator iter1 = v1.CreateIterator();
    Version::Iterator iter2 = v2.CreateIterator();
    while (iter1.HasNext() && iter2.HasNext()) {
        segmentid_t v1Seg = iter1.Next();
        segmentid_t v2Seg = iter2.Next();
        INDEXLIB_TEST_EQUAL(v1Seg, v2Seg);
    }
    INDEXLIB_TEST_TRUE(!iter1.HasNext());
    INDEXLIB_TEST_TRUE(!iter2.HasNext());

    for (auto iter = v1.mDesc.begin(); iter != v1.mDesc.end(); iter++) {
        string value;
        ASSERT_TRUE(v2.GetDescription(iter->first, value));
        ASSERT_EQ(value, iter->second);
    }
}

void VersionTest::TestCaseForAssignOperator()
{
    {
        Version orgVersion;
        Version copyVersion;
        copyVersion = orgVersion;
        CheckEqual(orgVersion, copyVersion);
    }
    {
        Version orgVersion(2, 100);
        orgVersion.AddSegment(1);

        Version copyVersion;
        copyVersion = orgVersion;
        CheckEqual(orgVersion, copyVersion);
    }
}

void VersionTest::TestCaseForMinus()
{
    SCOPED_TRACE("TestCaseForMinus Failed");

    DoTestMinus("0,1", "1,2", "0");
    DoTestMinus("0,1", "0,1", "");
    DoTestMinus("0,1", "2,3", "0,1");
    DoTestMinus("0,1", "1", "0");

    // test empty version
    DoTestMinus("", "1,2", "");
    DoTestMinus("0,1", "", "0,1");
    DoTestMinus("", "", "");

    DoTestMinus("0,1,3", "1,2,4", "0,3");
}

void VersionTest::DoTestMinus(const string& left, const string& right, const string& expect)
{
    Version version1 = VersionMaker::Make(0, left);
    Version version2 = VersionMaker::Make(1, right);
    Version expectResult = VersionMaker::Make(0, expect);
    expectResult.mLastSegmentId = version1.GetLastSegment();
    Version actualResult = version1 - version2;
    ASSERT_EQ(expectResult, actualResult);
}

void VersionTest::TestCaseForAddSegmentException()
{
    Version v = VersionMaker::Make(0, "1");
    ASSERT_THROW(v.AddSegment(1), IndexCollapsedException);

    Version v2 = VersionMaker::Make(0, "1,10");
    ASSERT_THROW(v2.AddSegment(9), IndexCollapsedException);
}

void VersionTest::TestValidateSegmentIds()
{
    ASSERT_THROW(Version::Validate(CreateSegmentVector("1,0")), IndexCollapsedException);
    ASSERT_THROW(Version::Validate(CreateSegmentVector("1,2,10,5")), IndexCollapsedException);
    ASSERT_THROW(Version::Validate(CreateSegmentVector("1,10,5,6")), IndexCollapsedException);
    ASSERT_THROW(Version::Validate(CreateSegmentVector("1,1")), IndexCollapsedException);
    ASSERT_THROW(Version::Validate(CreateSegmentVector("1,2,2,10")), IndexCollapsedException);
    Version::Validate(CreateSegmentVector(""));
    Version::Validate(CreateSegmentVector("1"));
    Version::Validate(CreateSegmentVector("1,10,100"));
}

Version::SegmentIdVec VersionTest::CreateSegmentVector(const std::string& data)
{
    Version::SegmentIdVec segIds;
    StringUtil::fromString(data, segIds, ",");
    return segIds;
}

void VersionTest::TestStore()
{
    file_system::DirectoryPtr rootDir = GET_PARTITION_DIRECTORY();
    ASSERT_TRUE(rootDir);

    Version version;
    ASSERT_THROW(version.Store(rootDir, true), InvalidVersionException);

    version.SetVersionId(0);
    version.AddSegment(1);

    version.Store(rootDir, true);
    const file_system::IFileSystemPtr& fileSystem = GET_FILE_SYSTEM();
    string fileName = util::PathUtil::JoinPath(rootDir->GetLogicalPath(), "version.0");
    ASSERT_TRUE(fileSystem->IsExist(fileName).GetOrThrow());
}

void VersionTest::TestCaseForFromStringWithLevelInfo()
{
    string str =
        R"(
    {
        "segments": [1,2,10,11],
        "versionid": 10,
        "level_info":
        {
             "level_num" : 2,
             "topology" : "sharding_mod",
             "level_metas" :
             [
                 {
                     "level_idx" : 0,
                     "cursor" : 0,
                     "topology" : "sequence",
                     "segments" :
                     [ 10, 11 ]
                 },
                 {
                     "level_idx" : 1,
                     "cursor" : 0,
                     "topology" : "hash_mod",
                     "segments" :
                     [ 1, 2 ]
                 }
            ]
        }
    })";
    Version v0;
    v0.FromString(str);
    string buf = v0.ToString();
    Version v;
    v.FromString(buf);

    indexlibv2::framework::LevelInfo info = v.GetLevelInfo();

    ASSERT_EQ(2u, info.GetLevelCount());
    ASSERT_EQ(2u, info.levelMetas.size());

    ASSERT_EQ(0, info.levelMetas[0].levelIdx);
    indexlibv2::framework::LevelTopology topo = info.levelMetas[0].topology;
    ASSERT_EQ(indexlibv2::framework::topo_sequence, topo);
    ASSERT_EQ(1, info.levelMetas[1].levelIdx);
    topo = info.levelMetas[1].topology;
    ASSERT_EQ(indexlibv2::framework::topo_hash_mod, info.levelMetas[1].topology);
}

void VersionTest::TestCaseForAddSegment()
{
    Version v;
    v.AddSegment(1);
    v.AddSegment(3);
    v.SetVersionId(10);

    indexlibv2::framework::LevelInfo info = v.GetLevelInfo();
    ASSERT_EQ(1u, info.GetLevelCount());
    ASSERT_EQ(1u, info.levelMetas.size());

    const vector<segmentid_t>& segments = info.levelMetas[0].segments;
    vector<segmentid_t> expected = {1, 3};
    ASSERT_EQ(expected, segments);
}

void VersionTest::TestCaseForRemoveSegment()
{
    Version v;
    v.AddSegment(1);
    v.AddSegment(3);
    v.AddSegment(9);
    v.SetVersionId(10);

    v.RemoveSegment(3);
    v.RemoveSegment(30);

    indexlibv2::framework::LevelInfo info = v.GetLevelInfo();
    ASSERT_EQ(1u, info.GetLevelCount());
    ASSERT_EQ(1u, info.levelMetas.size());

    const vector<segmentid_t>& segments = info.levelMetas[0].segments;
    vector<segmentid_t> expected = {1, 9};
    ASSERT_EQ(expected, segments);
}

void VersionTest::TestFromStringWithoutLevelInfo()
{
    string str =
        R"(
    {
        "segments": [1,2],
        "versionid": 10
    })";
    Version v;
    v.FromString(str);

    indexlibv2::framework::LevelInfo info = v.GetLevelInfo();
    ASSERT_EQ(1u, info.GetLevelCount());
    ASSERT_EQ(0, info.levelMetas[0].levelIdx);
    ASSERT_EQ(v.mSegmentIds, info.levelMetas[0].segments);
    indexlibv2::framework::LevelTopology topo = info.levelMetas[0].topology;
    ASSERT_EQ(indexlibv2::framework::topo_sequence, topo);
}

void VersionTest::TestSegmentStatistics()
{
    string str = R"(
    {
        "segments": [1,2],
        "versionid": 10,
        "segment_statistics": [
            {
                "segment_id": 1,
                "integer_stats": {
                    "eventtime": [100, 200]
                }
            },
            {
                "segment_id": 2,
                "integer_stats": {
                    "eventtime": [300, 400]
                }
            }
        ]
    })";
    Version v1;
    FromJsonString(v1, str);

    auto checkSegmentStats = [](const index_base::Version& version) {
        const std::vector<indexlibv2::framework::SegmentStatistics>& segStatsVec = version.GetSegmentStatisticsVector();
        ASSERT_EQ(2, segStatsVec.size());
        auto integerStats = segStatsVec[0].GetIntegerStats();
        ASSERT_EQ(1, segStatsVec[0].GetSegmentId());
        ASSERT_EQ(1, integerStats.size());
        auto it = integerStats.find("eventtime");
        ASSERT_TRUE(it != integerStats.end());
        ASSERT_EQ((std::make_pair<int64_t, int64_t>(100, 200)), it->second);

        integerStats = segStatsVec[1].GetIntegerStats();
        ASSERT_EQ(2, segStatsVec[1].GetSegmentId());
        ASSERT_EQ(1, integerStats.size());
        it = integerStats.find("eventtime");
        ASSERT_TRUE(it != integerStats.end());
        ASSERT_EQ((std::make_pair<int64_t, int64_t>(300, 400)), it->second);
    };

    checkSegmentStats(v1);

    Version v2;
    FromJsonString(v2, ToJsonString(v1));
    checkSegmentStats(v2);

    ASSERT_TRUE(v1 == v2);

    Version v3;
    v3 = v2;
    ASSERT_TRUE(v1 == v3);
    checkSegmentStats(v3);

    Version v4(v2);
    ASSERT_TRUE(v1 == v4);
    checkSegmentStats(v4);

    string str2 = R"(
    {
        "segments": [1,2],
        "versionid": 10,
        "segment_statistics": [
            {
                "segment_id": 1,
                "integer_stats": {
                    "eventtime": [100, 201]
                }
            },
            {
                "segment_id": 2,
                "integer_stats": {
                    "eventtime": [300, 400]
                }
            }
        ]
    })";
    Version v5;
    FromJsonString(v5, str2);
    ASSERT_TRUE(v1 != v5);
}

void VersionTest::TestGetSegmentStatsBySegmentId()
{
    string str = R"(
    {
        "segments": [1,2,3,5,10,11,20],
        "versionid": 10,
        "segment_statistics": [
            {
                "segment_id": 1,
                "integer_stats": {
                    "eventtime": [100, 200]
                }
            },
            {
                "segment_id": 2,
                "integer_stats": {
                    "eventtime": [200, 300]
                }
            },
            {
                "segment_id": 3,
                "integer_stats": {
                    "eventtime": [300, 400]
                }
            },
            {
                "segment_id": 5,
                "integer_stats": {
                    "eventtime": [500, 600]
                }
            },
            {
                "segment_id": 10,
                "integer_stats": {
                    "eventtime": [1000, 1100]
                }
            },
            {
                "segment_id": 11,
                "integer_stats": {
                    "eventtime": [1100, 1200]
                }
            },
            {
                "segment_id": 20,
                "integer_stats": {
                    "eventtime": [2000, 2100]
                }
            }
        ]
    })";
    Version v1;
    FromJsonString(v1, str);

    auto CheckSegmentStats = [&v1](segmentid_t segId) {
        indexlibv2::framework::SegmentStatistics stats;
        indexlibv2::framework::SegmentStatistics::IntegerRangeType range;
        if (v1.GetSegmentStatistics(segId, &stats)) {
            if (!stats.GetStatistic("eventtime", range)) {
                return false;
            }
            return range == std::make_pair<int64_t, int64_t>(segId * 100, (segId + 1) * 100);
        }
        return false;
    };
    ASSERT_TRUE(CheckSegmentStats(1));
    ASSERT_TRUE(CheckSegmentStats(2));
    ASSERT_TRUE(CheckSegmentStats(3));
    ASSERT_FALSE(CheckSegmentStats(4));
    ASSERT_TRUE(CheckSegmentStats(5));
    ASSERT_FALSE(CheckSegmentStats(6));
    ASSERT_TRUE(CheckSegmentStats(10));
    ASSERT_TRUE(CheckSegmentStats(11));
    ASSERT_FALSE(CheckSegmentStats(19));
    ASSERT_TRUE(CheckSegmentStats(20));
    ASSERT_FALSE(CheckSegmentStats(21));
}

}} // namespace indexlib::index_base
