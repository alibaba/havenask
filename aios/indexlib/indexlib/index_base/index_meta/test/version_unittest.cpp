#include "indexlib/index_base/index_meta/test/version_unittest.h"
#include "indexlib/test/version_maker.h"
#include "indexlib/util/path_util.h"
#include "indexlib/misc/exception.h"
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(index_base);

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
        
    string buf = v1.ToString();

    Version v2;
    v2.FromString(buf);

    CheckEqual(v1, v2);
}

void VersionTest::CheckEqual(const Version& v1, const Version& v2)
{
    INDEXLIB_TEST_EQUAL(v1.GetVersionId(), v2.GetVersionId());
    INDEXLIB_TEST_EQUAL(v1.GetSegmentCount(), v2.GetSegmentCount());
    INDEXLIB_TEST_EQUAL(v1.GetTimestamp(), v2.GetTimestamp());
    INDEXLIB_TEST_EQUAL(v1.GetLevelInfo(), v2.GetLevelInfo());
    Version::Iterator iter1 = v1.CreateIterator();
    Version::Iterator iter2 = v2.CreateIterator();
    while (iter1.HasNext() && iter2.HasNext())
    {
        segmentid_t v1Seg = iter1.Next();
        segmentid_t v2Seg = iter2.Next();
        INDEXLIB_TEST_EQUAL(v1Seg, v2Seg);
    }
    INDEXLIB_TEST_TRUE(!iter1.HasNext());
    INDEXLIB_TEST_TRUE(!iter2.HasNext());
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

void VersionTest::DoTestMinus(const string& left, const string& right, 
                              const string& expect)
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
    ASSERT_THROW(Version::Validate(CreateSegmentVector("1,0")), 
                 IndexCollapsedException);
    ASSERT_THROW(Version::Validate(CreateSegmentVector("1,2,10,5")), 
                 IndexCollapsedException);
    ASSERT_THROW(Version::Validate(CreateSegmentVector("1,10,5,6")), 
                 IndexCollapsedException);
    ASSERT_THROW(Version::Validate(CreateSegmentVector("1,1")), 
                 IndexCollapsedException);
    ASSERT_THROW(Version::Validate(CreateSegmentVector("1,2,2,10")), 
                 IndexCollapsedException);
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
    ASSERT_THROW(version.Store(rootDir->GetPath(), true), InvalidVersionException);
    
    version.SetVersionId(0);
    version.AddSegment(1);
    
    version.Store(rootDir, true);
    const file_system::IndexlibFileSystemPtr& fileSystem = GET_FILE_SYSTEM();
    string fileName = util::PathUtil::JoinPath(rootDir->GetPath(), "version.0");
    ASSERT_TRUE(fileSystem->IsExist(fileName));
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

    LevelInfo info = v.GetLevelInfo();

    ASSERT_EQ(2u, info.GetLevelCount());
    ASSERT_EQ(2u, info.levelMetas.size());

    ASSERT_EQ(0, info.levelMetas[0].levelIdx);
    LevelTopology topo = info.levelMetas[0].topology;
    ASSERT_EQ(topo_sequence, topo);
    ASSERT_EQ(1, info.levelMetas[1].levelIdx);
    topo = info.levelMetas[1].topology;
    ASSERT_EQ(topo_hash_mod, info.levelMetas[1].topology);
}

void VersionTest::TestCaseForAddSegment()
{
    Version v;
    v.AddSegment(1);
    v.AddSegment(3);
    v.SetVersionId(10);

    LevelInfo info = v.GetLevelInfo();
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

    LevelInfo info = v.GetLevelInfo();
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

    LevelInfo info = v.GetLevelInfo();
    ASSERT_EQ(1u, info.GetLevelCount());
    ASSERT_EQ(0, info.levelMetas[0].levelIdx);
    ASSERT_EQ(v.mSegmentIds, info.levelMetas[0].segments);
    LevelTopology topo = info.levelMetas[0].topology;
    ASSERT_EQ(topo_sequence, topo);
}

IE_NAMESPACE_END(index_base);
