#include "indexlib/framework/VersionCleaner.h"

#include <sstream>

#include "autil/EnvUtil.h"
#include "autil/StringTokenizer.h"
#include "indexlib/file_system/EntryTable.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/framework/Fence.h"
#include "indexlib/framework/Segment.h"
#include "indexlib/framework/SegmentFenceDirFinder.h"
#include "indexlib/framework/TabletId.h"
#include "unittest/unittest.h"

using namespace std;

namespace indexlibv2 { namespace framework {

class FakeSegmentFenceDirFinder : public framework::SegmentFenceDirFinder
{
public:
    FakeSegmentFenceDirFinder() {}
    virtual ~FakeSegmentFenceDirFinder() {}
    void AddSegmentFenceDir(segmentid_t segId, const std::string& fenceDir) { _map[segId] = fenceDir; }
    Status Init(const std::string& rootDir, const framework::Version& version) override { return Status::OK(); }
    std::pair<Status, std::string> GetFenceDir(segmentid_t segId) override
    {
        auto iter = _map.find(segId);
        if (iter == _map.end()) {
            return {Status::NotFound(), ""};
        }
        return {Status::OK(), iter->second};
    }

private:
    std::map<segmentid_t, std::string> _map;
};

class FakeVersionCleaner : public VersionCleaner
{
public:
    FakeVersionCleaner() {}
    ~FakeVersionCleaner() {}
    void AddSegmentFenceDirFinder(versionid_t vid, std::shared_ptr<framework::SegmentFenceDirFinder> finder)
    {
        _map[vid] = finder;
    }
    std::pair<Status, std::shared_ptr<framework::SegmentFenceDirFinder>>
    CreateSegmentFenceFinder(const std::string& root, const framework::Version& version) const override
    {
        auto iter = _map.find(version.GetVersionId());
        if (iter == _map.end()) {
            return {Status::NotFound(), nullptr};
        }
        return {Status::OK(), iter->second};
    }

private:
    std::map<versionid_t, std::shared_ptr<framework::SegmentFenceDirFinder>> _map;
};

class VersionCleanerTest : public TESTBASE
{
public:
    void setUp() override
    {
        _rootDir = indexlib::file_system::Directory::GetPhysicalDirectory(GET_TEMP_DATA_PATH())->GetIDirectory();
        int64_t currentTs = autil::TimeUtility::currentTimeInMicroSeconds();

        indexlib::framework::TabletId tid0;
        tid0.SetRange(0, 32767);
        indexlib::framework::TabletId tid1;
        tid1.SetRange(32768, 65535);

        fence_0_0_32767 = framework::Fence::GenerateNewFenceName(/*isPublicFence=*/true, currentTs, tid0);
        fence_1_0_32767 = framework::Fence::GenerateNewFenceName(/*isPublicFence=*/true, currentTs + 1, tid0);
        fence_2_0_32767 = framework::Fence::GenerateNewFenceName(/*isPublicFence=*/true, currentTs + 2, tid0);
        fence_3_0_32767 = framework::Fence::GenerateNewFenceName(/*isPublicFence=*/true, currentTs + 3, tid0);
        fence_1_32768_65535 = framework::Fence::GenerateNewFenceName(/*isPublicFence=*/true, currentTs + 1, tid1);
        fence_2_32768_65535 = framework::Fence::GenerateNewFenceName(/*isPublicFence=*/true, currentTs + 2, tid1);
    }
    void tearDown() override {}

private:
    enum Mode { BUILD = 0, MERGE = 1, BUILD_PRIVATE = 2 };
    versionid_t GetPublicVersionId(versionid_t id) const;
    versionid_t GetPrivateVersionId(versionid_t id) const;
    // use # as delimiter is public version,
    // use * as delimiter is private version
    //"mergeSeg1,mergeSeg2#buildSeg1,buildSeg2;mergeSeg3*buildSeg3..."
    void prepareIndex(const std::string& versionsStr, const std::vector<int64_t> versionCommitTimes,
                      const std::string fenceDir, FakeVersionCleaner& cleaner);
    void prepareSegments(const std::string& segStr, bool isBuildSegment, const std::string& fenceDir,
                         framework::Version& version, std::shared_ptr<FakeSegmentFenceDirFinder>& finder);
    bool IsVersionFileExist(versionid_t versionId, Mode mode = Mode::MERGE, std::string fenceDir = "");
    bool IsFenceDirExist(const std::string& fenceDir);
    bool IsSegmentExist(segmentid_t segmentId, Mode mode = Mode::MERGE, std::string fenceDir = "");

private:
    std::shared_ptr<indexlib::file_system::IDirectory> _rootDir;
    std::string GetBuildSegmentDir(segmentid_t segId);

    void AssertVersionsExist(Mode which, const std::vector<versionid_t>& versions, std::string fenceDir = "");
    void AssertVersionsNotExist(Mode which, const std::vector<versionid_t>& versions, std::string fenceDir = "");
    void AssertFenceExist(const std::string& fenceDir);
    void AssertSegmentsExist(Mode which, const std::vector<segmentid_t>& segments, std::string fenceDir = "");
    void AssertSegmentsNotExist(Mode which, const std::vector<segmentid_t>& segments, std::string fenceDir = "");
    void StoreVersion(const framework::Version& version,
                      const std::shared_ptr<indexlib::file_system::IDirectory>& fenceDir,
                      bool ignoreEmptyVersion = true);

    std::string fence_0_0_32767;
    std::string fence_1_0_32767;
    std::string fence_2_0_32767;
    std::string fence_3_0_32767;
    std::string fence_1_32768_65535;
    std::string fence_2_32768_65535;

    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(indexlib.framework, VersionCleanerTest);

void VersionCleanerTest::StoreVersion(const framework::Version& version,
                                      const std::shared_ptr<indexlib::file_system::IDirectory>& fenceDir,
                                      bool ignoreEmptyVersion)
{
    if (ignoreEmptyVersion && version.GetSegmentCount() == 0) {
        return;
    }
    std::string content = version.ToString();
    std::string versionFileName = VERSION_FILE_NAME_PREFIX + "." + std::to_string(version.GetVersionId());
    auto result = fenceDir->Store(versionFileName, content, indexlib::file_system::WriterOption::AtomicDump());
    ASSERT_TRUE(result.Status().IsOK());

    std::string deployMetaFileName =
        std::string(DEPLOY_META_FILE_NAME_PREFIX) + "." + std::to_string(version.GetVersionId());
    result = fenceDir->Store(deployMetaFileName, "", indexlib::file_system::WriterOption::AtomicDump());
    ASSERT_TRUE(result.Status().IsOK());

    std::string entryTableName = indexlib::file_system::EntryTable::GetEntryTableFileName(version.GetVersionId());
    result = fenceDir->Store(entryTableName, "", indexlib::file_system::WriterOption::AtomicDump());
    ASSERT_TRUE(result.Status().IsOK());
}

void VersionCleanerTest::prepareSegments(const std::string& segStr, bool isBuildSegment, const std::string& fenceDir,
                                         framework::Version& version,
                                         std::shared_ptr<FakeSegmentFenceDirFinder>& finder)
{
    autil::StringTokenizer st2(segStr, ",",
                               autil::StringTokenizer::TOKEN_IGNORE_EMPTY | autil::StringTokenizer::TOKEN_TRIM);

    for (size_t j = 0; j < st2.getNumTokens(); ++j) {
        segmentid_t segmentId;
        autil::StringUtil::strToInt32(st2[j].c_str(), segmentId);
        if (isBuildSegment) {
            segmentId = segmentId | framework::Segment::PUBLIC_SEGMENT_ID_MASK;
        }
        version.AddSegment(segmentId);
        finder->AddSegmentFenceDir(segmentId, fenceDir);
        std::string absPath = version.GetSegmentDirName(segmentId) + "/";
        if (isBuildSegment) {
            std::string fenceName = version.GetFenceName();
            absPath = fenceName + "/" + absPath;
        }
        if (_rootDir->IsExist(absPath).result) {
            continue;
        }
        indexlib::file_system::DirectoryOption option;
        auto result = _rootDir->MakeDirectory(absPath, option);
        ASSERT_TRUE(result.Status().IsOK());
    }
}

void VersionCleanerTest::prepareIndex(const std::string& versionsStr, const std::vector<int64_t> versionCommitTimes,
                                      std::string fenceDir, FakeVersionCleaner& cleaner)
{
    versionid_t versionId = 0;
    autil::StringTokenizer st(versionsStr, ";", autil::StringTokenizer::TOKEN_TRIM);
    auto [status, fenceDirectory] =
        _rootDir->MakeDirectory(fenceDir, indexlib::file_system::DirectoryOption()).StatusWith();
    int32_t idx = 0;
    for (size_t i = 0; i < st.getNumTokens(); ++i) {
        std::shared_ptr<FakeSegmentFenceDirFinder> finder(new FakeSegmentFenceDirFinder());
        versionid_t targetVersionId = versionId++;
        if (st[i].empty()) {
            continue;
        }
        framework::Version version(targetVersionId);
        version.SetFenceName(fenceDir);
        std::string split = "#";
        bool isPrivateVersion = false;
        if (st[i].find("*") != std::string::npos) {
            split = "*";
            isPrivateVersion = true;
        }
        autil::StringTokenizer st2(st[i], split, autil::StringTokenizer::TOKEN_TRIM);
        if (st2.getNumTokens() == 2) {
            prepareSegments(st2[0], false, "", version, finder);
            prepareSegments(st2[1], true, fenceDir, version, finder);
            versionid_t mask = framework::Version::PUBLIC_VERSION_ID_MASK;
            if (isPrivateVersion) {
                mask = framework::Version::PRIVATE_VERSION_ID_MASK;
            }
            targetVersionId = targetVersionId | mask;
            version.SetVersionId(targetVersionId);
        } else {
            ASSERT_TRUE(st2.getNumTokens() == 1);
            prepareSegments(st2[0], false, fenceDir, version, finder);
        }
        if (idx < versionCommitTimes.size()) {
            version.SetCommitTime(versionCommitTimes[idx++]);
        }
        cleaner.AddSegmentFenceDirFinder(targetVersionId, finder);
        if (!isPrivateVersion) {
            StoreVersion(version, _rootDir);
        }
        if (!fenceDir.empty()) {
            StoreVersion(version, fenceDirectory);
        }
    }
}

bool VersionCleanerTest::IsVersionFileExist(versionid_t versionId, Mode mode, std::string fenceDir)
{
    if (mode == Mode::BUILD) {
        versionId |= framework::Version::PUBLIC_VERSION_ID_MASK;
    } else if (mode == Mode::BUILD_PRIVATE) {
        versionId |= framework::Version::PRIVATE_VERSION_ID_MASK;
    }
    std::stringstream ss;
    ss << VERSION_FILE_NAME_PREFIX << "." << versionId;
    if (mode != Mode::BUILD_PRIVATE) {
        if (!_rootDir->IsExist(ss.str()).result) {
            return false;
        }
    }
    if (fenceDir.empty()) {
        return true;
    }
    std::stringstream ss1;
    ss1 << fenceDir << "/" << VERSION_FILE_NAME_PREFIX << "." << versionId;
    return _rootDir->IsExist(ss1.str()).result;
}

bool VersionCleanerTest::IsFenceDirExist(const std::string& fenceDir)
{
    if (fenceDir.empty()) {
        return false;
    }
    return _rootDir->IsExist(fenceDir).result;
}

bool VersionCleanerTest::IsSegmentExist(segmentid_t segmentId, Mode mode, std::string fenceDir)
{
    if (mode == Mode::BUILD || mode == Mode::BUILD_PRIVATE) {
        segmentId |= framework::Segment::PUBLIC_SEGMENT_ID_MASK;
    }
    std::stringstream ss;
    ss << fenceDir << "/" << SEGMENT_FILE_NAME_PREFIX << "_" << segmentId << "_level_0/";
    return _rootDir->IsExist(ss.str()).result;
}

versionid_t VersionCleanerTest::GetPublicVersionId(versionid_t id) const
{
    return id | framework::Version::PUBLIC_VERSION_ID_MASK;
}

versionid_t VersionCleanerTest::GetPrivateVersionId(versionid_t id) const
{
    return id | framework::Version::PRIVATE_VERSION_ID_MASK;
}

void VersionCleanerTest::AssertVersionsExist(Mode which, const std::vector<versionid_t>& versions, std::string fenceDir)
{
    for (auto versionId : versions) {
        ASSERT_TRUE(IsVersionFileExist(versionId, which, fenceDir))
            << "versionId:" << versionId << " which:" << which << " " << fenceDir;
    }
}

void VersionCleanerTest::AssertVersionsNotExist(Mode which, const std::vector<versionid_t>& versions,
                                                std::string fenceDir)
{
    for (auto versionId : versions) {
        ASSERT_FALSE(IsVersionFileExist(versionId, which, fenceDir))
            << "versionId:" << versionId << " which:" << which << " " << fenceDir;
    }
}

void VersionCleanerTest::AssertFenceExist(const std::string& fenceDir)
{
    ASSERT_TRUE(IsFenceDirExist(fenceDir)) << fenceDir;
}

void VersionCleanerTest::AssertSegmentsExist(Mode which, const std::vector<segmentid_t>& segments, std::string fenceDir)
{
    for (auto segmentId : segments) {
        ASSERT_TRUE(IsSegmentExist(segmentId, which, fenceDir))
            << "segmentId:" << segmentId << " which:" << which << " " << fenceDir;
    }
}
void VersionCleanerTest::AssertSegmentsNotExist(Mode which, const std::vector<segmentid_t>& segments,
                                                std::string fenceDir)
{
    for (auto segmentId : segments) {
        ASSERT_FALSE(IsSegmentExist(segmentId, which, fenceDir))
            << "segmentId:" << segmentId << " which:" << which << " " << fenceDir;
    }
}

std::string VersionCleanerTest::GetBuildSegmentDir(segmentid_t segId)
{
    std::stringstream ss;
    ss << "segment_" << (segId | framework::Segment::PUBLIC_SEGMENT_ID_MASK) << "_level_0";
    return ss.str();
}

TEST_F(VersionCleanerTest, TestKeepMergeSegmentByCommitTime)
{
    FakeVersionCleaner cleaner;
    std::string mergeVersionStr = ";1;2";
    std::string versionsStr = "#0,1,2,3;0#4;#4,5,6;";
    prepareIndex(mergeVersionStr, /*versionCommitTime=*/ {2, 4}, "", cleaner);
    prepareIndex(versionsStr, /*versionCommitTime=*/ {1, 1, 3}, fence_1_0_32767, cleaner);
    VersionCleaner::VersionCleanerOptions options;
    options.keepVersionCount = 2;
    options.currentMaxVersionId = GetPublicVersionId(2);
    options.fenceTsTolerantDeviation = 0;
    ASSERT_TRUE(cleaner.Clean(_rootDir, options).IsOK());

    AssertVersionsExist(Mode::MERGE, {1, 2});
    AssertVersionsExist(Mode::BUILD, {1, 2}, fence_1_0_32767);
    AssertVersionsNotExist(Mode::BUILD, {0}, fence_1_0_32767);

    AssertSegmentsExist(Mode::MERGE, {0, 1, 2});
    AssertSegmentsExist(Mode::BUILD, {4, 5, 6}, fence_1_0_32767);
    AssertSegmentsNotExist(Mode::BUILD, {0, 1, 2, 3}, fence_1_0_32767);
}

TEST_F(VersionCleanerTest, TestRemoveMergeSegmentByCommitTime)
{
    FakeVersionCleaner cleaner;
    std::string mergeVersionStr = ";1;2";
    std::string versionsStr = "#0,1,2,3;3#4;#4,5,6;";
    prepareIndex(mergeVersionStr, /*versionCommitTime=*/ {2, 4}, "", cleaner);
    prepareIndex(versionsStr, /*versionCommitTime=*/ {1, 4, 6}, fence_1_0_32767, cleaner);
    VersionCleaner::VersionCleanerOptions options;
    options.keepVersionCount = 2;
    options.currentMaxVersionId = GetPublicVersionId(2);
    options.fenceTsTolerantDeviation = 0;
    ASSERT_TRUE(cleaner.Clean(_rootDir, options).IsOK());

    AssertVersionsExist(Mode::MERGE, {2});
    AssertVersionsNotExist(Mode::MERGE, {1});
    AssertVersionsExist(Mode::BUILD, {1, 2}, fence_1_0_32767);
    AssertVersionsNotExist(Mode::BUILD, {0}, fence_1_0_32767);

    AssertSegmentsNotExist(Mode::MERGE, {1});
    AssertSegmentsExist(Mode::MERGE, {2});
    AssertSegmentsExist(Mode::BUILD, {4, 5, 6}, fence_1_0_32767);
    AssertSegmentsNotExist(Mode::BUILD, {0, 1, 2, 3}, fence_1_0_32767);
}

TEST_F(VersionCleanerTest, TestMergeSegmentNotInBuildVersion)
{
    FakeVersionCleaner cleaner;
    std::string mergeVersionStr = "0;1";
    std::string versionsStr = "#0,1,2,3;#4;#4,5,6;";
    prepareIndex(mergeVersionStr, /*versionCommitTime=*/ {0, 1}, "", cleaner);
    prepareIndex(versionsStr, /*versionCommitTime=*/ {1, 2, 3}, fence_1_0_32767, cleaner);
    VersionCleaner::VersionCleanerOptions options;
    options.keepVersionCount = 2;
    options.currentMaxVersionId = GetPublicVersionId(2);
    options.fenceTsTolerantDeviation = 0;
    ASSERT_TRUE(cleaner.Clean(_rootDir, options).IsOK());

    AssertVersionsExist(Mode::MERGE, {0, 1});
    AssertVersionsExist(Mode::BUILD, {1, 2}, fence_1_0_32767);
    AssertVersionsNotExist(Mode::BUILD, {0}, fence_1_0_32767);

    AssertSegmentsExist(Mode::MERGE, {0, 1});
    AssertSegmentsExist(Mode::BUILD, {4, 5, 6}, fence_1_0_32767);
    AssertSegmentsNotExist(Mode::BUILD, {0, 1, 2, 3}, fence_1_0_32767);
}

TEST_F(VersionCleanerTest, TestCaseForCleanVersionsWithBuildMergeSegment)
{
    FakeVersionCleaner cleaner;
    std::string mergeVersionStr = "0";
    std::string versionsStr = "#0,1,2,3;0#4;0#4,5,6;";
    prepareIndex(mergeVersionStr, /*versionCommitTime=*/ {0}, "", cleaner);
    prepareIndex(versionsStr, /*versionCommitTime=*/ {1, 2, 3}, fence_1_0_32767, cleaner);
    VersionCleaner::VersionCleanerOptions options;
    options.keepVersionCount = 2;
    options.currentMaxVersionId = GetPublicVersionId(2);
    options.fenceTsTolerantDeviation = 0;
    ASSERT_TRUE(cleaner.Clean(_rootDir, options).IsOK());
    AssertVersionsExist(Mode::MERGE, {0});
    AssertVersionsExist(Mode::BUILD, {1, 2}, fence_1_0_32767);
    AssertVersionsNotExist(Mode::BUILD, {0}, fence_1_0_32767);

    AssertSegmentsExist(Mode::MERGE, {0});
    AssertSegmentsExist(Mode::BUILD, {4, 5, 6}, fence_1_0_32767);
    AssertSegmentsNotExist(Mode::BUILD, {0, 1, 2, 3}, fence_1_0_32767);
}

TEST_F(VersionCleanerTest, TestBuildVersionNoMergeSegment)
{
    FakeVersionCleaner cleaner;
    std::string mergeVersionStr = "0,1,2,3;0,4;";
    prepareIndex(mergeVersionStr, /*versionCommitTime=*/ {0, 1}, "", cleaner);
    std::string versionsStr = "#0;#1;";
    prepareIndex(versionsStr, /*versionCommitTime=*/ {2, 3}, fence_1_0_32767, cleaner);
    VersionCleaner::VersionCleanerOptions options;
    options.keepVersionCount = 2;
    options.currentMaxVersionId = GetPublicVersionId(1);
    options.fenceTsTolerantDeviation = 0;
    ASSERT_TRUE(cleaner.Clean(_rootDir, options).IsOK());
    AssertVersionsExist(Mode::MERGE, {0, 1});
    AssertVersionsExist(Mode::BUILD, {0, 1}, fence_1_0_32767);

    AssertSegmentsExist(Mode::MERGE, {0, 1, 2, 3, 4});
    AssertSegmentsExist(Mode::BUILD, {0, 1}, fence_1_0_32767);
}

TEST_F(VersionCleanerTest, TestSmallerMaxVersionId)
{
    FakeVersionCleaner cleaner;
    std::string versionsStr = "0;1,2,3;4";
    prepareIndex(versionsStr, /*versionCommitTime=*/ {0, 2, 4}, "", cleaner);
    versionsStr = "0#4;1,2,3#5;4#5";
    prepareIndex(versionsStr, /*versionCommitTime=*/ {1, 3, 5}, fence_1_0_32767, cleaner);
    versionsStr = ";;;1,2,3#5;";
    prepareIndex(versionsStr, /*versionCommitTime=*/ {6, 6, 6, 6}, fence_2_0_32767, cleaner);
    VersionCleaner::VersionCleanerOptions options;
    options.keepVersionCount = 1;
    options.fenceTsTolerantDeviation = 0;
    options.currentMaxVersionId = GetPublicVersionId(2);
    std::set<VersionCoord> reservedVersionSet = {VersionCoord(GetPublicVersionId(3), "")};
    ASSERT_TRUE(cleaner.Clean(_rootDir, options, reservedVersionSet).IsOK());
    AssertVersionsNotExist(Mode::MERGE, {0, 1});
    AssertVersionsExist(Mode::MERGE, {2});
    AssertVersionsExist(Mode::BUILD, {3}, fence_2_0_32767);
    AssertVersionsExist(Mode::BUILD, {2}, fence_1_0_32767);
    AssertVersionsNotExist(Mode::BUILD, {0, 1}, fence_1_0_32767);

    AssertSegmentsExist(Mode::MERGE, {1, 2, 3, 4});
    AssertSegmentsNotExist(Mode::MERGE, {0});
    AssertSegmentsExist(Mode::BUILD, {5}, fence_1_0_32767);
    AssertSegmentsExist(Mode::BUILD, {5}, fence_2_0_32767);
}

TEST_F(VersionCleanerTest, TestMergeSegmentClean)
{
    FakeVersionCleaner cleaner;
    std::string versionsStr = "0;1,2,3;4";
    prepareIndex(versionsStr, /*versionCommitTime=*/ {0, 2, 4}, "", cleaner);
    versionsStr = "0#4;1,2,3#5;";
    prepareIndex(versionsStr, /*versionCommitTime=*/ {1, 3}, fence_1_0_32767, cleaner);
    versionsStr = ";;4#7;";
    prepareIndex(versionsStr, /*versionCommitTime=*/ {5, 5, 5}, fence_2_0_32767, cleaner);
    VersionCleaner::VersionCleanerOptions options;
    options.keepVersionCount = 1;
    options.fenceTsTolerantDeviation = 0;
    options.currentMaxVersionId = GetPublicVersionId(2);
    std::set<VersionCoord> reservedVersionSet = {VersionCoord(GetPublicVersionId(0), "")};
    ASSERT_TRUE(cleaner.Clean(_rootDir, options, reservedVersionSet).IsOK());
    AssertVersionsNotExist(Mode::MERGE, {0, 1});
    AssertVersionsExist(Mode::MERGE, {2});
    AssertVersionsExist(Mode::BUILD, {2}, fence_2_0_32767);
    AssertVersionsExist(Mode::BUILD, {0}, fence_1_0_32767);
    AssertVersionsNotExist(Mode::BUILD, {1}, fence_1_0_32767);

    AssertSegmentsExist(Mode::MERGE, {0, 4});
    AssertSegmentsNotExist(Mode::MERGE, {1, 2, 3});
    AssertSegmentsExist(Mode::BUILD, {4}, fence_1_0_32767);
    AssertSegmentsExist(Mode::BUILD, {7}, fence_2_0_32767);
}

TEST_F(VersionCleanerTest, TestCleanBuildAndMergeVersions)
{
    FakeVersionCleaner cleaner;
    std::string mergeVersion = "0,1,2,3;0,4;5;";
    prepareIndex(mergeVersion, /*versionCommitTime=*/ {0, 2, 4}, "", cleaner);
    std::string versionsStr = "#4;0,4#5;#6;#7;";
    prepareIndex(versionsStr, /*versionCommitTime=*/ {3, 5, 6, 7}, fence_2_0_32767, cleaner);
    VersionCleaner::VersionCleanerOptions options;
    options.keepVersionCount = 3;
    options.fenceTsTolerantDeviation = 0;
    options.currentMaxVersionId = GetPublicVersionId(3);
    ASSERT_TRUE(cleaner.Clean(_rootDir, options).IsOK());
    AssertVersionsExist(Mode::MERGE, {1, 2});
    AssertVersionsNotExist(Mode::MERGE, {0});
    AssertVersionsExist(Mode::BUILD, {1, 2, 3}, fence_2_0_32767);
    AssertVersionsNotExist(Mode::BUILD, {0}, fence_2_0_32767);

    AssertSegmentsExist(Mode::MERGE, {0, 4, 5});
    AssertSegmentsNotExist(Mode::MERGE, {1, 2, 3});
    AssertSegmentsExist(Mode::BUILD, {5, 6, 7}, fence_2_0_32767);
}

TEST_F(VersionCleanerTest, TestCaseForCleanVersionsWithNoClean)
{
    FakeVersionCleaner cleaner;
    std::string versionsStr = "#0,1,2,3;#0,4;#0,4,5,6;";
    prepareIndex(versionsStr, /*versionCommitTime=*/ {0, 1, 2}, fence_1_0_32767, cleaner);
    VersionCleaner::VersionCleanerOptions options;
    options.keepVersionCount = 3;
    options.currentMaxVersionId = GetPublicVersionId(2);
    options.fenceTsTolerantDeviation = 0;
    ASSERT_TRUE(cleaner.Clean(_rootDir, options).IsOK());
    AssertVersionsExist(Mode::BUILD, {0, 1, 2}, fence_1_0_32767);
    AssertSegmentsExist(Mode::BUILD, {0, 1, 2, 3, 4, 5, 6}, fence_1_0_32767);
}

TEST_F(VersionCleanerTest, TestCaseForCleanVersionsWithInvalidKeepCount)
{
    FakeVersionCleaner cleaner;
    std::string versionsStr = "#0,1,2,3;#0,4;#3,4,5,6;";
    prepareIndex(versionsStr, /*versionCommitTime=*/ {0, 1, 2}, fence_1_0_32767, cleaner);
    VersionCleaner::VersionCleanerOptions options;
    options.keepVersionCount = 0;
    options.currentMaxVersionId = GetPublicVersionId(2);
    ASSERT_TRUE(cleaner.Clean(_rootDir, options).IsOK());
    AssertVersionsExist(Mode::BUILD, {2}, fence_1_0_32767);
    AssertVersionsNotExist(Mode::BUILD, {0, 1}, fence_1_0_32767);

    AssertSegmentsExist(Mode::BUILD, {3, 4, 5, 6}, fence_1_0_32767);
    AssertSegmentsNotExist(Mode::BUILD, {0, 1, 2}, fence_1_0_32767);
}

TEST_F(VersionCleanerTest, TestCaseForCleanVersionsWithIncontinuous)
{
    FakeVersionCleaner cleaner;
    std::string versionsStr = "#0,1,2,3;;#0,5;#5,6,7;";
    prepareIndex(versionsStr, /*versionCommitTime=*/ {0, 1, 2, 3}, fence_1_0_32767, cleaner);
    VersionCleaner::VersionCleanerOptions options;
    options.keepVersionCount = 2;
    options.currentMaxVersionId = GetPublicVersionId(3);
    options.fenceTsTolerantDeviation = 0;
    ASSERT_TRUE(cleaner.Clean(_rootDir, options).IsOK());
    AssertVersionsExist(Mode::BUILD, {2, 3}, fence_1_0_32767);
    AssertVersionsNotExist(Mode::BUILD, {0, 1}, fence_1_0_32767);

    AssertSegmentsExist(Mode::BUILD, {0, 5, 6, 7}, fence_1_0_32767);
    AssertSegmentsNotExist(Mode::BUILD, {1, 2, 3}, fence_1_0_32767);
}

TEST_F(VersionCleanerTest, TestCaseForCleanVersionsWithManyIncontinuous)
{
    FakeVersionCleaner cleaner;
    std::string versionsStr = "#0,1,4,5;;#0,4,5;;#0,4;;#0,6;#5,6,7;";
    prepareIndex(versionsStr, /*versionCommitTime=*/ {0, 1, 2, 3, 4, 5, 6, 7}, fence_1_0_32767, cleaner);
    VersionCleaner::VersionCleanerOptions options;
    options.keepVersionCount = 3;
    options.currentMaxVersionId = GetPublicVersionId(7);
    options.fenceTsTolerantDeviation = 0;
    ASSERT_TRUE(cleaner.Clean(_rootDir, options).IsOK());
    AssertVersionsExist(Mode::BUILD, {4, 6, 7}, fence_1_0_32767);
    AssertVersionsNotExist(Mode::BUILD, {0, 1, 2, 3, 5}, fence_1_0_32767);

    AssertSegmentsExist(Mode::BUILD, {0, 4, 5, 6, 7}, fence_1_0_32767);
    AssertSegmentsNotExist(Mode::BUILD, {1}, fence_1_0_32767);
}

TEST_F(VersionCleanerTest, TestCaseForCleanVersionsWithInvalidSegments)
{
    FakeVersionCleaner cleaner;
    std::string versionsStr = "#0,1;#2,3;;#7,8,9;";
    std::string versionsStr2 = ";;;;#11,12;";
    prepareIndex(versionsStr, /*versionCommitTime=*/ {0, 1, 2, 3}, fence_2_0_32767, cleaner);
    prepareIndex(versionsStr2, /*versionCommitTime=*/ {5, 5, 5, 5, 5}, fence_3_0_32767, cleaner);

    indexlib::file_system::DirectoryOption option;
    ASSERT_TRUE(_rootDir->MakeDirectory(fence_1_0_32767 + '/' + GetBuildSegmentDir(4), option).Status().IsOK());
    ASSERT_TRUE(_rootDir->MakeDirectory(fence_1_0_32767 + '/' + GetBuildSegmentDir(5), option).Status().IsOK());
    ASSERT_TRUE(_rootDir->MakeDirectory(fence_2_0_32767 + '/' + GetBuildSegmentDir(10), option).Status().IsOK());
    VersionCleaner::VersionCleanerOptions options;
    options.keepVersionCount = 2;
    options.fenceTsTolerantDeviation = 0;
    options.currentMaxVersionId = GetPublicVersionId(3);
    ASSERT_TRUE(cleaner.Clean(_rootDir, options).IsOK());
    AssertVersionsExist(Mode::BUILD, {1, 3}, fence_2_0_32767);
    AssertVersionsNotExist(Mode::BUILD, {0, 2}, fence_2_0_32767);

    AssertSegmentsExist(Mode::BUILD, {2, 3, 7, 8, 9, 10}, fence_2_0_32767);
    AssertSegmentsNotExist(Mode::BUILD, {0, 1, 4, 5}, fence_2_0_32767);
    AssertSegmentsExist(Mode::BUILD, {11, 12}, fence_3_0_32767);
    ASSERT_FALSE(_rootDir->IsExist(fence_1_0_32767).result);
}

TEST_F(VersionCleanerTest, TestForCleanVersionInDiffFenceDirWithSameSegment)
{
    FakeVersionCleaner cleaner;
    std::string versionsStr = "#0,1;#2,3;#4,5";
    prepareIndex(versionsStr, /*versionCommitTime=*/ {0, 1, 2}, fence_1_0_32767, cleaner);
    versionsStr = ";;;#2,3;#4,5;#6,7";
    prepareIndex(versionsStr, /*versionCommitTime=*/ {0, 1, 2, 3, 4, 5}, fence_2_0_32767, cleaner);
    VersionCleaner::VersionCleanerOptions options;
    options.keepVersionCount = 2;
    options.currentMaxVersionId = GetPublicVersionId(5);
    options.fenceTsTolerantDeviation = 0;
    ASSERT_TRUE(cleaner.Clean(_rootDir, options).IsOK());
    AssertVersionsExist(Mode::BUILD, {4, 5}, fence_2_0_32767);
    AssertVersionsNotExist(Mode::BUILD, {0, 1, 2, 3}, fence_2_0_32767);

    AssertSegmentsExist(Mode::BUILD, {4, 5, 6, 7}, fence_2_0_32767);
    AssertSegmentsNotExist(Mode::BUILD, {0, 1, 2, 3}, fence_2_0_32767);
    AssertSegmentsNotExist(Mode::BUILD, {0, 1, 2, 3, 4, 5}, fence_1_0_32767);
    ASSERT_TRUE(_rootDir->IsExist(fence_2_0_32767).result);
    ASSERT_FALSE(_rootDir->IsExist(fence_1_0_32767).result);
}

TEST_F(VersionCleanerTest, TestForCleanVersionInDiffFenceDirNoSameSegment)
{
    FakeVersionCleaner cleaner;
    std::string versionsStr = "#0,1;#2,3;#4,5";
    prepareIndex(versionsStr, {}, fence_1_0_32767, cleaner);
    versionsStr = ";;;#6,7;#8,9;#10,11";
    prepareIndex(versionsStr, {}, fence_2_0_32767, cleaner);
    VersionCleaner::VersionCleanerOptions options;
    options.keepVersionCount = 2;
    options.fenceTsTolerantDeviation = 0;
    options.currentMaxVersionId = GetPublicVersionId(5);
    ASSERT_TRUE(cleaner.Clean(_rootDir, options).IsOK());
    AssertVersionsExist(Mode::BUILD, {4, 5}, fence_2_0_32767);
    AssertVersionsNotExist(Mode::BUILD, {0, 1, 2, 3}, fence_2_0_32767);

    AssertSegmentsExist(Mode::BUILD, {8, 9, 10, 11}, fence_2_0_32767);
    AssertSegmentsNotExist(Mode::BUILD, {6, 7}, fence_2_0_32767);
    AssertSegmentsNotExist(Mode::BUILD, {0, 1, 2, 3, 4, 5}, fence_1_0_32767);
    ASSERT_FALSE(_rootDir->IsExist(fence_1_0_32767).result);
}

TEST_F(VersionCleanerTest, TestVersionWithBiggerVersionIdIsKept)
{
    FakeVersionCleaner cleaner;
    std::string versionsStr = "#0,1;#2,3;#5,6";
    prepareIndex(versionsStr, {}, fence_1_0_32767, cleaner);
    versionsStr = ";;;#7;#8,9";
    prepareIndex(versionsStr, {}, fence_2_0_32767, cleaner);
    VersionCleaner::VersionCleanerOptions options;
    options.keepVersionCount = 2;
    options.keepVersionHour = 0;
    options.currentMaxVersionId = GetPublicVersionId(1);
    options.fenceTsTolerantDeviation = 0;
    ASSERT_TRUE(cleaner.Clean(_rootDir, options).IsOK());
    AssertVersionsExist(Mode::BUILD, {0, 1, 2}, fence_1_0_32767);
    AssertVersionsExist(Mode::BUILD, {3, 4}, fence_2_0_32767);
    AssertSegmentsExist(Mode::BUILD, {0, 1, 2, 3, 5, 6}, fence_1_0_32767);
    AssertSegmentsExist(Mode::BUILD, {7, 8, 9}, fence_2_0_32767);
}

TEST_F(VersionCleanerTest, testIncompleteVersion)
{
    FakeVersionCleaner cleaner;
    std::string versionsStr = "#0,1;#2,3;#5,6";
    prepareIndex(versionsStr, {}, fence_1_0_32767, cleaner);
    segmentid_t segmentId = 0 | framework::Segment::PUBLIC_SEGMENT_ID_MASK;
    stringstream ss;
    ss << SEGMENT_FILE_NAME_PREFIX << "_" << segmentId << "_level_0/";
    ASSERT_TRUE(_rootDir->RemoveFile(ss.str(), indexlib::file_system::RemoveOption()).Status().IsOK());
    VersionCleaner::VersionCleanerOptions options;
    options.keepVersionCount = 1;
    options.currentMaxVersionId = GetPublicVersionId(1);
    options.fenceTsTolerantDeviation = 0;
    ASSERT_TRUE(cleaner.Clean(_rootDir, options).IsOK());
    AssertVersionsExist(Mode::BUILD, {1, 2}, fence_1_0_32767);
    AssertVersionsNotExist(Mode::BUILD, {0}, fence_1_0_32767);
    AssertSegmentsExist(Mode::BUILD, {2, 3, 5, 6}, fence_1_0_32767);
    AssertSegmentsNotExist(Mode::BUILD, {0, 1}, fence_1_0_32767);
}

TEST_F(VersionCleanerTest, TestNewSegmentIsKept)
{
    FakeVersionCleaner cleaner;
    std::string versionsStr = "#0,1;#2,3;#5,6";
    prepareIndex(versionsStr, {}, fence_1_0_32767, cleaner);
    versionsStr = ";;;#4";
    prepareIndex(versionsStr, {}, fence_2_0_32767, cleaner);
    std::stringstream ss;
    versionid_t versionId = 3 | framework::Version::PUBLIC_VERSION_ID_MASK;
    ss << VERSION_FILE_NAME_PREFIX << "." << versionId;
    ASSERT_TRUE(_rootDir->RemoveFile(ss.str(), indexlib::file_system::RemoveOption()).Status().IsOK());
    VersionCleaner::VersionCleanerOptions options;
    options.keepVersionCount = 1;
    options.currentMaxVersionId = GetPublicVersionId(1);
    options.fenceTsTolerantDeviation = 0;
    ASSERT_TRUE(cleaner.Clean(_rootDir, options).IsOK());
    AssertVersionsExist(Mode::BUILD, {1, 2}, fence_1_0_32767);
    AssertVersionsNotExist(Mode::BUILD, {0}, fence_1_0_32767);
    AssertVersionsNotExist(Mode::BUILD, {3}, fence_2_0_32767);
    AssertSegmentsExist(Mode::BUILD, {2, 3, 5, 6}, fence_1_0_32767);
    AssertSegmentsExist(Mode::BUILD, {4}, fence_2_0_32767);
}

TEST_F(VersionCleanerTest, TestMultiFenceWithDiffTabletName)
{
    FakeVersionCleaner cleaner;

    std::string mergedVersionStr = "0;1;2;3,4;";
    std::string buildVersionStr = "#0;0#1;1#2;1#2,3,4,5;";
    std::string buildVersionStr2 = ";;;;2#6;";
    std::string buildVersionStr3 = ";;;;;3,4#7;";

    prepareIndex(mergedVersionStr, /*versionCommitTime=*/ {0, 2, 4, 6}, "", cleaner);
    prepareIndex(buildVersionStr, /*versionCommitTime=*/ {1, 1, 3, 4}, fence_1_0_32767, cleaner);
    prepareIndex(buildVersionStr2, /*versionCommitTime=*/ {5, 5, 5, 5, 5}, fence_2_0_32767, cleaner);
    prepareIndex(buildVersionStr3, /*versionCommitTime=*/ {7, 7, 7, 7, 7, 7}, fence_1_32768_65535, cleaner);

    auto [status, dir1] =
        _rootDir->MakeDirectory(fence_2_32768_65535, indexlib::file_system::DirectoryOption()).StatusWith();
    ASSERT_TRUE(status.IsOK());
    auto [status2, dir2] =
        _rootDir->MakeDirectory(fence_3_0_32767, indexlib::file_system::DirectoryOption()).StatusWith();
    ASSERT_TRUE(status2.IsOK());
    auto [status3, dir3] =
        _rootDir->MakeDirectory(fence_0_0_32767, indexlib::file_system::DirectoryOption()).StatusWith();
    ASSERT_TRUE(status3.IsOK());

    VersionCleaner::VersionCleanerOptions options;
    options.currentMaxVersionId = GetPublicVersionId(5);
    options.fenceTsTolerantDeviation = 0;
    options.keepVersionCount = 1;
    std::set<VersionCoord> reservedVersionSet = {VersionCoord(GetPublicVersionId(0), fence_1_0_32767),
                                                 VersionCoord(GetPublicVersionId(3), fence_1_0_32767)};
    ASSERT_TRUE(cleaner.Clean(_rootDir, options, reservedVersionSet).IsOK());
    AssertVersionsExist(Mode::MERGE, {3});
    AssertVersionsNotExist(Mode::MERGE, {0, 1, 2});
    AssertVersionsExist(Mode::BUILD, {0, 3}, fence_1_0_32767);
    AssertVersionsNotExist(Mode::BUILD, {4}, fence_2_0_32767);
    AssertVersionsExist(Mode::BUILD, {5}, fence_1_32768_65535);
    AssertVersionsNotExist(Mode::BUILD, {1, 2}, fence_1_0_32767);

    AssertSegmentsExist(Mode::MERGE, {1, 3, 4});
    AssertSegmentsNotExist(Mode::MERGE, {0, 2});
    AssertSegmentsExist(Mode::BUILD, {0, 2, 3, 4, 5}, fence_1_0_32767);
    AssertSegmentsExist(Mode::BUILD, {7}, fence_1_32768_65535);
    ASSERT_TRUE(_rootDir->IsExist(fence_1_0_32767).result);
    ASSERT_TRUE(_rootDir->IsExist(fence_2_0_32767).result);
    ASSERT_TRUE(_rootDir->IsExist(fence_3_0_32767).result);
    ASSERT_TRUE(_rootDir->IsExist(fence_1_32768_65535).result);
    ASSERT_TRUE(_rootDir->IsExist(fence_2_32768_65535).result);
    ASSERT_FALSE(_rootDir->IsExist(fence_0_0_32767).result);
}

TEST_F(VersionCleanerTest, TestCaseForCleanVersionWithKeepOneEmptyVersion)
{
    FakeVersionCleaner cleaner;
    std::string versionsStr = "#0,1;#2,3;;#4,5";
    prepareIndex(versionsStr, {}, fence_1_0_32767, cleaner);
    framework::Version version2(GetPublicVersionId(2));
    version2.SetLastSegmentId(3 | framework::Segment::PUBLIC_SEGMENT_ID_MASK);
    version2.SetFenceName(fence_1_0_32767);
    auto [status, fenceDir] =
        _rootDir->MakeDirectory(fence_1_0_32767, indexlib::file_system::DirectoryOption()).StatusWith();
    ASSERT_TRUE(status.IsOK());
    StoreVersion(version2, fenceDir, false);
    StoreVersion(version2, _rootDir, false);
    std::shared_ptr<FakeSegmentFenceDirFinder> finder(new FakeSegmentFenceDirFinder());
    cleaner.AddSegmentFenceDirFinder(GetPublicVersionId(2), finder);
    VersionCleaner::VersionCleanerOptions options;
    options.keepVersionCount = 1;
    options.currentMaxVersionId = GetPublicVersionId(2);
    options.fenceTsTolerantDeviation = 0;
    ASSERT_TRUE(cleaner.Clean(_rootDir, options).IsOK());
    AssertVersionsExist(Mode::BUILD, {2, 3}, fence_1_0_32767);
    AssertVersionsNotExist(Mode::BUILD, {0, 1}, fence_1_0_32767);
    AssertSegmentsExist(Mode::BUILD, {4, 5}, fence_1_0_32767);
    AssertSegmentsNotExist(Mode::BUILD, {0, 1, 2, 3}, fence_1_0_32767);

    VersionCleaner::VersionCleanerOptions options2;
    options2.keepVersionCount = 1;
    options2.keepVersionHour = 0;
    options2.currentMaxVersionId = GetPublicVersionId(3);
    ASSERT_TRUE(cleaner.Clean(_rootDir, options2).IsOK());
    AssertVersionsExist(Mode::BUILD, {3}, fence_1_0_32767);
    AssertVersionsNotExist(Mode::BUILD, {0, 1, 2}, fence_1_0_32767);
    AssertSegmentsExist(Mode::BUILD, {4, 5}, fence_1_0_32767);
    AssertSegmentsNotExist(Mode::BUILD, {0, 1, 2, 3}, fence_1_0_32767);
}

TEST_F(VersionCleanerTest, TestCaseForCleanVersionWithKeepAllEmptyVersion)
{
    FakeVersionCleaner cleaner;
    std::string versionsStr = "#0,1";
    prepareIndex(versionsStr, {}, fence_1_0_32767, cleaner);
    framework::Version version1(GetPublicVersionId(1));
    version1.SetFenceName(fence_1_0_32767);
    version1.SetLastSegmentId(1 | framework::Segment::PUBLIC_SEGMENT_ID_MASK);
    auto [status, fenceDir] =
        _rootDir->MakeDirectory(fence_1_0_32767, indexlib::file_system::DirectoryOption()).StatusWith();
    ASSERT_TRUE(status.IsOK());
    StoreVersion(version1, fenceDir, false);
    StoreVersion(version1, _rootDir, false);
    std::shared_ptr<FakeSegmentFenceDirFinder> finder(new FakeSegmentFenceDirFinder());
    cleaner.AddSegmentFenceDirFinder(GetPublicVersionId(1), finder);
    framework::Version version2(GetPublicVersionId(2));
    version2.SetFenceName(fence_1_0_32767);
    version2.SetLastSegmentId(1 | framework::Segment::PUBLIC_SEGMENT_ID_MASK);
    StoreVersion(version2, fenceDir, false);
    StoreVersion(version2, _rootDir, false);
    std::shared_ptr<FakeSegmentFenceDirFinder> finder1(new FakeSegmentFenceDirFinder());
    cleaner.AddSegmentFenceDirFinder(GetPublicVersionId(2), finder1);
    VersionCleaner::VersionCleanerOptions options;
    options.keepVersionCount = 1;
    options.currentMaxVersionId = GetPublicVersionId(2);
    options.fenceTsTolerantDeviation = 0;
    ASSERT_TRUE(cleaner.Clean(_rootDir, options).IsOK());
    AssertVersionsExist(Mode::BUILD, {2}, fence_1_0_32767);
    AssertVersionsNotExist(Mode::BUILD, {0, 1}, fence_1_0_32767);
    // all segments cleaned
    AssertSegmentsNotExist(Mode::BUILD, {0, 1}, fence_1_0_32767);
}

TEST_F(VersionCleanerTest, TestCaseForCleanVersionsWithKeepVersionIdFirst)
{
    FakeVersionCleaner cleaner;
    std::string versionsStr = "#0,1,2,3;#0,4;#5,6;";
    prepareIndex(versionsStr, {}, fence_1_0_32767, cleaner);
    std::set<VersionCoord> keepVersionSet;
    keepVersionSet.insert(VersionCoord(GetPublicVersionId(0), fence_1_0_32767));
    VersionCleaner::VersionCleanerOptions options;
    options.keepVersionCount = 1;
    options.currentMaxVersionId = GetPublicVersionId(2);
    options.fenceTsTolerantDeviation = 0;
    ASSERT_TRUE(cleaner.Clean(_rootDir, options, keepVersionSet).IsOK());

    AssertVersionsExist(Mode::BUILD, {0, 2}, fence_1_0_32767);
    AssertVersionsNotExist(Mode::BUILD, {1}, fence_1_0_32767);
    AssertSegmentsExist(Mode::BUILD, {0, 1, 2, 3, 5, 6}, fence_1_0_32767);
    AssertSegmentsNotExist(Mode::BUILD, {4}, fence_1_0_32767);
}

TEST_F(VersionCleanerTest, TestCaseForCleanVersionsWithKeepVersionIdFromEnv)
{
    autil::EnvUtil::setEnv("KEEP_VERSIONS", autil::StringUtil::toString(GetPublicVersionId(0)), true);
    FakeVersionCleaner cleaner;
    std::string versionsStr = "#0,1,2,3;#0,4;#5,6;";
    prepareIndex(versionsStr, {}, fence_1_0_32767, cleaner);
    VersionCleaner::VersionCleanerOptions options;
    options.keepVersionCount = 1;
    options.currentMaxVersionId = GetPublicVersionId(2);
    options.fenceTsTolerantDeviation = 0;
    ASSERT_TRUE(cleaner.Clean(_rootDir, options).IsOK());

    AssertVersionsExist(Mode::BUILD, {0, 2}, fence_1_0_32767);
    AssertVersionsNotExist(Mode::BUILD, {1}, fence_1_0_32767);
    AssertSegmentsExist(Mode::BUILD, {0, 1, 2, 3, 5, 6}, fence_1_0_32767);
    AssertSegmentsNotExist(Mode::BUILD, {4}, fence_1_0_32767);
    autil::EnvUtil::unsetEnv("KEEP_VERSIONS");
}

TEST_F(VersionCleanerTest, TestCaseForCleanVersionsWithKeepVersionIdLast)
{
    FakeVersionCleaner cleaner;
    std::string versionsStr = "#0,1,2,3;#0,4;#5,6;";
    prepareIndex(versionsStr, {}, fence_1_0_32767, cleaner);
    std::set<VersionCoord> keepVersionSet;
    keepVersionSet.insert(VersionCoord(GetPublicVersionId(2), fence_1_0_32767));
    VersionCleaner::VersionCleanerOptions options;
    options.keepVersionCount = 1;
    options.keepVersionHour = 0;
    options.fenceTsTolerantDeviation = 0;
    options.currentMaxVersionId = GetPublicVersionId(2);
    ASSERT_TRUE(cleaner.Clean(_rootDir, options, keepVersionSet).IsOK());

    AssertVersionsExist(Mode::BUILD, {2}, fence_1_0_32767);
    AssertVersionsNotExist(Mode::BUILD, {0, 1}, fence_1_0_32767);
    AssertSegmentsExist(Mode::BUILD, {5, 6}, fence_1_0_32767);
    AssertSegmentsNotExist(Mode::BUILD, {0, 1, 2, 3, 4}, fence_1_0_32767);
}

TEST_F(VersionCleanerTest, TestCaseForCleanVersionsWithKeepVersionIdMiddle)
{
    FakeVersionCleaner cleaner;
    std::string versionsStr = "#0,1,2,3;#0,4;#5,6;";
    prepareIndex(versionsStr, {}, fence_1_0_32767, cleaner);
    std::set<VersionCoord> keepVersionSet;
    keepVersionSet.insert(VersionCoord(GetPublicVersionId(1), fence_1_0_32767));
    VersionCleaner::VersionCleanerOptions options;
    options.keepVersionCount = 1;
    options.currentMaxVersionId = GetPublicVersionId(2);
    options.fenceTsTolerantDeviation = 0;
    ASSERT_TRUE(cleaner.Clean(_rootDir, options, keepVersionSet).IsOK());

    AssertVersionsExist(Mode::BUILD, {1, 2}, fence_1_0_32767);
    AssertVersionsNotExist(Mode::BUILD, {0}, fence_1_0_32767);
    AssertSegmentsExist(Mode::BUILD, {0, 4, 5, 6}, fence_1_0_32767);
    AssertSegmentsNotExist(Mode::BUILD, {1, 2, 3}, fence_1_0_32767);
}

TEST_F(VersionCleanerTest, TestCaseForCleanVersionsForWhiteList)
{
    FakeVersionCleaner cleaner;
    std::set<VersionCoord> reservedVersionSet;
    reservedVersionSet.insert(VersionCoord(GetPublicVersionId(0), fence_1_0_32767));
    reservedVersionSet.insert(VersionCoord(GetPublicVersionId(3), fence_1_0_32767));
    // v0: 0;  v1 : 0,1  v2: 2,3 ; v3: 4;  v4:5,6,7
    std::string mergedVersionStr = "0;1;2;3";
    std::string buildVersionStr = "#0;0#0,1;1#2,3;2#4;3#5,6,7;";
    prepareIndex(mergedVersionStr, /*versionCommitTime=*/ {0, 2, 4, 6}, "", cleaner);
    prepareIndex(buildVersionStr, /*versionCommitTime=*/ {1, 1, 3, 5, 7}, fence_1_0_32767, cleaner);
    VersionCleaner::VersionCleanerOptions options;
    options.keepVersionCount = 3;
    options.currentMaxVersionId = GetPublicVersionId(4);
    options.fenceTsTolerantDeviation = 0;
    ASSERT_TRUE(cleaner.Clean(_rootDir, options, reservedVersionSet).IsOK());

    AssertVersionsExist(Mode::BUILD, {0, 2, 3, 4}, fence_1_0_32767);
    AssertVersionsNotExist(Mode::BUILD, {1}, fence_1_0_32767);
    AssertVersionsExist(Mode::MERGE, {1, 2, 3});
    AssertVersionsNotExist(Mode::MERGE, {0});

    AssertSegmentsExist(Mode::BUILD, {0, 2, 3, 4, 5, 6, 7}, fence_1_0_32767);
    AssertSegmentsNotExist(Mode::BUILD, {1}, fence_1_0_32767);
    AssertSegmentsExist(Mode::MERGE, {1, 2, 3});
    AssertSegmentsNotExist(Mode::MERGE, {0});
}

TEST_F(VersionCleanerTest, TestCleanMergeVersion)
{
    FakeVersionCleaner cleaner;
    std::set<VersionCoord> reservedVersionSet;
    reservedVersionSet.insert(VersionCoord(GetPublicVersionId(1), fence_1_0_32767));
    std::string mergedVersionStr = "0;1;2,3";
    std::string buildVersionStr = "#0;0#1;1#2,3;2,3#4;";
    prepareIndex(mergedVersionStr, /*versionCommitTime=*/ {0, 2, 4}, "", cleaner);
    prepareIndex(buildVersionStr, /*versionCommitTime=*/ {1, 1, 3, 5}, fence_1_0_32767, cleaner);
    VersionCleaner::VersionCleanerOptions options;
    options.keepVersionCount = 1;
    options.currentMaxVersionId = GetPublicVersionId(3);
    options.fenceTsTolerantDeviation = 0;
    ASSERT_TRUE(cleaner.Clean(_rootDir, options, reservedVersionSet).IsOK());

    AssertVersionsExist(Mode::BUILD, {1, 3}, fence_1_0_32767);
    AssertVersionsNotExist(Mode::BUILD, {0, 2}, fence_1_0_32767);
    AssertVersionsExist(Mode::MERGE, {2});
    AssertVersionsNotExist(Mode::MERGE, {0, 1});

    AssertSegmentsExist(Mode::BUILD, {1, 4}, fence_1_0_32767);
    AssertSegmentsNotExist(Mode::BUILD, {0, 2, 3}, fence_1_0_32767);
    AssertSegmentsExist(Mode::MERGE, {0, 2, 3});
    AssertSegmentsNotExist(Mode::MERGE, {1});
}

TEST_F(VersionCleanerTest, TestManyFence)
{
    // 3 fence dir case
    FakeVersionCleaner cleaner;

    std::string mergedVersionStr = "0;1;2;3,4;";
    std::string buildVersionStr = "#0;0#1;1#2;1#2,3,4,5;";
    std::string buildVersionStr1 = ";;;;2#6;";
    std::string buildVersionStr2 = ";;;;;3,4#7;";

    prepareIndex(mergedVersionStr, /*versionCommitTime=*/ {0, 2, 4, 6}, "", cleaner);
    prepareIndex(buildVersionStr, /*versionCommitTime=*/ {1, 1, 3, 3}, fence_1_0_32767, cleaner);
    prepareIndex(buildVersionStr1, /*versionCommitTime=*/ {5, 5, 5, 5, 5}, fence_2_0_32767, cleaner);
    prepareIndex(buildVersionStr2, /*versionCommitTime=*/ {7, 7, 7, 7, 7, 7}, fence_3_0_32767, cleaner);

    std::set<VersionCoord> reservedVersionSet;
    reservedVersionSet.insert(VersionCoord(GetPublicVersionId(1), fence_1_0_32767));
    VersionCleaner::VersionCleanerOptions options;
    options.keepVersionCount = 3;
    options.currentMaxVersionId = GetPublicVersionId(5);
    options.fenceTsTolerantDeviation = 0;
    ASSERT_TRUE(cleaner.Clean(_rootDir, options, reservedVersionSet).IsOK());
    AssertVersionsExist(Mode::BUILD, {1, 3}, fence_1_0_32767);
    AssertVersionsExist(Mode::BUILD, {4}, fence_2_0_32767);
    AssertVersionsExist(Mode::BUILD, {5}, fence_3_0_32767);
    AssertVersionsNotExist(Mode::BUILD, {0, 2}, fence_1_0_32767);
    // TODO(wanghan) fix this removed merge version file
    AssertVersionsExist(Mode::MERGE, {1, 2, 3});
    AssertVersionsNotExist(Mode::MERGE, {0});

    AssertSegmentsExist(Mode::BUILD, {1, 2, 3, 4, 5}, fence_1_0_32767);
    AssertSegmentsNotExist(Mode::BUILD, {0}, fence_1_0_32767);
    AssertSegmentsExist(Mode::BUILD, {6}, fence_2_0_32767);
    AssertSegmentsExist(Mode::BUILD, {7}, fence_3_0_32767);
    AssertSegmentsExist(Mode::MERGE, {0, 1, 2, 3, 4});

    ASSERT_TRUE(cleaner.Clean(_rootDir, options).IsOK());

    AssertVersionsExist(Mode::BUILD, {3}, fence_1_0_32767);
    AssertVersionsExist(Mode::BUILD, {4}, fence_2_0_32767);
    AssertVersionsExist(Mode::BUILD, {5}, fence_3_0_32767);
    AssertVersionsNotExist(Mode::BUILD, {0, 1, 2});
    AssertVersionsExist(Mode::MERGE, {1, 2, 3});
    AssertVersionsNotExist(Mode::MERGE, {0});

    AssertSegmentsExist(Mode::BUILD, {2, 3, 4, 5}, fence_1_0_32767);
    AssertSegmentsNotExist(Mode::BUILD, {0, 1}, fence_1_0_32767);
    AssertSegmentsExist(Mode::BUILD, {6}, fence_2_0_32767);
    AssertSegmentsExist(Mode::BUILD, {7}, fence_3_0_32767);
    AssertSegmentsExist(Mode::MERGE, {1, 2, 3, 4});
    AssertSegmentsNotExist(Mode::MERGE, {0});

    VersionCleaner::VersionCleanerOptions options2;
    options2.keepVersionCount = 2;
    options2.fenceTsTolerantDeviation = 0;
    options2.currentMaxVersionId = GetPublicVersionId(5);
    ASSERT_TRUE(cleaner.Clean(_rootDir, options2).IsOK());
    AssertVersionsExist(Mode::BUILD, {4}, fence_2_0_32767);
    AssertVersionsExist(Mode::BUILD, {5}, fence_3_0_32767);
    AssertVersionsNotExist(Mode::BUILD, {0, 1, 2, 3}, fence_1_0_32767);
    AssertVersionsExist(Mode::MERGE, {2, 3});
    AssertVersionsNotExist(Mode::MERGE, {0, 1});

    AssertSegmentsExist(Mode::BUILD, {6}, fence_2_0_32767);
    AssertSegmentsExist(Mode::BUILD, {7}, fence_3_0_32767);
    AssertSegmentsExist(Mode::MERGE, {2, 3, 4});
    AssertSegmentsNotExist(Mode::MERGE, {0, 1});
    ASSERT_FALSE(_rootDir->IsExist(fence_1_0_32767).result);

    VersionCleaner::VersionCleanerOptions options3;
    options3.keepVersionCount = 1;
    options3.fenceTsTolerantDeviation = 0;
    options3.currentMaxVersionId = GetPublicVersionId(5);
    ASSERT_TRUE(cleaner.Clean(_rootDir, options3).IsOK());

    AssertVersionsExist(Mode::BUILD, {5}, fence_3_0_32767);
    AssertVersionsNotExist(Mode::BUILD, {0, 1, 2, 3}, fence_1_0_32767);
    AssertVersionsNotExist(Mode::BUILD, {4}, fence_2_0_32767);
    AssertVersionsExist(Mode::MERGE, {3});
    AssertVersionsNotExist(Mode::MERGE, {0, 1, 2});

    AssertSegmentsExist(Mode::BUILD, {7}, fence_3_0_32767);
    AssertSegmentsExist(Mode::MERGE, {3, 4});
    AssertSegmentsNotExist(Mode::MERGE, {0, 1, 2});
    ASSERT_FALSE(_rootDir->IsExist(fence_1_0_32767).result);
    ASSERT_FALSE(_rootDir->IsExist(fence_2_0_32767).result);
}

TEST_F(VersionCleanerTest, TestManyFence2)
{
    FakeVersionCleaner cleaner;
    std::set<VersionCoord> reservedVersionSet;
    reservedVersionSet.insert(VersionCoord(GetPublicVersionId(2), fence_1_0_32767));

    std::string mergedVersionStr = "0;1;2";
    std::string buildVersionStr = "#0;#0,1,2,3;0#0,4,5;1#6";

    prepareIndex(mergedVersionStr, /*versionCommitTime=*/ {0, 2, 4}, "", cleaner);
    prepareIndex(buildVersionStr, /*versionCommitTime=*/ {1, 1, 1, 3}, fence_1_0_32767, cleaner);

    VersionCleaner::VersionCleanerOptions options;
    options.keepVersionCount = 1;
    options.currentMaxVersionId = GetPublicVersionId(3);
    options.fenceTsTolerantDeviation = 0;
    ASSERT_TRUE(cleaner.Clean(_rootDir, options, reservedVersionSet).IsOK());
    AssertVersionsExist(Mode::BUILD, {2, 3}, fence_1_0_32767);
    AssertVersionsNotExist(Mode::BUILD, {0, 1}, fence_1_0_32767);
    // TODO(wanghan) fix this merge version too
    AssertVersionsExist(Mode::MERGE, {1, 2});
    AssertVersionsNotExist(Mode::MERGE, {0});

    AssertSegmentsExist(Mode::BUILD, {0, 4, 5, 6}, fence_1_0_32767);
    AssertSegmentsNotExist(Mode::BUILD, {1, 2, 3}, fence_1_0_32767);
    AssertSegmentsExist(Mode::MERGE, {0, 1, 2});

    std::string buildVersionStr2 = ";;;;2#7";
    prepareIndex(buildVersionStr2, /*versionCommitTime=*/ {5, 5, 5, 5, 5}, fence_2_0_32767, cleaner);
    VersionCleaner::VersionCleanerOptions options2;
    options2.keepVersionCount = 1;
    options2.fenceTsTolerantDeviation = 0;
    options2.currentMaxVersionId = GetPublicVersionId(4);
    ASSERT_TRUE(cleaner.Clean(_rootDir, options2).IsOK());
    AssertVersionsExist(Mode::BUILD, {4}, fence_2_0_32767);
    AssertVersionsNotExist(Mode::BUILD, {0, 1, 2, 3}, fence_1_0_32767);
    AssertVersionsExist(Mode::MERGE, {2});
    AssertVersionsNotExist(Mode::MERGE, {0, 1});

    AssertSegmentsExist(Mode::BUILD, {7}, fence_2_0_32767);
    ASSERT_FALSE(_rootDir->IsExist(fence_1_0_32767).result);
    AssertSegmentsExist(Mode::MERGE, {2});
    AssertSegmentsNotExist(Mode::MERGE, {0, 1});
}

TEST_F(VersionCleanerTest, TestKeepVersionHour)
{
    FakeVersionCleaner cleaner;
    auto currentTimeInSecond = autil::TimeUtility::currentTimeInSeconds();

    std::string versionsStr = "#0,1,2,3;#0,4;#5,6;";
    std::vector<int64_t> versionCommitTimes({1000000 * (currentTimeInSecond - 10800),
                                             1000000 * (currentTimeInSecond - 7200),
                                             1000000 * (currentTimeInSecond - 100)});
    prepareIndex(versionsStr, versionCommitTimes, fence_1_0_32767, cleaner);
    VersionCleaner::VersionCleanerOptions options;
    options.keepVersionCount = 1;
    options.keepVersionHour = 1;
    options.currentMaxVersionId = GetPublicVersionId(2);
    options.fenceTsTolerantDeviation = 0;
    ASSERT_TRUE(cleaner.Clean(_rootDir, options).IsOK());
    AssertVersionsExist(Mode::BUILD, {1, 2}, fence_1_0_32767);
    AssertVersionsNotExist(Mode::BUILD, {0}, fence_1_0_32767);
    AssertSegmentsExist(Mode::BUILD, {0, 4, 5, 6}, fence_1_0_32767);
    AssertSegmentsNotExist(Mode::BUILD, {1, 2, 3}, fence_1_0_32767);
}

TEST_F(VersionCleanerTest, TestReservedFences)
{
    // fence used by reserved version cannot be cleaned
    FakeVersionCleaner cleaner;
    std::set<VersionCoord> reservedVersionSet;
    reservedVersionSet.insert(VersionCoord(GetPublicVersionId(1), fence_1_0_32767));
    reservedVersionSet.insert(VersionCoord(GetPrivateVersionId(0), fence_2_0_32767));

    std::string mergedVersionStr = "0;1;2";
    std::string buildVersionStr1 = "0#0,1;1#";
    std::string privateBuildVersionStr2 = "*2";
    std::string buildVersionStr3 = ";;2#3";

    prepareIndex(mergedVersionStr, /*versionCommitTime=*/ {0, 2, 4}, "", cleaner);
    prepareIndex(buildVersionStr1, /*versionCommitTime=*/ {1, 3}, fence_1_0_32767, cleaner);
    prepareIndex(privateBuildVersionStr2, /*versionCommitTime=*/ {5}, fence_2_0_32767, cleaner);
    prepareIndex(buildVersionStr3, /*versionCommitTime=*/ {5, 5, 5}, fence_3_0_32767, cleaner);

    VersionCleaner::VersionCleanerOptions options;
    options.keepVersionCount = 1;
    options.fenceTsTolerantDeviation = 0;
    options.currentMaxVersionId = GetPublicVersionId(2);
    ASSERT_TRUE(cleaner.Clean(_rootDir, options, reservedVersionSet).IsOK());
    AssertVersionsExist(Mode::BUILD, {1}, fence_1_0_32767);
    AssertVersionsExist(Mode::BUILD_PRIVATE, {0}, fence_2_0_32767);
    AssertVersionsExist(Mode::BUILD, {2}, fence_3_0_32767);
    AssertVersionsNotExist(Mode::BUILD, {0}, fence_1_0_32767);
    AssertVersionsExist(Mode::MERGE, {2});
    AssertVersionsNotExist(Mode::MERGE, {0, 1});

    AssertFenceExist(fence_1_0_32767); // empty but used by public version 1(reserved version)
    AssertFenceExist(fence_2_0_32767);
    AssertFenceExist(fence_3_0_32767);

    AssertSegmentsExist(Mode::BUILD, {3}, fence_3_0_32767);
    AssertSegmentsExist(Mode::BUILD_PRIVATE, {2}, fence_2_0_32767);
    AssertSegmentsNotExist(Mode::BUILD, {0, 1}, fence_1_0_32767);
    AssertSegmentsExist(Mode::MERGE, {1, 2});
    AssertSegmentsNotExist(Mode::MERGE, {0});
}

TEST_F(VersionCleanerTest, TestReservedPrivateVersion)
{
    FakeVersionCleaner cleaner;
    std::set<VersionCoord> reservedVersionSet;
    reservedVersionSet.insert(VersionCoord(GetPrivateVersionId(0), fence_2_0_32767));

    std::string mergedVersionStr = "0;1;2";
    std::string buildVersionStr1 = "0#0,1;1#";
    std::string privateBuildVersionStr2 = "0*2";

    prepareIndex(mergedVersionStr, /*versionCommitTime=*/ {0, 2, 4}, "", cleaner);
    prepareIndex(buildVersionStr1, /*versionCommitTime=*/ {1, 3}, fence_1_0_32767, cleaner);
    prepareIndex(privateBuildVersionStr2, /*versionCommitTime=*/ {5}, fence_2_0_32767, cleaner);

    VersionCleaner::VersionCleanerOptions options;
    options.keepVersionCount = 1;
    options.currentMaxVersionId = GetPublicVersionId(1);
    options.fenceTsTolerantDeviation = 0;
    ASSERT_TRUE(cleaner.Clean(_rootDir, options, reservedVersionSet).IsOK());
    AssertVersionsExist(Mode::BUILD_PRIVATE, {0}, fence_2_0_32767);
    AssertVersionsNotExist(Mode::BUILD, {0}, fence_1_0_32767);
    AssertVersionsExist(Mode::MERGE, {2});
    AssertVersionsNotExist(Mode::MERGE, {0});

    AssertFenceExist(fence_1_0_32767); // empty but used by public version 1(reserved version)
    AssertFenceExist(fence_2_0_32767);

    AssertSegmentsExist(Mode::BUILD_PRIVATE, {2}, fence_2_0_32767);
    AssertSegmentsNotExist(Mode::BUILD, {0, 1}, fence_1_0_32767);
    AssertSegmentsExist(Mode::MERGE, {0, 1, 2});
}

TEST_F(VersionCleanerTest, TestIgnoreKeepVersionCountNormal)
{
    FakeVersionCleaner cleaner;
    std::string oldVersionStr = ";2,3,4"; // v1 [Seg2,Seg3,Seg4]
    prepareIndex(oldVersionStr, /*versionCommitTime=*/ {1}, "", cleaner);
    std::string newMergeVersion = ";;2,3,4,5"; // v2 [Seg2,Seg3,Seg4,Seg5]
    prepareIndex(newMergeVersion, /*versionCommitTime=*/ {3}, "", cleaner);
    std::string newBuildVersionStr =
        "2,3,4#0,1;2,3,4,5#2"; // vp0 [Seg2,Seg3,Seg4,SegP0,SegP1] vp1 [Seg2,Seg3,Seg4,Seg5,SegP2]
    prepareIndex(newBuildVersionStr, /*versionCommitTime=*/ {2, 4}, fence_1_0_32767, cleaner);

    VersionCleaner::VersionCleanerOptions options;
    options.keepVersionCount = 0;
    options.currentMaxVersionId = GetPublicVersionId(1);
    options.fenceTsTolerantDeviation = 0;
    options.ignoreKeepVersionCount = true;
    ASSERT_TRUE(cleaner.Clean(_rootDir, options, {1}).IsOK());

    AssertVersionsExist(Mode::MERGE, {1});
    AssertVersionsNotExist(Mode::MERGE, {2});
    AssertVersionsNotExist(Mode::BUILD, {0, 1}, fence_1_0_32767);

    AssertSegmentsExist(Mode::MERGE, {2, 3, 4});
    AssertSegmentsNotExist(Mode::MERGE, {5});
    AssertSegmentsNotExist(Mode::BUILD, {0, 1, 2}, fence_1_0_32767);
    ASSERT_FALSE(IsFenceDirExist(fence_1_0_32767));
}

// some old segment has expired in new versions
TEST_F(VersionCleanerTest, TestIgnoreKeepVersionCountWithExpiredSegment)
{
    FakeVersionCleaner cleaner;
    std::string oldVersionStr = ";2,3,4"; // v1 [Seg2,Seg3,Seg4]
    prepareIndex(oldVersionStr, /*versionCommitTime=*/ {1}, "", cleaner);
    std::string newMergeVersion = ";;3,4,5"; // v2 [Seg3,Seg4,Seg5]
    prepareIndex(newMergeVersion, /*versionCommitTime=*/ {3}, "", cleaner);
    // old Seg2 has expired
    std::string newBuildVersionStr = "2,3,4#0,1;3,4,5#2"; // vp0 [Seg2,Seg3,Seg4,SegP0,SegP1] vp1 [Seg3,Seg4,Seg5,SegP2]
    prepareIndex(newBuildVersionStr, /*versionCommitTime=*/ {2, 4}, fence_1_0_32767, cleaner);

    VersionCleaner::VersionCleanerOptions options;
    options.keepVersionCount = 0;
    options.currentMaxVersionId = GetPublicVersionId(1);
    options.fenceTsTolerantDeviation = 0;
    options.ignoreKeepVersionCount = true;
    ASSERT_TRUE(cleaner.Clean(_rootDir, options, {1}).IsOK());

    AssertVersionsExist(Mode::MERGE, {1});
    AssertVersionsNotExist(Mode::MERGE, {2});
    AssertVersionsNotExist(Mode::BUILD, {0, 1}, fence_1_0_32767);

    AssertSegmentsExist(Mode::MERGE, {2, 3, 4});
    AssertSegmentsNotExist(Mode::MERGE, {5});
    AssertSegmentsNotExist(Mode::BUILD, {0, 1, 2}, fence_1_0_32767);
    ASSERT_FALSE(IsFenceDirExist(fence_1_0_32767));
}

// build segment does not in any version
TEST_F(VersionCleanerTest, TestIgnoreKeepVersionCountBuildSegNotExistInAnyVersion)
{
    FakeVersionCleaner cleaner;
    std::string oldVersionStr = ";2,3,4"; // v1 [Seg2,Seg3,Seg4]
    prepareIndex(oldVersionStr, /*versionCommitTime=*/ {1}, "", cleaner);
    std::string newMergeVersion = ";;3,4,5"; // v2 [Seg3,Seg4,Seg5]
    prepareIndex(newMergeVersion, /*versionCommitTime=*/ {3}, "", cleaner);
    // old Seg2 has expired
    std::string newBuildVersionStr = "2,3,4#0,1;3,4,5#2"; // vp0 [Seg2,Seg3,Seg4,SegP0,SegP1] vp1 [Seg3,Seg4,Seg5,SegP2]
    prepareIndex(newBuildVersionStr, /*versionCommitTime=*/ {2, 4}, fence_1_0_32767, cleaner);

    std::string segNotInVersion = "3"; // SegP3
    std::shared_ptr<FakeSegmentFenceDirFinder> fakeFinder(new FakeSegmentFenceDirFinder());
    Version fakeVersion;
    fakeVersion.SetFenceName(fence_1_0_32767);
    prepareSegments(segNotInVersion, /*isBuildSegment*/ true, fence_1_0_32767, fakeVersion, fakeFinder);

    VersionCleaner::VersionCleanerOptions options;
    options.keepVersionCount = 0;
    options.currentMaxVersionId = GetPublicVersionId(1);
    options.fenceTsTolerantDeviation = 0;
    options.ignoreKeepVersionCount = true;
    ASSERT_TRUE(cleaner.Clean(_rootDir, options, {1}).IsOK());

    AssertVersionsExist(Mode::MERGE, {1});
    AssertVersionsNotExist(Mode::MERGE, {2});
    AssertVersionsNotExist(Mode::BUILD, {0, 1}, fence_1_0_32767);

    AssertSegmentsExist(Mode::MERGE, {2, 3, 4});
    AssertSegmentsNotExist(Mode::MERGE, {5});
    AssertSegmentsNotExist(Mode::BUILD, {0, 1, 2, 3}, fence_1_0_32767);
    ASSERT_FALSE(IsFenceDirExist(fence_1_0_32767));
}

// merge segment does not in any version
TEST_F(VersionCleanerTest, TestIgnoreKeepVersionCountMergeSegNotExistInAnyVersion)
{
    FakeVersionCleaner cleaner;
    std::string oldVersionStr = ";2,3,4"; // v1 [Seg2,Seg3,Seg4]
    prepareIndex(oldVersionStr, /*versionCommitTime=*/ {1}, "", cleaner);
    std::string newMergeVersion = ";;3,4,5"; // v2 [Seg3,Seg4,Seg5]
    prepareIndex(newMergeVersion, /*versionCommitTime=*/ {3}, "", cleaner);
    // old Seg2 has expired
    std::string newBuildVersionStr = "2,3,4#0,1;3,4,5#2"; // vp0 [Seg2,Seg3,Seg4,SegP0,SegP1] vp1 [Seg3,Seg4,Seg5,SegP2]
    prepareIndex(newBuildVersionStr, /*versionCommitTime=*/ {2, 4}, fence_1_0_32767, cleaner);

    std::string segNotInVersion = "6"; // Seg6
    std::shared_ptr<FakeSegmentFenceDirFinder> fakeFinder(new FakeSegmentFenceDirFinder());
    Version fakeVersion;
    prepareSegments(segNotInVersion, /*isBuildSegment*/ false, /*fenceDir*/ "", fakeVersion, fakeFinder);

    VersionCleaner::VersionCleanerOptions options;
    options.keepVersionCount = 0;
    options.currentMaxVersionId = GetPublicVersionId(1);
    options.fenceTsTolerantDeviation = 0;
    options.ignoreKeepVersionCount = true;
    ASSERT_TRUE(cleaner.Clean(_rootDir, options, {1}).IsOK());

    AssertVersionsExist(Mode::MERGE, {1});
    AssertVersionsNotExist(Mode::MERGE, {2});
    AssertVersionsNotExist(Mode::BUILD, {0, 1}, fence_1_0_32767);

    AssertSegmentsExist(Mode::MERGE, {2, 3, 4});
    AssertSegmentsNotExist(Mode::MERGE, {5});
    // if a merge version in root directory does not in any version, it will not be cleaned
    AssertSegmentsExist(Mode::MERGE, {6});

    AssertSegmentsNotExist(Mode::BUILD, {0, 1, 2}, fence_1_0_32767);
    ASSERT_FALSE(IsFenceDirExist(fence_1_0_32767));
}

// test multi-fence
TEST_F(VersionCleanerTest, TestIgnoreKeepVersionCountMultiFence)
{
    FakeVersionCleaner cleaner;
    std::string oldVersionStr = ";2,3,4"; // v1 [Seg2,Seg3,Seg4]
    prepareIndex(oldVersionStr, /*versionCommitTime=*/ {1}, "", cleaner);
    std::string newMergeVersion = ";;3,4,5"; // v2 [Seg3,Seg4,Seg5]
    prepareIndex(newMergeVersion, /*versionCommitTime=*/ {3}, "", cleaner);
    std::string newBuildVersionStr = "2,3,4#0,1;3,4,5#2"; // vp0 [Seg2,Seg3,Seg4,SegP0,SegP1] vp1 [Seg3,Seg4,Seg5,SegP2]
    prepareIndex(newBuildVersionStr, /*versionCommitTime=*/ {2, 4}, fence_0_0_32767, cleaner);

    std::string newBuildVersionStr1 = ";;3,4,5#3"; // vp2 [Seg3,Seg4,Seg5,SegP3]
    prepareIndex(newBuildVersionStr1, /*versionCommitTime=*/ {5, 6}, fence_1_0_32767, cleaner);

    VersionCleaner::VersionCleanerOptions options;
    options.keepVersionCount = 0;
    options.currentMaxVersionId = GetPublicVersionId(2);
    options.fenceTsTolerantDeviation = 0;
    options.ignoreKeepVersionCount = true;
    ASSERT_TRUE(cleaner.Clean(_rootDir, options, {1}).IsOK());

    AssertVersionsExist(Mode::MERGE, {1});
    AssertVersionsNotExist(Mode::MERGE, {2});
    AssertVersionsNotExist(Mode::BUILD, {0, 1}, fence_0_0_32767);
    AssertVersionsNotExist(Mode::BUILD, {0, 1}, fence_1_0_32767);

    AssertSegmentsExist(Mode::MERGE, {2, 3, 4});
    AssertSegmentsNotExist(Mode::MERGE, {5});
    AssertSegmentsNotExist(Mode::BUILD, {0, 1, 2}, fence_0_0_32767);
    AssertSegmentsNotExist(Mode::BUILD, {3}, fence_1_0_32767);
    ASSERT_FALSE(IsFenceDirExist(fence_0_0_32767));
    ASSERT_FALSE(IsFenceDirExist(fence_1_0_32767));
}

}} // namespace indexlibv2::framework
