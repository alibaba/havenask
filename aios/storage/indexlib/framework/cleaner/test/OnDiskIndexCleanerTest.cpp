#include "indexlib/framework/cleaner/OnDiskIndexCleaner.h"

#include <string>

#include "indexlib/base/PathUtil.h"
#include "indexlib/file_system/DeployIndexMeta.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/framework/Fence.h"
#include "indexlib/framework/ResourceMap.h"
#include "indexlib/framework/SegmentFenceDirFinder.h"
#include "indexlib/framework/SegmentMeta.h"
#include "indexlib/framework/TabletReaderContainer.h"
#include "indexlib/framework/Version.h"
#include "indexlib/framework/VersionDeployDescription.h"
#include "indexlib/framework/VersionLoader.h"
#include "indexlib/framework/test/FakeTabletReader.h"
#include "unittest/unittest.h"

using namespace std;

namespace indexlibv2::framework {
class MockSegment : public Segment
{
public:
    MockSegment(const SegmentMeta& meta) : Segment(SegmentStatus::ST_BUILT) { SetSegmentMeta(meta); }
    ~MockSegment() = default;

    size_t EvaluateCurrentMemUsed() override
    {
        // TODO(xiuchen) to impl
        return 0;
    }
    std::pair<Status, size_t> EstimateMemUsed(const std::shared_ptr<config::ITabletSchema>& schema) override
    {
        return {Status::OK(), 0};
    }
};

namespace {
std::map<versionid_t, Version> versions;
std::map<versionid_t, std::vector<segmentid_t>> version2SegIds;
std::map<segmentid_t, std::string> segId2FenceName;
} // namespace

class FakeSegmentFenceDirFinder : public SegmentFenceDirFinder
{
public:
    FakeSegmentFenceDirFinder(versionid_t versionId) : _versionId(versionId) {}
    std::pair<Status, std::string> GetFenceDir(segmentid_t segId) override
    {
        std::vector<segmentid_t> segIds = version2SegIds[_versionId];
        if (std::find(segIds.begin(), segIds.end(), segId) == segIds.end()) {
            return {Status::NotFound(), ""};
        }
        return {Status::OK(), segId2FenceName[segId]};
    }

private:
    versionid_t _versionId;
};

namespace {
std::map<versionid_t, std::shared_ptr<FakeSegmentFenceDirFinder>> fenceDirFinders;
}

class FakeOnDiskIndexCleaner : public OnDiskIndexCleaner
{
    FakeOnDiskIndexCleaner(const std::string& indexRoot, uint32_t keepVersionCount,
                           TabletReaderContainer* tabletReaderContainer)
        : OnDiskIndexCleaner(indexRoot, "demo", keepVersionCount, tabletReaderContainer)
    {
    }
};

class OnDiskIndexCleanerTest : public TESTBASE
{
public:
    OnDiskIndexCleanerTest() {}
    ~OnDiskIndexCleanerTest() {}

    void setUp() override
    {
        _physicalRootDir = indexlib::file_system::Directory::GetPhysicalDirectory(GET_TEMP_DATA_PATH());
        _rootDir = _physicalRootDir->GetIDirectory();
    }
    void tearDown() override
    {
        versions.clear();
        version2SegIds.clear();
        segId2FenceName.clear();
        fenceDirFinders.clear();
    }

private:
    void MakeVersion(const std::string& fenceName, versionid_t versonId, const std::vector<segmentid_t>& segIds);
    void MakeSegments(const std::string& fenceName, const std::vector<segmentid_t>& segIds);
    std::shared_ptr<TabletReaderContainer>
    MakeTabletReaderContainer(const std::vector<versionid_t>& usingIncVersions,
                              const std::vector<std::pair<versionid_t, VersionDeployDescription>>& versionDeployDescs);
    void CleanVersion(uint32_t keepVersionCount, const std::vector<versionid_t>& usingIncVersions,
                      const std::vector<versionid_t>& reservedVersionIds,
                      const std::vector<std::pair<versionid_t, VersionDeployDescription>>& versionDeployDescs);

    std::pair<Status, std::shared_ptr<indexlib::file_system::IDirectory>> GetUseDirectory(const std::string& fenceName);
    std::pair<Status, bool> IsVersionFileExist(versionid_t versionId, const std::string& fenceName);
    std::pair<Status, bool> IsSegmentExist(segmentid_t segmentId, std::string fenceDir);
    void AssertExist(const std::string& fenceName, const std::vector<versionid_t>& versionIds,
                     const std::vector<segmentid_t>& segIds);

    std::shared_ptr<indexlib::file_system::IDirectory> _rootDir;
    std::shared_ptr<indexlib::file_system::Directory> _physicalRootDir;
};

std::pair<Status, std::shared_ptr<indexlib::file_system::IDirectory>>
OnDiskIndexCleanerTest::GetUseDirectory(const std::string& fenceName)
{
    return fenceName.empty()
               ? std::make_pair(Status::OK(), _rootDir)
               : (_rootDir->MakeDirectory(fenceName, indexlib::file_system::DirectoryOption()).StatusWith());
}

void OnDiskIndexCleanerTest::MakeVersion(const std::string& fenceName, versionid_t versionId,
                                         const std::vector<segmentid_t>& segIds)
{
    version2SegIds[versionId] = segIds;
    fenceDirFinders[versionId] = std::make_shared<FakeSegmentFenceDirFinder>(versionId);
    Version version(versionId);
    version.SetFenceName(fenceName);
    for (auto segId : segIds) {
        version.AddSegment(segId);
    }

    versions[versionId] = version;
    auto writerOption = indexlib::file_system::WriterOption::AtomicDump();
    auto [status, dir] = GetUseDirectory(fenceName);
    ASSERT_TRUE(status.IsOK());
    if (!fenceName.empty()) {
        ASSERT_TRUE(_rootDir->Store(version.GetVersionFileName(), version.ToString(), writerOption).Status().IsOK());
    }
    ASSERT_TRUE(dir->Store(version.GetVersionFileName(), version.ToString(), writerOption).Status().IsOK());
    ASSERT_TRUE(dir->Store(PathUtil::GetEntryTableFileName(versionId), "", writerOption).Status().IsOK());
}

void OnDiskIndexCleanerTest::MakeSegments(const std::string& fenceName, const std::vector<segmentid_t>& segIds)
{
    // we assume segment id not conflict (overlap)
    auto [status, dir] = GetUseDirectory(fenceName);
    auto writerOption = indexlib::file_system::WriterOption::AtomicDump();
    ASSERT_TRUE(status.IsOK());
    for (auto segId : segIds) {
        ASSERT_FALSE(segId2FenceName.count(segId));
        segId2FenceName[segId] = fenceName;
        std::stringstream ss;
        ss << SEGMENT_FILE_NAME_PREFIX << "_" << segId << "_level_" << 0;
        auto dirResult = dir->MakeDirectory(ss.str(), indexlib::file_system::DirectoryOption());
        auto status = dirResult.Status();
        ASSERT_TRUE(status.IsOK());
        auto segDir = dirResult.Value();
        ASSERT_TRUE(segDir->Store("data1", "data1", writerOption).Status().IsOK());
        ASSERT_TRUE(segDir->Store("data2", "data2", writerOption).Status().IsOK());
    }
}

std::shared_ptr<TabletReaderContainer> OnDiskIndexCleanerTest::MakeTabletReaderContainer(
    const std::vector<versionid_t>& usingIncVersions,
    const std::vector<std::pair<versionid_t, VersionDeployDescription>>& versionDeployDescs)
{
    auto tabletReaderContainer = std::make_shared<TabletReaderContainer>("ut");
    for (auto versionId : usingIncVersions) {
        auto mockTabletReader = std::make_shared<FakeTabletReader>("");
        auto tabletData = std::make_shared<TabletData>("");
        const Version& version = versions[versionId];
        std::vector<std::shared_ptr<Segment>> segments;
        for (auto [segId, _] : version) {
            segments.push_back(std::make_shared<MockSegment>(SegmentMeta(segId)));
        }
        if (!tabletData->Init(version.Clone(), segments, std::make_shared<ResourceMap>()).IsOK()) {
            return nullptr;
        }
        std::shared_ptr<VersionDeployDescription> versionDeployDescription = nullptr;
        for (auto [deployVersionId, deployDesc] : versionDeployDescs) {
            if (deployVersionId == versionId) {
                versionDeployDescription = std::make_shared<VersionDeployDescription>();
                *versionDeployDescription = deployDesc;
            }
        }
        tabletReaderContainer->AddTabletReader(tabletData, mockTabletReader, versionDeployDescription);
    }
    return tabletReaderContainer;
}

void OnDiskIndexCleanerTest::CleanVersion(
    uint32_t keepVersionCount, const std::vector<versionid_t>& usingIncVersions,
    const std::vector<versionid_t>& reservedVersionIds,
    const std::vector<std::pair<versionid_t, VersionDeployDescription>>& versionDeployDescs)
{
    auto tabletReaderContainer = MakeTabletReaderContainer(usingIncVersions, versionDeployDescs);
    ASSERT_TRUE(tabletReaderContainer != nullptr);
    FakeOnDiskIndexCleaner cleaner(GET_TEMP_DATA_PATH(), keepVersionCount, tabletReaderContainer.get());
    ASSERT_TRUE(cleaner.Clean(reservedVersionIds).IsOK());
}

std::pair<Status, bool> OnDiskIndexCleanerTest::IsVersionFileExist(versionid_t versionId, const std::string& fenceName)
{
    std::stringstream ss;
    ss << VERSION_FILE_NAME_PREFIX << "." << versionId;
    auto [status, ret] = _rootDir->IsExist(ss.str()).StatusWith();
    if (fenceName.empty() || !status.IsOK() || !ret) {
        return {status, ret};
    }
    std::stringstream ss1;
    ss1 << fenceName << "/" << VERSION_FILE_NAME_PREFIX << "." << versionId;
    return _rootDir->IsExist(ss1.str()).StatusWith();
}

std::pair<Status, bool> OnDiskIndexCleanerTest::IsSegmentExist(segmentid_t segmentId, std::string fenceDir)
{
    std::stringstream ss;
    ss << fenceDir << "/" << SEGMENT_FILE_NAME_PREFIX << "_" << segmentId << "_level_0/";
    auto [st1, exist1] = _rootDir->IsExist(ss.str()).StatusWith();
    if (!st1.IsOK() || !exist1) {
        return {st1, exist1};
    }
    ss << "data1";
    return _rootDir->IsExist(ss.str()).StatusWith();
}

void OnDiskIndexCleanerTest::AssertExist(const std::string& fenceName, const std::vector<versionid_t>& versionIds,
                                         const std::vector<segmentid_t>& segIds)
{
    std::cerr << "check begin!!" << std::endl;
    std::vector<std::string> fileList;
    ASSERT_TRUE(_rootDir->ListDir(fenceName, indexlib::file_system::ListOption(), fileList).Status().IsOK());
    auto isNotSegmentOrVersion = [](std::string& f) -> bool {
        return f.find(SEGMENT_FILE_NAME_PREFIX) == std::string::npos &&
               f.find(VERSION_FILE_NAME_PREFIX) == std::string::npos;
    };
    fileList.erase(std::remove_if(fileList.begin(), fileList.end(), isNotSegmentOrVersion), fileList.end());
    ASSERT_EQ(versionIds.size() + segIds.size(), fileList.size())
        << fenceName << "\n"
        << "real file list :" << autil::StringUtil::toString(fileList) << "\n"
        << "expect version ids :" << autil::StringUtil::toString(versionIds) << "\n"
        << "expect seg ids : " << autil::StringUtil::toString(segIds);
    for (auto versionId : versionIds) {
        auto [status, existed] = IsVersionFileExist(versionId, fenceName);
        ASSERT_TRUE(status.IsOK());
        ASSERT_TRUE(existed) << fenceName << "\n" << versionId;
    }
    for (auto segId : segIds) {
        auto [status, existed] = IsSegmentExist(segId, fenceName);
        ASSERT_TRUE(status.IsOK());
        ASSERT_TRUE(existed) << fenceName << "\n" << segId;
    }
}

namespace {
const std::string ROOT = "";
const std::string FENCE_NAME_RT = Fence::GetFenceName(RT_INDEX_PARTITION_DIR_NAME);
const std::string FENCE_NAME_1 = Fence::GetFenceName("1");
const std::string FENCE_NAME_2 = Fence::GetFenceName("2");
} // namespace

versionid_t PublicVersion(versionid_t versionId) { return versionId | Version::PUBLIC_VERSION_ID_MASK; }
versionid_t MergedVersion(versionid_t versionId) { return versionId | Version::MERGED_VERSION_ID_MASK; }
versionid_t PrivateVersion(versionid_t versionId) { return versionId | Version::PRIVATE_VERSION_ID_MASK; }
segmentid_t PublicSegment(segmentid_t segmentId) { return segmentId | Segment::PUBLIC_SEGMENT_ID_MASK; }
segmentid_t MergedSegment(segmentid_t segmentId) { return segmentId | Segment::MERGED_SEGMENT_ID_MASK; }
segmentid_t PrivateSegment(segmentid_t segmentId) { return segmentId | Segment::PRIVATE_SEGMENT_ID_MASK; }

TEST_F(OnDiskIndexCleanerTest, CleanLeaderDisk_1)
{
    // assume : only version.0 (merged version 0) will dp, no other merge version
    std::vector<segmentid_t> mergeSegIds {MergedSegment(0), MergedSegment(1)};
    MakeVersion(ROOT, MergedVersion(0), mergeSegIds);
    MakeSegments(ROOT, mergeSegIds);
    CleanVersion(1, {}, {MergedVersion(0)}, {});
    AssertExist(ROOT, {MergedVersion(0)}, mergeSegIds);
}

TEST_F(OnDiskIndexCleanerTest, CleanLeaderDisk_2)
{
    // only merge version and segment
    MakeVersion(ROOT, MergedVersion(0), {MergedSegment(0)});
    MakeSegments(ROOT, {MergedSegment(0)});
    CleanVersion(1, {}, {}, {});
    AssertExist(ROOT, {MergedVersion(0)}, {MergedSegment(0)});

    std::vector<segmentid_t> publicSegIds = {PublicSegment(1), PublicSegment(2)};
    std::vector<segmentid_t> current = {MergedSegment(0), PublicSegment(1), PublicSegment(2)};
    MakeVersion(FENCE_NAME_1, PublicVersion(1), current);
    MakeSegments(FENCE_NAME_1, publicSegIds);
    MakeVersion(FENCE_NAME_1, PublicVersion(2), current);
    CleanVersion(1, {}, {PublicVersion(2)}, {});
    AssertExist(ROOT, {PublicVersion(2)}, {MergedSegment(0)});
    AssertExist(FENCE_NAME_1, {PublicVersion(2)}, publicSegIds);

    MakeVersion(FENCE_NAME_1, PublicVersion(3), {MergedSegment(4)});
    MakeSegments(ROOT, {MergedSegment(4)});
    CleanVersion(1, {}, {PublicVersion(2), PublicVersion(3)}, {});
    AssertExist(ROOT, {PublicVersion(2), PublicVersion(3)}, {MergedSegment(0), MergedSegment(4)});
    AssertExist(FENCE_NAME_1, {PublicVersion(2), PublicVersion(3)}, publicSegIds);

    CleanVersion(1, {}, {PublicVersion(3)}, {});
    AssertExist(ROOT, {PublicVersion(3)}, {MergedSegment(4)});
    AssertExist(FENCE_NAME_1, {PublicVersion(3)}, {});

    MakeVersion(FENCE_NAME_2, PublicVersion(4), {PublicSegment(5)});
    MakeSegments(FENCE_NAME_2, {PublicSegment(5)});
    CleanVersion(1, {PublicVersion(3)}, {PublicVersion(4)}, {});
    AssertExist(ROOT, {PublicVersion(3), PublicVersion(4)}, {MergedSegment(4)});
    AssertExist(FENCE_NAME_1, {PublicVersion(3)}, {});
    AssertExist(FENCE_NAME_2, {PublicVersion(4)}, {PublicSegment(5)});
}

TEST_F(OnDiskIndexCleanerTest, CleanLeaderDisk_3)
{
    // assume : only version.0 (merged version 0) will dp, no other merge version
    MakeVersion(ROOT, MergedVersion(0), {MergedSegment(0)});
    MakeSegments(ROOT, {MergedSegment(0)});

    MakeVersion(FENCE_NAME_1, PublicVersion(1), {MergedSegment(0), PublicSegment(1), PublicSegment(2)});
    MakeSegments(FENCE_NAME_1, {PublicSegment(1), PublicSegment(2)});
    MakeVersion(FENCE_NAME_1, PublicVersion(2), {MergedSegment(1)});
    MakeSegments(ROOT, {MergedSegment(1)});
    MakeVersion(FENCE_NAME_1, PublicVersion(3), {MergedSegment(1), PublicSegment(3)});
    MakeSegments(FENCE_NAME_1, {PublicSegment(3)});

    CleanVersion(3, {PublicVersion(3)}, {}, {});
    AssertExist(ROOT, {PublicVersion(1), PublicVersion(2), PublicVersion(3)}, {MergedSegment(0), MergedSegment(1)});
    AssertExist(FENCE_NAME_1, {PublicVersion(1), PublicVersion(2), PublicVersion(3)},
                {PublicSegment(1), PublicSegment(2), PublicSegment(3)});

    CleanVersion(2, {PublicVersion(3)}, {}, {});
    AssertExist(ROOT, {PublicVersion(2), PublicVersion(3)}, {MergedSegment(1)});
    AssertExist(FENCE_NAME_1, {PublicVersion(2), PublicVersion(3)}, {PublicSegment(3)});

    CleanVersion(1, {PublicVersion(3)}, {}, {});
    AssertExist(ROOT, {PublicVersion(3)}, {MergedSegment(1)});
    AssertExist(FENCE_NAME_1, {PublicVersion(3)}, {PublicSegment(3)});
    MakeSegments(FENCE_NAME_RT, {PrivateSegment(0)});
    CleanVersion(1, {PublicVersion(3)}, {}, {});
    AssertExist(FENCE_NAME_RT, {}, {PrivateSegment(0)});
}

TEST_F(OnDiskIndexCleanerTest, CleanWithVersionDeployDescription)
{
    /*
      local:
          version0    - segment0
          version1    - segment1
          version2    - segment2
          version3
          version4
                      - segment5

      rt
          version.1073741827
     */
    MakeVersion(ROOT, MergedVersion(0), {MergedSegment(0)});
    MakeSegments(ROOT, {MergedSegment(0)});
    VersionDeployDescription desc0;
    auto deployIndexMeta0 = std::make_shared<indexlib::file_system::DeployIndexMeta>();
    deployIndexMeta0->Append({"segment_0_level_0/", -1, 100});
    deployIndexMeta0->Append({"segment_0_level_0/data1", 4, 100});
    desc0.localDeployIndexMetas.push_back(deployIndexMeta0);

    MakeVersion(ROOT, MergedVersion(1), {MergedSegment(1)});
    MakeSegments(ROOT, {MergedSegment(1)});

    MakeVersion(ROOT, MergedVersion(2), {MergedSegment(2)});
    MakeSegments(ROOT, {MergedSegment(2)});
    VersionDeployDescription desc2;

    MakeVersion(ROOT, MergedVersion(3), {});
    // empty segment
    MakeSegments(ROOT, {MergedSegment(3)});

    MakeVersion(ROOT, MergedVersion(4), {});
    MakeVersion(FENCE_NAME_RT, MergedVersion(Version::PRIVATE_VERSION_ID_MASK | 3), {});

    CleanVersion(/*keep version count*/ 1, /*usingIncVersions*/ {0, 1, 2}, /*reverved versions*/ {4},
                 /*version deploy description*/ {{0, desc0}, {2, desc2}});
    AssertExist(ROOT, {MergedVersion(0), MergedVersion(1), MergedVersion(2), MergedVersion(4)},
                {MergedSegment(0), MergedSegment(1), MergedSegment(2)});

    CleanVersion(/*keep version count*/ 1, /*usingIncVersions*/ {0, 1, 2}, /*reverved versions*/ {4},
                 /*version deploy description*/ {{0, desc0}, {2, desc2}});
    AssertExist(ROOT, {MergedVersion(0), MergedVersion(1), MergedVersion(2), MergedVersion(4)},
                {MergedSegment(0), MergedSegment(1), MergedSegment(2)});
}

TEST_F(OnDiskIndexCleanerTest, CleanWhenDeployingVersion)
{
    //测试本地version不齐的时候不会做清理操作
    /*
      local:
      version.0    - segment0
      version.1    - segemnt0 segment1
      version.536870912    - segment0 segment1 segment536870912

      remote
      version.536870913

      rt
      version.1073741824   - segemnt0 segment1 segment536870912 segment1073741824

    */
    // version.0
    MakeVersion(ROOT, MergedVersion(0), {MergedSegment(0)});
    MakeSegments(ROOT, {MergedSegment(0)});
    // version.1
    MakeVersion(ROOT, MergedVersion(1), {MergedSegment(0), MergedSegment(1)});
    MakeSegments(ROOT, {MergedSegment(1)});

    // version.536870912
    MakeVersion(FENCE_NAME_1, PublicVersion(0), {MergedSegment(0), MergedSegment(1), PublicSegment(0)});
    MakeSegments(FENCE_NAME_1, {PublicSegment(0)});

    auto [status, dir] = GetUseDirectory(FENCE_NAME_2);
    ASSERT_TRUE(status.IsOK());
    auto writerOption = indexlib::file_system::WriterOption::AtomicDump();
    ASSERT_TRUE(dir->Store("deploying_file", "tmp", writerOption).Status().IsOK());

    // version.1073741824
    MakeVersion(FENCE_NAME_RT, PrivateVersion(0),
                {MergedSegment(0), MergedSegment(1), PublicSegment(0), PrivateSegment(0)});
    MakeSegments(FENCE_NAME_RT, {PrivateVersion(0)});

    CleanVersion(/*keep version count*/ 1, /*usingIncVersions*/ {1},
                 /*reverved versions*/ {PublicVersion(1)},
                 /*version deploy description*/ {});
    AssertExist(ROOT, {MergedVersion(0), MergedVersion(1), PublicVersion(0), PrivateVersion(0)},
                {MergedSegment(1), MergedSegment(0)});
    AssertExist(FENCE_NAME_1, {PublicVersion(0)}, {PublicSegment(0)});

    auto [status1, exist] = dir->IsExist("deploying_file").StatusWith();
    ASSERT_TRUE(status1.IsOK());
    ASSERT_TRUE(exist);
}

TEST_F(OnDiskIndexCleanerTest, TestCleanUnreferencedSegments)
{
    // version.0
    MakeVersion(ROOT, MergedVersion(0),
                {MergedSegment(0), MergedSegment(1), MergedSegment(2), MergedSegment(3), MergedSegment(4)});
    MakeSegments(ROOT, {MergedSegment(0), MergedSegment(1), MergedSegment(2), MergedSegment(3), MergedSegment(4)});

    std::set<std::string> toKeepFiles;
    toKeepFiles.insert("segment_" + std::to_string(MergedSegment(0)) + "_level_0/deploy_index");
    toKeepFiles.insert("segment_" + std::to_string(MergedSegment(3)) + "_level_0/deploy_index");

    auto tabletReaderContainer = std::make_shared<TabletReaderContainer>("ut");
    FakeOnDiskIndexCleaner cleaner(GET_TEMP_DATA_PATH(), 0, tabletReaderContainer.get());
    ASSERT_TRUE(cleaner.CleanUnreferencedSegments(_physicalRootDir, _rootDir, Version(-1), {}));
    ASSERT_TRUE(cleaner.CleanUnreferencedSegments(_physicalRootDir, _rootDir, Version(2), {}));
    AssertExist(ROOT, {MergedVersion(0)},
                {MergedSegment(0), MergedSegment(1), MergedSegment(2), MergedSegment(3), MergedSegment(4)});
    ASSERT_TRUE(cleaner.CleanUnreferencedSegments(_physicalRootDir, _rootDir, versions[0], toKeepFiles));
    AssertExist(ROOT, {MergedVersion(0)}, {MergedSegment(0), MergedSegment(3)});
}

TEST_F(OnDiskIndexCleanerTest, TestCleanUnreferencedLocalFiles)
{
    // version.0
    MakeVersion(ROOT, MergedVersion(0),
                {MergedSegment(0), MergedSegment(1), MergedSegment(2), MergedSegment(3), MergedSegment(4)});
    MakeSegments(ROOT, {MergedSegment(0), MergedSegment(1), MergedSegment(2), MergedSegment(3), MergedSegment(4)});

    std::set<std::string> toKeepFiles;
    toKeepFiles.insert("segment_" + std::to_string(MergedSegment(0)) + "_level_0/data1");
    toKeepFiles.insert("segment_" + std::to_string(MergedSegment(1)) + "_level_0/data2");
    toKeepFiles.insert("segment_" + std::to_string(MergedSegment(2)) + "_level_0/data1");
    toKeepFiles.insert("segment_" + std::to_string(MergedSegment(3)) + "_level_0/data2");
    auto tabletReaderContainer = std::make_shared<TabletReaderContainer>("ut");
    FakeOnDiskIndexCleaner cleaner(GET_TEMP_DATA_PATH(), 0, tabletReaderContainer.get());
    ASSERT_TRUE(cleaner.CleanUnreferencedLocalFiles(_physicalRootDir, _rootDir, versions[0], toKeepFiles));

    std::set<std::string> expectExistFiles;
    expectExistFiles.insert("segment_" + std::to_string(MergedSegment(0)) + "_level_0/data1");
    expectExistFiles.insert("segment_" + std::to_string(MergedSegment(3)) + "_level_0/data2");
    expectExistFiles.insert("segment_" + std::to_string(MergedSegment(1)) + "_level_0/data2");
    expectExistFiles.insert("segment_" + std::to_string(MergedSegment(2)) + "_level_0/data1");
    std::set<std::string> expectRemoveFiles;
    expectRemoveFiles.insert("segment_" + std::to_string(MergedSegment(0)) + "_level_0/data2");
    expectRemoveFiles.insert("segment_" + std::to_string(MergedSegment(1)) + "_level_0/data1");
    expectRemoveFiles.insert("segment_" + std::to_string(MergedSegment(2)) + "_level_0/data2");
    expectRemoveFiles.insert("segment_" + std::to_string(MergedSegment(3)) + "_level_0/data1");
    expectRemoveFiles.insert("segment_" + std::to_string(MergedSegment(4)) + "_level_0/data1");
    expectRemoveFiles.insert("segment_" + std::to_string(MergedSegment(4)) + "_level_0/data2");
    for (const auto& file : expectExistFiles) {
        auto res = _rootDir->IsExist(file).StatusWith();
        ASSERT_TRUE(res.first.IsOK());
        ASSERT_TRUE(res.second);
    }
    for (const auto& file : expectRemoveFiles) {
        auto res = _rootDir->IsExist(file).StatusWith();
        ASSERT_TRUE(res.first.IsOK());
        ASSERT_FALSE(res.second);
    }
}

TEST_F(OnDiskIndexCleanerTest, TestCleanWithKeepFiles)
{
    // version.0
    MakeVersion(ROOT, MergedVersion(0), {MergedSegment(0), MergedSegment(1), MergedSegment(2)});
    MakeSegments(ROOT, {MergedSegment(0), MergedSegment(1), MergedSegment(2)});
    // version.1
    MakeVersion(ROOT, MergedVersion(1), {MergedSegment(2), MergedSegment(3)});
    MakeSegments(ROOT, {MergedSegment(3)});
    // version.2
    MakeVersion(ROOT, MergedVersion(2), {MergedSegment(3), MergedSegment(4), MergedSegment(5)});
    MakeSegments(ROOT, {MergedSegment(4), MergedSegment(5)});
    // version.3
    MakeVersion(ROOT, MergedVersion(3),
                {MergedSegment(3), MergedSegment(4), MergedSegment(5), MergedSegment(6), MergedSegment(7)});
    MakeSegments(ROOT, {MergedSegment(6), MergedSegment(7)});

    std::set<std::string> toKeepFiles;
    toKeepFiles.insert("segment_" + std::to_string(MergedSegment(6)) + "_level_0/data1");

    std::vector<std::pair<versionid_t, VersionDeployDescription>> versionDeployDescs;
    VersionDeployDescription versionDpDes2;
    auto deployIndexMeta2 = std::make_shared<indexlib::file_system::DeployIndexMeta>();
    deployIndexMeta2->deployFileMetas.push_back("segment_" + std::to_string(MergedSegment(5)) + "_level_0/data2");
    versionDpDes2.localDeployIndexMetas.push_back(deployIndexMeta2);
    versionDeployDescs.push_back({2, versionDpDes2});
    VersionDeployDescription versionDpDes3;
    auto deployIndexMeta3 = std::make_shared<indexlib::file_system::DeployIndexMeta>();
    deployIndexMeta3->deployFileMetas.push_back("segment_" + std::to_string(MergedSegment(7)) + "_level_0/data1");
    deployIndexMeta3->deployFileMetas.push_back("segment_" + std::to_string(MergedSegment(7)) + "_level_0/data2");
    versionDpDes3.localDeployIndexMetas.push_back(deployIndexMeta3);
    versionDeployDescs.push_back({3, versionDpDes3});
    auto tabletReaderContainer = MakeTabletReaderContainer({2, 3}, versionDeployDescs);
    ASSERT_TRUE(tabletReaderContainer != nullptr);
    FakeOnDiskIndexCleaner cleaner(GET_TEMP_DATA_PATH(), 1, tabletReaderContainer.get());
    ASSERT_TRUE(cleaner.Clean(_physicalRootDir, versions[3], toKeepFiles).IsOK());

    std::set<std::string> expectExistFiles;
    // in read container version 2, deploy segment_5_level_0/data2
    expectExistFiles.insert("segment_" + std::to_string(MergedSegment(5)) + "_level_0/data2");
    // in to keep files, deploy segment_6_level_0/data1
    expectExistFiles.insert("segment_" + std::to_string(MergedSegment(6)) + "_level_0/data1");
    // in read container version 3, deploy segment_7_level_0/data1, segment_7_level_0/data2
    expectExistFiles.insert("segment_" + std::to_string(MergedSegment(7)) + "_level_0/data1");
    expectExistFiles.insert("segment_" + std::to_string(MergedSegment(7)) + "_level_0/data2");
    // segment not in version 3 not clean
    expectExistFiles.insert("segment_" + std::to_string(MergedSegment(0)) + "_level_0/data1");
    expectExistFiles.insert("segment_" + std::to_string(MergedSegment(0)) + "_level_0/data2");
    expectExistFiles.insert("segment_" + std::to_string(MergedSegment(1)) + "_level_0/data1");
    expectExistFiles.insert("segment_" + std::to_string(MergedSegment(1)) + "_level_0/data2");
    expectExistFiles.insert("segment_" + std::to_string(MergedSegment(2)) + "_level_0/data1");
    expectExistFiles.insert("segment_" + std::to_string(MergedSegment(2)) + "_level_0/data2");

    std::set<std::string> expectRemoveFiles;
    expectRemoveFiles.insert("segment_" + std::to_string(MergedSegment(3)) + "_level_0/data1");
    expectRemoveFiles.insert("segment_" + std::to_string(MergedSegment(3)) + "_level_0/data2");
    expectRemoveFiles.insert("segment_" + std::to_string(MergedSegment(4)) + "_level_0/data1");
    expectRemoveFiles.insert("segment_" + std::to_string(MergedSegment(4)) + "_level_0/data2");
    expectRemoveFiles.insert("segment_" + std::to_string(MergedSegment(5)) + "_level_0/data1");
    expectRemoveFiles.insert("segment_" + std::to_string(MergedSegment(6)) + "_level_0/data2");
    for (const auto& file : expectExistFiles) {
        auto res = _rootDir->IsExist(file).StatusWith();
        ASSERT_TRUE(res.first.IsOK());
        ASSERT_TRUE(res.second);
    }
    for (const auto& file : expectRemoveFiles) {
        auto res = _rootDir->IsExist(file).StatusWith();
        ASSERT_TRUE(res.first.IsOK());
        ASSERT_FALSE(res.second);
    }
}

TEST_F(OnDiskIndexCleanerTest, TestListFilesInSegmentDirs)
{
    MakeVersion(ROOT, MergedVersion(0), {MergedSegment(0)});
    MakeSegments(ROOT, {MergedSegment(0)});
    auto statusWithSegDir = _rootDir->GetDirectory("segment_0_level_0").StatusWith();
    ASSERT_TRUE(statusWithSegDir.first.IsOK());
    auto segmentDir = statusWithSegDir.second;
    auto res = segmentDir->MakeDirectory("testdir", indexlib::file_system::DirectoryOption()).StatusWith();
    ASSERT_TRUE(res.first.IsOK());
    auto testDir = res.second;
    auto writerOption = indexlib::file_system::WriterOption::AtomicDump();
    ASSERT_TRUE(testDir->Store("data1", "data1", writerOption).Status().IsOK());
    auto tabletReaderContainer = std::make_shared<TabletReaderContainer>("ut");
    FakeOnDiskIndexCleaner cleaner(GET_TEMP_DATA_PATH(), 0, tabletReaderContainer.get());
    std::set<std::string> files;
    ASSERT_TRUE(cleaner.ListFilesInSegmentDirs(_physicalRootDir, _rootDir, versions[0], &files));
    ASSERT_THAT(files, ::testing::ElementsAre("segment_0_level_0/data1", "segment_0_level_0/data2",
                                              "segment_0_level_0/testdir/data1"));
}

TEST_F(OnDiskIndexCleanerTest, TestCleanWithReservedVersions)
{
    // version.0
    MakeVersion(ROOT, MergedVersion(0), {MergedSegment(0), MergedSegment(1), MergedSegment(2)});
    MakeSegments(ROOT, {MergedSegment(0), MergedSegment(1), MergedSegment(2)});
    // version.1
    MakeVersion(ROOT, MergedVersion(1), {MergedSegment(2), MergedSegment(3)});
    MakeSegments(ROOT, {MergedSegment(3)});
    // version.2
    MakeVersion(ROOT, MergedVersion(2), {MergedSegment(3), MergedSegment(4)});
    MakeSegments(ROOT, {MergedSegment(4)});
    // version.3
    MakeVersion(ROOT, MergedVersion(3), {MergedSegment(4), MergedSegment(6)});
    MakeSegments(ROOT, {MergedSegment(6)});

    std::vector<std::pair<versionid_t, VersionDeployDescription>> versionDeployDescs;
    versionDeployDescs.push_back({1, VersionDeployDescription()});
    versionDeployDescs.push_back({3, VersionDeployDescription()});
    auto tabletReaderContainer = MakeTabletReaderContainer({1, 3}, versionDeployDescs);
    ASSERT_TRUE(tabletReaderContainer != nullptr);

    FakeOnDiskIndexCleaner cleaner(GET_TEMP_DATA_PATH(), 1, tabletReaderContainer.get());
    ASSERT_TRUE(cleaner.Clean({2, 3}).IsOK());

    std::set<std::string> expectExistFiles;
    // in read container, versionDpDesc disable manifest checking
    expectExistFiles.insert("segment_" + std::to_string(MergedSegment(2)) + "_level_0/data1");
    expectExistFiles.insert("segment_" + std::to_string(MergedSegment(2)) + "_level_0/data2");
    expectExistFiles.insert("segment_" + std::to_string(MergedSegment(3)) + "_level_0/data1");
    expectExistFiles.insert("segment_" + std::to_string(MergedSegment(3)) + "_level_0/data2");
    expectExistFiles.insert("segment_" + std::to_string(MergedSegment(4)) + "_level_0/data1");
    expectExistFiles.insert("segment_" + std::to_string(MergedSegment(4)) + "_level_0/data2");
    expectExistFiles.insert("segment_" + std::to_string(MergedSegment(6)) + "_level_0/data1");
    expectExistFiles.insert("segment_" + std::to_string(MergedSegment(6)) + "_level_0/data2");

    std::set<std::string> expectRemoveFiles;
    expectRemoveFiles.insert("segment_" + std::to_string(MergedSegment(0)) + "_level_0/data1");
    expectRemoveFiles.insert("segment_" + std::to_string(MergedSegment(0)) + "_level_0/data2");
    expectRemoveFiles.insert("segment_" + std::to_string(MergedSegment(1)) + "_level_0/data1");
    expectRemoveFiles.insert("segment_" + std::to_string(MergedSegment(1)) + "_level_0/data2");

    for (const auto& file : expectExistFiles) {
        auto res = _rootDir->IsExist(file).StatusWith();
        ASSERT_TRUE(res.first.IsOK());
        ASSERT_TRUE(res.second) << file << std::endl;
    }
    for (const auto& file : expectRemoveFiles) {
        auto res = _rootDir->IsExist(file).StatusWith();
        ASSERT_TRUE(res.first.IsOK());
        ASSERT_FALSE(res.second) << file << std::endl;
    }
}

} // namespace indexlibv2::framework
