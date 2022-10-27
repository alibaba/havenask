#include "indexlib/merger/test/parallel_partition_data_merger_unittest.h"
#include "indexlib/test/version_maker.h"
#include "indexlib/index_base/index_meta/parallel_build_info.h"
#include "indexlib/index_base/deploy_index_wrapper.h"
#include "indexlib/index_base/version_loader.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/common/index_locator.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/file_system/directory_creator.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/test/schema_maker.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(document);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(test);
IE_NAMESPACE_USE(index_base);

IE_NAMESPACE_BEGIN(merger);
IE_LOG_SETUP(merger, ParallelPartitionDataMergerTest);

ParallelPartitionDataMergerTest::ParallelPartitionDataMergerTest()
{
}

ParallelPartitionDataMergerTest::~ParallelPartitionDataMergerTest()
{
}

void ParallelPartitionDataMergerTest::CaseSetUp()
{
    string field = "string1:string;string2:string;price:uint32";
    string index = "index2:string:string2;"
                   "pk:primarykey64:string1";
    string attribute = "string1;price";
    mSchema = SchemaMaker::MakeSchema(field, index, attribute, "");
}

void ParallelPartitionDataMergerTest::CaseTearDown()
{
}

void ParallelPartitionDataMergerTest::TestSimpleProcess()
{
    string rootPath = GET_TEST_DATA_PATH();
    ParallelBuildInfo info(2, 0, 0), info1(2, 0, 1);
    FileSystemWrapper::MkDir(info.GetParallelPath(rootPath));
    string instance0 = info.GetParallelInstancePath(rootPath);
    string instance1 = info1.GetParallelInstancePath(rootPath);
    FileSystemWrapper::MkDir(instance0);
    FileSystemWrapper::MkDir(instance1);
    info.StoreIfNotExist(instance0);
    info1.StoreIfNotExist(instance1);

    DirectoryPtr rootDir = GET_PARTITION_DIRECTORY();
    DirectoryPtr instanceRootDir = DirectoryCreator::Create(instance0, true);
    DirectoryPtr instanceRootDir1 = DirectoryCreator::Create(instance1, true);

    VersionMaker::MakeIncSegment(rootDir, 1, true);
    VersionMaker::MakeIncSegment(rootDir, 2, true);
    VersionMaker::MakeIncSegment(instanceRootDir, 3, true);
    VersionMaker::MakeIncSegment(instanceRootDir, 4, true);
    VersionMaker::MakeIncSegment(instanceRootDir1, 4, true);
    VersionMaker::MakeIncSegment(instanceRootDir1, 5, true);
    Version lastVersion, version, version1;
    lastVersion.AddSegment(1);
    lastVersion.AddSegment(2);
    
    version.AddSegment(1);
    version.AddSegment(2);
    version.AddSegment(3);
    version.AddSegment(4);

    version1.AddSegment(1);
    version1.AddSegment(2);
    version1.AddSegment(4);
    version1.AddSegment(5);
    
    lastVersion.SetVersionId(0);
    version.SetVersionId(1);
    version1.SetVersionId(1);
    lastVersion.Store(rootPath, false);
    version.Store(instance0, false);
    version1.Store(instance1, false);
    ParallelPartitionDataMerger merger(rootPath, mSchema);
    ASSERT_THROW(merger.MergeSegmentData({instance0, instance1}),
                 InconsistentStateException);
}

void ParallelPartitionDataMergerTest::TestLocatorAndTimestamp()
{
    string rootPath = GET_TEST_DATA_PATH();
    ParallelBuildInfo info(2, 0, 0), info1(2, 0, 1);
    FileSystemWrapper::MkDir(info.GetParallelPath(rootPath));
    DirectoryPtr rootDir = GET_PARTITION_DIRECTORY();
    VersionMaker::MakeIncSegment(rootDir, 1, true);
    VersionMaker::MakeIncSegment(rootDir, 3, true);
    VersionMaker::MakeIncSegment(rootDir, 5, true);
    Version lastVersion, version0, version1;
    lastVersion.AddSegment(1);
    version0.AddSegment(1);
    version0.AddSegment(3);
    version1.AddSegment(1);
    version1.AddSegment(5);
    lastVersion.SetVersionId(0);
    version0.SetVersionId(1);
    version1.SetVersionId(2);

    lastVersion.SetLocator(Locator(IndexLocator(1, 10).toString()));
    version0.SetLocator(Locator(IndexLocator(1, 9).toString()));
    version1.SetLocator(Locator(IndexLocator(1, 12).toString()));

    lastVersion.SetTimestamp(11);
    version0.SetTimestamp(13);
    version1.SetTimestamp(12);
    lastVersion.Store(rootPath, false);
    version0.Store(rootPath, false);
    SetLastSegmentInfo(rootPath, 1, 10, 8, 15);
    version1.Store(rootPath, false);
    SetLastSegmentInfo(rootPath, 1, 13, 9, 20);
    std::vector<ParallelPartitionDataMerger::VersionPair> versionInfos = {
        make_pair(version0, 0),
        make_pair(version1, 1)
    };
    ParallelPartitionDataMerger merger(rootPath, mSchema);
    bool hasNewSegment = false;
    Version targetVersion = merger.MakeNewVersion(lastVersion, versionInfos, hasNewSegment);
    SegmentInfo segInfo = merger.CreateMergedSegmentInfo(versionInfos);
    Locator segmentLocator = segInfo.locator;

    IndexLocator locator;
    locator.fromString(targetVersion.GetLocator().ToString());
    ASSERT_EQ(9, locator.getOffset());
    ASSERT_EQ(1, locator.getSrc());
    ASSERT_EQ(13, targetVersion.GetTimestamp());

    IndexLocator segLocator;
    segLocator.fromString(segmentLocator.ToString());
    ASSERT_EQ(10, segLocator.getOffset());
    ASSERT_EQ(1, segLocator.getSrc());
    ASSERT_EQ(9, segInfo.timestamp);
    ASSERT_EQ(20, segInfo.maxTTL);
}

void ParallelPartitionDataMergerTest::TestMergeEmptyInstanceDir()
{
    string rootPath = GET_TEST_DATA_PATH();
    DirectoryPtr rootDir = GET_PARTITION_DIRECTORY();
    VersionMaker::MakeIncSegment(rootDir, 1, true);
    Version lastVersion, version0, version1;
    lastVersion.AddSegment(1);
    lastVersion.SetVersionId(0);
    lastVersion.SetLocator(Locator(IndexLocator(1, 10).toString()));
    lastVersion.SetTimestamp(11);
    lastVersion.Store(rootDir, false);
    vector<string> mergeSrc;
    {
        ParallelBuildInfo info(2, 0, 0);
        info.SetBaseVersion(lastVersion.GetVersionId());
        string instanceDir = info.GetParallelInstancePath(rootDir->GetPath());
        mergeSrc.push_back(instanceDir);
        FileSystemWrapper::MkDir(instanceDir, true);
        DirectoryPtr instanceRootDir = DirectoryCreator::Create(instanceDir, true);
        info.StoreIfNotExist(instanceDir);
        Version version;
        version.AddSegment(1);
        version.SetVersionId(2);
        version.SetTimestamp(100);
        version.Store(instanceDir, false);
    }
    {
        ParallelBuildInfo info(2, 0, 1);
        info.SetBaseVersion(lastVersion.GetVersionId());
        string instanceDir = info.GetParallelInstancePath(rootDir->GetPath());
        mergeSrc.push_back(instanceDir);
        FileSystemWrapper::MkDir(instanceDir, true);
        DirectoryPtr instanceRootDir = DirectoryCreator::Create(instanceDir, true);
        info.StoreIfNotExist(instanceDir);
        Version version;
        version.AddSegment(1);
        version.SetVersionId(2);
        version.Store(instanceDir, false);
    }

    {
        ParallelPartitionDataMerger merger(rootDir->GetPath(), mSchema);
        Version targetVersion = merger.MergeSegmentData(mergeSrc);
        ASSERT_EQ(1, targetVersion.GetVersionId());
        ASSERT_EQ(100, targetVersion.GetTimestamp());
        ASSERT_TRUE(rootDir->IsExist(
                        DeployIndexWrapper::GetDeployMetaFileName(targetVersion.GetVersionId())));
    }
    {
        // merger again, will not generate new version
        ParallelPartitionDataMerger merger(rootDir->GetPath(), mSchema);
        Version targetVersion = merger.MergeSegmentData(mergeSrc);
        ASSERT_EQ(1, targetVersion.GetVersionId());
        ASSERT_EQ(100, targetVersion.GetTimestamp());
    }
    
}

void ParallelPartitionDataMergerTest::SetLastSegmentInfo(
        const string& rootPath, int src, int offset,
        int64_t ts, int64_t maxTTL)
{
    Version version;
    VersionLoader::GetVersion(rootPath, version, INVALID_VERSION);
    string segDir = version.GetSegmentDirName(version.GetLastSegment());
    string segmentPath = FileSystemWrapper::JoinPath(rootPath, segDir);
    SegmentInfo info;
    string infoFile = FileSystemWrapper::JoinPath(segmentPath, SEGMENT_INFO_FILE_NAME);
    ASSERT_TRUE(info.Load(infoFile));
    IndexLocator indexLocator(-1);
    indexLocator.fromString(info.locator.ToString());
    indexLocator.setSrc(src);
    indexLocator.setOffset(offset);
    info.locator.SetLocator(indexLocator.toString());
    info.timestamp = ts;
    info.maxTTL = maxTTL;
    FileSystemWrapper::DeleteDir(infoFile);
    info.Store(infoFile);
}

void ParallelPartitionDataMergerTest::TestNoNeedMerge()
{
    vector<ParallelPartitionDataMerger::VersionPair> versionPairs =
        MakeVersionPair("1,2,4;1,3,5", 0);

    Version lastVersion;
    lastVersion.AddSegment(1);
    lastVersion.AddSegment(2);
    lastVersion.AddSegment(3);
    lastVersion.AddSegment(4);
    lastVersion.SetVersionId(1);

    ASSERT_THROW(ParallelPartitionDataMerger::NoNeedMerge(
                    versionPairs, lastVersion, 2), InconsistentStateException);
    ASSERT_FALSE(ParallelPartitionDataMerger::NoNeedMerge(
                    versionPairs, lastVersion, 1));
    ASSERT_FALSE(ParallelPartitionDataMerger::NoNeedMerge(
                    versionPairs, lastVersion, 0));

    lastVersion.AddSegment(5);
    ASSERT_TRUE(ParallelPartitionDataMerger::NoNeedMerge(
                    versionPairs, lastVersion, 0));
}

int64_t ParallelPartitionDataMergerTest::GetLastSegmentLocator(const string& rootPath)
{
    Version version;
    VersionLoader::GetVersion(rootPath, version, INVALID_VERSION);
    string segDir = version.GetSegmentDirName(version.GetLastSegment());
    string segmentPath = FileSystemWrapper::JoinPath(rootPath, segDir);
    SegmentInfo info;
    string infoFile = FileSystemWrapper::JoinPath(segmentPath, SEGMENT_INFO_FILE_NAME);
    info.Load(infoFile);
    IndexLocator indexLocator(-1);
    indexLocator.fromString(info.locator.ToString());
    return indexLocator.getOffset();
}

vector<pair<Version, size_t> > ParallelPartitionDataMergerTest::MakeVersionPair(
        const string& versionInfosStr, versionid_t baseVersion)
{
    vector<pair<Version, size_t>> versionInfos;
    vector<vector<segmentid_t>> segIdInfos;
    StringUtil::fromString(versionInfosStr, segIdInfos, ",", ";");

    for (size_t i = 0; i < segIdInfos.size(); i++)
    {
        Version version;
        for (auto segId : segIdInfos[i])
        {
            version.AddSegment(segId);
        }
        version.SetVersionId(baseVersion + 1);
        versionInfos.push_back(make_pair(version, i));
    }
    return versionInfos;
}


IE_NAMESPACE_END(merger);

