#include "indexlib/merger/test/parallel_partition_data_merger_unittest.h"

#include "autil/EnvUtil.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/document/index_locator.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index_base/branch_fs.h"
#include "indexlib/index_base/deploy_index_wrapper.h"
#include "indexlib/index_base/index_meta/parallel_build_info.h"
#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/index_base/index_meta/version.h"
#include "indexlib/index_base/index_meta/version_loader.h"
#include "indexlib/merger/merger_branch_hinter.h"
#include "indexlib/test/directory_creator.h"
#include "indexlib/test/schema_maker.h"
#include "indexlib/test/version_maker.h"
#include "indexlib/util/EpochIdUtil.h"
#include "indexlib/util/PathUtil.h"

using namespace std;
using namespace autil;
using namespace indexlib::file_system;
using namespace indexlib::document;
using namespace indexlib::common;
using namespace indexlib::config;
using namespace indexlib::test;
using namespace indexlib::index_base;
using namespace indexlib::util;

namespace indexlib { namespace merger {
IE_LOG_SETUP(merger, ParallelPartitionDataMergerTest);

ParallelPartitionDataMergerTest::ParallelPartitionDataMergerTest() {}

ParallelPartitionDataMergerTest::~ParallelPartitionDataMergerTest() {}

void ParallelPartitionDataMergerTest::CaseSetUp()
{
    string field = "string1:string;string2:string;price:uint32";
    string index = "index2:string:string2;"
                   "pk:primarykey64:string1";
    string attribute = "string1;price";
    mSchema = SchemaMaker::MakeSchema(field, index, attribute, "");
}

void ParallelPartitionDataMergerTest::CaseTearDown() {}

void ParallelPartitionDataMergerTest::TestSimpleProcess()
{
    string rootPath = GET_TEMP_DATA_PATH();
    ParallelBuildInfo info(2, 0, 0), info1(2, 0, 1);
    FslibWrapper::MkDirE(info.GetParallelPath(rootPath));
    string instancePath0 = info.ParallelBuildInfo::GetParallelInstancePath(GET_TEMP_DATA_PATH());
    string instancePath1 = info1.ParallelBuildInfo::GetParallelInstancePath(GET_TEMP_DATA_PATH());
    auto instance0 = DirectoryCreator::CreateOfflineRootDir(instancePath0);
    auto instance1 = DirectoryCreator::CreateOfflineRootDir(instancePath1);
    info.StoreIfNotExist(instance0);
    info1.StoreIfNotExist(instance1);

    DirectoryPtr rootDir = GET_PARTITION_DIRECTORY();
    VersionMaker::MakeIncSegment(rootDir, 1, true);
    VersionMaker::MakeIncSegment(rootDir, 2, true);
    VersionMaker::MakeIncSegment(instance0, 3, true);
    VersionMaker::MakeIncSegment(instance0, 4, true);
    VersionMaker::MakeIncSegment(instance1, 4, true);
    VersionMaker::MakeIncSegment(instance1, 5, true);
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
    ASSERT_EQ(FSEC_OK, FslibWrapper::Store(rootPath + lastVersion.GetVersionFileName(), lastVersion.ToString()).Code());
    version.Store(instance0, false);
    version1.Store(instance1, false);
    ParallelPartitionDataMerger merger(rootDir, EpochIdUtil::TEST_GenerateEpochId(), mSchema);
    ASSERT_THROW(merger.MergeSegmentData({instancePath0, instancePath1}), InconsistentStateException);
}

void ParallelPartitionDataMergerTest::TestOldWorkerCannotMoveNewDir()
{
    string rootPath = GET_TEMP_DATA_PATH();
    ParallelBuildInfo info(2, 0, 0), info1(2, 0, 1);
    FslibWrapper::MkDirE(info.GetParallelPath(rootPath));
    string instancePath0 = info.ParallelBuildInfo::GetParallelInstancePath(GET_TEMP_DATA_PATH());
    string instancePath1 = info1.ParallelBuildInfo::GetParallelInstancePath(GET_TEMP_DATA_PATH());
    DirectoryPtr rootDir = GET_PARTITION_DIRECTORY();
    config::IndexPartitionOptions indexOptions;
    ParallelPartitionDataMerger merger(rootDir, EpochIdUtil::TEST_GenerateEpochId(), mSchema, indexOptions);

    file_system::FileSystemOptions options = FileSystemOptions::OfflineWithBlockCache(nullptr);
    MergerBranchHinter hinter(MergerBranchHinter::Option::Test());
    auto fs0 = BranchFS::CreateWithAutoFork(instancePath0, options, nullptr, &hinter);
    auto fs1 = BranchFS::CreateWithAutoFork(instancePath1, options, nullptr, &hinter);

    auto instance0 = Directory::Get(fs0->GetFileSystem());
    auto instance1 = Directory::Get(fs1->GetFileSystem());

    info.StoreIfNotExist(instance0);
    info1.StoreIfNotExist(instance1);

    VersionMaker::MakeIncSegment(rootDir, 1, true);
    VersionMaker::MakeIncSegment(rootDir, 2, true);
    VersionMaker::MakeIncSegment(instance0, 3, true);
    VersionMaker::MakeIncSegment(instance0, 4, true);
    // VersionMaker::MakeIncSegment(instance1, 4, true);
    VersionMaker::MakeIncSegment(instance1, 5, true);
    Version lastVersion, version, version1;
    lastVersion.AddSegment(1);
    lastVersion.AddSegment(2);

    version.AddSegment(3);
    version.AddSegment(4);

    version1.AddSegment(5);

    lastVersion.SetVersionId(0);
    version.SetVersionId(1);
    version1.SetVersionId(1);
    lastVersion.Store(rootDir, false);
    version.Store(instance0, false);
    version1.Store(instance1, false);
    fs0->CommitToDefaultBranch(nullptr);
    fs1->CommitToDefaultBranch(nullptr);

    ASSERT_THROW(merger.MergeSegmentData({instancePath0, instancePath1}), util::RuntimeException);
}

void ParallelPartitionDataMergerTest::TestMergeVersionWithSegmentMetric()
{
    string rootPath = GET_TEMP_DATA_PATH();
    ParallelBuildInfo info(2, 0, 0), info1(2, 0, 1);
    FslibWrapper::MkDirE(info.GetParallelPath(rootPath));
    string instancePath0 = info.ParallelBuildInfo::GetParallelInstancePath(GET_TEMP_DATA_PATH());
    string instancePath1 = info1.ParallelBuildInfo::GetParallelInstancePath(GET_TEMP_DATA_PATH());
    auto instance0 = DirectoryCreator::CreateOfflineRootDir(instancePath0);
    auto instance1 = DirectoryCreator::CreateOfflineRootDir(instancePath1);
    info.StoreIfNotExist(instance0);
    info1.StoreIfNotExist(instance1);

    vector<SegmentTemperatureMeta> temperatures;
    for (size_t i = 1; i <= 5; i++) {
        SegmentTemperatureMeta temperatureMeta;
        temperatureMeta.segmentId = i;
        temperatureMeta.segTemperature = "HOT";
        temperatureMeta.segTemperatureDetail = "HOT:1;WARM:0;COLD:1";
        temperatures.push_back(temperatureMeta);
    }

    DirectoryPtr rootDir = GET_PARTITION_DIRECTORY();
    VersionMaker::MakeIncSegment(rootDir, 1, true);
    VersionMaker::MakeIncSegment(rootDir, 2, true);
    VersionMaker::MakeIncSegment(instance0, 3, true);
    VersionMaker::MakeIncSegment(instance1, 4, true);
    VersionMaker::MakeIncSegment(instance1, 5, true);
    Version lastVersion, version, version1;
    lastVersion.AddSegment(1);
    lastVersion.AddSegment(2);
    lastVersion.AddSegTemperatureMeta(temperatures[0]);
    lastVersion.AddSegTemperatureMeta(temperatures[1]);

    version.AddSegment(3);
    version.AddSegTemperatureMeta(temperatures[2]);

    version1.AddSegment(4);
    version1.AddSegment(5);
    version1.AddSegTemperatureMeta(temperatures[3]);
    version1.AddSegTemperatureMeta(temperatures[4]);

    lastVersion.SetVersionId(0);
    version.SetVersionId(1);
    version1.SetVersionId(1);
    ASSERT_EQ(FSEC_OK, FslibWrapper::Store(rootPath + lastVersion.GetVersionFileName(), lastVersion.ToString()).Code());
    lastVersion.Store(rootDir, true);
    version.Store(instance0, true);
    version1.Store(instance1, true);

    IndexPartitionOptions options;
    string schemaPatch = GET_PRIVATE_TEST_DATA_PATH() + "schema_patch.json";
    string patchContent;
    ASSERT_EQ(FSEC_OK, FslibWrapper::AtomicLoad(schemaPatch, patchContent).Code());
    config::UpdateableSchemaStandards patch;
    FromJsonString(patch, patchContent);
    options.SetUpdateableSchemaStandards(patch);
    ParallelPartitionDataMerger merger(rootDir, EpochIdUtil::TEST_GenerateEpochId(), mSchema, options);
    Version newVersion = merger.MergeSegmentData({instancePath0, instancePath1});
    ASSERT_EQ(6, newVersion.GetSegmentCount());
    ASSERT_EQ(6, newVersion.GetSegTemperatureMetas().size());
    ASSERT_FALSE(newVersion.GetUpdateableSchemaStandards().IsEmpty());
}

void ParallelPartitionDataMergerTest::TestMergeVersionWithSegmentStatistics()
{
    Version baseVersion, version1, version2;
    baseVersion.SetVersionId(0);
    baseVersion.AddSegment(0);
    baseVersion.AddSegTemperatureMeta(SegmentTemperatureMeta(0, "hot", ""));
    indexlibv2::framework::SegmentStatistics baseSegmentStatistics;
    baseSegmentStatistics.SetSegmentId(0);
    std::pair<int, int> stat(0, 1);
    baseSegmentStatistics.AddStatistic("key", stat);
    baseVersion.AddSegmentStatistics(baseSegmentStatistics);
    version1.SetVersionId(1);
    version2.SetVersionId(2);
    std::vector<ParallelPartitionDataMerger::VersionPair> versionInfos {{version1, 1}, {version2, 2}};
    for (segmentid_t segmentId = 1; segmentId < 5; segmentId++) {
        size_t belongToVersion = segmentId % 2;
        SegmentTemperatureMeta temperatureMeta {segmentId, "hot", ""};
        indexlibv2::framework::SegmentStatistics segStats;
        segStats.SetSegmentId(segmentId);
        stat = {segmentId, segmentId + 1};
        segStats.AddStatistic("key", stat);
        versionInfos[belongToVersion].first.AddSegment(segmentId);
        versionInfos[belongToVersion].first.AddSegTemperatureMeta(temperatureMeta);
        versionInfos[belongToVersion].first.AddSegmentStatistics(segStats);
    }

    bool hasNewSegment = false;
    ParallelPartitionDataMerger merger(GET_PARTITION_DIRECTORY(), EpochIdUtil::TEST_GenerateEpochId(), mSchema);
    Version newVersion = merger.MakeNewVersion(baseVersion, versionInfos, hasNewSegment);
    ASSERT_TRUE(hasNewSegment);
    auto segmentIdVec = newVersion.GetSegmentVector();
    auto segmentStats = newVersion.GetSegmentStatisticsVector();
    auto temperatures = newVersion.GetSegTemperatureMetas();
    ASSERT_EQ(5, segmentIdVec.size());
    ASSERT_EQ(5, segmentStats.size());
    ASSERT_EQ(5, temperatures.size());
    for (segmentid_t segmentId = 0; segmentId < 5; segmentId++) {
        ASSERT_EQ(segmentId, segmentIdVec[segmentId]);
        ASSERT_EQ(segmentId, segmentStats[segmentId].GetSegmentId());
        auto segStats = segmentStats[segmentId].GetIntegerStats();
        ASSERT_EQ(1, segStats.size());
        ASSERT_TRUE(segStats.find("key") != segStats.end());
        ASSERT_EQ((std::pair<int64_t, int64_t>(segmentId, segmentId + 1)), segStats["key"]);
    }
}

void ParallelPartitionDataMergerTest::TestMultiWorkerToMoveData()
{
    string rootPath = GET_TEMP_DATA_PATH();
    ParallelBuildInfo info(2, 0, 0), info1(2, 0, 1);
    FslibWrapper::MkDirE(info.GetParallelPath(rootPath));
    string instancePath0 = info.ParallelBuildInfo::GetParallelInstancePath(GET_TEMP_DATA_PATH());
    string instancePath1 = info1.ParallelBuildInfo::GetParallelInstancePath(GET_TEMP_DATA_PATH());
    DirectoryPtr rootDir = GET_PARTITION_DIRECTORY();
    config::IndexPartitionOptions indexOptions;
    file_system::FileSystemOptions options = FileSystemOptions::OfflineWithBlockCache(nullptr);
    merger::MergerBranchHinter hinter(MergerBranchHinter::Option::Test());
    auto fs0 = BranchFS::CreateWithAutoFork(instancePath0, options, nullptr, &hinter);
    auto fs1 = BranchFS::CreateWithAutoFork(instancePath1, options, nullptr, &hinter);

    auto instance0 = Directory::Get(fs0->GetFileSystem());
    auto instance1 = Directory::Get(fs1->GetFileSystem());

    info.StoreIfNotExist(instance0);
    info1.StoreIfNotExist(instance1);

    VersionMaker::MakeIncSegment(rootDir, 1, true);
    VersionMaker::MakeIncSegment(rootDir, 2, true);
    VersionMaker::MakeIncSegment(instance0, 3, true);
    VersionMaker::MakeIncSegment(instance0, 4, true);
    // VersionMaker::MakeIncSegment(instance1, 4, true);
    VersionMaker::MakeIncSegment(instance1, 5, true);
    Version lastVersion, version, version1;
    lastVersion.AddSegment(1);
    lastVersion.AddSegment(2);

    version.AddSegment(3);
    version.AddSegment(4);

    version1.AddSegment(5);

    lastVersion.SetVersionId(0);
    version.SetVersionId(1);
    version1.SetVersionId(1);
    lastVersion.Store(rootDir, false);
    version.Store(instance0, false);
    version1.Store(instance1, false);
    fs0->CommitToDefaultBranch(nullptr);
    fs1->CommitToDefaultBranch(nullptr);

    srand((unsigned)time(NULL));
    bool needFailOver = (rand() % 2 == 0);

    int32_t mergeCount = 10;
    int32_t failedCount = 0;
    std::mutex lock;
    vector<autil::ThreadPtr> threads;
    {
        for (size_t i = 0; i < mergeCount; i++) {
            usleep(10);
            autil::ThreadPtr thread = autil::Thread::createThread([&]() {
                lock.lock();
                RESET_FILE_SYSTEM();
                DirectoryPtr rootDir = GET_PARTITION_DIRECTORY();
                lock.unlock();
                try {
                    ParallelPartitionDataMerger merger(rootDir, EpochIdUtil::TEST_GenerateEpochId(), mSchema,
                                                       indexOptions);
                    merger.MergeSegmentData({instancePath0, instancePath1});
                } catch (std::exception& e) {
                    failedCount++;
                }
            });
            threads.push_back(thread);
        }
    }
    for (auto& thread : threads) {
        thread->join();
    }

    // all failed or try fail over
    if (failedCount == 10 || needFailOver) {
        RESET_FILE_SYSTEM();
        DirectoryPtr rootDir = GET_PARTITION_DIRECTORY();
        ParallelPartitionDataMerger merger(rootDir, EpochIdUtil::TEST_GenerateEpochId(), mSchema, indexOptions);
        merger.MergeSegmentData({instancePath0, instancePath1});
    }
    VersionLoader versionLoader;
    Version targetVersion;
    versionLoader.GetVersionS(rootPath, targetVersion, INVALID_VERSION);

    ASSERT_EQ(1, targetVersion.GetVersionId());
    ASSERT_EQ(6, targetVersion.GetSegmentCount());
}

void ParallelPartitionDataMergerTest::TestLocatorAndTimestamp()
{
    string rootPath = GET_TEMP_DATA_PATH();
    ParallelBuildInfo info(2, 0, 0), info1(2, 0, 1);
    FslibWrapper::MkDirE(info.GetParallelPath(rootPath));
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

    lastVersion.SetLocator(Locator(IndexLocator(1, 10, /*userData=*/"").toString()));
    version0.SetLocator(Locator(IndexLocator(1, 9, /*userData=*/"").toString()));
    version1.SetLocator(Locator(IndexLocator(1, 12, /*userData=*/"").toString()));

    lastVersion.SetTimestamp(11);
    version0.SetTimestamp(13);
    version1.SetTimestamp(12);
    lastVersion.Store(rootDir, false);
    version0.Store(rootDir, false);
    SetLastSegmentInfo(rootDir, 1, 10, 8, 15);
    version1.Store(rootDir, false);
    SetLastSegmentInfo(rootDir, 1, 13, 9, 20);
    std::vector<ParallelPartitionDataMerger::VersionPair> versionInfos = {make_pair(version0, 0),
                                                                          make_pair(version1, 1)};
    ParallelPartitionDataMerger merger(GET_PARTITION_DIRECTORY(), EpochIdUtil::TEST_GenerateEpochId(), mSchema);
    bool hasNewSegment = false;
    Version targetVersion = merger.MakeNewVersion(lastVersion, versionInfos, hasNewSegment);
    SegmentInfo segInfo = merger.CreateMergedSegmentInfo(versionInfos);
    Locator segmentLocator(segInfo.GetLocator().Serialize());

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
    // baseVersion
    auto rootDir = DirectoryCreator::CreateOfflineRootDir(GET_TEMP_DATA_PATH());
    VersionMaker::MakeIncSegment(rootDir, 1, true);
    Version lastVersion, version0, version1;
    lastVersion.AddSegment(1);
    lastVersion.SetVersionId(0);
    lastVersion.SetLocator(Locator(IndexLocator(1, 10, /*userData=*/"").toString()));
    lastVersion.SetTimestamp(11);
    lastVersion.AddDescription("key1", "value1");
    lastVersion.AddDescription("batchId", "1");
    lastVersion.Store(rootDir, false);
    vector<std::string> mergeSrc;
    {
        ParallelBuildInfo info(2, 0, 0);
        info.SetBaseVersion(lastVersion.GetVersionId());
        auto instancePath = info.ParallelBuildInfo::GetParallelInstancePath(GET_TEMP_DATA_PATH());
        auto instanceDir = DirectoryCreator::CreateOfflineRootDir(instancePath);
        mergeSrc.push_back(instancePath);
        info.StoreIfNotExist(instanceDir);
        Version version;
        version.AddSegment(1);
        version.SetVersionId(2);
        version.SetTimestamp(100);
        version.Store(instanceDir, false);
        ASSERT_EQ(FSEC_OK, instanceDir->GetFileSystem()->MountFile(
                               instancePath, ParallelBuildInfo::PARALLEL_BUILD_INFO_FILE,
                               ParallelBuildInfo::PARALLEL_BUILD_INFO_FILE, FSMT_READ_WRITE, -1, true));
    }
    {
        ParallelBuildInfo info(2, 0, 1);
        info.SetBaseVersion(lastVersion.GetVersionId());
        auto instancePath = info.ParallelBuildInfo::GetParallelInstancePath(GET_TEMP_DATA_PATH());
        auto instanceDir = DirectoryCreator::CreateOfflineRootDir(instancePath);
        mergeSrc.push_back(instancePath);
        info.StoreIfNotExist(instanceDir);
        Version version;
        version.AddSegment(1);
        version.SetVersionId(2);
        version.Store(instanceDir, false);
        ASSERT_EQ(FSEC_OK, instanceDir->GetFileSystem()->MountFile(
                               instancePath, ParallelBuildInfo::PARALLEL_BUILD_INFO_FILE,
                               ParallelBuildInfo::PARALLEL_BUILD_INFO_FILE, FSMT_READ_WRITE, -1, true));
    }
    {
        rootDir = DirectoryCreator::CreateOfflineRootDir(GET_TEMP_DATA_PATH());
        ASSERT_EQ(FSEC_OK, rootDir->GetFileSystem()->MountDir(GET_TEMP_DATA_PATH(), "", "", FSMT_READ_WRITE, true));
        IndexPartitionOptions options;
        options.AddVersionDesc("batchId", "1234");
        ParallelPartitionDataMerger merger(rootDir, EpochIdUtil::TEST_GenerateEpochId(), mSchema, options);
        Version targetVersion = merger.MergeSegmentData(mergeSrc);
        ASSERT_EQ(1, targetVersion.GetVersionId());
        ASSERT_EQ(100, targetVersion.GetTimestamp());
        string batchId;
        string key1;
        ASSERT_TRUE(targetVersion.GetDescription("batchId", batchId));
        ASSERT_EQ("1234", batchId);
        ASSERT_TRUE(targetVersion.GetDescription("key1", key1));
        ASSERT_EQ("value1", key1);
        ASSERT_TRUE(rootDir->IsExist(DeployIndexWrapper::GetDeployMetaFileName(targetVersion.GetVersionId())));
    }
    {
        rootDir = DirectoryCreator::CreateOfflineRootDir(GET_TEMP_DATA_PATH());
        ASSERT_EQ(FSEC_OK, rootDir->GetFileSystem()->MountDir(GET_TEMP_DATA_PATH(), "", "", FSMT_READ_WRITE, true));
        // merger again, will not generate new version
        ParallelPartitionDataMerger merger(rootDir, EpochIdUtil::TEST_GenerateEpochId(), mSchema);
        Version targetVersion = merger.MergeSegmentData(mergeSrc);
        ASSERT_EQ(1, targetVersion.GetVersionId());
        ASSERT_EQ(100, targetVersion.GetTimestamp());
    }
}

void ParallelPartitionDataMergerTest::SetLastSegmentInfo(const indexlib::file_system::DirectoryPtr& rootDir, int src,
                                                         int offset, int64_t ts, int64_t maxTTL)
{
    Version version;
    VersionLoader::GetVersion(rootDir, version, INVALID_VERSION);
    string segDirName = version.GetSegmentDirName(version.GetLastSegment());
    auto segDir = rootDir->GetDirectory(segDirName, false);
    ASSERT_NE(segDir, nullptr);
    SegmentInfo info;
    ASSERT_TRUE(info.Load(segDir));
    IndexLocator indexLocator(-1);
    indexLocator.fromString(info.GetLocator().Serialize());
    indexLocator.setSrc(src);
    indexLocator.setOffset(offset);
    info.SetLocator(indexLocator.ToLocator());
    info.timestamp = ts;
    info.maxTTL = maxTTL;
    indexlib::file_system::RemoveOption removeOption = indexlib::file_system::RemoveOption::MayNonExist();
    segDir->RemoveFile(SEGMENT_INFO_FILE_NAME, removeOption);
    info.Store(segDir);
}

void ParallelPartitionDataMergerTest::TestNoNeedMerge()
{
    vector<ParallelPartitionDataMerger::VersionPair> versionPairs = MakeVersionPair("1,2,4;1,3,5", 0);

    Version lastVersion;
    lastVersion.AddSegment(1);
    lastVersion.AddSegment(2);
    lastVersion.AddSegment(3);
    lastVersion.AddSegment(4);
    lastVersion.SetVersionId(1);

    ASSERT_THROW(ParallelPartitionDataMerger::NoNeedMerge(versionPairs, lastVersion, 2), InconsistentStateException);
    ASSERT_FALSE(ParallelPartitionDataMerger::NoNeedMerge(versionPairs, lastVersion, 1));
    ASSERT_FALSE(ParallelPartitionDataMerger::NoNeedMerge(versionPairs, lastVersion, 0));

    lastVersion.AddSegment(5);
    ASSERT_TRUE(ParallelPartitionDataMerger::NoNeedMerge(versionPairs, lastVersion, 0));
}

int64_t ParallelPartitionDataMergerTest::GetLastSegmentLocator(const file_system::DirectoryPtr& rootDir)
{
    Version version;
    VersionLoader::GetVersion(rootDir, version, INVALID_VERSION);
    string segDirName = version.GetSegmentDirName(version.GetLastSegment());
    auto segDir = rootDir->GetDirectory(segDirName, false);
    assert(segDir != nullptr);
    SegmentInfo info;
    info.Load(segDir);
    IndexLocator indexLocator(-1);
    indexLocator.fromString(info.GetLocator().Serialize());
    return indexLocator.getOffset();
}

vector<pair<Version, size_t>> ParallelPartitionDataMergerTest::MakeVersionPair(const string& versionInfosStr,
                                                                               versionid_t baseVersion)
{
    vector<pair<Version, size_t>> versionInfos;
    vector<vector<segmentid_t>> segIdInfos;
    StringUtil::fromString(versionInfosStr, segIdInfos, ",", ";");

    for (size_t i = 0; i < segIdInfos.size(); i++) {
        Version version;
        for (auto segId : segIdInfos[i]) {
            version.AddSegment(segId);
        }
        version.SetVersionId(baseVersion + 1);
        versionInfos.push_back(make_pair(version, i));
    }
    return versionInfos;
}
}} // namespace indexlib::merger
