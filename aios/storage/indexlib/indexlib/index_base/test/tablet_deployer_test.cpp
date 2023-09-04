#include "autil/StringUtil.h"
#include "indexlib/config/OnlineConfig.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/file_system/FileInfo.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/LifecycleTable.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/framework/SegmentDescriptions.h"
#include "indexlib/framework/TabletDeployer.h"
#include "indexlib/framework/Version.h"
#include "indexlib/framework/VersionDeployDescription.h"
#include "unittest/unittest.h"

using namespace std;
using namespace indexlib::file_system;

namespace indexlibv2::framework {

class TabletDeployerTest : public TESTBASE
{
public:
    TabletDeployerTest() = default;
    ~TabletDeployerTest() = default;

public:
    void setUp() override
    {
        _directory =
            IDirectory::Get(FileSystemCreator::Create("TabletDeployerTest", GET_TEMP_DATA_PATH()).GetOrThrow());
        _rawDirectory = _directory->MakeDirectory("raw", DirectoryOption()).GetOrThrow();
        _localDirectory = _directory->MakeDirectory("local", DirectoryOption()).GetOrThrow();
        _remoteDirectory = _directory->MakeDirectory("remote", DirectoryOption()).GetOrThrow();
    }
    void tearDown() override {}

    bool GetDeployIndexMeta(DeployIndexMeta& remoteDeployIndexMeta, DeployIndexMeta& localDeployIndexMeta,
                            versionid_t targetVersionId, versionid_t baseVersionId,
                            const config::TabletOptions& targetTabletOptions,
                            const config::TabletOptions& baseTabletOptions,
                            const VersionDeployDescription* baseVersionDeployDescription,
                            VersionDeployDescription* targetVersionDeployDescription)
    {
        SCOPED_TRACE("GetDeployIndexMeta");
        TabletDeployer::GetDeployIndexMetaInputParams inputParams;
        TabletDeployer::GetDeployIndexMetaOutputParams outputParams;
        inputParams.rawPath = _rawDirectory->GetOutputPath();
        inputParams.localPath = _localDirectory->GetOutputPath();
        inputParams.remotePath = _remoteDirectory->GetOutputPath();
        inputParams.targetTabletOptions = &targetTabletOptions;
        inputParams.baseTabletOptions = &baseTabletOptions;
        inputParams.targetVersionId = targetVersionId;
        inputParams.baseVersionId = baseVersionId;

        if (baseVersionDeployDescription) {
            inputParams.baseVersionDeployDescription = *baseVersionDeployDescription;
        }

        bool ret = TabletDeployer::GetDeployIndexMeta(inputParams, &outputParams);
        if (outputParams.remoteDeployIndexMetaVec.size() == 1) {
            remoteDeployIndexMeta = *outputParams.remoteDeployIndexMetaVec[0];
        }
        if (outputParams.localDeployIndexMetaVec.size() == 1) {
            localDeployIndexMeta = *outputParams.localDeployIndexMetaVec[0];
        }
        *targetVersionDeployDescription = outputParams.versionDeployDescription;
        return ret;
    }

    void CheckDeployMeta(const DeployIndexMeta& deployMeta, vector<string> expectFileNames,
                         vector<string> expectFinalFileNames, const char* file = "", int32_t line = -1)
    {
        string msg = string(file) + ":" + autil::StringUtil::toString(line);
        // ASSERT_TRUE(deployMeta.isComplete);
        vector<string> actualFileNames, actualFinalFileNames;
        for (const FileInfo& fileInfo : deployMeta.deployFileMetas) {
            ASSERT_TRUE(fileInfo.isValid()) << msg << "\n";
            actualFileNames.push_back(fileInfo.filePath);
        }
        EXPECT_THAT(actualFileNames, testing::UnorderedElementsAreArray(expectFileNames))
            << msg << ": deployFileMetas\n";
        for (const FileInfo& fileInfo : deployMeta.finalDeployFileMetas) {
            ASSERT_TRUE(fileInfo.isValid()) << msg;
            actualFinalFileNames.push_back(fileInfo.filePath);
        }
        EXPECT_THAT(actualFinalFileNames, testing::UnorderedElementsAreArray(expectFinalFileNames))
            << msg << ": finalDeployFileMetas\n";
    }

    void CheckLifecycleTable(const std::shared_ptr<LifecycleTable>& expectLifecycleTable,
                             const std::shared_ptr<LifecycleTable>& actualLifecycleTable)
    {
        EXPECT_EQ(expectLifecycleTable->Size(), actualLifecycleTable->Size());
        for (auto it = expectLifecycleTable->Begin(); it != expectLifecycleTable->End(); it++) {
            auto actualLifecycle = actualLifecycleTable->GetLifecycle(it->first);
            EXPECT_EQ(it->second, actualLifecycle);
        }
    }

    std::shared_ptr<Version> CreateVersion(versionid_t versionId, const string& segmentsStr = "",
                                           const string& segmentTemperatureMetaStr = "")
    {
        auto version = std::make_shared<Version>(versionId);

        version->SetFenceName("");
        vector<segmentid_t> segmentIdList;
        vector<string> temperatureList;

        autil::StringUtil::fromString(segmentsStr, segmentIdList, ",");
        autil::StringUtil::fromString(segmentTemperatureMetaStr, temperatureList, ",");

        for (const auto& segmentId : segmentIdList) {
            version->AddSegment(segmentId);
        }

        if (!temperatureList.empty() && segmentIdList.size() != temperatureList.size()) {
            // ASSERT_EQ(segmentIdList.size(), temperatureList.size());
            return nullptr;
        }
        std::vector<SegmentStatistics> segStats;

        for (size_t i = 0; i < temperatureList.size(); i++) {
            SegmentStatistics segStat;
            segStat.SetSegmentId(segmentIdList[i]);
            segStat.AddStatistic("temperature", temperatureList[i]);
            segStats.push_back(segStat);
        }
        auto segDescPtr = version->GetSegmentDescriptions();
        if (segDescPtr == nullptr) {
            return nullptr;
        }
        segDescPtr->SetSegmentStatistics(segStats);
        //  ASSERT_TRUE(segDescPtr != nullptr);
        return version;
    }

    void StoreVersion(versionid_t versionId, std::shared_ptr<IDirectory> directory, const string& segmentsStr = "",
                      const string& segmentTemperatureMetaStr = "")
    {
        auto version = CreateVersion(versionId, segmentsStr, segmentTemperatureMetaStr);
        ASSERT_TRUE(directory->Store(version->GetVersionFileName(), version->ToString(), WriterOption()).OK());
    }

    void CreateFiles(const std::shared_ptr<IDirectory>& directory, const string& filePaths)
    {
        vector<string> fileNames;
        fileNames = autil::StringUtil::split(filePaths, ";");
        for (size_t i = 0; i < fileNames.size(); ++i) {
            ASSERT_TRUE(directory->Store(fileNames[i], "", WriterOption()).OK());
        }
    }

private:
    std::shared_ptr<IDirectory> _directory;
    std::shared_ptr<IDirectory> _localDirectory;
    std::shared_ptr<IDirectory> _rawDirectory;
    std::shared_ptr<IDirectory> _remoteDirectory;
};

// does not support legacy index, neither entry table nor deploy meta
TEST_F(TabletDeployerTest, DISABLED_TestGetDeployIndexMetaDeployMetaNotExist)
{
    DeployIndexMeta remoteDeployMeta, localDeployMeta;
    config::TabletOptions baseTabletOptions, targetTabletOptions;

    IndexFileList seg0DeployMeta, seg2DeployMeta;
    // fileName [r1d1-r0d1] means: base loadConfig remote [true] deploy [true]
    //                             dest loadConfig remote [false] deploy [true]
    // base
    seg0DeployMeta.Append(FileInfo("r1d1-r1d1", 10));
    seg0DeployMeta.Append(FileInfo("r1d1-r1d0", 10));
    seg0DeployMeta.Append(FileInfo("r1d1-r0d1", 10));
    seg0DeployMeta.Append(FileInfo("r1d0-r1d1", 10));
    seg0DeployMeta.Append(FileInfo("r1d0-r1d0", 10));
    seg0DeployMeta.Append(FileInfo("r1d0-r0d1", 10));
    seg0DeployMeta.Append(FileInfo("r0d1-r1d1", 10));
    seg0DeployMeta.Append(FileInfo("r0d1-r1d0", 10));
    seg0DeployMeta.Append(FileInfo("r0d1-r0d1", 10));
    const auto seg0Dir = _rawDirectory->MakeDirectory("segment_0_level_0", DirectoryOption()).GetOrThrow();
    seg0Dir->Store("deploy_index", seg0DeployMeta.ToString(), WriterOption()).GetOrThrow();
    CreateFiles(seg0Dir, "r1d1-r1d1;r1d1-r1d0;r1d1-r0d1;r1d0-r1d1;r1d0-r1d0;r1d0-r0d1;r0d1-r1d1;r0d1-r1d0;r0d1-"
                         "r0d1");
    StoreVersion(0, _rawDirectory, "0");
    // target
    seg2DeployMeta.Append(FileInfo("-----r1d1", 10));
    seg2DeployMeta.Append(FileInfo("-----r1d0", 10));
    seg2DeployMeta.Append(FileInfo("-----r0d1", 10));
    const auto seg2Dir = _rawDirectory->MakeDirectory("segment_2_level_0", DirectoryOption()).GetOrThrow();
    seg2Dir->Store("deploy_index", seg2DeployMeta.ToString(), WriterOption()).GetOrThrow();
    CreateFiles(seg2Dir, "-----r1d1;-----r1d0;-----r0d1");

    // local
    const auto localSeg1Dir = _localDirectory->MakeDirectory("segment_1_level_0", DirectoryOption()).GetOrThrow();
    StoreVersion(1, _localDirectory, "1");
    StoreVersion(2, _rawDirectory, "0,2");

    FromJsonString(baseTabletOptions.TEST_GetOnlineConfig(), R"( {
        "load_config": [
            {"file_patterns": ["r1d1-.*"], "remote": true, "deploy": true },
            {"file_patterns": ["r1d0-.*"], "remote": true, "deploy": false },
            {"file_patterns": ["r0d1-.*"], "remote": false, "deploy": true },
            {"file_patterns": ["deploy_index"], "remote": true, "deploy": false }
        ],
        "need_read_remote_index": true
    } )");
    FromJsonString(targetTabletOptions.TEST_GetOnlineConfig(), R"( {
        "load_config": [
            {"file_patterns": [".*-r1d1"], "remote": true, "deploy": true },
            {"file_patterns": [".*-r1d0"], "remote": true, "deploy": false },
            {"file_patterns": [".*-r0d1"], "remote": false, "deploy": true }
        ],
        "need_read_remote_index": true
    } )");
    VersionDeployDescription versionDeployDescription;
    ASSERT_TRUE(GetDeployIndexMeta(remoteDeployMeta, localDeployMeta, 2, 0, targetTabletOptions, baseTabletOptions,
                                   nullptr, &versionDeployDescription));
    CheckDeployMeta(remoteDeployMeta,
                    {"segment_0_level_0/r0d1-r1d1", "segment_0_level_0/r0d1-r1d0", "segment_2_level_0/-----r1d1",
                     "segment_2_level_0/-----r1d0"},
                    {}, __FILE__, __LINE__);
    CheckDeployMeta(localDeployMeta,
                    {"segment_0_level_0/r1d0-r1d1", "segment_0_level_0/r1d0-r0d1", "segment_0_level_0/deploy_index",
                     "segment_2_level_0/-----r1d1", "segment_2_level_0/-----r0d1", "segment_2_level_0/deploy_index",
                     "segment_2_level_0/"},
                    {"version.2"}, __FILE__, __LINE__);
}

TEST_F(TabletDeployerTest, TestGetDeployIndexMetaWithDisableAttributeNoRemote)
{
    DeployIndexMeta remoteDeployMeta, localDeployMeta;
    config::TabletOptions tabletOptions;
    tabletOptions.SetIsLegacy(true);

    IndexFileList baseVersionDeployMeta, targetVersionDeployMeta;
    targetVersionDeployMeta.Append(FileInfo("segment_1_level_0/", -1));
    targetVersionDeployMeta.Append(FileInfo("segment_1_level_0/attribute/", -1));
    targetVersionDeployMeta.Append(FileInfo("segment_1_level_0/attribute/norm_attr_1/", -1));
    targetVersionDeployMeta.Append(FileInfo("segment_1_level_0/attribute/disable_attr_1/", -1));
    targetVersionDeployMeta.Append(FileInfo("segment_1_level_0/attribute/norm_attr_1/data", 10));
    targetVersionDeployMeta.Append(FileInfo("segment_1_level_0/attribute/disable_attr_1/data", 10));
    targetVersionDeployMeta.Append(FileInfo("deploy_meta.2", -1));
    targetVersionDeployMeta.AppendFinal(FileInfo("version.2", 10));
    ASSERT_TRUE(_rawDirectory->Store("deploy_meta.2", targetVersionDeployMeta.ToString(), WriterOption()).OK());
    StoreVersion(2, _rawDirectory);

    FromJsonString(tabletOptions, R"( {
        "online_index_config": {
            "load_config": [{"file_patterns": ["attribute"], "remote": true, "deploy": true }],
            "need_read_remote_index": false,
            "disable_fields": { "attributes": ["disable_attr_1"] }
        }
    } )");
    VersionDeployDescription versionDeployDescription;
    ASSERT_TRUE(GetDeployIndexMeta(remoteDeployMeta, localDeployMeta, 2, INVALID_VERSIONID, tabletOptions,
                                   tabletOptions, nullptr, &versionDeployDescription));
    CheckDeployMeta(remoteDeployMeta, {}, {});
    CheckDeployMeta(localDeployMeta,
                    {"segment_1_level_0/", "segment_1_level_0/attribute/", "segment_1_level_0/attribute/norm_attr_1/",
                     "segment_1_level_0/attribute/norm_attr_1/data", "deploy_meta.2"},
                    {"version.2"});
}

TEST_F(TabletDeployerTest, TestGetDeployIndexMetaWithDisableAttribute)
{
    DeployIndexMeta remoteDeployMeta, localDeployMeta;
    config::TabletOptions tabletOptions;
    tabletOptions.SetIsLegacy(true);

    IndexFileList baseVersionDeployMeta, targetVersionDeployMeta;
    targetVersionDeployMeta.Append(FileInfo("segment_1_level_0/", -1));
    targetVersionDeployMeta.Append(FileInfo("segment_1_level_0/attribute/", -1));
    targetVersionDeployMeta.Append(FileInfo("segment_1_level_0/attribute/norm_attr_1/", -1));
    targetVersionDeployMeta.Append(FileInfo("segment_1_level_0/attribute/norm_attr_1/data", 10));
    targetVersionDeployMeta.Append(FileInfo("segment_1_level_0/attribute/disable_attr_1/", -1));
    targetVersionDeployMeta.Append(FileInfo("segment_1_level_0/attribute/disable_attr_1/data", 10));
    targetVersionDeployMeta.Append(FileInfo("deploy_meta.2", -1));
    targetVersionDeployMeta.AppendFinal(FileInfo("version.2", 10));
    ASSERT_TRUE(_rawDirectory->Store("deploy_meta.2", targetVersionDeployMeta.ToString(), WriterOption()).OK());
    StoreVersion(2, _rawDirectory);

    FromJsonString(tabletOptions, R"( {
        "online_index_config" : {
            "load_config": [{"file_patterns": ["attribute"], "remote": true, "deploy": true }],
            "need_read_remote_index": true,
            "disable_fields": { "attributes": ["disable_attr_1"] }
        }
    } )");
    VersionDeployDescription versionDeployDescription;
    ASSERT_TRUE(GetDeployIndexMeta(remoteDeployMeta, localDeployMeta, 2, INVALID_VERSIONID, tabletOptions,
                                   tabletOptions, nullptr, &versionDeployDescription));
    CheckDeployMeta(remoteDeployMeta,
                    {//"segment_1_level_0/",
                     "segment_1_level_0/attribute/", "segment_1_level_0/attribute/norm_attr_1/",
                     "segment_1_level_0/attribute/norm_attr_1/data"},
                    {}, __FILE__, __LINE__);
    CheckDeployMeta(localDeployMeta,
                    {"segment_1_level_0/", "segment_1_level_0/attribute/", "segment_1_level_0/attribute/norm_attr_1/",
                     "segment_1_level_0/attribute/norm_attr_1/data", "deploy_meta.2"},
                    {"version.2"}, __FILE__, __LINE__);
}

TEST_F(TabletDeployerTest, TestGetDeployIndexMetaWithDisableSummary)
{
    DeployIndexMeta remoteDeployMeta, localDeployMeta;
    config::TabletOptions tabletOptions;
    tabletOptions.SetIsLegacy(true);

    IndexFileList baseVersionDeployMeta, targetVersionDeployMeta;
    targetVersionDeployMeta.Append(FileInfo("segment_1_level_0/", -1));
    targetVersionDeployMeta.Append(FileInfo("segment_1_level_0/summary/", -1));
    targetVersionDeployMeta.Append(FileInfo("segment_1_level_0/summary/data", 10));
    targetVersionDeployMeta.Append(FileInfo("segment_1_level_0/summary/offset", 10));
    targetVersionDeployMeta.Append(FileInfo("deploy_meta.2", -1));
    targetVersionDeployMeta.AppendFinal(FileInfo("version.2", 10));
    ASSERT_TRUE(_rawDirectory->Store("deploy_meta.2", targetVersionDeployMeta.ToString(), WriterOption()).OK());
    StoreVersion(2, _rawDirectory);

    FromJsonString(tabletOptions, R"( {
        "online_index_config" : {
            "load_config": [{"file_patterns": ["summary"], "remote": true, "deploy": true }],
            "need_read_remote_index": true,
            "disable_fields": { "summarys": "__SUMMARY_FIELD_ALL__" }
        }
    } )");
    VersionDeployDescription versionDeployDescription;
    ASSERT_TRUE(GetDeployIndexMeta(remoteDeployMeta, localDeployMeta, 2, INVALID_VERSIONID, tabletOptions,
                                   tabletOptions, nullptr, &versionDeployDescription));
    CheckDeployMeta(remoteDeployMeta, {}, {}, __FILE__, __LINE__);
    CheckDeployMeta(localDeployMeta, {"deploy_meta.2", "segment_1_level_0/"}, {"version.2"}, __FILE__, __LINE__);
}

TEST_F(TabletDeployerTest, TestGetDeployIndexMetaWithDisableSource)
{
    DeployIndexMeta remoteDeployMeta, localDeployMeta;
    config::TabletOptions tabletOptions;
    tabletOptions.SetIsLegacy(true);

    IndexFileList baseVersionDeployMeta, targetVersionDeployMeta;
    targetVersionDeployMeta.Append(FileInfo("segment_1_level_0/", -1));
    targetVersionDeployMeta.Append(FileInfo("segment_1_level_0/source/", -1));
    targetVersionDeployMeta.Append(FileInfo("segment_1_level_0/source/data", 10));
    targetVersionDeployMeta.Append(FileInfo("segment_1_level_0/source/offset", 10));
    targetVersionDeployMeta.Append(FileInfo("deploy_meta.2", -1));
    targetVersionDeployMeta.AppendFinal(FileInfo("version.2", 10));
    ASSERT_TRUE(_rawDirectory->Store("deploy_meta.2", targetVersionDeployMeta.ToString(), WriterOption()).OK());
    StoreVersion(2, _rawDirectory);

    FromJsonString(tabletOptions, R"( {
        "online_index_config" : {
            "load_config": [{"file_patterns": ["summary"], "remote": true, "deploy": true }],
            "need_read_remote_index": true,
            "disable_fields": { "sources": "__SOURCE_FIELD_ALL__" }
        }
    } )");
    VersionDeployDescription versionDeployDescription;
    ASSERT_TRUE(GetDeployIndexMeta(remoteDeployMeta, localDeployMeta, 2, INVALID_VERSIONID, tabletOptions,
                                   tabletOptions, nullptr, &versionDeployDescription));
    CheckDeployMeta(remoteDeployMeta, {}, {}, __FILE__, __LINE__);
    CheckDeployMeta(localDeployMeta, {"deploy_meta.2", "segment_1_level_0/"}, {"version.2"}, __FILE__, __LINE__);
}

TEST_F(TabletDeployerTest, TestGetDeployIndexMetaWithDisableSourceGroup)
{
    DeployIndexMeta remoteDeployMeta, localDeployMeta;
    config::TabletOptions tabletOptions;
    tabletOptions.SetIsLegacy(true);

    IndexFileList baseVersionDeployMeta, targetVersionDeployMeta;
    targetVersionDeployMeta.Append(FileInfo("segment_1_level_0/", -1));
    targetVersionDeployMeta.Append(FileInfo("segment_1_level_0/source/", -1));
    targetVersionDeployMeta.Append(FileInfo("segment_1_level_0/source/group_1/", -1));
    targetVersionDeployMeta.Append(FileInfo("segment_1_level_0/source/group_1/data", 10));
    targetVersionDeployMeta.Append(FileInfo("segment_1_level_0/source/group_1/offset", 8));
    targetVersionDeployMeta.Append(FileInfo("segment_1_level_0/source/group_0/", -1));
    targetVersionDeployMeta.Append(FileInfo("segment_1_level_0/source/group_0/data", 9));
    targetVersionDeployMeta.Append(FileInfo("segment_1_level_0/source/group_0/offset", 6));

    targetVersionDeployMeta.Append(FileInfo("deploy_meta.2", -1));
    targetVersionDeployMeta.AppendFinal(FileInfo("version.2", 10));
    ASSERT_TRUE(_rawDirectory->Store("deploy_meta.2", targetVersionDeployMeta.ToString(), WriterOption()).OK());
    StoreVersion(2, _rawDirectory);

    FromJsonString(tabletOptions, R"( {
        "online_index_config": {
            "load_config": [{"file_patterns": ["summary"], "remote": true, "deploy": true }],
            "need_read_remote_index": true,
            "disable_fields": { "sources": "__SOURCE_FIELD_GROUP__:0" }
        }
    } )");
    VersionDeployDescription versionDeployDescription;
    ASSERT_TRUE(GetDeployIndexMeta(remoteDeployMeta, localDeployMeta, 2, INVALID_VERSIONID, tabletOptions,
                                   tabletOptions, nullptr, &versionDeployDescription));
    CheckDeployMeta(remoteDeployMeta, {}, {});
    CheckDeployMeta(localDeployMeta,
                    {"segment_1_level_0/", "segment_1_level_0/source/", "segment_1_level_0/source/group_1/",
                     "segment_1_level_0/source/group_1/data", "segment_1_level_0/source/group_1/offset",
                     "deploy_meta.2"},
                    {"version.2"});
}

} // namespace indexlibv2::framework
