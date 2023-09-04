#include "indexlib/framework/TabletDeployer.h"

#include "autil/StringUtil.h"
#include "indexlib/config/OnlineConfig.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/file_system/FileInfo.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/LifecycleTable.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/framework/SegmentDescriptions.h"
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

private:
    std::shared_ptr<IDirectory> _directory;
    std::shared_ptr<IDirectory> _localDirectory;
    std::shared_ptr<IDirectory> _rawDirectory;
    std::shared_ptr<IDirectory> _remoteDirectory;
};

TEST_F(TabletDeployerTest, TestAbnormal)
{
    config::TabletOptions tabletOptions;
    TabletDeployer::GetDeployIndexMetaInputParams inputParams;
    TabletDeployer::GetDeployIndexMetaOutputParams outputParams;
    inputParams.rawPath = _rawDirectory->GetOutputPath();
    inputParams.localPath = _localDirectory->GetOutputPath();
    inputParams.remotePath = _remoteDirectory->GetOutputPath();
    inputParams.targetTabletOptions = &tabletOptions;
    inputParams.baseTabletOptions = &tabletOptions;
    inputParams.targetVersionId = INVALID_VERSIONID;
    inputParams.baseVersionId = INVALID_VERSIONID;
    // no version
    ASSERT_FALSE(TabletDeployer::GetDeployIndexMeta(inputParams, &outputParams));
    // path not exist
    inputParams.rawPath = GET_TEMP_DATA_PATH() + "notexist";
    ASSERT_FALSE(TabletDeployer::GetDeployIndexMeta(inputParams, &outputParams));
}

TEST_F(TabletDeployerTest, TestGetDeployIndexMetaBaseVersionInvalid)
{
    DeployIndexMeta remoteDeployMeta, localDeployMeta;
    config::TabletOptions tabletOptions;

    IndexFileList baseVersionDeployMeta, targetVersionDeployMeta;
    targetVersionDeployMeta.Append(FileInfo("remote_1_deploy_1", 10));
    targetVersionDeployMeta.Append(FileInfo("remote_1_deploy_0", 10));
    targetVersionDeployMeta.Append(FileInfo("remote_0_deploy_1", 10));
    targetVersionDeployMeta.Append(FileInfo("deploy_meta.2", -1));
    targetVersionDeployMeta.AppendFinal(FileInfo("version.2", 10));
    ASSERT_TRUE(_rawDirectory->Store("deploy_meta.2", targetVersionDeployMeta.ToString(), WriterOption()).OK());
    StoreVersion(2, _rawDirectory);

    FromJsonString(tabletOptions.TEST_GetOnlineConfig(), R"( { 
            "load_config": [
                {"file_patterns": ["remote_1_deploy_1"], "remote": true, "deploy": true }, 
                {"file_patterns": ["remote_1_deploy_0"], "remote": true, "deploy": false },
                {"file_patterns": ["remote_0_deploy_1"], "remote": false, "deploy": true }
            ],
            "need_read_remote_index": true
        } )");
    VersionDeployDescription versionDeployDescription;
    ASSERT_TRUE(GetDeployIndexMeta(remoteDeployMeta, localDeployMeta, 2, INVALID_VERSIONID, tabletOptions,
                                   tabletOptions, nullptr, &versionDeployDescription));
    CheckDeployMeta(remoteDeployMeta, {"remote_1_deploy_1", "remote_1_deploy_0"}, {}, __FILE__, __LINE__);
    CheckDeployMeta(localDeployMeta, {"remote_1_deploy_1", "remote_0_deploy_1", "deploy_meta.2"}, {"version.2"},
                    __FILE__, __LINE__);
}

TEST_F(TabletDeployerTest, TestGetDeployIndexMetaBaseVersionLost)
{
    DeployIndexMeta remoteDeployMeta, localDeployMeta;
    config::TabletOptions tabletOptions;

    IndexFileList targetVersionDeployMeta;
    targetVersionDeployMeta.Append(FileInfo("remote_1_deploy_1", 10));
    targetVersionDeployMeta.Append(FileInfo("remote_1_deploy_0", 10));
    targetVersionDeployMeta.Append(FileInfo("remote_0_deploy_1", 10));
    targetVersionDeployMeta.Append(FileInfo("deploy_meta.2", -1));
    targetVersionDeployMeta.AppendFinal(FileInfo("version.2", 10));
    ASSERT_TRUE(_rawDirectory->Store("deploy_meta.2", targetVersionDeployMeta.ToString(), WriterOption()).OK());
    StoreVersion(2, _rawDirectory);

    autil::legacy::FromJsonString(tabletOptions.TEST_GetOnlineConfig(), R"( {
            "load_config": [
                {"file_patterns": ["remote_1_deploy_1"], "remote": true, "deploy": true },
                {"file_patterns": ["remote_1_deploy_0"], "remote": true, "deploy": false },
                {"file_patterns": ["remote_0_deploy_1"], "remote": false, "deploy": true }
            ],
            "need_read_remote_index": true
        } )");
    VersionDeployDescription versionDeployDescription;
    ASSERT_TRUE(GetDeployIndexMeta(remoteDeployMeta, localDeployMeta, 2, 1, tabletOptions, tabletOptions, nullptr,
                                   &versionDeployDescription));
    CheckDeployMeta(remoteDeployMeta, {"remote_1_deploy_1", "remote_1_deploy_0"}, {}, __FILE__, __LINE__);
    CheckDeployMeta(localDeployMeta, {"remote_1_deploy_1", "remote_0_deploy_1", "deploy_meta.2"}, {"version.2"},
                    __FILE__, __LINE__);
}

TEST_F(TabletDeployerTest, TestGetDeployIndexMetaComplex)
{
    DeployIndexMeta remoteDeployMeta, localDeployMeta;
    config::TabletOptions baseTabletOptions, targetTabletOptions;

    IndexFileList baseVersionDeployMeta, targetVersionDeployMeta;
    // fileName [r1d1-r0d1] means: base loadConfig remote [true] deploy [true]
    //                             dest loadConfig remote [false] deploy [true]
    baseVersionDeployMeta.Append(FileInfo("r1d1-r1d1", 10));
    baseVersionDeployMeta.Append(FileInfo("r1d1-r1d0", 10));
    baseVersionDeployMeta.Append(FileInfo("r1d1-r0d1", 10));
    baseVersionDeployMeta.Append(FileInfo("r1d0-r1d1", 10));
    baseVersionDeployMeta.Append(FileInfo("r1d0-r1d0", 10));
    baseVersionDeployMeta.Append(FileInfo("r1d0-r0d1", 10));
    baseVersionDeployMeta.Append(FileInfo("r0d1-r1d1", 10));
    baseVersionDeployMeta.Append(FileInfo("r0d1-r1d0", 10));
    baseVersionDeployMeta.Append(FileInfo("r0d1-r0d1", 10));
    targetVersionDeployMeta = baseVersionDeployMeta;
    // base
    baseVersionDeployMeta.Append(FileInfo("deploy_meta.1", -1));
    baseVersionDeployMeta.AppendFinal(FileInfo("version.1", 10));
    ASSERT_TRUE(_localDirectory->Store("deploy_meta.1", baseVersionDeployMeta.ToString(), WriterOption()).OK());
    ASSERT_TRUE(_rawDirectory->Store("deploy_meta.1", baseVersionDeployMeta.ToString(), WriterOption()).OK());
    StoreVersion(1, _rawDirectory);

    // target
    targetVersionDeployMeta.Append(FileInfo("-----r1d1", 10)); // new files
    targetVersionDeployMeta.Append(FileInfo("-----r1d0", 10));
    targetVersionDeployMeta.Append(FileInfo("-----r0d1", 10));
    targetVersionDeployMeta.Append(FileInfo("deploy_meta.2", -1));
    targetVersionDeployMeta.AppendFinal(FileInfo("version.2", 10));
    ASSERT_TRUE(_rawDirectory->Store("deploy_meta.2", targetVersionDeployMeta.ToString(), WriterOption()).OK());
    StoreVersion(2, _rawDirectory);

    FromJsonString(targetTabletOptions.TEST_GetOnlineConfig(), R"( { 
            "load_config": [
                {"file_patterns": ["r1d1-.*"], "remote": true, "deploy": true }, 
                {"file_patterns": ["r1d0-.*"], "remote": true, "deploy": false },
                {"file_patterns": ["r0d1-.*"], "remote": false, "deploy": true }
            ],
            "need_read_remote_index": true
        } )");
    VersionDeployDescription v1DpDesc;
    ASSERT_TRUE(GetDeployIndexMeta(remoteDeployMeta, localDeployMeta, 1, -1, targetTabletOptions, baseTabletOptions,
                                   nullptr, &v1DpDesc));
    CheckDeployMeta(remoteDeployMeta, {"r1d1-r1d1", "r1d1-r1d0", "r1d1-r0d1", "r1d0-r1d1", "r1d0-r1d0", "r1d0-r0d1"},
                    {}, __FILE__, __LINE__);
    CheckDeployMeta(localDeployMeta,
                    {"r1d1-r1d1", "r1d1-r1d0", "r1d1-r0d1", "r0d1-r1d1", "r0d1-r1d0", "r0d1-r0d1", "deploy_meta.1"},
                    {"version.1"}, __FILE__, __LINE__);

    FromJsonString(baseTabletOptions.TEST_GetOnlineConfig(), R"( { 
            "load_config": [
                {"file_patterns": ["r1d1-.*"], "remote": true, "deploy": true }, 
                {"file_patterns": ["r1d0-.*"], "remote": true, "deploy": false },
                {"file_patterns": ["r0d1-.*"], "remote": false, "deploy": true }
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

    // test base versionDpDesc is legacy format and target versionDpDesc is new format
    VersionDeployDescription v2DpDesc;
    ASSERT_TRUE(GetDeployIndexMeta(remoteDeployMeta, localDeployMeta, 2, 1, targetTabletOptions, baseTabletOptions,
                                   &v1DpDesc, &v2DpDesc));
    CheckDeployMeta(remoteDeployMeta, {"r0d1-r1d1", "r0d1-r1d0", "-----r1d1", "-----r1d0"}, {}, __FILE__, __LINE__);
    CheckDeployMeta(localDeployMeta, {"r1d0-r1d1", "r1d0-r0d1", "-----r1d1", "-----r0d1", "deploy_meta.2"},
                    {"version.2"}, __FILE__, __LINE__);

    v1DpDesc.editionId = 0;
    v1DpDesc.localDeployIndexMetas.clear();
    v1DpDesc.remoteDeployIndexMetas.clear();
    ASSERT_TRUE(GetDeployIndexMeta(remoteDeployMeta, localDeployMeta, 2, 1, targetTabletOptions, baseTabletOptions,
                                   &v1DpDesc, &v2DpDesc));
    CheckDeployMeta(remoteDeployMeta, {"r0d1-r1d1", "r0d1-r1d0", "-----r1d1", "-----r1d0"}, {}, __FILE__, __LINE__);
    CheckDeployMeta(localDeployMeta, {"r1d0-r1d1", "r1d0-r0d1", "-----r1d1", "-----r0d1", "deploy_meta.2"},
                    {"version.2"}, __FILE__, __LINE__);
}

TEST_F(TabletDeployerTest, TestGetDeployIndexMetaVersionFile)
{
    IndexFileList targetVersionDeployMeta;
    targetVersionDeployMeta.Append(FileInfo("deploy_meta.2", -1));
    targetVersionDeployMeta.AppendFinal(FileInfo("version.2", 10));
    Version version(2);
    ASSERT_TRUE(
        FslibWrapper::Store(_rawDirectory->GetOutputPath() + "/deploy_meta.2", targetVersionDeployMeta.ToString())
            .OK());
    ASSERT_TRUE(FslibWrapper::Store(_rawDirectory->GetOutputPath() + "/version.2", version.ToString()).OK());

    {
        config::TabletOptions baseTabletOptions, targetTabletOptions;
        DeployIndexMeta remoteDeployMeta, localDeployMeta;
        FromJsonString(targetTabletOptions.TEST_GetOnlineConfig(), R"( {
            "load_config": [
                {"file_patterns": ["version.*"], "remote": true, "deploy": true }
            ],
            "need_read_remote_index": true
        } )");
        VersionDeployDescription versionDeployDescription;
        ASSERT_TRUE(GetDeployIndexMeta(remoteDeployMeta, localDeployMeta, 2, INVALID_VERSIONID, targetTabletOptions,
                                       baseTabletOptions, nullptr, &versionDeployDescription));
        CheckDeployMeta(remoteDeployMeta, {}, {"version.2"}, __FILE__, __LINE__);
        CheckDeployMeta(localDeployMeta, {"deploy_meta.2"}, {"version.2"}, __FILE__, __LINE__);
    }
    {
        config::TabletOptions baseTabletOptions, targetTabletOptions;
        DeployIndexMeta remoteDeployMeta, localDeployMeta;
        FromJsonString(targetTabletOptions.TEST_GetOnlineConfig(), R"( {
            "load_config": [
                {"file_patterns": ["version.*"], "remote": true, "deploy": false }
            ],
            "need_read_remote_index": true
        } )");
        VersionDeployDescription versionDeployDescription;
        ASSERT_TRUE(GetDeployIndexMeta(remoteDeployMeta, localDeployMeta, 2, INVALID_VERSIONID, targetTabletOptions,
                                       baseTabletOptions, nullptr, &versionDeployDescription));
        CheckDeployMeta(remoteDeployMeta, {}, {"version.2"}, __FILE__, __LINE__);
        CheckDeployMeta(localDeployMeta, {"deploy_meta.2"}, {}, __FILE__, __LINE__);
    }
    {
        config::TabletOptions baseTabletOptions, targetTabletOptions;
        DeployIndexMeta remoteDeployMeta, localDeployMeta;
        FromJsonString(targetTabletOptions.TEST_GetOnlineConfig(), R"( {
            "load_config": [
                {"file_patterns": ["version.*"], "remote": false, "deploy": true }
            ],
            "need_read_remote_index": true
        } )");
        VersionDeployDescription versionDeployDescription;
        ASSERT_TRUE(GetDeployIndexMeta(remoteDeployMeta, localDeployMeta, 2, INVALID_VERSIONID, targetTabletOptions,
                                       baseTabletOptions, nullptr, &versionDeployDescription));
        CheckDeployMeta(remoteDeployMeta, {}, {}, __FILE__, __LINE__);
        CheckDeployMeta(localDeployMeta, {"deploy_meta.2"}, {"version.2"}, __FILE__, __LINE__);
    }
}

TEST_F(TabletDeployerTest, TestGetDeployIndexMetaWithStaticLifecycleStrategy)
{
    DeployIndexMeta remoteDeployMeta, localDeployMeta;
    config::TabletOptions tabletOptions;

    IndexFileList baseVersionDeployMeta, targetVersionDeployMeta;
    targetVersionDeployMeta.Append(FileInfo("segment_1_level_0/", -1));
    targetVersionDeployMeta.Append(FileInfo("segment_1_level_0/attribute/", -1));
    targetVersionDeployMeta.Append(FileInfo("segment_1_level_0/attribute/attr1", 10));
    targetVersionDeployMeta.Append(FileInfo("segment_1_level_0/index/", -1));
    targetVersionDeployMeta.Append(FileInfo("segment_1_level_0/index/index1", 10));
    targetVersionDeployMeta.Append(FileInfo("segment_2_level_0/", -1));
    targetVersionDeployMeta.Append(FileInfo("segment_2_level_0/attribute/", -1));
    targetVersionDeployMeta.Append(FileInfo("segment_2_level_0/attribute/attr1", 10));
    targetVersionDeployMeta.Append(FileInfo("segment_2_level_0/index/", -1));
    targetVersionDeployMeta.Append(FileInfo("segment_2_level_0/index/index1", 10));
    targetVersionDeployMeta.Append(FileInfo("segment_3_level_0/", -1));
    targetVersionDeployMeta.Append(FileInfo("segment_3_level_0/attribute/", -1));
    targetVersionDeployMeta.Append(FileInfo("segment_3_level_0/attribute/attr1", 10));
    targetVersionDeployMeta.Append(FileInfo("segment_3_level_0/index/", -1));
    targetVersionDeployMeta.Append(FileInfo("segment_3_level_0/index/index1", 10));

    targetVersionDeployMeta.Append(FileInfo("deploy_meta.2", -1));
    targetVersionDeployMeta.AppendFinal(FileInfo("version.2", 10));
    ASSERT_TRUE(_rawDirectory->Store("deploy_meta.2", targetVersionDeployMeta.ToString(), WriterOption()).OK());
    StoreVersion(2, _rawDirectory, "1,2,3", "hot,warm,cold");

    FromJsonString(tabletOptions.TEST_GetOnlineConfig(), R"( { 
            "load_config": [
                {"file_patterns": ["attribute"], "remote": true, "deploy": true, "lifecycle": "hot"}, 
                {"file_patterns": ["index"], "remote": false, "deploy": true, "lifecycle": "hot"}, 
                {"file_patterns": ["index"], "remote": true, "deploy": true, "lifecycle": "warm"}, 
                {"file_patterns": ["attribute"], "remote": true, "deploy": false, "lifecycle": "warm"}, 
                {"file_patterns": [".+"], "remote": true, "deploy": false, "lifecycle": "cold"}
            ],
            "lifecycle_config": {
                "strategy" : "static",
                "patterns" : [
                    {"statistic_field" : "temperature", "statistic_type" : "string", "lifecycle": "hot", "range" : ["hot"]},
                    {"statistic_field" : "temperature", "statistic_type" : "string", "lifecycle": "warm", "range" : ["warm"]},
                    {"statistic_field" : "temperature", "statistic_type" : "string", "lifecycle": "cold", "range" : ["cold"]}
                ]
            },
            "need_read_remote_index": true
        } )");
    VersionDeployDescription versionDeployDescription;
    ASSERT_TRUE(GetDeployIndexMeta(remoteDeployMeta, localDeployMeta, 2, INVALID_VERSIONID, tabletOptions,
                                   tabletOptions, nullptr, &versionDeployDescription));

    auto expectLifecycleTable = std::make_shared<LifecycleTable>();
    expectLifecycleTable->AddDirectory("segment_1_level_0", "hot");
    expectLifecycleTable->AddDirectory("segment_2_level_0", "warm");
    expectLifecycleTable->AddDirectory("segment_3_level_0", "cold");
    CheckLifecycleTable(expectLifecycleTable, versionDeployDescription.GetLifecycleTable());

    CheckDeployMeta(remoteDeployMeta,
                    {"segment_1_level_0/attribute/", "segment_1_level_0/attribute/attr1", "segment_2_level_0/index/",
                     "segment_2_level_0/index/index1", "segment_2_level_0/attribute/",
                     "segment_2_level_0/attribute/attr1", "segment_3_level_0/", "segment_3_level_0/index/",
                     "segment_3_level_0/index/index1", "segment_3_level_0/attribute/",
                     "segment_3_level_0/attribute/attr1"},
                    {}, __FILE__, __LINE__);

    CheckDeployMeta(localDeployMeta,
                    {"deploy_meta.2", "segment_1_level_0/", "segment_1_level_0/attribute/",
                     "segment_1_level_0/attribute/attr1", "segment_1_level_0/index/", "segment_1_level_0/index/index1",
                     "segment_2_level_0/", "segment_2_level_0/index/", "segment_2_level_0/index/index1"},
                    {"version.2"}, __FILE__, __LINE__);
}

TEST_F(TabletDeployerTest, TestGetDeployIndexMetaWithDynamicLifecycleStrategy)
{
    DeployIndexMeta remoteDeployMeta, localDeployMeta;
    config::TabletOptions tabletOptions;

    IndexFileList targetVersionDeployMeta;
    targetVersionDeployMeta.Append(FileInfo("segment_1_level_0/", -1));
    targetVersionDeployMeta.Append(FileInfo("segment_1_level_0/attribute/", -1));
    targetVersionDeployMeta.Append(FileInfo("segment_1_level_0/attribute/attr1", 10));
    targetVersionDeployMeta.Append(FileInfo("segment_1_level_0/index/", -1));
    targetVersionDeployMeta.Append(FileInfo("segment_1_level_0/index/index1", 10));
    targetVersionDeployMeta.Append(FileInfo("segment_2_level_0/", -1));
    targetVersionDeployMeta.Append(FileInfo("segment_2_level_0/attribute/", -1));
    targetVersionDeployMeta.Append(FileInfo("segment_2_level_0/attribute/attr1", 10));
    targetVersionDeployMeta.Append(FileInfo("segment_2_level_0/index/", -1));
    targetVersionDeployMeta.Append(FileInfo("segment_2_level_0/index/index1", 10));
    targetVersionDeployMeta.Append(FileInfo("segment_3_level_0/", -1));
    targetVersionDeployMeta.Append(FileInfo("segment_3_level_0/attribute/", -1));
    targetVersionDeployMeta.Append(FileInfo("segment_3_level_0/attribute/attr1", 10));
    targetVersionDeployMeta.Append(FileInfo("segment_3_level_0/index/", -1));
    targetVersionDeployMeta.Append(FileInfo("segment_3_level_0/index/index1", 10));
    targetVersionDeployMeta.Append(FileInfo("segment_4_level_0/", -1));
    targetVersionDeployMeta.Append(FileInfo("segment_4_level_0/attribute/", -1));
    targetVersionDeployMeta.Append(FileInfo("segment_4_level_0/attribute/attr1", 10));
    targetVersionDeployMeta.Append(FileInfo("segment_4_level_0/index/", -1));
    targetVersionDeployMeta.Append(FileInfo("segment_4_level_0/index/index1", 10));

    targetVersionDeployMeta.Append(FileInfo("deploy_meta.2", -1));
    targetVersionDeployMeta.AppendFinal(FileInfo("version.2", 10));

    ASSERT_TRUE(_rawDirectory->Store("deploy_meta.2", targetVersionDeployMeta.ToString(), WriterOption()).OK());

    auto version = CreateVersion(2, "1,2,3,4");
    ASSERT_TRUE(version != nullptr);

    auto segDescriptions = version->GetSegmentDescriptions();
    ASSERT_TRUE(segDescriptions != nullptr);

    // segment 1,2,3 has stats, seg 4 has no stats
    std::vector<SegmentStatistics> segmentStatistic(3);
    segmentStatistic[0].SetSegmentId(1);
    segmentStatistic[0].AddStatistic("event_time", std::make_pair<int64_t, int64_t>(100, 200));
    segmentStatistic[1].SetSegmentId(2);
    segmentStatistic[1].AddStatistic("event_time", std::make_pair<int64_t, int64_t>(300, 400));
    segmentStatistic[2].SetSegmentId(3);
    segmentStatistic[2].AddStatistic("event_time", std::make_pair<int64_t, int64_t>(500, 600));
    segDescriptions->SetSegmentStatistics(segmentStatistic);

    ASSERT_TRUE(_rawDirectory->Store(version->GetVersionFileName(), version->ToString(), WriterOption()).OK());

    FromJsonString(tabletOptions.TEST_GetOnlineConfig(), R"( { 
            "load_config": [
                {"file_patterns": ["attribute"], "remote": true, "deploy": true, "lifecycle": "hot"}, 
                {"file_patterns": ["index"], "remote": false, "deploy": true, "lifecycle": "hot"}, 
                {"file_patterns": ["index"], "remote": true, "deploy": true, "lifecycle": "warm"}, 
                {"file_patterns": ["attribute"], "remote": true, "deploy": false, "lifecycle": "warm"}, 
                {"file_patterns": [".+"], "remote": true, "deploy": false, "lifecycle": "cold"}
            ],
            "lifecycle_config": {
                "strategy" : "dynamic",
                "patterns" : [
                    {"statistic_field" : "event_time", "statistic_type" : "integer", "offset_base" : "CURRENT_TIME","lifecycle": "hot", "range" : [550, 700], "is_offset": false}, 
                    {"statistic_field" : "event_time", "statistic_type" : "integer", "offset_base" : "CURRENT_TIME","lifecycle": "warm", "range" : [200, 500]}, 
                    {"statistic_field" : "event_time", "statistic_type" : "integer", "offset_base" : "CURRENT_TIME","lifecycle": "cold", "range" : [110, 130]}
                ]
            },
            "need_read_remote_index": true
        } )");
    VersionDeployDescription versionDeployDescription;
    ASSERT_TRUE(GetDeployIndexMeta(remoteDeployMeta, localDeployMeta, 2, INVALID_VERSIONID, tabletOptions,
                                   tabletOptions, nullptr, &versionDeployDescription));

    auto expectLifecycleTable = std::make_shared<LifecycleTable>();
    expectLifecycleTable->AddDirectory("segment_1_level_0", "cold");
    expectLifecycleTable->AddDirectory("segment_2_level_0", "warm");
    expectLifecycleTable->AddDirectory("segment_3_level_0", "hot");
    expectLifecycleTable->AddDirectory("segment_4_level_0", "");
    CheckLifecycleTable(expectLifecycleTable, versionDeployDescription.GetLifecycleTable());

    CheckDeployMeta(remoteDeployMeta,
                    {"segment_3_level_0/attribute/", "segment_3_level_0/attribute/attr1", "segment_2_level_0/index/",
                     "segment_2_level_0/index/index1", "segment_2_level_0/attribute/",
                     "segment_2_level_0/attribute/attr1", "segment_1_level_0/", "segment_1_level_0/index/",
                     "segment_1_level_0/index/index1", "segment_1_level_0/attribute/",
                     "segment_1_level_0/attribute/attr1"},
                    {}, __FILE__, __LINE__);

    CheckDeployMeta(localDeployMeta,
                    {"deploy_meta.2", "segment_4_level_0/", "segment_4_level_0/attribute/",
                     "segment_4_level_0/attribute/attr1", "segment_4_level_0/index/", "segment_4_level_0/index/index1",
                     "segment_3_level_0/", "segment_3_level_0/attribute/", "segment_3_level_0/attribute/attr1",
                     "segment_3_level_0/index/", "segment_3_level_0/index/index1", "segment_2_level_0/",
                     "segment_2_level_0/index/", "segment_2_level_0/index/index1"},
                    {"version.2"}, __FILE__, __LINE__);
}

} // namespace indexlibv2::framework
