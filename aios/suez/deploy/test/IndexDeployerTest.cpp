#include "suez/deploy/IndexDeployer.h"

#include <algorithm>
#include <assert.h>
#include <functional>
#include <gmock/gmock-actions.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "autil/CommonMacros.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/File.h"
#include "fslib/fs/FileSystem.h"
#include "fslib/util/FileUtil.h"
#include "indexlib/config/OnlineConfig.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/file_system/load_config/LoadConfigList.h"
#include "indexlib/framework/TabletDeployer.h"
#include "indexlib/framework/VersionDeployDescription.h"
#include "suez/common/InnerDef.h"
#include "suez/common/TableMeta.h"
#include "suez/common/TablePathDefine.h"
#include "suez/deploy/FileDeployer.h"
#include "suez/deploy/test/MockFileDeployer.h"
#include "suez/sdk/PathDefine.h"
#include "unittest/unittest.h"
using namespace std;
using namespace testing;
using namespace fslib::fs;

namespace suez {

AUTIL_DECLARE_AND_SETUP_LOGGER(suez, IndexDeployerTest);

class IndexDeployerTest : public TESTBASE {
public:
    void setUp();
    void tearDown();
    DeployStatus touchVersion(const string &path, IncVersion version, const std::string &content);
    bool InnerGetDeployFiles(IndexDeployer &indexDeployer,
                             const string &localPartitionPath,
                             const string &baseLoadConfigs,
                             const string &targetLoadConfigs,
                             std::string &indexRoot,
                             std::string &localPath,
                             std::string &remotePaht,
                             IncVersion oldVersionId,
                             IncVersion newVersionID,
                             DeployFilesVec &localDeployFilesVec);

private:
    PartitionId _pid;
    SuezPartitionType _type;
    vector<string> createdPaths;
    indexlibv2::config::TabletOptions _tabletOptions;
    std::string _localPath;
};

bool IndexDeployerTest::InnerGetDeployFiles(IndexDeployer &indexDeployer,
                                            const string &localPartitionPath,
                                            const string &baseLoadConfigs,
                                            const string &targetLoadConfigs,
                                            std::string &indexRoot,
                                            std::string &localPath,
                                            std::string &remotePath,
                                            IncVersion oldVersionId,
                                            IncVersion newVersionId,
                                            DeployFilesVec &localDeployFilesVec) {

    indexlibv2::config::TabletOptions targetTabletOptions, baseTabletOptions;
    indexlibv2::framework::VersionDeployDescription baseVersionDpDesc, targetVersionDpDesc;
    baseTabletOptions.TEST_GetOnlineConfig().TEST_SetNeedReadRemoteIndex(true);
    baseTabletOptions.TEST_GetOnlineConfig().TEST_SetNeedDeployIndex(true);

    string baseLoadJson = R"({"load_config":)" + baseLoadConfigs + R"(})";
    string targetLoadJson = R"({"load_config":)" + targetLoadConfigs + R"(})";
    autil::legacy::FastFromJsonString(baseTabletOptions.TEST_GetOnlineConfig().MutableLoadConfigList(), baseLoadJson);
    autil::legacy::FastFromJsonString(targetTabletOptions.TEST_GetOnlineConfig().MutableLoadConfigList(),
                                      targetLoadJson);
    localDeployFilesVec.clear();
    auto baseDoneFile = PathDefine::join(
        localPartitionPath, indexlibv2::framework::VersionDeployDescription::GetDoneFileNameByVersionId(oldVersionId));
    if (oldVersionId != -1 && !IndexDeployer::loadDeployDone(baseDoneFile, baseVersionDpDesc)) {
        return false;
    }
    if (!indexDeployer.getDeployFiles(indexRoot,
                                      localPath,
                                      remotePath,
                                      baseTabletOptions,
                                      targetTabletOptions,
                                      oldVersionId,
                                      newVersionId,
                                      baseVersionDpDesc,
                                      localDeployFilesVec,
                                      targetVersionDpDesc)) {
        return false;
    }
    auto targetDoneFile = PathDefine::join(
        localPartitionPath, indexlibv2::framework::VersionDeployDescription::GetDoneFileNameByVersionId(newVersionId));
    if (!suez::IndexDeployer::markDeployDone(targetDoneFile, targetVersionDpDesc)) {
        return false;
    }
    return true;
}

MATCHER_P3(PathMatch, remotePath, localPath, includeFile, "") {
    if (arg.size() != 1) {
        return false;
    }
    AUTIL_LOG(INFO,
              "source %s, target %s, files %s",
              arg[0].sourceRootPath.c_str(),
              arg[0].targetRootPath.c_str(),
              autil::StringUtil::toString(arg[0].deployFiles).c_str());
    if (arg[0].sourceRootPath != remotePath) {
        return false;
    }
    if (arg[0].targetRootPath != localPath) {
        return false;
    }
    for (const auto &file : arg[0].deployFiles) {
        if (file.find(includeFile) != std::string::npos) {
            return true;
        }
    }
    return false;
}

void IndexDeployerTest::setUp() {
    _type = SPT_INDEXLIB;
    _pid.from = 0;
    _pid.to = 65535;
    TableId tableId;
    tableId.fullVersion = 0;
    tableId.tableName = "model_table";
    _pid.tableId = tableId;
    _localPath = GET_TEST_DATA_PATH() + "testIndexDeployer_temp_dir";
    IndexDeployer::setIgnoreLocalIndexCheckFlag(false);
}

void IndexDeployerTest::tearDown() {
    for (const auto &path : createdPaths) {
        FileSystem::remove(path);
    }
    IndexDeployer::setIgnoreLocalIndexCheckFlag(true);
}

DeployStatus IndexDeployerTest::touchVersion(const string &path, IncVersion version, const string &content) {
    auto parentPath = PathDefine::join(path, TablePathDefine::getIndexPartitionPrefix(_pid));
    auto versionFile = PathDefine::join(parentPath, IndexDeployer::getVersionFileName(version));
    {
        File *file = FileSystem::openFile(versionFile, fslib::WRITE);
        assert(file);
        file->close();
        DELETE_AND_SET_NULL(file);
    }
    auto versionDoneFile = versionFile + ".done";
    {
        File *file = FileSystem::openFile(versionDoneFile, fslib::WRITE);
        file->write(content.data(), content.length());
        file->close();
        DELETE_AND_SET_NULL(file);
    }
    createdPaths.push_back(path);
    return DS_DEPLOYDONE;
}

TEST_F(IndexDeployerTest, testDedupFiles) {
    vector<string> files;
    files.push_back("a/");
    files.push_back("a/b/");
    files.push_back("a/b/c");
    files.push_back("c");
    files.push_back("c/d");
    files.push_back("f");
    files.push_back("e/c/d");
    files.push_back("e/c/d/e");
    files.push_back("e/c/d/e_c");
    EXPECT_THAT(IndexDeployer::dedupFiles(files), ElementsAre("a/b/c", "c/d", "f", "e/c/d/e", "e/c/d/e_c"));
}

TEST_F(IndexDeployerTest, testDedupFilesEmptyList) {
    vector<string> files;
    vector<string> ret = IndexDeployer::dedupFiles(files);
    EXPECT_TRUE(ret.empty());
}

TEST_F(IndexDeployerTest, testGetDeployFiles) {
    string testPath = GET_TEST_DATA_PATH() + "table_test/";
    string indexRoot = testPath + "table_deployer/index/";
    PartitionId pid;
    pid.from = 0;
    pid.to = 65535;
    TableId tableId;
    tableId.fullVersion = 0;
    tableId.tableName = "model_table";
    pid.tableId = tableId;
    IncVersion oldVersionId = -1;
    IncVersion newVersionId = 0;

    DeployFilesVec localDeployFilesVec;
    string localPath = testPath + "table_deployer/local_index/";
    string remotePath = indexRoot;
    string rawPartitionPath = indexRoot + "model_table/generation_0/partition_0_65535";
    string localPartitionPath = localPath + "model_table/generation_0/partition_0_65535";
    IndexDeployer indexDeployer(_pid, nullptr);
    indexlibv2::framework::VersionDeployDescription baseVersionDpDesc, targetVersionDpDesc;
    auto baseDoneFile = PathDefine::join(
        localPartitionPath, indexlibv2::framework::VersionDeployDescription::GetDoneFileNameByVersionId(oldVersionId));
    ASSERT_FALSE(IndexDeployer::loadDeployDone(baseDoneFile, baseVersionDpDesc));

    ASSERT_TRUE(indexDeployer.getDeployFiles(indexRoot,
                                             localPath,
                                             remotePath,
                                             _tabletOptions,
                                             _tabletOptions,
                                             oldVersionId,
                                             newVersionId,
                                             baseVersionDpDesc,
                                             localDeployFilesVec,
                                             targetVersionDpDesc));
    ASSERT_EQ(1, localDeployFilesVec.size());
    DeployFiles &deployFiles = localDeployFilesVec[0];
    ASSERT_TRUE(deployFiles.deploySize > 0);
    ASSERT_EQ(7u, deployFiles.deployFiles.size());
    EXPECT_EQ(rawPartitionPath, deployFiles.sourceRootPath);
    EXPECT_EQ(localPartitionPath, deployFiles.targetRootPath);
    EXPECT_THAT(deployFiles.deployFiles,
                ElementsAre("index_format_version",
                            "index_partition_meta",
                            "schema.json",
                            "segment_0/deploy_index",
                            "segment_0/package_file.__data__0",
                            "segment_0/package_file.__meta__",
                            "segment_0/segment_info"));

    auto srcFileMetas = deployFiles.srcFileMetas;
    ASSERT_EQ(7u, srcFileMetas.size());
    EXPECT_EQ("index_format_version", srcFileMetas[0].path);
    EXPECT_EQ("index_partition_meta", srcFileMetas[1].path);
    EXPECT_EQ("schema.json", srcFileMetas[2].path);
    EXPECT_EQ("segment_0/deploy_index", srcFileMetas[3].path);
    EXPECT_EQ("segment_0/package_file.__data__0", srcFileMetas[4].path);
    EXPECT_EQ("segment_0/package_file.__meta__", srcFileMetas[5].path);
    EXPECT_EQ("segment_0/segment_info", srcFileMetas[6].path);
    localDeployFilesVec.clear();

    auto targetDoneFile = PathDefine::join(
        localPartitionPath, indexlibv2::framework::VersionDeployDescription::GetDoneFileNameByVersionId(newVersionId));
    EXPECT_TRUE(suez::IndexDeployer::markDeployDone(targetDoneFile, targetVersionDpDesc));

    oldVersionId = 0;
    newVersionId = 1;
    baseDoneFile = PathDefine::join(
        localPartitionPath, indexlibv2::framework::VersionDeployDescription::GetDoneFileNameByVersionId(oldVersionId));
    IndexDeployer::loadDeployDone(baseDoneFile, baseVersionDpDesc);
    ASSERT_TRUE(indexDeployer.getDeployFiles(indexRoot,
                                             localPath,
                                             remotePath,
                                             _tabletOptions,
                                             _tabletOptions,
                                             oldVersionId,
                                             newVersionId,
                                             baseVersionDpDesc,
                                             localDeployFilesVec,
                                             targetVersionDpDesc));
    EXPECT_EQ(remotePath + "model_table/generation_0/partition_0_65535", deployFiles.sourceRootPath);
    EXPECT_EQ(localPath + "model_table/generation_0/partition_0_65535", deployFiles.targetRootPath);
    EXPECT_THAT(localDeployFilesVec[0].deployFiles,
                ElementsAre("segment_1/attribute/feature/data",
                            "segment_1/attribute/feature/offset",
                            "segment_1/attribute/weight/data",
                            "segment_1/deletionmap/",
                            "segment_1/deploy_index",
                            "segment_1/index/model_pk/data",
                            "segment_1/segment_info"));
    targetDoneFile = PathDefine::join(
        localPartitionPath, indexlibv2::framework::VersionDeployDescription::GetDoneFileNameByVersionId(newVersionId));
    EXPECT_TRUE(suez::IndexDeployer::markDeployDone(targetDoneFile, targetVersionDpDesc));
}

TEST_F(IndexDeployerTest, testConfigUpdateGetDeployFiles) {
    string testPath = GET_TEST_DATA_PATH() + "table_test/";
    string indexRoot = testPath + "table_deployer/index/";
    PartitionId pid;
    pid.from = 0;
    pid.to = 65535;
    TableId tableId;
    tableId.fullVersion = 0;
    tableId.tableName = "model_table";
    pid.tableId = tableId;
    DeployFilesVec localDeployFilesVec;
    string localPath = testPath + "table_deployer/local_index/";
    string remotePath = indexRoot;
    string rawPartitionPath = indexRoot + "model_table/generation_0/partition_0_65535";
    string localPartitionPath = localPath + "model_table/generation_0/partition_0_65535";
    IndexDeployer indexDeployer(pid, nullptr);

    ASSERT_TRUE(InnerGetDeployFiles(indexDeployer,
                                    localPartitionPath,
                                    R"([{"file_patterns": [".+"], "remote": false, "deploy": true}])",
                                    R"([{"file_patterns": ["attribute"], "remote": true, "deploy": false}])",
                                    indexRoot,
                                    localPath,
                                    remotePath,
                                    -1,
                                    1,
                                    localDeployFilesVec));

    ASSERT_TRUE(localDeployFilesVec.size() > 0);
    EXPECT_EQ(remotePath + "model_table/generation_0/partition_0_65535", localDeployFilesVec[0].sourceRootPath);
    EXPECT_EQ(localPath + "model_table/generation_0/partition_0_65535", localDeployFilesVec[0].targetRootPath);
    EXPECT_THAT(localDeployFilesVec[0].deployFiles,
                ElementsAre("index_format_version",
                            "index_partition_meta",
                            "schema.json",
                            "segment_1/deletionmap/",
                            "segment_1/deploy_index",
                            "segment_1/index/model_pk/data",
                            "segment_1/segment_info"));

    ASSERT_TRUE(InnerGetDeployFiles(indexDeployer,
                                    localPartitionPath,
                                    R"([{"file_patterns": ["attribute"], "remote": true, "deploy": false}])",
                                    R"([{"file_patterns": [".+"], "remote": false, "deploy": true}])",
                                    indexRoot,
                                    localPath,
                                    remotePath,
                                    1,
                                    1,
                                    localDeployFilesVec));
    ASSERT_TRUE(localDeployFilesVec.size() > 0);
    EXPECT_EQ(remotePath + "model_table/generation_0/partition_0_65535", localDeployFilesVec[0].sourceRootPath);
    EXPECT_EQ(localPath + "model_table/generation_0/partition_0_65535", localDeployFilesVec[0].targetRootPath);
    EXPECT_THAT(localDeployFilesVec[0].deployFiles,
                ElementsAre("segment_1/attribute/feature/data",
                            "segment_1/attribute/feature/offset",
                            "segment_1/attribute/weight/data"));

    ASSERT_TRUE(InnerGetDeployFiles(indexDeployer,
                                    localPartitionPath,
                                    R"([{"file_patterns": [".+"], "remote": false, "deploy": true}])",
                                    R"([{"file_patterns": ["attribute"], "remote": true, "deploy": false}])",
                                    indexRoot,
                                    localPath,
                                    remotePath,
                                    1,
                                    1,
                                    localDeployFilesVec));
    ASSERT_TRUE(localDeployFilesVec.size() == 0);

    ASSERT_TRUE(InnerGetDeployFiles(indexDeployer,
                                    localPartitionPath,
                                    R"([{"file_patterns": ["attribute"], "remote": true, "deploy": false}])",
                                    R"([{"file_patterns": [".+"], "remote": false, "deploy": true}])",
                                    indexRoot,
                                    localPath,
                                    remotePath,
                                    1,
                                    2,
                                    localDeployFilesVec));
    ASSERT_TRUE(localDeployFilesVec.size() > 0);
    EXPECT_EQ(remotePath + "model_table/generation_0/partition_0_65535", localDeployFilesVec[0].sourceRootPath);
    EXPECT_EQ(localPath + "model_table/generation_0/partition_0_65535", localDeployFilesVec[0].targetRootPath);
    EXPECT_THAT(localDeployFilesVec[0].deployFiles,
                ElementsAre("segment_1/attribute/feature/data",
                            "segment_1/attribute/feature/offset",
                            "segment_1/attribute/weight/data",
                            "segment_2/attribute/feature/data",
                            "segment_2/attribute/feature/offset",
                            "segment_2/attribute/weight/data",
                            "segment_2/deletionmap/",
                            "segment_2/deploy_index",
                            "segment_2/index/model_pk/data",
                            "segment_2/segment_info"));

    ASSERT_TRUE(InnerGetDeployFiles(indexDeployer,
                                    localPartitionPath,
                                    R"([{"file_patterns": ["attribute"], "remote": true, "deploy": false}])",
                                    R"([{"file_patterns": ["attribute"], "remote": true, "deploy": false}])",
                                    indexRoot,
                                    localPath,
                                    remotePath,
                                    1,
                                    2,
                                    localDeployFilesVec));
    ASSERT_TRUE(localDeployFilesVec.size() > 0);
    EXPECT_EQ(remotePath + "model_table/generation_0/partition_0_65535", localDeployFilesVec[0].sourceRootPath);
    EXPECT_EQ(localPath + "model_table/generation_0/partition_0_65535", localDeployFilesVec[0].targetRootPath);
    EXPECT_THAT(localDeployFilesVec[0].deployFiles,
                ElementsAre("segment_2/deletionmap/",
                            "segment_2/deploy_index",
                            "segment_2/index/model_pk/data",
                            "segment_2/segment_info"));

    ASSERT_TRUE(InnerGetDeployFiles(
        indexDeployer,
        localPartitionPath,
        R"([{"file_patterns": ["attribute"], "remote": true, "deploy": false}])",
        R"([{"file_patterns": ["attribute"], "remote": true, "deploy": false},{"file_patterns": ["index"], "remote": true, "deploy": false}])",
        indexRoot,
        localPath,
        remotePath,
        1,
        2,
        localDeployFilesVec));
    ASSERT_TRUE(localDeployFilesVec.size() > 0);
    EXPECT_EQ(remotePath + "model_table/generation_0/partition_0_65535", localDeployFilesVec[0].sourceRootPath);
    EXPECT_EQ(localPath + "model_table/generation_0/partition_0_65535", localDeployFilesVec[0].targetRootPath);
    EXPECT_THAT(localDeployFilesVec[0].deployFiles, ElementsAre("segment_2/deletionmap/", "segment_2/segment_info"));

    ASSERT_TRUE(InnerGetDeployFiles(
        indexDeployer,
        localPartitionPath,
        R"([{"file_patterns": ["attribute"], "remote": true, "deploy": false}])",
        R"([{"file_patterns": ["attribute"], "remote": true, "deploy": false},{"file_patterns": ["index"], "remote": true, "deploy": false}])",
        indexRoot,
        localPath,
        remotePath,
        1,
        2,
        localDeployFilesVec));
    ASSERT_TRUE(localDeployFilesVec.size() > 0);
    EXPECT_EQ(remotePath + "model_table/generation_0/partition_0_65535", localDeployFilesVec[0].sourceRootPath);
    EXPECT_EQ(localPath + "model_table/generation_0/partition_0_65535", localDeployFilesVec[0].targetRootPath);
    EXPECT_THAT(localDeployFilesVec[0].deployFiles, ElementsAre("segment_2/deletionmap/", "segment_2/segment_info"));
}

TEST_F(IndexDeployerTest, testConstructor) {
    PartitionId pid;
    pid.tableId.tableName = "model_table";
    pid.tableId.fullVersion = 0;
    pid.from = 0;
    pid.to = 65535;

    string baseDir = "baseDir";
    IndexDeployer indexDeployer(pid, nullptr);
    EXPECT_EQ(pid, indexDeployer._pid);
}

TEST_F(IndexDeployerTest, testGetVersionFileName) { EXPECT_EQ("version.3", IndexDeployer::getVersionFileName(3)); }

TEST_F(IndexDeployerTest, testDeployVersion) {
    PartitionId pid;
    pid.tableId.tableName = "model_table";
    pid.tableId.fullVersion = 0;
    pid.from = 0;
    pid.to = 65535;
    string localRootPath = GET_TEMPLATE_DATA_PATH();
    string localPath = PathDefine::join(localRootPath, "model_table/generation_0/partition_0_65535");
    FileSystem::remove(localRootPath);
    IndexDeployer indexDeployer(pid, nullptr);
    auto mockDeployer = new MockFileDeployer;
    indexDeployer._fileDeployer.reset(mockDeployer);
    string remoteIndexRoot = GET_TEST_DATA_PATH() + "table_test/table_deployer/index/";
    string remotePath = PathDefine::join(remoteIndexRoot, "model_table/generation_0/partition_0_65535");
    string configPath = "online_config";

    // deploy file failed
    EXPECT_CALL(*mockDeployer, doDeploy(PathMatch(remotePath, localPath, "data"))).WillOnce(Return(DS_FAILED));
    EXPECT_EQ(
        DS_FAILED,
        indexDeployer.deploy(
            {remoteIndexRoot, remoteIndexRoot, localRootPath, "", configPath}, -1, 0, _tabletOptions, _tabletOptions));

    // deploy file success, version failed
    EXPECT_CALL(*mockDeployer, doDeploy(PathMatch(remotePath, localPath, "data"))).WillOnce(Return(DS_DEPLOYDONE));
    EXPECT_CALL(*mockDeployer, doDeploy(PathMatch(remotePath, localPath, "version.0"))).WillOnce(Return(DS_FAILED));
    EXPECT_CALL(*mockDeployer, doDeploy(PathMatch(remoteIndexRoot, localRootPath, "generation_meta")))
        .WillOnce(Return(DS_DEPLOYDONE));
    EXPECT_EQ(
        DS_FAILED,
        indexDeployer.deploy(
            {remoteIndexRoot, remoteIndexRoot, localRootPath, "", configPath}, -1, 0, _tabletOptions, _tabletOptions));

    // deploy file success, version success, version not exist
    EXPECT_CALL(*mockDeployer, doDeploy(PathMatch(remotePath, localPath, "data"))).WillOnce(Return(DS_DEPLOYDONE));
    EXPECT_CALL(*mockDeployer, doDeploy(PathMatch(remotePath, localPath, "version.0"))).WillOnce(Return(DS_DEPLOYDONE));
    EXPECT_CALL(*mockDeployer, doDeploy(PathMatch(remoteIndexRoot, localRootPath, "generation_meta")))
        .WillOnce(Return(DS_DEPLOYDONE));
    EXPECT_EQ(
        DS_FAILED,
        indexDeployer.deploy(
            {remoteIndexRoot, remoteIndexRoot, localRootPath, "", configPath}, -1, 0, _tabletOptions, _tabletOptions));

    // deploy file success, version success, version exist
    EXPECT_CALL(*mockDeployer, doDeploy(PathMatch(remotePath, localPath, "data"))).WillOnce(Return(DS_DEPLOYDONE));
    EXPECT_CALL(*mockDeployer, doDeploy(PathMatch(remotePath, localPath, "version.0")))
        .WillOnce(Invoke(std::bind(&IndexDeployerTest::touchVersion,
                                   this,
                                   localRootPath,
                                   0,
                                   remotePath + ":" + remotePath + ":" + configPath)));
    EXPECT_CALL(*mockDeployer, doDeploy(PathMatch(remoteIndexRoot, localRootPath, "generation_meta")))
        .WillOnce(Return(DS_DEPLOYDONE));

    EXPECT_EQ(
        DS_DEPLOYDONE,
        indexDeployer.deploy(
            {remoteIndexRoot, remoteIndexRoot, localRootPath, "", configPath}, -1, 0, _tabletOptions, _tabletOptions));
}

TEST_F(IndexDeployerTest, testCheckIndex) {
    PartitionId pid;
    pid.tableId.tableName = "model_table";
    pid.tableId.fullVersion = 0;
    pid.from = 0;
    pid.to = 65535;
    string localRootPath = GET_TEST_DATA_PATH() + "testCheckIndex_temp_dir/";
    string localPath = PathDefine::join(localRootPath, "model_table/generation_0/partition_0_65535");
    FileSystem::remove(localRootPath);
    IndexDeployer indexDeployer(pid, nullptr);
    string remoteIndexRoot = GET_TEST_DATA_PATH() + "table_test/table_deployer/index/";
    string remotePath = PathDefine::join(remoteIndexRoot, "model_table/generation_0/partition_0_65535");
    string configPath = "online_config";

    auto mockDeployer = new MockFileDeployer;
    indexDeployer._fileDeployer.reset(mockDeployer);

    // deploy file success, version success, version exist
    EXPECT_CALL(*mockDeployer, doDeploy(PathMatch(remotePath, localPath, "data")))
        .WillOnce(Return(DS_DEPLOYDONE)); // part file
    EXPECT_CALL(*mockDeployer, doDeploy(PathMatch(remotePath, localPath, "version.0")))
        .WillOnce(Invoke(std::bind(&IndexDeployerTest::touchVersion,
                                   this,
                                   localRootPath,
                                   0,
                                   remotePath + ":" + remotePath + ":" + configPath))); // version
    EXPECT_CALL(*mockDeployer, doDeploy(PathMatch(remoteIndexRoot, localRootPath, "generation_meta")))
        .WillOnce(Return(DS_DEPLOYDONE)); // generation meta

    auto checkPath = PathDefine::join(remotePath, "version.0");
    EXPECT_EQ(
        DS_DEPLOYDONE,
        indexDeployer.deploy(
            {remoteIndexRoot, remoteIndexRoot, localRootPath, "", configPath}, -1, 0, _tabletOptions, _tabletOptions));

    touchVersion(localRootPath, 1, remotePath + ":" + remotePath + ":" + configPath);
    EXPECT_EQ(
        DS_DEPLOYDONE,
        indexDeployer.deploy(
            {remoteIndexRoot, remoteIndexRoot, localRootPath, "", configPath}, -1, 1, _tabletOptions, _tabletOptions));
}

TEST_F(IndexDeployerTest, testCheckDeployDone) {
    string localRootPath = GET_TEST_DATA_PATH() + "testCheckDeployDone_temp_dir";
    string localPath = PathDefine::join(localRootPath, "model_table/generation_0/partition_0_65535");
    FileSystem::remove(localRootPath);
    string remoteIndexRoot = GET_TEST_DATA_PATH() + "table_test/table_deployer/index/";
    string remotePath = PathDefine::join(remoteIndexRoot, "model_table/generation_0/partition_0_65535");
    string loadConfigPath = "online_config";
    touchVersion(localRootPath, 1, remotePath + ":" + remotePath + ":" + loadConfigPath);
    indexlibv2::framework::VersionDeployDescription versionDeployDescription;
    versionDeployDescription.rawPath = remotePath;
    versionDeployDescription.remotePath = remotePath;
    versionDeployDescription.configPath = loadConfigPath;
    EXPECT_TRUE(IndexDeployer::checkDeployDone(PathDefine::join(localPath, "version.1"),
                                               PathDefine::join(localPath, "version.1.done"),
                                               versionDeployDescription));
    FileSystem::remove(PathDefine::join(localPath, "version.1"));
    EXPECT_FALSE(IndexDeployer::checkDeployDone(PathDefine::join(localPath, "version.1"),
                                                PathDefine::join(localPath, "version.1.done"),
                                                versionDeployDescription));

    EXPECT_TRUE(
        IndexDeployer::checkDeployDone("", PathDefine::join(localPath, "version.1.done"), versionDeployDescription));
}

TEST_F(IndexDeployerTest, testExtractVersionId) {
    IncVersion version;
    string fileName1 = "version.1.done";
    ASSERT_TRUE(indexlibv2::framework::VersionDeployDescription::ExtractVersionId(fileName1, version));
    EXPECT_EQ(1, version);

    string fileName2 = "version.1234.done";
    ASSERT_TRUE(indexlibv2::framework::VersionDeployDescription::ExtractVersionId(fileName2, version));
    EXPECT_EQ(1234, version);

    string fileName3 = "version.1";
    ASSERT_FALSE(indexlibv2::framework::VersionDeployDescription::ExtractVersionId(fileName3, version));

    string fileName4 = "index_format_version";
    ASSERT_FALSE(indexlibv2::framework::VersionDeployDescription::ExtractVersionId(fileName4, version));
}

TEST_F(IndexDeployerTest, testCleanDoneFiles) {
    PartitionId pid;
    pid.tableId.tableName = "model_table";
    pid.tableId.fullVersion = 0;
    pid.from = 0;
    pid.to = 65535;
    IndexDeployer indexDeployer(pid, nullptr);

    string localRootPath = GET_TEST_DATA_PATH() + "testCheckDeployDone_temp_dir";
    string localPath = PathDefine::join(localRootPath, "model_table/generation_0/partition_0_65535");
    FileSystem::remove(localRootPath);

    std::set<IncVersion> inuseVersions = {2, 3};

    // list dir not exist
    ASSERT_FALSE(indexDeployer.cleanDoneFiles(localRootPath, inuseVersions));

    touchVersion(localRootPath, 1, "version1");
    touchVersion(localRootPath, 2, "version2");
    touchVersion(localRootPath, 3, "version3");
    ASSERT_TRUE(indexDeployer.cleanDoneFiles(localRootPath, inuseVersions));
    EXPECT_EQ(fslib::EC_TRUE, FileSystem::isExist(PathDefine::join(localPath, "version.1")));
    EXPECT_EQ(fslib::EC_TRUE, FileSystem::isExist(PathDefine::join(localPath, "version.2")));
    EXPECT_EQ(fslib::EC_TRUE, FileSystem::isExist(PathDefine::join(localPath, "version.3")));
    EXPECT_EQ(fslib::EC_FALSE, FileSystem::isExist(PathDefine::join(localPath, "version.1.done")));
    EXPECT_EQ(fslib::EC_TRUE, FileSystem::isExist(PathDefine::join(localPath, "version.2.done")));
    EXPECT_EQ(fslib::EC_TRUE, FileSystem::isExist(PathDefine::join(localPath, "version.3.done")));

    ASSERT_TRUE(
        indexDeployer.cleanDoneFiles(localRootPath, [](IncVersion version) { return version == 2 || version == 3; }));
    EXPECT_EQ(fslib::EC_FALSE, FileSystem::isExist(PathDefine::join(localPath, "version.2.done")));
    EXPECT_EQ(fslib::EC_FALSE, FileSystem::isExist(PathDefine::join(localPath, "version.3.done")));
}

TEST_F(IndexDeployerTest, testConfigUpdateNotEffectDeployFileList) {
    // update online conifg will change target.configPath
    // if the deployFileList generated by new config is identical with the one generated by the old config
    // then no file will be deployed

    string testPath = GET_TEST_DATA_PATH() + "table_test/";
    string indexRoot = testPath + "table_deployer/index/";
    PartitionId pid;
    pid.from = 0;
    pid.to = 65535;
    TableId tableId;
    tableId.fullVersion = 0;
    tableId.tableName = "model_table";
    pid.tableId = tableId;
    DeployFilesVec localDeployFilesVec;
    string localPath = testPath + "table_deployer/local_index/";
    string remotePath = indexRoot;
    string rawPartitionPath = indexRoot + "model_table/generation_0/partition_0_65535";
    string localPartitionPath = localPath + "model_table/generation_0/partition_0_65535";
    IndexDeployer indexDeployer(_pid, nullptr);
    indexlibv2::config::TabletOptions targetTabletOptions, baseTabletOptions;

    std::string targetLoadConfigJsonStr = R"({
        "need_read_remote_index": true,
        "need_deploy": true,
        "load_config": [
            {"file_patterns": ["attribute"], "remote": true, "deploy": false}
        ]
    })";

    autil::legacy::FastFromJsonString(targetTabletOptions.TEST_GetOnlineConfig(), targetLoadConfigJsonStr);
    IncVersion baseVersionId = -1;
    IncVersion targetVersionId = 1;
    indexlibv2::framework::VersionDeployDescription baseVersionDpDesc, targetVersionDpDesc;

    ASSERT_TRUE(indexDeployer.getDeployFiles(indexRoot,
                                             localPath,
                                             remotePath,
                                             baseTabletOptions,
                                             targetTabletOptions,
                                             baseVersionId,
                                             targetVersionId,
                                             baseVersionDpDesc,
                                             localDeployFilesVec,
                                             targetVersionDpDesc));

    ASSERT_EQ(1, localDeployFilesVec.size());
    DeployFiles &deployFiles = localDeployFilesVec[0];
    ASSERT_TRUE(deployFiles.deploySize > 0);
    ASSERT_EQ(7u, deployFiles.deployFiles.size());
    EXPECT_EQ(rawPartitionPath, deployFiles.sourceRootPath);
    EXPECT_EQ(localPartitionPath, deployFiles.targetRootPath);
    EXPECT_THAT(localDeployFilesVec[0].deployFiles,
                UnorderedElementsAre("index_format_version",
                                     "index_partition_meta",
                                     "schema.json",
                                     "segment_1/deletionmap/",
                                     "segment_1/deploy_index",
                                     "segment_1/index/model_pk/data",
                                     "segment_1/segment_info"));

    targetVersionDpDesc.rawPath = rawPartitionPath;
    targetVersionDpDesc.remotePath = remotePath;
    targetVersionDpDesc.configPath = "configPath";

    EXPECT_EQ(1, targetVersionDpDesc.localDeployIndexMetas.size());
    auto targetDoneFile =
        PathDefine::join(localPartitionPath,
                         indexlibv2::framework::VersionDeployDescription::GetDoneFileNameByVersionId(targetVersionId));
    ASSERT_TRUE(IndexDeployer::markDeployDone(targetDoneFile, targetVersionDpDesc));

    targetLoadConfigJsonStr = R"({
        "need_read_remote_index": true,
        "need_deploy": true,
        "load_config": [
            {"file_patterns": ["attribute"], "remote": true, "deploy": false},
            {"file_patterns": ["summary"], "remote": true, "deploy": false}
        ]
    })";

    autil::legacy::FastFromJsonString(targetTabletOptions.TEST_GetOnlineConfig(), targetLoadConfigJsonStr);

    indexlibv2::framework::VersionDeployDescription newVersionDpDesc;

    ASSERT_TRUE(indexDeployer.getDeployFiles(indexRoot,
                                             localPath,
                                             remotePath,
                                             baseTabletOptions,
                                             targetTabletOptions,
                                             baseVersionId,
                                             targetVersionId,
                                             baseVersionDpDesc,
                                             localDeployFilesVec,
                                             newVersionDpDesc));

    newVersionDpDesc.rawPath = targetVersionDpDesc.rawPath;
    newVersionDpDesc.remotePath = targetVersionDpDesc.remotePath;
    newVersionDpDesc.configPath = targetVersionDpDesc.configPath;

    EXPECT_EQ(1, newVersionDpDesc.localDeployIndexMetas.size());
    EXPECT_EQ(targetVersionDpDesc.localDeployIndexMetas[0]->deployFileMetas.size(),
              newVersionDpDesc.localDeployIndexMetas[0]->deployFileMetas.size());

    auto versionFile = PathDefine::join(rawPartitionPath, IndexDeployer::getVersionFileName(targetVersionId));
    ASSERT_TRUE(IndexDeployer::checkDeployDone(versionFile, targetDoneFile, newVersionDpDesc));

    newVersionDpDesc.rawPath = targetVersionDpDesc.rawPath + ".modified";
    ASSERT_FALSE(IndexDeployer::checkDeployDone(versionFile, targetDoneFile, newVersionDpDesc));

    newVersionDpDesc.rawPath = targetVersionDpDesc.rawPath;
    newVersionDpDesc.remotePath = targetVersionDpDesc.remotePath + ".modified";
    ASSERT_FALSE(IndexDeployer::checkDeployDone(versionFile, targetDoneFile, newVersionDpDesc));

    newVersionDpDesc.rawPath = targetVersionDpDesc.rawPath;
    newVersionDpDesc.remotePath = targetVersionDpDesc.remotePath;
    newVersionDpDesc.configPath = newVersionDpDesc.configPath + ".modified";
    ASSERT_TRUE(IndexDeployer::checkDeployDone(versionFile, targetDoneFile, newVersionDpDesc));
}

TEST_F(IndexDeployerTest, testDeployDoneFile) {
    string remotePath = "remotePath";
    string originalPath = "originalPath";
    string configPath = "config";
    auto doneFile = PathDefine::join(_localPath, FileDeployer::MARK_FILE_NAME);
    indexlibv2::framework::VersionDeployDescription versionDeployDescription;
    versionDeployDescription.rawPath = remotePath;
    versionDeployDescription.remotePath = originalPath;
    versionDeployDescription.configPath = configPath;

    EXPECT_TRUE(IndexDeployer::markDeployDone(doneFile, versionDeployDescription));
    EXPECT_EQ(fslib::EC_TRUE, fslib::fs::FileSystem::isFile(doneFile));
    EXPECT_TRUE(IndexDeployer::checkDeployDone("", doneFile, versionDeployDescription));
    // dup mark
    EXPECT_TRUE(IndexDeployer::markDeployDone(doneFile, versionDeployDescription));
    string content;
    ASSERT_EQ(fslib::EC_OK, fslib::fs::FileSystem::readFile(doneFile, content));
    EXPECT_EQ(content, FastToJsonString(versionDeployDescription));

    // test markDeployDone will overwrite existed done file
    versionDeployDescription.deployTime += 1;
    EXPECT_TRUE(IndexDeployer::markDeployDone(doneFile, versionDeployDescription));
    string content2;
    ASSERT_EQ(fslib::EC_OK, fslib::fs::FileSystem::readFile(doneFile, content2));
    EXPECT_EQ(content2, FastToJsonString(versionDeployDescription));
}

TEST_F(IndexDeployerTest, testVersionDoneFileBackwardCompatibility) {
    string remotePath = "remotePath";
    string rawPath = "originalPath";
    string configPath = "configPath";
    auto doneFile = PathDefine::join(_localPath, FileDeployer::MARK_FILE_NAME);
    using VersionDeployDescription = indexlibv2::framework::VersionDeployDescription;
    VersionDeployDescription versionDeployDescription;
    versionDeployDescription.rawPath = rawPath;
    versionDeployDescription.remotePath = remotePath;
    versionDeployDescription.configPath = configPath;
    // done file Version1
    {
        string version1Content = remotePath;
        ASSERT_TRUE(fslib::util::FileUtil::writeFile(doneFile, version1Content));
        EXPECT_TRUE(IndexDeployer::checkDeployDone("", doneFile, versionDeployDescription));
        ASSERT_TRUE(fslib::util::FileUtil::remove(doneFile));
    }
    // done file Version2
    {
        string version2Content = rawPath + ":" + remotePath;
        ASSERT_TRUE(fslib::util::FileUtil::writeFile(doneFile, version2Content));
        EXPECT_TRUE(IndexDeployer::checkDeployDone("", doneFile, versionDeployDescription));
        ASSERT_TRUE(fslib::util::FileUtil::remove(doneFile));
    }
    // done file Version3
    {
        string version3Content = rawPath + ":" + remotePath + ":" + configPath;
        ASSERT_TRUE(fslib::util::FileUtil::writeFile(doneFile, version3Content));
        EXPECT_TRUE(IndexDeployer::checkDeployDone("", doneFile, versionDeployDescription));
        ASSERT_TRUE(fslib::util::FileUtil::remove(doneFile));
    }
}

TEST_F(IndexDeployerTest, testGetNeedKeepDeplopyFiles) {
    PartitionId pid;
    pid.tableId.tableName = "model_table";
    pid.tableId.fullVersion = 0;
    pid.from = 0;
    pid.to = 65535;
    IndexDeployer indexDeployer(pid, nullptr);

    string localRootPath = GET_TEST_DATA_PATH() + "testGetNeedKeepDeplopyFiles_temp_dir";
    FileSystem::remove(localRootPath);
    string indexRoot = TablePathDefine::constructIndexPath(localRootPath, pid);

    std::string versionDpDescJsonStr0 = R"({
        "local_deploy_index_metas": [
            {
                "deploy_file_metas" : [
                    {"path" : "segment_0_level_0/file0"},
                    {"path" : "segment_0_level_0/file1"}
                ]
            }
        ]
    } )";

    std::string versionDpDescJsonStr1 = R"({
        "local_deploy_index_metas": [
            {
                "deploy_file_metas" : [
                    {"path" : "segment_0_level_0/file1"},
                    {"path" : "segment_0_level_0/file2"}
                ]
            }
        ]
    } )";

    std::string versionDpDescJsonStr2 = R"({
        "local_deploy_index_metas": [
            {
                "deploy_file_metas" : [
                    {"path" : "segment_0_level_0/file2"},
                    {"path" : "segment_0_level_0/file3"}
                ]
            }
        ]
    } )";

    std::string versionDpDescJsonStr3 = R"({
        "local_deploy_index_metas": [
            {
                "deploy_file_metas" : [
                    {"path" : "segment_0_level_0/file3"},
                    {"path" : "segment_0_level_0/file4"}
                ]
            }
        ]
    } )";
    using FslibWrapper = indexlib::file_system::FslibWrapper;
    ASSERT_TRUE(FslibWrapper::Store(FslibWrapper::JoinPath(indexRoot, "version.0.done"), versionDpDescJsonStr0).OK());
    ASSERT_TRUE(FslibWrapper::Store(FslibWrapper::JoinPath(indexRoot, "version.1.done"), versionDpDescJsonStr1).OK());
    ASSERT_TRUE(FslibWrapper::Store(FslibWrapper::JoinPath(indexRoot, "version.2.done"), versionDpDescJsonStr2).OK());

    ASSERT_TRUE(FslibWrapper::Store(FslibWrapper::JoinPath(indexRoot, "version.3.done."), versionDpDescJsonStr3).OK());
    ASSERT_TRUE(FslibWrapper::Store(FslibWrapper::JoinPath(indexRoot, ".version.3.done"), versionDpDescJsonStr3).OK());

    std::set<std::string> whiteList;
    EXPECT_TRUE(indexDeployer.getNeedKeepDeployFiles(localRootPath, 1, &whiteList));
    EXPECT_THAT(whiteList,
                UnorderedElementsAre("segment_0_level_0/file1", "segment_0_level_0/file2", "segment_0_level_0/file3"));
}
} // namespace suez
