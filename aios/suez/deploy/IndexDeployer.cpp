/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "suez/deploy/IndexDeployer.h"

#include <algorithm>
#include <assert.h>
#include <functional>
#include <memory>
#include <ostream>
#include <stddef.h>

#include "alog/Logger.h"
#include "autil/EnvUtil.h"
#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "build_service/common_define.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/File.h"
#include "fslib/fs/FileSystem.h"
#include "fslib/util/FileUtil.h"
#include "indexlib/config/OnlineConfig.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/file_system/DeployIndexMeta.h"
#include "indexlib/file_system/FileInfo.h"
#include "indexlib/file_system/IndexFileList.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/framework/TabletDeployer.h"
#include "indexlib/framework/VersionDeployDescription.h"
#include "indexlib/index_define.h"
#include "indexlib/indexlib.h"
#include "suez/common/TablePathDefine.h"
#include "suez/deploy/DeployFiles.h"
#include "suez/deploy/FileDeployer.h"
#include "suez/sdk/PathDefine.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace fslib;
using namespace fslib::fs;
using worker_framework::DataFileMeta;
using namespace fslib::util;
AUTIL_DECLARE_AND_SETUP_LOGGER(suez, IndexDeployer);

namespace suez {

// move IGNORE_LOCAL_INDEX_CHECK from FileDeployer to here
// original commit: 913f16bcce24df213d0aeb671a28ef7ed9dcb4ca
// in repo: xxxx://invalid:isearch/suez.git
bool IndexDeployer::IGNORE_LOCAL_INDEX_CHECK = true;

IndexDeployer::IndexDeployer(const PartitionId &pid, DeployManager *deployManager)
    : _pid(pid), _fileDeployer(make_unique<FileDeployer>(deployManager)) {}

IndexDeployer::~IndexDeployer() {}

// to be removed after dp2 support
bool IndexDeployer::deployVersionFile(const string &rawPartitionPath,
                                      const string &localPartitionPath,
                                      IncVersion oldVersionId,
                                      IncVersion newVersionId) {

    DeployFiles deployFiles;
    deployFiles.sourceRootPath = rawPartitionPath;
    deployFiles.targetRootPath = localPartitionPath;
    string versionFile = getVersionFileName(newVersionId);
    deployFiles.deployFiles.push_back(versionFile);
    DataFileMeta dfm;
    dfm.path = versionFile;
    deployFiles.srcFileMetas.push_back(dfm);

    AUTIL_LOG(
        INFO, "begin deploy index version file [%s], remotePath [%s]", versionFile.c_str(), rawPartitionPath.c_str());
    DeployStatus ret = _fileDeployer->deploy(
        {deployFiles}, []() { return false; }, []() { return true; });
    logError(rawPartitionPath, oldVersionId, newVersionId, ret);
    if (DS_DEPLOYDONE != ret) {
        AUTIL_LOG(
            ERROR, "deploy version file %s failed. remotePath [%s]", versionFile.c_str(), rawPartitionPath.c_str());
        return false;
    }
    bool exist = false;
    if (!FileUtil::isFile(PathDefine::join(deployFiles.targetRootPath, versionFile), exist) || !exist) {
        string errMsg =
            formatMsg("BUG: version not exist after deploy success", oldVersionId, newVersionId, rawPartitionPath);
        AUTIL_LOG(ERROR, "%s", errMsg.c_str());
        return false;
    }
    return true;
}

static bool warmUpRemote(const indexlib::file_system::DeployIndexMetaVec &remoteDeployIndexMetaVec,
                         const indexlibv2::config::TabletOptions &tabletOptions) {
    std::stringstream arg;
    for (const auto &deployIndexMeta : remoteDeployIndexMetaVec) {
        // ugly implementation
        if (PathDefine::getFsType(deployIndexMeta->targetRootPath) != "dcache") {
            continue;
        }
        AUTIL_LOG(INFO, "building warm up string for [%s]", deployIndexMeta->targetRootPath.c_str());
        for (const auto &fileInfo : deployIndexMeta->deployFileMetas) {
            if (fileInfo.isFile()) {
                arg << PathDefine::join(deployIndexMeta->targetRootPath, fileInfo.filePath);
                arg << ":::" << std::to_string(fileInfo.fileLength) << "\n";
            }
        }
    }
    if (arg.str().empty()) {
        return true;
    }
    AUTIL_LOG(INFO, "warm up remote online config: [%s]", FastToJsonString(tabletOptions, true).c_str());
    std::string output;
    auto ec = fslib::fs::FileSystem::forward("ServerLoad", "dcache://dummy", arg.str(), output);
    if (ec != fslib::EC_OK) {
        AUTIL_LOG(WARN, "warm up dcache failed with error code:[%d]", ec);
        return false;
    }
    AUTIL_LOG(INFO, "warm up command executed with output:[%s]", output.c_str());
    return true;
}

DeployStatus IndexDeployer::deploy(const IndexPathDetail &pathDetail,
                                   IncVersion oldVersionId,
                                   IncVersion newVersionId,
                                   const indexlibv2::config::TabletOptions &baseTabletOptions,
                                   const indexlibv2::config::TabletOptions &targetTabletOptions) {
    if (oldVersionId < 0) {
        oldVersionId = INVALID_VERSION;
    }
    string rawPartitionPath = TablePathDefine::constructIndexPath(pathDetail.rawIndexRoot, _pid);
    string remotePartitionPath = TablePathDefine::constructIndexPath(pathDetail.indexRoot, _pid);
    string localPartitionPath = TablePathDefine::constructIndexPath(pathDetail.localIndexRoot, _pid);
    string loadConfigPath = pathDetail.loadConfigPath;
    auto versionFile = PathDefine::join(localPartitionPath, getVersionFileName(newVersionId));
    auto baseDoneFile = PathDefine::join(
        localPartitionPath, indexlibv2::framework::VersionDeployDescription::GetDoneFileNameByVersionId(oldVersionId));
    auto targetDoneFile = PathDefine::join(
        localPartitionPath, indexlibv2::framework::VersionDeployDescription::GetDoneFileNameByVersionId(newVersionId));
    DeployFilesVec localDeployFilesVec;

    indexlibv2::framework::VersionDeployDescription baseVersionDpDesc;
    if (!IndexDeployer::loadDeployDone(baseDoneFile, baseVersionDpDesc)) {
        AUTIL_LOG(WARN, "base version[%d] done file load failed", oldVersionId);
    }

    indexlibv2::framework::VersionDeployDescription targetVersionDpDesc;
    if (!getDeployFiles(pathDetail.rawIndexRoot,
                        pathDetail.localIndexRoot,
                        pathDetail.indexRoot,
                        baseTabletOptions,
                        targetTabletOptions,
                        oldVersionId,
                        newVersionId,
                        baseVersionDpDesc,
                        localDeployFilesVec,
                        targetVersionDpDesc)) {
        AUTIL_LOG(WARN, "get deploy files failed for [%s]", pathDetail.rawIndexRoot.c_str());
        if (targetTabletOptions.GetNeedDeployIndex()) {
            return DS_FAILED;
        }
    }
    targetVersionDpDesc.rawPath = rawPartitionPath;
    targetVersionDpDesc.remotePath = remotePartitionPath;
    targetVersionDpDesc.configPath = loadConfigPath;

    if (!targetTabletOptions.GetNeedDeployIndex()) {
        AUTIL_LOG(INFO,
                  "no need to deploy any files, oldVersion [%d], "
                  "newVersion [%d]",
                  oldVersionId,
                  newVersionId);
        return DS_DEPLOYDONE;
    }
    if (!pathDetail.checkIndexPath.empty()) {
        auto ret = _indexChecker.waitIndexReady(pathDetail.checkIndexPath);
        if (ret != DS_DEPLOYDONE) {
            return ret;
        }
    }
    const auto &checkDeployDoneFunc = [&]() -> bool {
        return IndexDeployer::checkDeployDone(versionFile, targetDoneFile, targetVersionDpDesc);
    };

    const auto &markDeployDoneFunc = [&]() {
        return deployExtraFiles(pathDetail.rawIndexRoot, pathDetail.localIndexRoot) &&
               deployVersionFile(rawPartitionPath, localPartitionPath, oldVersionId, newVersionId) &&
               IndexDeployer::markDeployDone(targetDoneFile, targetVersionDpDesc);
    };
    DeployStatus ret = DS_FAILED;
    if (localDeployFilesVec.empty()) {
        ret = markDeployDoneFunc() ? DS_DEPLOYDONE : DS_FAILED;
    } else {
        ret = _fileDeployer->deploy(localDeployFilesVec, checkDeployDoneFunc, markDeployDoneFunc);
    }

    logError(rawPartitionPath, oldVersionId, newVersionId, ret);
    return ret;
}

void IndexDeployer::cancel() {
    _fileDeployer->cancel();
    _indexChecker.cancel();
}

using DeployFileMetaVec = indexlib::file_system::IndexFileList::FileInfoVec;
using DeployFileMeta = indexlib::file_system::FileInfo;

static DeployFileMetaVec dedupDeployFileMetas(DeployFileMetaVec &inputs) {
    struct DeployFileMetaCmp {
        bool operator()(const DeployFileMeta &lhs, const DeployFileMeta &rhs) const {
            return lhs.filePath < rhs.filePath;
        }
    };
    std::sort(inputs.begin(), inputs.end(), DeployFileMetaCmp());
    DeployFileMetaVec outputs;
    if (inputs.empty()) {
        return outputs;
    }
    for (size_t i = 0; i + 1 < inputs.size(); ++i) {
        const string &fileName = inputs[i].filePath;
        const string &nextFileName = inputs[i + 1].filePath;
        if (fileName.empty()) {
            continue;
        }
        if ('/' == *fileName.rbegin() && 0 == nextFileName.find(fileName)) {
            continue;
        }
        if (0 == nextFileName.find(fileName + '/')) {
            continue;
        }
        outputs.push_back(inputs[i]);
    }
    if (!inputs.empty()) {
        outputs.push_back(inputs.back());
    }
    return outputs;
}

static bool maybeFixFileLength(const string &remotePath, DataFileMeta &dfm) {
    if (dfm.length != -1) {
        return true;
    }
    // in case of empty dir
    if (fslib::EC_TRUE == FileSystem::isDirectory(PathDefine::join(remotePath, dfm.path))) {
        dfm.length = 0;
        return true;
    }
    FileMeta fileMeta;
    auto ec = FileSystem::getFileMeta(PathDefine::join(remotePath, dfm.path), fileMeta);
    if (ec != fslib::EC_OK) {
        return false;
    }
    dfm.length = fileMeta.fileLength;
    return true;
}

static bool fillLocalDeployFilesVec(const indexlib::file_system::DeployIndexMetaVec &localDeployIndexMetaVec,
                                    DeployFilesVec &localDeployFilesVec)

{
    for (size_t i = 0; i < localDeployIndexMetaVec.size(); ++i) {
        const auto &deployIndexMeta = localDeployIndexMetaVec[i];
        auto dedupedMetas = dedupDeployFileMetas(deployIndexMeta->deployFileMetas);
        if (dedupedMetas.empty()) {
            continue;
        }
        string sourceRootPath = deployIndexMeta->sourceRootPath;
        string targetRootPath = deployIndexMeta->targetRootPath;

        assert(*sourceRootPath.rbegin() != '/');

        DeployFiles &deployFiles = localDeployFilesVec.emplace_back();
        deployFiles.sourceRootPath = sourceRootPath;
        deployFiles.targetRootPath = targetRootPath;
        deployFiles.deploySize = 0;
        deployFiles.isComplete = true;
        for (auto &meta : dedupedMetas) {
            DataFileMeta dataFileMeta;
            dataFileMeta.path = meta.filePath;
            dataFileMeta.modifyTime = meta.modifyTime;
            dataFileMeta.length = meta.fileLength;
            dataFileMeta.isCheckExist = false;
            if (!maybeFixFileLength(sourceRootPath, dataFileMeta)) {
                AUTIL_LOG(ERROR, "get file meta for %s failed", dataFileMeta.path.c_str());
                return false;
            }
            deployFiles.srcFileMetas.push_back(dataFileMeta);
            deployFiles.deployFiles.push_back(dataFileMeta.path);
            deployFiles.deploySize += dataFileMeta.length;
        }
        // only vesion.x in deployIndexMeta->finalDeployFileMetas, processed by self up to now.
    }
    return true;
}

bool IndexDeployer::getDeployFiles(const string &rawIndexRoot,
                                   const string &localRootPath,
                                   const string &remotePath,
                                   const indexlibv2::config::TabletOptions &baseTabletOptions,
                                   const indexlibv2::config::TabletOptions &targetTabletOptions,
                                   IncVersion oldVersionId,
                                   IncVersion newVersionId,
                                   const indexlibv2::framework::VersionDeployDescription &baseVersionDpDesc,
                                   DeployFilesVec &localDeployFilesVec,
                                   indexlibv2::framework::VersionDeployDescription &targetVersionDpDesc) const {
    AUTIL_LOG(INFO,
              "getDeployFiles with oldVersionId:[%d], newVersionId:[%d], rawIndexRoot:[%s], "
              "localRootPath:[%s], remotePath:[%s]",
              oldVersionId,
              newVersionId,
              rawIndexRoot.c_str(),
              localRootPath.c_str(),
              remotePath.c_str());

    const string &indexPartitionPrefix = TablePathDefine::getIndexPartitionPrefix(_pid);
    auto rawPartitionPath = PathDefine::join(rawIndexRoot, indexPartitionPrefix);
    auto localPartitionPath = PathDefine::join(localRootPath, indexPartitionPrefix);
    auto remotePartitionPath = PathDefine::join(remotePath, indexPartitionPrefix);

    indexlibv2::framework::TabletDeployer::GetDeployIndexMetaInputParams inputParams;
    inputParams.rawPath = rawPartitionPath;
    inputParams.localPath = localPartitionPath;
    inputParams.remotePath = remotePartitionPath;
    inputParams.baseTabletOptions = &baseTabletOptions;
    inputParams.targetTabletOptions = &targetTabletOptions;
    inputParams.baseVersionId = oldVersionId;
    inputParams.targetVersionId = newVersionId;
    inputParams.baseVersionDeployDescription = baseVersionDpDesc;

    indexlibv2::framework::TabletDeployer::GetDeployIndexMetaOutputParams outputParams;

    if (!indexlibv2::framework::TabletDeployer::GetDeployIndexMeta(inputParams, &outputParams)) {
        string errMsg = formatMsg("deployIndexWrapper GetDeployMetas failed", oldVersionId, newVersionId, rawIndexRoot);
        AUTIL_LOG(ERROR, "%s", errMsg.c_str());
        return false;
    }
    warmUpRemote(outputParams.remoteDeployIndexMetaVec, targetTabletOptions);

    if (!fillLocalDeployFilesVec(outputParams.localDeployIndexMetaVec, localDeployFilesVec)) {
        return false;
    }
    targetVersionDpDesc = outputParams.versionDeployDescription;
    return true;
}

bool IndexDeployer::deployExtraFiles(const string &rawIndexRoot, const string &localRootPath) const {
    DeployFiles deployFiles;
    deployFiles.sourceRootPath = rawIndexRoot;
    deployFiles.targetRootPath = localRootPath;

    vector<string> generationLevelFiles = {TablePathDefine::GENERATION_META_FILE,
                                           build_service::REALTIME_INFO_FILE_NAME};
    for (const auto &file : generationLevelFiles) {
        const string relFilePath = PathDefine::join(TablePathDefine::getIndexGenerationPrefix(_pid), file);
        string remoteFile = PathDefine::join(rawIndexRoot, relFilePath);
        bool exist = false;
        if (!FileUtil::isFile(remoteFile, exist)) {
            AUTIL_LOG(ERROR, "failed to check whether %s exist.", remoteFile.c_str());
            return false;
        }
        if (exist) {
            deployFiles.deployFiles.push_back(relFilePath);
            DataFileMeta dfm;
            dfm.path = relFilePath;
            deployFiles.srcFileMetas.push_back(dfm);
        }
    }

    // dp task will try dp full folder if empty, this situation not add dp task
    if (deployFiles.deployFiles.empty() && deployFiles.srcFileMetas.empty()) {
        return true;
    }

    DeployStatus ret = _fileDeployer->deploy(
        {deployFiles}, []() { return false; }, []() { return true; });
    if (DS_DEPLOYDONE != ret) {
        AUTIL_LOG(ERROR, "deploy generation failed. remotePath [%s]", rawIndexRoot.c_str());
        return false;
    }

    return true;
}

vector<string> IndexDeployer::dedupFiles(const vector<string> &fileNames) {
    vector<string> result;
    if (fileNames.empty()) {
        return result;
    }
    for (size_t i = 0; i < fileNames.size() - 1; i++) {
        string filename = fileNames[i];
        string nextfilename = fileNames[i + 1];
        if (filename.empty()) {
            continue;
        }
        if ('/' == filename[filename.size() - 1] && 0 == nextfilename.find(filename)) {
            continue;
        }
        if (0 == nextfilename.find(filename + '/')) {
            continue;
        }
        result.push_back(filename);
    }
    if (!fileNames.empty()) {
        result.push_back(fileNames.back());
    }
    return result;
}

// version.x
// x 为 incVersion，indexlib管理，此文件为增量版本元信息
string IndexDeployer::getVersionFileName(IncVersion version) {
    string versionFileName(indexlib::VERSION_FILE_NAME_PREFIX);
    versionFileName += ".";
    versionFileName += StringUtil::toString(version);
    return versionFileName;
}

string IndexDeployer::formatMsg(const string &msg,
                                IncVersion oldVersionId,
                                IncVersion newVersionId,
                                const string &remoteDataPath) const {
    string errMsg;
    stringstream ss;
    ss << msg;
    ss << ", remotePath: " << remoteDataPath << ", newVersionId: " << newVersionId
       << ", oldVersionId: " << oldVersionId;
    return ss.str();
}

void IndexDeployer::logError(const string &remoteDataPath,
                             IncVersion oldVersionId,
                             IncVersion newVersionId,
                             DeployStatus ret) {
    if (DS_CANCELLED == ret) {
        AUTIL_LOG(INFO, "deploy index cancelled, remoteDataPath [%s]", remoteDataPath.c_str());
    } else if (DS_FAILED == ret) {
        string errMsg = formatMsg("deploy index failed", oldVersionId, newVersionId, remoteDataPath);
        AUTIL_LOG(INFO, "%s", errMsg.c_str());
    }
}

bool IndexDeployer::cleanDoneFiles(const std::string &localRootPath, function<bool(IncVersion)> removePredicate) {
    string localIndexPath = TablePathDefine::constructIndexPath(localRootPath, _pid);
    vector<pair<string, IncVersion>> doneFileList;
    if (!listDoneFiles(localIndexPath, &doneFileList)) {
        AUTIL_LOG(ERROR, "list done files from dir [%s] failed", localIndexPath.c_str());
        return false;
    }
    for (const auto &kv : doneFileList) {
        const auto &doneFileName = kv.first;
        const auto &versionId = kv.second;
        if (removePredicate(versionId)) {
            if (!FileDeployer::cleanDeployDone(PathDefine::join(localIndexPath, doneFileName))) {
                AUTIL_LOG(ERROR, "clean done file[%s] failed", doneFileName.c_str());
                return false;
            }
        }
    }
    return true;
}

bool IndexDeployer::cleanDoneFiles(const std::string &localRootPath, const std::set<IncVersion> &inUseVersions) {
    auto predicate = [&inUseVersions](IncVersion version) { return inUseVersions.count(version) == 0; };
    return cleanDoneFiles(localRootPath, predicate);
}

bool IndexDeployer::checkDeployDone(const string &versionFile,
                                    const string &doneFile,
                                    const indexlibv2::framework::VersionDeployDescription &targetDpDescription) {
    if (IGNORE_LOCAL_INDEX_CHECK && autil::StringUtil::startsWith(targetDpDescription.rawPath, "/")) {
        AUTIL_LOG(WARN,
                  "ignore local file: [%s] with remote path: [%s]",
                  doneFile.c_str(),
                  targetDpDescription.rawPath.c_str());
        return true;
    }
    bool exist = false;
    if (!FileUtil::isFile(doneFile, exist) || !exist) {
        AUTIL_LOG(WARN, "doneFile:[%s] not exist", doneFile.c_str());
        return false;
    }
    exist = false;
    if (!versionFile.empty() && (!FileUtil::isFile(versionFile, exist) || !exist)) {
        AUTIL_LOG(WARN, "versionFile:[%s] not exist", versionFile.c_str());
        return false;
    }
    string actualContent;
    if (!FileUtil::readFile(doneFile, actualContent)) {
        AUTIL_LOG(WARN, "failed to read doneFile:[%s] content", doneFile.c_str());
        return false;
    }
    if (!targetDpDescription.CheckDeployDone(actualContent)) {
        AUTIL_LOG(WARN, "DeployDescription diff from last deployment, will trigger new deployment");
        return false;
    }
    AUTIL_LOG(INFO, "doneFile:[%s] already exists, deploy success", doneFile.c_str());
    return true;
}

bool IndexDeployer::markDeployDone(const string &doneFile,
                                   const indexlibv2::framework::VersionDeployDescription &targetDpDescription) {
    if (!FileDeployer::cleanDeployDone(doneFile)) {
        return false;
    }
    string content = FastToJsonString(targetDpDescription);
    bool ret = FileUtil::writeFile(doneFile, content);
    if (!ret) {
        AUTIL_LOG(ERROR, "mark deploy done failed, content: %s, doneFile: %s", content.c_str(), doneFile.c_str());
    }
    return ret;
}

bool IndexDeployer::loadDeployDone(const string &doneFile,
                                   indexlibv2::framework::VersionDeployDescription &versionDpDescription) {

    bool exist = false;
    if (!FileUtil::isFile(doneFile, exist) || !exist) {
        AUTIL_LOG(WARN, "doneFile:[%s] not exist", doneFile.c_str());
        return false;
    }
    string fileContent;
    if (!FileUtil::readFile(doneFile, fileContent)) {
        AUTIL_LOG(WARN, "failed to read doneFile:[%s] content", doneFile.c_str());
        return false;
    }
    if (!versionDpDescription.Deserialize(fileContent)) {
        AUTIL_LOG(ERROR, "deserialize doneFile[%s] failed, content=[%s]", doneFile.c_str(), fileContent.c_str());
        return false;
    }
    return true;
}

bool IndexDeployer::listDoneFiles(const std::string &localPartitionRoot,
                                  vector<pair<string, IncVersion>> *doneFileList) {
    fslib::FileList fileList;
    auto ec = FileSystem::listDir(localPartitionRoot, fileList);
    if (ec != fslib::EC_OK) {
        AUTIL_LOG(ERROR, "list dir [%s] failed", localPartitionRoot.c_str());
        return false;
    }
    IncVersion versionId = -1;
    for (const auto &fileName : fileList) {
        versionId = -1;
        if (!indexlibv2::framework::VersionDeployDescription::ExtractVersionId(fileName, versionId)) {
            continue;
        }
        doneFileList->emplace_back(fileName, versionId);
    }
    return true;
}

bool IndexDeployer::getNeedKeepDeployFiles(const std::string &localRootPath,
                                           IncVersion keepBeginVersionId,
                                           std::set<std::string> *whiteList) const {
    string localPartitionPath = TablePathDefine::constructIndexPath(localRootPath, _pid);
    std::vector<std::pair<std::string, IncVersion>> doneFileList;
    if (!listDoneFiles(localPartitionPath, &doneFileList)) {
        return false;
    }
    for (const auto &kv : doneFileList) {
        if (kv.second >= keepBeginVersionId) {
            auto doneFilePath = PathDefine::join(localPartitionPath, kv.first);
            if (!IndexDeployer::getLocalDeployFilesInVersion(doneFilePath, whiteList)) {
                return false;
            }
        }
    }
    return true;
}

bool IndexDeployer::getLocalDeployFilesInVersion(const std::string &doneFilePath,
                                                 std::set<std::string> *localDeployedFiles) {
    indexlibv2::framework::VersionDeployDescription versionDpDesc;
    if (!loadDeployDone(doneFilePath, versionDpDesc)) {
        AUTIL_LOG(ERROR, "get local deploy files failed, fileName[%s]", doneFilePath.c_str());
        return false;
    }
    if (!versionDpDesc.SupportFeature(
            indexlibv2::framework::VersionDeployDescription::FeatureType::DEPLOY_META_MANIFEST)) {
        return false;
    }
    versionDpDesc.GetLocalDeployFileList(localDeployedFiles);
    return true;
}

void IndexDeployer::setIgnoreLocalIndexCheckFlag(bool ignoreLocalIndexCheck) {
    IGNORE_LOCAL_INDEX_CHECK = ignoreLocalIndexCheck;
}

} // namespace suez
