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
#include "suez/deploy/FileDeployer.h"

#include "autil/EnvUtil.h"
#include "autil/StringUtil.h"
#include "fslib/util/FileUtil.h"
#include "indexlib/framework/VersionDeployDescription.h"
#include "suez/deploy/DeployItem.h"
#include "suez/deploy/DeployManager.h"
#include "suez/sdk/PathDefine.h"

using namespace std;

namespace suez {

AUTIL_DECLARE_AND_SETUP_LOGGER(suez, FileDeployer);

const string FileDeployer::MARK_FILE_NAME = "suez_deploy.done";
bool FileDeployer::IGNORE_LOCAL_FILE_CHECK = true;

FileDeployer::FileDeployer(DeployManager *deployManager) : _deployManager(deployManager) {}

FileDeployer::~FileDeployer() {}

DeployStatus FileDeployer::deploy(const string &remoteDataPath, const string &localPath) {
    const auto doneFile = PathDefine::join(localPath, FileDeployer::MARK_FILE_NAME);
    const auto &checkDeployDoneFunc = std::bind(&FileDeployer::checkDeployDone, doneFile, remoteDataPath);
    const auto &markDeployDoneFunc = std::bind(&FileDeployer::markDeployDone, doneFile, remoteDataPath);
    DeployFiles deployFilesStruct;
    deployFilesStruct.sourceRootPath = remoteDataPath;
    deployFilesStruct.targetRootPath = localPath;
    DeployFilesVec deployFilesVec = {deployFilesStruct};
    return deploy(deployFilesVec, checkDeployDoneFunc, markDeployDoneFunc);
}

DeployStatus FileDeployer::deploy(const DeployFilesVec &deployFilesVec,
                                  const std::function<bool()> &checkDeployDoneFunc,
                                  const std::function<bool()> &markDeployDoneFunc) {
    if (checkDeployDoneFunc()) {
        return DS_DEPLOYDONE;
    }
    if (auto ret = doDeploy(deployFilesVec); ret != DS_DEPLOYDONE) {
        return ret;
    }
    if (!markDeployDoneFunc()) {
        AUTIL_LOG(ERROR, "mark deployDone file failed!");
        return DS_FAILED;
    }
    return DS_DEPLOYDONE;
}

DeployStatus FileDeployer::doDeploy(const DeployFilesVec &deployFilesVec) {
    auto deployItem = _deployManager->deploy(deployFilesVec);
    {
        unique_lock<mutex> lock(_mu);
        _deployItem = deployItem;
    }
    if (auto ret = deployItem->deployAndWaitDone(); ret != DS_DEPLOYDONE) {
        return ret;
    }
    {
        unique_lock<mutex> lock(_mu);
        _deployManager->erase(deployFilesVec);
        _deployItem.reset();
    }
    return DS_DEPLOYDONE;
}

void FileDeployer::cancel() {
    unique_lock<mutex> lock(_mu);
    if (_deployItem) {
        _deployItem->cancel();
    }
}

bool FileDeployer::checkDeployDone(const string &doneFile, const string &rawPath) {
    if (IGNORE_LOCAL_FILE_CHECK && autil::StringUtil::startsWith(rawPath, "/")) {
        AUTIL_LOG(WARN, "ignore local file: [%s] with remote path: [%s]", doneFile.c_str(), rawPath.c_str());
        return true;
    }
    bool exist = false;
    if (!fslib::util::FileUtil::isFile(doneFile, exist) || !exist) {
        AUTIL_LOG(WARN, "doneFile:[%s] not exist", doneFile.c_str());
        return false;
    }
    exist = false;

    string expectContent = rawPath;
    string actualContent;
    if (!fslib::util::FileUtil::readFile(doneFile, actualContent)) {
        AUTIL_LOG(WARN, "failed to read doneFile:[%s] content", doneFile.c_str());
        return false;
    }

    string contentV2 = rawPath + ":";
    string contentV3 = rawPath + "::";

    if (actualContent != expectContent && actualContent != contentV2 && actualContent != contentV3) {
        AUTIL_LOG(WARN,
                  "doneFile:[%s] content [%s] not match with expected: [%s] or [%s] or [%s]",
                  doneFile.c_str(),
                  actualContent.c_str(),
                  expectContent.c_str(),
                  contentV2.c_str(),
                  contentV3.c_str());
        return false;
    }
    AUTIL_LOG(INFO, "doneFile:[%s] already exists, deploy success", doneFile.c_str());
    return true;
}

bool FileDeployer::markDeployDone(const string &doneFile, const string &rawPath) {
    if (!FileDeployer::cleanDeployDone(doneFile)) {
        return false;
    }
    bool ret = fslib::util::FileUtil::writeFile(doneFile, rawPath);
    if (!ret) {
        AUTIL_LOG(ERROR, "mark deploy done failed, content: %s, doneFile: %s", rawPath.c_str(), doneFile.c_str());
    }
    return ret;
}

bool FileDeployer::cleanDeployDone(const string &doneFile) {
    bool exist = false;
    if (!fslib::util::FileUtil::isFile(doneFile, exist)) {
        AUTIL_LOG(ERROR, "failed to check whether doneFile exist, localPath: %s", doneFile.c_str());
        return false;
    }
    if (!exist) {
        return true;
    }
    bool ret = fslib::util::FileUtil::remove(doneFile);
    if (!ret) {
        AUTIL_LOG(ERROR, "clean duplicate deploy done file failed, localPath: %s", doneFile.c_str());
    } else {
        AUTIL_LOG(INFO, "clean duplicate deploy done file succeeded, localPath: %s", doneFile.c_str());
    }
    return ret;
}

} // namespace suez
