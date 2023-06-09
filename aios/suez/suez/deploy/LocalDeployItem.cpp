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
#include "suez/deploy/LocalDeployItem.h"

#include "autil/Log.h"
#include "fslib/fs/FileSystem.h"
#include "suez/common/DiskQuotaController.h"
#include "suez/sdk/PathDefine.h"

using namespace std;
using namespace autil;
using namespace fslib;
using namespace fslib::fs;

AUTIL_DECLARE_AND_SETUP_LOGGER(suez, LocalDeployItem);

namespace suez {

LocalDeployItem::LocalDeployItem(const DeployResource &deployResource, const DeployFilesVec &deployFilesVec)
    : DeployItem(deployResource, deployFilesVec) {}

#define SET_STATUS_AND_RETURN(deployStatus)                                                                            \
    _status = deployStatus;                                                                                            \
    return _status;

DeployStatus LocalDeployItem::deployAndWaitDone() {
    unique_lock<mutex> lock(_mu);
    _status = DS_DEPLOYING;

    for (auto &deployFiles : _deployFilesVec) {
        if (_needCancel) {
            _needCancel = false;
            SET_STATUS_AND_RETURN(DS_CANCELLED);
        }

        if (!checkAndFillDeployFiles(deployFiles)) {
            SET_STATUS_AND_RETURN(DS_FAILED);
        }

        const auto &remoteDataPath = deployFiles.sourceRootPath;
        const auto &localDataPath = deployFiles.targetRootPath;
        AUTIL_LOG(DEBUG, "deploy files: %s", deployFilesSummary(deployFiles).c_str());
        AUTIL_LOG(INFO,
                  "%p begin deploy (local mode), remotePath[%s],"
                  "fileCount[%lu, %lu], deploySize[%lu], isComplete[%d], [%s --> %s]",
                  this,
                  remoteDataPath.c_str(),
                  deployFiles.deployFiles.size(),
                  deployFiles.srcFileMetas.size(),
                  deployFiles.deploySize,
                  deployFiles.isComplete,
                  deployFiles.sourceRootPath.c_str(),
                  deployFiles.targetRootPath.c_str());

        auto ret = reserveDiskQuota(remoteDataPath, localDataPath, deployFiles);
        DiskQuotaReleaseHelper release(
            _deployResource.diskQuotaController, remoteDataPath, localDataPath, deployFiles.deployFiles);
        if (ret != DS_UNKNOWN) {
            return ret;
        }

        ret = doDeploy(remoteDataPath, localDataPath, deployFiles);
        if (ret != DS_UNKNOWN) {
            SET_STATUS_AND_RETURN(ret);
        }
    }

    return SET_STATUS_AND_RETURN(DS_DEPLOYDONE);
}

DeployStatus
LocalDeployItem::doDeploy(const string &remoteDataPath, const string &localDataPath, DeployFiles &deployFiles) const {
    for (auto &fileName : deployFiles.deployFiles) {
        std::string remoteFileDir = PathDefine::join(remoteDataPath, fileName);
        std::string localFileDir = PathDefine::join(localDataPath, fileName);

        if (fslib::EC_TRUE == FileSystem::isExist(localFileDir)) {
            if (fslib::EC_OK != FileSystem::remove(localFileDir)) {
                AUTIL_LOG(INFO, "file already exist, and failed to remove file: [%s]", localFileDir.c_str());
                return DS_FAILED;
            }
        }

        auto ec = FileSystem::copy(remoteFileDir, localFileDir);
        if (ec != fslib::EC_OK) {
            AUTIL_LOG(INFO, "failed to copy file from [%s] to [%s]", remoteFileDir.c_str(), localFileDir.c_str());
            return DS_FAILED;
        }
    }

    return DS_UNKNOWN;
}

void LocalDeployItem::cancel() {
    if (_status == DS_DEPLOYING) {
        _needCancel = true;
    }
}

#undef SET_STATUS_AND_RETURN

} // namespace suez
