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
#include "suez/deploy/NormalDeployItem.h"

#include <sstream>
#include <unistd.h>

#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"
#include "suez/common/DiskQuotaController.h"
#include "worker_framework/DataClient.h"

using namespace std;
using namespace autil;

AUTIL_DECLARE_AND_SETUP_LOGGER(suez, NormalDeployItem);

namespace suez {

using worker_framework::DataItemPtr;
using worker_framework::DataOption;

static const int CHECK_DP_STATUS_INVERVAL = 2; // 2s

DeployStatus waitDataItemDone(const DataItemPtr &dataItem) {
    if (!dataItem) {
        AUTIL_LOG(ERROR, "DataItem is NULL");
        return DS_FAILED;
    }
    do {
        auto dataStatus = dataItem->getStatus();
        string dstDir = dataItem->getDstDir();
        switch (dataStatus) {
        case worker_framework::DS_FINISHED:
            AUTIL_LOG(INFO, "DataItem for [%s] done", dstDir.c_str());
            return DS_DEPLOYDONE;
        case worker_framework::DS_FAILED:
        case worker_framework::DS_STOPPED:
        case worker_framework::DS_DEST:
            AUTIL_LOG(ERROR, "DataItem for [%s] failed", dstDir.c_str());
            return DS_FAILED;
        case worker_framework::DS_CANCELED:
            AUTIL_LOG(INFO, "DataItem for [%s] canceled", dstDir.c_str());
            return DS_CANCELLED;
        case worker_framework::DS_UNKNOWN:
        case worker_framework::DS_DEPLOYING:
        case worker_framework::DS_RETRYING:
            sleep(CHECK_DP_STATUS_INVERVAL);
            continue;
        default:
            AUTIL_LOG(ERROR, "DataItem for [%s] failed, unknown error code %d", dstDir.c_str(), dataStatus);
            return DS_FAILED;
        }
    } while (true);
    return DS_FAILED;
}

NormalDeployItem::NormalDeployItem(const DeployResource &deployResource,
                                   const DataOption &dataOption,
                                   const DeployFilesVec &deployFilesVec)
    : DeployItem(deployResource, deployFilesVec), _dataOption(dataOption) {}

DeployStatus NormalDeployItem::deployAndWaitDone() {
    vector<DataItemPtr> dataItems;
    if (auto ret = deploy(dataItems); ret != DS_UNKNOWN) {
        return ret;
    }
    auto ret = waitDone(dataItems);
    if (ret != DS_DEPLOYDONE) {
        cancel();
    }
    return ret;
};

DeployStatus NormalDeployItem::deploy(vector<DataItemPtr> &dataItems) {
    unique_lock<mutex> lock(_mu);
    for (auto &deployFiles : _deployFilesVec) {
        if (!checkAndFillDeployFiles(deployFiles)) {
            return DS_FAILED;
        }
    }
    if (_dataItems.empty()) {
        if (auto ret = doDeploy(_dataItems); ret != DS_UNKNOWN) {
            return ret;
        }
    }
    dataItems = _dataItems;
    return DS_UNKNOWN;
};

DeployStatus NormalDeployItem::doDeploy(vector<DataItemPtr> &dataItems) const {
    for (auto &deployFiles : _deployFilesVec) {
        const auto &remoteDataPath = deployFiles.sourceRootPath;
        const auto &localDataPath = deployFiles.targetRootPath;
        AUTIL_LOG(DEBUG, "deploy files: %s", deployFilesSummary(deployFiles).c_str());
        AUTIL_LOG(INFO,
                  "%p begin deploy, remotePath[%s], deploy_config[%s], "
                  "fileCount[%lu, %lu], deploySize[%lu], isComplete[%d], [%s --> %s]",
                  this,
                  remoteDataPath.c_str(),
                  FastToJsonString(_dataOption, true).c_str(),
                  deployFiles.deployFiles.size(),
                  deployFiles.srcFileMetas.size(),
                  deployFiles.deploySize,
                  deployFiles.isComplete,
                  deployFiles.sourceRootPath.c_str(),
                  deployFiles.targetRootPath.c_str());
        DataItemPtr dataItem = nullptr;
        auto ret = doDeployFilesAsync(remoteDataPath, localDataPath, deployFiles, _dataOption, dataItem);
        if (dataItem == nullptr) {
            std::for_each(dataItems.begin(), dataItems.end(), [](const DataItemPtr &dataItem) { dataItem->cancel(); });
            dataItems.clear();
            return ret;
        }
        assert(ret == DS_UNKNOWN);
        dataItems.push_back(dataItem);
    }
    return DS_UNKNOWN;
};

DeployStatus NormalDeployItem::waitDone(const vector<DataItemPtr> &dataItems) const {
    assert(dataItems.size() == _deployFilesVec.size());
    for (size_t i = 0; i < dataItems.size(); ++i) {
        auto &item = dataItems[i];
        DeployStatus ret = waitDataItemDone(item);
        if (DS_DEPLOYDONE != ret) {
            AUTIL_LOG(WARN,
                      "wait deploy done failed, status: [%s] src: [%s], dest: [%s]",
                      enumToCStr(ret),
                      _deployFilesVec[i].sourceRootPath.c_str(),
                      _deployFilesVec[i].targetRootPath.c_str());
            return ret;
        }
    }

    return DS_DEPLOYDONE;
}

DeployStatus NormalDeployItem::doDeployFilesAsync(const string &remoteDataPath,
                                                  const string &localDataPath,
                                                  DeployFiles deployFiles,
                                                  const DataOption &dataOption,
                                                  DataItemPtr &dataItem) const {
    auto ret = reserveDiskQuota(remoteDataPath, localDataPath, deployFiles);
    DiskQuotaReleaseHelper release(
        _deployResource.diskQuotaController, remoteDataPath, localDataPath, deployFiles.deployFiles);
    if (ret != DS_UNKNOWN) {
        return ret;
    }

    stringstream ss;
    ss << " localPath: " << localDataPath << " remotePath: " << remoteDataPath
       << " files: " << deployFilesSummary(deployFiles);

    DataOption finalDataOption = dataOption;
    if (deployFiles.srcFileMetas.empty()) {
        AUTIL_LOG(INFO, "srcFileMetas is empty for %s", ss.str().c_str());
        dataItem = _deployResource.dataClient->getData(
            remoteDataPath, deployFiles.deployFiles, localDataPath, finalDataOption);
    } else {
        finalDataOption.needExpand = !deployFiles.isComplete;
        dataItem = _deployResource.dataClient->getData(
            remoteDataPath, deployFiles.srcFileMetas, localDataPath, finalDataOption);
    }

    if (!dataItem) {
        AUTIL_LOG(WARN, "create dataItem failed, %s", ss.str().c_str());
        return DS_FAILED;
    }

    return DS_UNKNOWN; // means OK
}

void NormalDeployItem::cancel() {
    unique_lock<mutex> lock(_mu);
    std::for_each(_dataItems.begin(), _dataItems.end(), [](const DataItemPtr &dataItem) { dataItem->cancel(); });
    _dataItems.clear();
}

} // namespace suez
