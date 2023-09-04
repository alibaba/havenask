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
#include "suez/deploy/DeployItem.h"

#include <sstream>

#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/FileSystem.h"
#include "suez/common/DiskQuotaController.h"
#include "suez/common/InnerDef.h"

using namespace std;
using namespace autil;
using namespace fslib;
using namespace fslib::fs;

AUTIL_DECLARE_AND_SETUP_LOGGER(suez, DeployItem);

namespace suez {

DeployItem::DeployItem(const DeployResource &deployResource, const DeployFilesVec &deployFilesVec)
    : _deployResource(deployResource), _deployFilesVec(deployFilesVec) {}

DeployStatus DeployItem::reserveDiskQuota(const string &remoteDataPath,
                                          const string &localDataPath,
                                          DeployFiles &deployFiles) const {
    stringstream ss;
    ss << " localPath: " << localDataPath << " remotePath: " << remoteDataPath
       << " files: " << deployFilesSummary(deployFiles);

    if (localDataPath.empty()) {
        AUTIL_LOG(WARN, "invalid local path, %s", ss.str().c_str());
        return DS_FAILED;
    }

    if (_deployResource.diskQuotaController != nullptr) {
        auto dqs = _deployResource.diskQuotaController->reserveWithRetry(
            remoteDataPath, localDataPath, deployFiles.deployFiles, deployFiles.deploySize);
        if (dqs == DQS_NOTENOUGH) {
            AUTIL_LOG(WARN, "disk quota not enough for %s", ss.str().c_str());
            return DS_DISKQUOTA;
        } else if (dqs != DQS_OK) {
            AUTIL_LOG(WARN, "disk quota reserve failed for %s", ss.str().c_str());
            return DS_FAILED;
        }
    } else {
        AUTIL_LOG(DEBUG, "skip reserve disk quota");
    }

    return DS_UNKNOWN;
}

bool DeployItem::checkAndFillDeployFiles(DeployFiles &deployFiles) {
    if (deployFiles.deployFiles.empty()) {
        if (!getFilesAndSize(deployFiles.sourceRootPath, deployFiles.deployFiles, deployFiles.deploySize)) {
            AUTIL_LOG(WARN, "get files size for %s failed", deployFiles.sourceRootPath.c_str());
            return false;
        }
    } else if (0 == deployFiles.deploySize) {
        if (!getFilesSize(deployFiles.sourceRootPath, deployFiles.deployFiles, deployFiles.deploySize)) {
            AUTIL_LOG(WARN, "get files size for %s failed", deployFiles.sourceRootPath.c_str());
            return false;
        }
    }
    return true;
}

std::string DeployItem::deployFilesSummary(const DeployFiles &files) {
    std::string str;
    str += "size: " + StringUtil::toString(files.deploySize) + ' ';
    for (size_t i = 0; i < min(files.deployFiles.size(), 2ul); i++) {
        str += files.deployFiles[i] + " ";
    }
    str + " ... ";
    return str;
}

bool DeployItem::getFilesAndSize(const string &remotePath, vector<string> &files, int64_t &size) {
    assert(files.empty());
    files.clear();
    size = 0;
    EntryInfoMap entryInfos;
    AUTIL_LOG(INFO, "get files by list [%s]", remotePath.c_str());
    auto ec = FileSystem::listDir(remotePath, entryInfos, 4 /* threadnum */);
    if (fslib::EC_OK != ec) {
        AUTIL_LOG(ERROR, "list path %s failed", remotePath.c_str());
        return false;
    }
    for (const auto &kv : entryInfos) {
        size += kv.second.length;
        files.push_back(kv.second.entryName);
    }
    return true;
}

bool DeployItem::getFilesSize(const string &remotePath, const vector<string> &files, int64_t &size) {
    size = 0;
    EntryInfoMap entryInfos;
    for (const auto &file : files) {
        string remoteFile = FileSystem::joinFilePath(remotePath, file);
        FileMeta remoteFileMeta;
        if (fslib::EC_OK != FileSystem::getFileMeta(remoteFile, remoteFileMeta)) {
            AUTIL_LOG(ERROR, "get file meta %s failed", remoteFile.c_str());
            return false;
        }
        size += remoteFileMeta.fileLength;
    }
    return true;
}

} // namespace suez
