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
#include "indexlib/framework/DeployIndexUtil.h"

#include "autil/NetUtil.h"
#include "indexlib/base/Constant.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/IndexFileDeployer.h"
#include "indexlib/file_system/IndexFileList.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/file_system/load_config/LoadConfigList.h"
#include "indexlib/util/PathUtil.h"

using indexlib::util::PathUtil;

namespace indexlibv2::framework {
AUTIL_DECLARE_AND_SETUP_LOGGER(indexlib, DeployIndexUtil);

Status DeployIndexUtil::GetDeployIndexMeta(const std::string& targetRawPath, versionid_t targetVersionId,
                                           indexlib::file_system::DeployIndexMetaVec* localDeployIndexMetaVec,
                                           indexlib::file_system::DeployIndexMetaVec* remoteDeployIndexMetaVec)
{
    std::string rawPath = PathUtil::NormalizePath(targetRawPath);
    localDeployIndexMetaVec->clear();
    remoteDeployIndexMetaVec->clear();

    indexlib::file_system::DeployIndexMetaVec targetLocalDeployIndexMetaVec;
    indexlib::file_system::DeployIndexMetaVec targetRemoteDeployIndexMetaVec;

    indexlib::file_system::IndexFileDeployer indexFileDeployer(&targetLocalDeployIndexMetaVec,
                                                               &targetRemoteDeployIndexMetaVec);
    indexlib::file_system::LoadConfigList loadConfigList;
    auto status = indexlib::file_system::toStatus(
        indexFileDeployer.FillDeployIndexMetaVec(targetVersionId, rawPath, loadConfigList, /*lifecycleTable*/ nullptr));
    RETURN_IF_STATUS_ERROR(status, "fill deploy index meta failed, root[%s], version[%d], local[%lu], remote[%lu]",
                           rawPath.c_str(), targetVersionId, targetLocalDeployIndexMetaVec.size(),
                           targetRemoteDeployIndexMetaVec.size());

    AUTIL_LOG(INFO, "fill deploy index meta, root[%s], version[%d], local[%lu], remote[%lu]", rawPath.c_str(),
              targetVersionId, targetLocalDeployIndexMetaVec.size(), targetRemoteDeployIndexMetaVec.size());

    *localDeployIndexMetaVec = std::move(targetLocalDeployIndexMetaVec);
    *remoteDeployIndexMetaVec = std::move(targetRemoteDeployIndexMetaVec);
    return Status::OK();
}

std::pair<Status, int64_t> DeployIndexUtil::GetIndexSize(const std::string& rawPath, const versionid_t targetVersion)
{
    indexlib::file_system::DeployIndexMetaVec localDeployIndexMetaVec;
    indexlib::file_system::DeployIndexMetaVec remoteDeployIndexMetaVec;
    auto status = GetDeployIndexMeta(rawPath, targetVersion, &localDeployIndexMetaVec, &remoteDeployIndexMetaVec);
    RETURN2_IF_STATUS_ERROR(status, -1, "get deploy index meta failed, rawPath[%s], version[%d]", rawPath.c_str(),
                            targetVersion);
    auto sumFunc = [](const indexlib::file_system::IndexFileList::FileInfoVec& fileInfos) {
        int64_t totalLength = 0;
        for (const auto& fileInfo : fileInfos) {
            totalLength += (fileInfo.fileLength != -1) ? fileInfo.fileLength : 0;
        }
        return totalLength;
    };

    int64_t totalLength = 0;
    for (const auto& deployIndexMeta : localDeployIndexMetaVec) {
        totalLength += sumFunc(deployIndexMeta->deployFileMetas);
        totalLength += sumFunc(deployIndexMeta->finalDeployFileMetas);
    }
    for (const auto& deployIndexMeta : remoteDeployIndexMetaVec) {
        totalLength += sumFunc(deployIndexMeta->deployFileMetas);
        totalLength += sumFunc(deployIndexMeta->finalDeployFileMetas);
    }
    return {Status::OK(), totalLength};
}

bool DeployIndexUtil::NeedSubscribeRemoteIndex(const std::string& remoteIndexRoot)
{
    return !remoteIndexRoot.empty() && PathUtil::SupportPanguInlineFile(remoteIndexRoot);
}

void DeployIndexUtil::SubscribeRemoteIndex(const std::string& remoteIndexRoot, versionid_t remoteVersionId)
{
    assert(!remoteIndexRoot.empty());
    std::string lockFileName = std::string(DEPLOY_META_FILE_NAME_PREFIX) + "." + std::to_string(remoteVersionId) +
                               indexlib::file_system::DEPLOY_META_LOCK_SUFFIX;
    std::string lockPath = PathUtil::JoinPath(remoteIndexRoot, lockFileName);
    std::string ipAddr = autil::NetUtil::getBindIp();
    if (!indexlib::file_system::FslibWrapper::UpdatePanguInlineFile(lockPath, ipAddr).OK()) {
        AUTIL_LOG(ERROR, "update lock file [%s] failed!, ipAddr [%s]", lockPath.c_str(), ipAddr.c_str());
    } else {
        AUTIL_LOG(INFO, "update lock file [%s] successfully, ipAddr [%s]", lockPath.c_str(), ipAddr.c_str());
    }
}

} // namespace indexlibv2::framework
