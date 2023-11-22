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
#include "indexlib/framework/VersionDeployDescription.h"

#include <algorithm>
#include <assert.h>
#include <cstdint>
#include <ext/alloc_traits.h>
#include <stddef.h>

#include "autil/StringUtil.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "indexlib/file_system/DeployIndexMeta.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/FSResult.h"
#include "indexlib/file_system/FileInfo.h"
#include "indexlib/file_system/IndexFileList.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"

namespace indexlibv2::framework {

AUTIL_LOG_SETUP(indexlib.framework, VersionDeployDescription);

uint32_t VersionDeployDescription::CURRENT_EDITION_ID = 1;
std::string VersionDeployDescription::DONE_FILE_NAME_PREFIX = "version.";
std::string VersionDeployDescription::DONE_FILE_NAME_SUFFIX = ".done";

bool VersionDeployDescription::IsJson(const std::string& fileContent)
{
    // To compatible with old format version.done file
    // old format is like:
    // V1: $remotePath;
    // V2: $rawPath:$remotePath
    // V3: $rawPath:$remotePath:$configPath
    return fileContent.find('{') == 0;
}

bool VersionDeployDescription::SupportFeature(FeatureType id) const
{
    switch (id) {
    case FeatureType::DEPLOY_META_MANIFEST:
        return editionId >= 1;
    case FeatureType::DEPLOY_TIME:
        return editionId >= 1;
    default:
        return false;
    }
    return false;
}

bool VersionDeployDescription::DisableFeature(FeatureType id)
{
    switch (id) {
    case FeatureType::DEPLOY_META_MANIFEST: {
        editionId = 0;
        return true;
    }
    case FeatureType::DEPLOY_TIME: {
        editionId = 0;
        return true;
    }
    default: {
        return false;
    }
    }
    return false;
}

bool VersionDeployDescription::CheckDeployDoneByLegacyContent(const std::string& legacyContent) const
{
    std::string contentV1 = remotePath;
    std::string contentV2 = rawPath + ":" + remotePath;
    std::string contentV3 = rawPath + ":" + remotePath + ":" + configPath;
    if (legacyContent == contentV1 || legacyContent == contentV2 || legacyContent == contentV3) {
        return true;
    }
    return false;
}

bool VersionDeployDescription::Deserialize(const std::string& fileContent)
{
    if (!IsJson(fileContent)) {
        AUTIL_LOG(WARN, "Unable to deserialize legacy content[%s]", fileContent.c_str());
        editionId = 0;
        return true;
    }
    try {
        FromJsonString(*this, fileContent);
    } catch (const autil::legacy::ExceptionBase& e) {
        localDeployIndexMetas.clear();
        remoteDeployIndexMetas.clear();
        return false;
    }
    return true;
}

bool VersionDeployDescription::CheckDeployDoneByEdition(uint32_t targetEdition,
                                                        const VersionDeployDescription& target) const
{
    switch (targetEdition) {
    case 0: {
        AUTIL_LOG(ERROR, "Unexpected call edition[0], use CheckDeployDoneByLegacyContent instead");
        return false;
    }
    case 1: {
        return CheckDeployMetaEquality(target) && CheckLifecycleTableEquality(target);
    }
    default: {
        AUTIL_LOG(WARN, "Unable to check deployDone for edition[%u]", targetEdition);
        return false;
    }
    }
    return false;
}

bool VersionDeployDescription::CheckDeployDone(const std::string& fileContent) const
{
    if (!IsJson(fileContent)) {
        return CheckDeployDoneByLegacyContent(fileContent);
    }

    VersionDeployDescription lastVersionDpDesc;
    try {
        FromJsonString(lastVersionDpDesc, fileContent);
    } catch (const autil::legacy::ExceptionBase& e) {
        AUTIL_LOG(ERROR, "unable to check done file content [%s], bad json", fileContent.c_str());
        return false;
    }
    return CheckDeployDoneByEdition(lastVersionDpDesc.editionId, lastVersionDpDesc);
}

void VersionDeployDescription::Jsonize(JsonWrapper& json)
{
    json.Jsonize("edition_id", editionId, editionId);
    json.Jsonize("deploy_time", deployTime, deployTime);
    json.Jsonize("raw_path", rawPath, rawPath);
    json.Jsonize("remote_path", remotePath, remotePath);
    json.Jsonize("config_path", configPath, configPath);
    json.Jsonize("local_deploy_index_metas", localDeployIndexMetas, localDeployIndexMetas);
    json.Jsonize("remote_deploy_index_metas", remoteDeployIndexMetas, remoteDeployIndexMetas);
    json.Jsonize("lifecycle_table", lifecycleTable, lifecycleTable);
}

bool VersionDeployDescription::CheckDeployMetaEquality(const VersionDeployDescription& other) const
{
    if ((rawPath != other.rawPath) || (remotePath != other.remotePath)) {
        return false;
    }
    auto checkDeployIndexMetas = [](const indexlib::file_system::DeployIndexMetaVec& lhs,
                                    const indexlib::file_system::DeployIndexMetaVec& rhs) {
        if (lhs.size() != rhs.size()) {
            return false;
        }
        for (size_t i = 0; i < lhs.size(); i++) {
            if (lhs[i] == nullptr && rhs[i] == nullptr) {
                continue;
            }
            if (lhs[i] != nullptr && rhs[i] != nullptr && (*lhs[i] == *rhs[i])) {
                continue;
            }
            return false;
        }
        return true;
    };
    return checkDeployIndexMetas(localDeployIndexMetas, other.localDeployIndexMetas) &&
           checkDeployIndexMetas(remoteDeployIndexMetas, other.remoteDeployIndexMetas);
}

bool VersionDeployDescription::CheckLifecycleTableEquality(const VersionDeployDescription& other) const
{
    if (lifecycleTable != nullptr && other.lifecycleTable != nullptr) {
        return *lifecycleTable == *(other.lifecycleTable);
    }
    return (lifecycleTable == nullptr && other.lifecycleTable == nullptr);
}

void VersionDeployDescription::GetLocalDeployFileList(std::set<std::string>* localDeployFiles)
{
    assert(localDeployFiles != nullptr);
    for (const auto& deployIndexMetaPtr : localDeployIndexMetas) {
        for (const auto& fileInfo : deployIndexMetaPtr->deployFileMetas) {
            localDeployFiles->insert(fileInfo.filePath);
        }
    }
}

std::string VersionDeployDescription::GetDoneFileNameByVersionId(versionid_t versionId)
{
    std::string versionDoneFileName(DONE_FILE_NAME_PREFIX);
    versionDoneFileName += autil::StringUtil::toString(versionId) + DONE_FILE_NAME_SUFFIX;
    return versionDoneFileName;
}

bool VersionDeployDescription::LoadDeployDescription(const std::string& rootPath, versionid_t versionId,
                                                     VersionDeployDescription* versionDpDesc)
{
    auto doneFileName = indexlib::file_system::FslibWrapper::JoinPath(rootPath, GetDoneFileNameByVersionId(versionId));
    auto [ec, exist] = indexlib::file_system::FslibWrapper::IsExist(doneFileName);

    if (ec != indexlib::file_system::FSEC_OK) {
        AUTIL_LOG(WARN, "fail to judge existence of doneFile:[%s], ec = [%d]", doneFileName.c_str(), int(ec));
        return false;
    }
    if (!exist) {
        AUTIL_LOG(WARN, "doneFile:[%s] not exist", doneFileName.c_str());
        return false;
    }
    std::string fileContent;
    ec = indexlib::file_system::FslibWrapper::AtomicLoad(doneFileName, fileContent);
    if (ec != indexlib::file_system::FSEC_OK) {
        AUTIL_LOG(WARN, "read from doneFile:[%s] failed, ec = [%d]", doneFileName.c_str(), int(ec));
        return false;
    }

    if (!versionDpDesc->Deserialize(fileContent)) {
        AUTIL_LOG(ERROR, "deserialize doneFile[%s] failed, content=[%s]", doneFileName.c_str(), fileContent.c_str());
        return false;
    }
    return true;
}

bool VersionDeployDescription::ExtractVersionId(const std::string& fileName, versionid_t& versionId)
{
    std::string prefix = DONE_FILE_NAME_PREFIX;
    std::string suffix = DONE_FILE_NAME_SUFFIX;
    if (fileName.size() <= (prefix.size() + suffix.size())) {
        return false;
    }
    return autil::StringUtil::startsWith(fileName, prefix) && autil::StringUtil::endsWith(fileName, suffix) &&
           autil::StringUtil::fromString<versionid_t>(
               fileName.substr(prefix.size(), fileName.size() - prefix.size() - suffix.size()), versionId);
}

} // namespace indexlibv2::framework
