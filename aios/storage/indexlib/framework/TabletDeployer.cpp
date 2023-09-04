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
#include "indexlib/framework/TabletDeployer.h"

#include "indexlib/config/OnlineConfig.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/file_system/DeployIndexMeta.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/IndexFileDeployer.h"
#include "indexlib/file_system/IndexFileList.h"
#include "indexlib/file_system/LifecycleConfig.h"
#include "indexlib/file_system/LifecycleTable.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/file_system/load_config/LoadConfig.h"
#include "indexlib/file_system/load_config/LoadConfigList.h"
#include "indexlib/framework/SegmentDescriptions.h"
#include "indexlib/framework/TabletSchemaLoader.h"
#include "indexlib/framework/Version.h"
#include "indexlib/framework/VersionLoader.h"
#include "indexlib/framework/hooks/ITabletDeployerHook.h"
#include "indexlib/framework/hooks/TabletHooksCreator.h"
#include "indexlib/framework/lifecycle/LifecycleStrategyFactory.h"
#include "indexlib/util/PathUtil.h"

namespace indexlibv2::framework {
AUTIL_DECLARE_AND_SETUP_LOGGER(indexlib.framework, TabletDeployer);

static bool FillDeployIndexMeta(const std::string& rootPath, const Version& version,
                                const indexlib::file_system::LoadConfigList& loadConfigList,
                                const std::shared_ptr<indexlib::file_system::LifecycleTable>& lifecycleTable,
                                indexlib::file_system::DeployIndexMetaVec* localDeployIndexMetaVec,
                                indexlib::file_system::DeployIndexMetaVec* remoteDeployIndexMetaVec)
{
    assert(localDeployIndexMetaVec);
    assert(remoteDeployIndexMetaVec);

    versionid_t versionId = version.GetVersionId();
    const std::string& fenceName = version.GetFenceName();
    indexlib::file_system::IndexFileDeployer indexFileDeployer(localDeployIndexMetaVec, remoteDeployIndexMetaVec);
    indexlib::file_system::ErrorCode ec = indexFileDeployer.FillDeployIndexMetaVec(
        versionId, indexlib::util::PathUtil::JoinPath(rootPath, fenceName), loadConfigList, lifecycleTable);
    if (ec == indexlib::file_system::FSEC_OK) {
        AUTIL_LOG(INFO, "fill deploy index meta, root[%s], fenceName[%s], version[%d], local[%lu], remote[%lu]",
                  rootPath.c_str(), fenceName.c_str(), versionId, localDeployIndexMetaVec->size(),
                  remoteDeployIndexMetaVec->size());
        return true;
    } else if (ec == indexlib::file_system::FSEC_NOENT) {
        AUTIL_LOG(WARN, "fill deploy index meta failed, root[%s], fenceName[%s], version[%d], local[%lu], remote[%lu]",
                  rootPath.c_str(), fenceName.c_str(), versionId, localDeployIndexMetaVec->size(),
                  remoteDeployIndexMetaVec->size());
        return false;
    }
    AUTIL_LOG(ERROR, "fill deploy index meta failed, root[%s], fenceName[%s], versionId[%d], ec[%d]", rootPath.c_str(),
              fenceName.c_str(), versionId, ec);
    return false;
}

static bool DifferenceDeployIndexMeta(indexlib::file_system::DeployIndexMetaVec& baseDeployIndexMetaVec,
                                      indexlib::file_system::DeployIndexMetaVec& targetDeployIndexMetaVec,
                                      indexlib::file_system::DeployIndexMetaVec* resultDeployIndexMetaVec)
{
    assert(resultDeployIndexMetaVec);

    // targetRootPath -> [DeployIndexMeta]
    std::map<std::string, std::vector<std::shared_ptr<indexlib::file_system::DeployIndexMeta>>> baseDeployIndexMetaMap;
    for (auto& curDeployIndexMeta : baseDeployIndexMetaVec) {
        auto [iter, success] =
            baseDeployIndexMetaMap.insert({curDeployIndexMeta->targetRootPath, {curDeployIndexMeta}});
        if (!success) {
            iter->second.push_back(curDeployIndexMeta);
        }
    }
    // resultDeployIndexMetaVec = targetDeployIndexMetaVec - baseDeployIndexMetaVec
    for (auto& curDeployIndexMeta : targetDeployIndexMetaVec) {
        auto iter = baseDeployIndexMetaMap.find(curDeployIndexMeta->targetRootPath);
        if (iter == baseDeployIndexMetaMap.end()) {
            resultDeployIndexMetaVec->push_back(curDeployIndexMeta);
            continue;
        }
        std::shared_ptr<indexlib::file_system::DeployIndexMeta> lastDeployIndexMeta = curDeployIndexMeta;
        for (auto& baseDeployIndexMeta : iter->second) {
            auto result = std::make_shared<indexlib::file_system::DeployIndexMeta>();
            result->targetRootPath = lastDeployIndexMeta->targetRootPath;
            result->sourceRootPath = lastDeployIndexMeta->sourceRootPath;
            result->FromDifference(*lastDeployIndexMeta, *baseDeployIndexMeta);
            lastDeployIndexMeta = result;
        }
        resultDeployIndexMetaVec->push_back(lastDeployIndexMeta);
    }
    return true;
}

static indexlib::file_system::LoadConfigList RewriteLoadConfigList(const std::string& rootPath,
                                                                   const config::TabletOptions& tabletOptions,
                                                                   versionid_t versionId, const std::string& localPath,
                                                                   const std::string& remotePath)
{
    indexlib::file_system::LoadConfigList loadConfigList = tabletOptions.GetLoadConfigList();
    loadConfigList.SetDefaultRootPath(localPath, remotePath);

    std::string tableType = indexlib::LEGACY_TABLE_TYPE;
    if (!tabletOptions.IsLegacy()) {
        auto rootDir = indexlib::file_system::IDirectory::GetPhysicalDirectory(rootPath);
        auto schema = framework::TabletSchemaLoader::GetSchema(rootDir, DEFAULT_SCHEMAID);
        if (!schema) {
            AUTIL_LOG(ERROR, "load schema failed from path [%s]", rootPath.c_str());
            return loadConfigList;
        }
        tableType = schema->GetTableType();
    }
    auto hook = indexlib::framework::TabletHooksCreator::GetInstance()->CreateTabletDeployerHook(tableType);
    if (hook) {
        AUTIL_LOG(INFO, "rewrite load config list for table [%s] by hook", tableType.c_str());
        hook->RewriteLoadConfigList(rootPath, tabletOptions, versionId, localPath, remotePath, &loadConfigList);
    }
    return loadConfigList;
}

static std::unique_ptr<Version> LoadVersion(const std::string& rawIndexPath, versionid_t versionId) noexcept
{
    auto directory = indexlib::file_system::IDirectory::GetPhysicalDirectory(rawIndexPath);
    auto [st, version] = VersionLoader::GetVersion(directory, versionId);
    if (unlikely(!st.IsOK())) {
        AUTIL_LOG(ERROR, "load version[%d] failed, rawPath[%s]", versionId, rawIndexPath.c_str());
        return nullptr;
    };
    return std::move(version);
}

static bool IsDeployMetaExist(const std::string& rootPath, const Version& version)
{
    versionid_t versionId = version.GetVersionId();
    const std::string& fenceName = version.GetFenceName();
    indexlib::file_system::FSResult<bool> ret = indexlib::file_system::IndexFileDeployer::IsDeployMetaExist(
        indexlib::util::PathUtil::JoinPath(rootPath, fenceName), versionId);
    if (ret.OK()) {
        return ret.Value();
    }
    // treat io error as not exist
    AUTIL_LOG(ERROR, "check version exists failed, root[%s], fenceName[%s], versionId[%d], ec[%d]", rootPath.c_str(),
              fenceName.c_str(), versionId, ret.ec);
    return false;
}

static void FillLifecycleTable(const std::unique_ptr<indexlib::framework::LifecycleStrategy>& lifecycleStrategy,
                               const std::unique_ptr<Version>& version,
                               const std::shared_ptr<indexlib::file_system::LifecycleTable>& lifecycleTable)
{
    assert(lifecycleTable != nullptr);
    assert(version != nullptr);
    auto segLifecycles = lifecycleStrategy->GetSegmentLifecycles(version->GetSegmentDescriptions());
    std::set<std::string> segmentsWithLifecycle;
    for (const auto& kv : segLifecycles) {
        std::string segmentDir = version->GetSegmentDirName(kv.first);
        lifecycleTable->AddDirectory(segmentDir, kv.second);
        segmentsWithLifecycle.insert(segmentDir);
    }
    for (auto it = version->begin(); it != version->end(); it++) {
        std::string segmentDir = version->GetSegmentDirName(it->segmentId);
        if (segmentsWithLifecycle.find(segmentDir) == segmentsWithLifecycle.end()) {
            lifecycleTable->AddDirectory(segmentDir, "");
        }
    }
}

static bool FillDeployIndexMetaById(versionid_t versionId, const std::string& versionPath, const std::string& localPath,
                                    const std::string& remotePath, const config::TabletOptions& tabletOptions,
                                    const std::unique_ptr<indexlib::framework::LifecycleStrategy>& lifecycleStrategy,
                                    const std::shared_ptr<indexlib::file_system::LifecycleTable>& outputLifecycleTable,
                                    indexlib::file_system::DeployIndexMetaVec* localDeployIndexMetaVec,
                                    indexlib::file_system::DeployIndexMetaVec* remoteDeployIndexMetaVec)
{
    auto version = LoadVersion(versionPath, versionId);
    if (!version) {
        return false;
    }
    if ((versionPath == localPath) && !IsDeployMetaExist(versionPath, *version)) {
        // deploy meta may not exists in locaPath, avoid to print useless error log
        return false;
    }

    auto lifecycleTable = outputLifecycleTable;
    if (lifecycleTable == nullptr) {
        lifecycleTable = std::make_shared<indexlib::file_system::LifecycleTable>();
    }
    FillLifecycleTable(lifecycleStrategy, version, lifecycleTable);
    indexlib::file_system::LoadConfigList loadConfigList =
        RewriteLoadConfigList(versionPath, tabletOptions, versionId, localPath, remotePath);
    return FillDeployIndexMeta(versionPath, *version, loadConfigList, lifecycleTable, localDeployIndexMetaVec,
                               remoteDeployIndexMetaVec);
}

static bool FillDeployIndexMetaByBaseVersion(versionid_t baseVersionId, const std::string& rawPath,
                                             const std::string& localPath, const std::string& remotePath,
                                             const config::TabletOptions& baseTabletOptions,
                                             const indexlibv2::framework::VersionDeployDescription& baseVersionDpDesc,
                                             indexlib::file_system::DeployIndexMetaVec* baseLocalDeployIndexMetaVec,
                                             indexlib::file_system::DeployIndexMetaVec* baseRemoteDeployIndexMetaVec)
{
    // new done file
    if (baseVersionDpDesc.SupportFeature(VersionDeployDescription::FeatureType::DEPLOY_META_MANIFEST)) {
        *baseLocalDeployIndexMetaVec = baseVersionDpDesc.localDeployIndexMetas;
        *baseRemoteDeployIndexMetaVec = baseVersionDpDesc.remoteDeployIndexMetas;
        return true;
    }

    indexlib::file_system::LifecycleConfig lifecycleConfig = baseTabletOptions.GetOnlineConfig().GetLifecycleConfig();
    if (lifecycleConfig.GetStrategy() == indexlib::file_system::LifecycleConfig::DYNAMIC_STRATEGY) {
        AUTIL_LOG(ERROR, "legacy done file don't support dynamic lifecycle config");
        return false;
    }
    auto lifecycleStrategy = indexlib::framework::LifecycleStrategyFactory::CreateStrategy(lifecycleConfig, {});
    if (lifecycleStrategy == nullptr) {
        AUTIL_LOG(ERROR, "cannot create lifecycleStrategy, versionId[%d], raw[%s], local[%s], remote[%s]",
                  baseVersionId, rawPath.c_str(), localPath.c_str(), remotePath.c_str());
        return false;
    }
    // to compatible with old-format version.done file
    if (!FillDeployIndexMetaById(baseVersionId, localPath, localPath, remotePath, baseTabletOptions, lifecycleStrategy,
                                 nullptr, baseLocalDeployIndexMetaVec, baseRemoteDeployIndexMetaVec) &&
        !FillDeployIndexMetaById(baseVersionId, rawPath, localPath, remotePath, baseTabletOptions, lifecycleStrategy,
                                 nullptr, baseLocalDeployIndexMetaVec, baseRemoteDeployIndexMetaVec)) {
        AUTIL_LOG(WARN, "get base deploy index meta [%d] failed, raw[%s], local[%s] remote[%s], deploy all",
                  baseVersionId, rawPath.c_str(), localPath.c_str(), remotePath.c_str());
        return false;
    }
    return true;
}

bool TabletDeployer::GetDeployIndexMeta(const GetDeployIndexMetaInputParams& inputParams,
                                        GetDeployIndexMetaOutputParams* outputParams) noexcept
{
    const std::string& rawPath = indexlib::util::PathUtil::NormalizePath(inputParams.rawPath);
    const std::string& localPath = indexlib::util::PathUtil::NormalizePath(
        inputParams.localPath.empty() ? inputParams.rawPath : inputParams.localPath);
    const std::string& remotePath = indexlib::util::PathUtil::NormalizePath(inputParams.remotePath);
    const config::TabletOptions& targetTabletOptions =
        inputParams.targetTabletOptions ? *inputParams.targetTabletOptions : config::TabletOptions();
    const config::TabletOptions& baseTabletOptions =
        inputParams.baseTabletOptions ? *inputParams.baseTabletOptions : config::TabletOptions();

    versionid_t baseVersionId = inputParams.baseVersionId;
    versionid_t targetVersionId = inputParams.targetVersionId;

    if (outputParams == nullptr) {
        AUTIL_LOG(ERROR,
                  "get target deploy index meta [%d] failed due to NULL outputParam, raw[%s], local[%s], remote[%s]",
                  targetVersionId, rawPath.c_str(), localPath.c_str(), remotePath.c_str());
        return false;
    }

    auto& localDeployIndexMetaVec = outputParams->localDeployIndexMetaVec;
    auto& remoteDeployIndexMetaVec = outputParams->remoteDeployIndexMetaVec;
    auto& versionDeployDesp = outputParams->versionDeployDescription;
    auto lifecycleTable = versionDeployDesp.GetLifecycleTable();
    localDeployIndexMetaVec.clear();
    remoteDeployIndexMetaVec.clear();

    versionDeployDesp.deployTime = indexlib::file_system::LifecycleConfig::CurrentTimeInSeconds();
    indexlib::file_system::LifecycleConfig lifecycleConfig = targetTabletOptions.GetOnlineConfig().GetLifecycleConfig();
    auto lifecycleStrategy = indexlib::framework::LifecycleStrategyFactory::CreateStrategy(
        lifecycleConfig,
        {{indexlib::file_system::LifecyclePatternBase::CURRENT_TIME, std::to_string(versionDeployDesp.deployTime)}});
    if (lifecycleStrategy == nullptr) {
        AUTIL_LOG(
            ERROR,
            "get target deploy index meta [%d] failed due to NULL lifecycleStrategy, raw[%s], local[%s], remote[%s]",
            targetVersionId, rawPath.c_str(), localPath.c_str(), remotePath.c_str());
        return false;
    }
    // get target DeployIndexMetaVec
    indexlib::file_system::DeployIndexMetaVec targetLocalDeployIndexMetaVec;
    indexlib::file_system::DeployIndexMetaVec targetRemoteDeployIndexMetaVec;
    if (!FillDeployIndexMetaById(targetVersionId, rawPath, localPath, remotePath, targetTabletOptions,
                                 lifecycleStrategy, lifecycleTable, &targetLocalDeployIndexMetaVec,
                                 &targetRemoteDeployIndexMetaVec)) {
        AUTIL_LOG(ERROR, "get target deploy index meta [%d] failed, raw[%s], local[%s], remote[%s]", targetVersionId,
                  rawPath.c_str(), localPath.c_str(), remotePath.c_str());
        return false;
    }
    versionDeployDesp.localDeployIndexMetas = targetLocalDeployIndexMetaVec;
    versionDeployDesp.remoteDeployIndexMetas = targetRemoteDeployIndexMetaVec;

    if (baseVersionId < 0) {
        localDeployIndexMetaVec = targetLocalDeployIndexMetaVec;
        remoteDeployIndexMetaVec = targetRemoteDeployIndexMetaVec;
        return true;
    }

    indexlib::file_system::DeployIndexMetaVec baseLocalDeployIndexMetaVec;
    indexlib::file_system::DeployIndexMetaVec baseRemoteDeployIndexMetaVec;
    if (!FillDeployIndexMetaByBaseVersion(baseVersionId, rawPath, localPath, remotePath, baseTabletOptions,
                                          inputParams.baseVersionDeployDescription, &baseLocalDeployIndexMetaVec,
                                          &baseRemoteDeployIndexMetaVec)) {
        localDeployIndexMetaVec = targetLocalDeployIndexMetaVec;
        remoteDeployIndexMetaVec = targetRemoteDeployIndexMetaVec;
        return true;
    }
    // return (target - base)
    if (!DifferenceDeployIndexMeta(baseLocalDeployIndexMetaVec, targetLocalDeployIndexMetaVec,
                                   &localDeployIndexMetaVec)) {
        AUTIL_LOG(ERROR, "diff local deploy index meta failed, path[%s -> %s], version[%d -> %d]", rawPath.c_str(),
                  localPath.c_str(), baseVersionId, targetVersionId);
        return false;
    }
    if (!DifferenceDeployIndexMeta(baseRemoteDeployIndexMetaVec, targetRemoteDeployIndexMetaVec,
                                   &remoteDeployIndexMetaVec)) {
        AUTIL_LOG(ERROR, "diff remote deploy index meta failed, path[%s -> %s], version[%d -> %d]", rawPath.c_str(),
                  remotePath.c_str(), baseVersionId, targetVersionId);
        return false;
    }
    return true;
}

} // namespace indexlibv2::framework
