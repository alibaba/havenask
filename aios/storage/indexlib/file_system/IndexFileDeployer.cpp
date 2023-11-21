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
#include "indexlib/file_system/IndexFileDeployer.h"

#include <assert.h>
#include <cstddef>
#include <memory>
#include <utility>
#include <vector>

#include "autil/CommonMacros.h"
#include "autil/StringUtil.h"
#include "indexlib/base/Constant.h"
#include "indexlib/base/PathUtil.h"
#include "indexlib/file_system/EntryMeta.h"
#include "indexlib/file_system/EntryTable.h"
#include "indexlib/file_system/EntryTableBuilder.h"
#include "indexlib/file_system/FileInfo.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/LifecycleTable.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/file_system/load_config/LoadConfig.h"
#include "indexlib/file_system/load_config/LoadConfigList.h"
#include "indexlib/util/PathUtil.h"

using namespace std;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, IndexFileDeployer);

IndexFileDeployer::IndexFileDeployer(DeployIndexMetaVec* localDeployIndexMetaVec,
                                     DeployIndexMetaVec* remoteDeployIndexMetaVec)
    : _localDeployIndexMetaVec(localDeployIndexMetaVec)
    , _remoteDeployIndexMetaVec(remoteDeployIndexMetaVec)
{
    assert(localDeployIndexMetaVec);
    assert(remoteDeployIndexMetaVec);
}

IndexFileDeployer::~IndexFileDeployer() {}

static void FillDeployIndexMeta(const EntryMeta& entryMeta, const string& targetRootPath,
                                std::unordered_map<std::string, DeployIndexMeta*>* rootMap,
                                DeployIndexMetaVec* deployIndexMetaVec)
{
    // mv fenceName from PhysicalRoot to PhysicalPath
    auto [physicalRoot, fenceName] = indexlibv2::PathUtil::ExtractFenceName(entryMeta.GetPhysicalRoot());
    string physicalPath = util::PathUtil::JoinPath(fenceName, entryMeta.GetPhysicalPath());

    DeployIndexMeta* deployIndexMeta = nullptr;
    auto it = rootMap->find(physicalRoot);
    if (it == rootMap->end()) {
        deployIndexMeta = new DeployIndexMeta();
        deployIndexMeta->sourceRootPath = physicalRoot;
        deployIndexMeta->targetRootPath = targetRootPath;
        deployIndexMetaVec->emplace_back(deployIndexMeta);
        rootMap->insert({physicalRoot, deployIndexMeta});
    } else {
        deployIndexMeta = it->second;
    }

    assert(*physicalPath.rbegin() != '/');
    if (entryMeta.IsFile()) {
        int64_t length = entryMeta.GetLength();
        if (unlikely(autil::StringUtil::startsWith(entryMeta.GetPhysicalPath(), VERSION_FILE_NAME_DOT_PREFIX))) {
            deployIndexMeta->AppendFinal(FileInfo(physicalPath, length));
        } else {
            deployIndexMeta->Append(FileInfo(physicalPath, length));
        }
    } else if (entryMeta.IsDir()) {
        // OPT, no need deploy dir. how about deleteionmap
        deployIndexMeta->Append(FileInfo(physicalPath + "/", -1));
    } else {
        assert(false);
    }
}

static const LoadConfig& MatchLoadConfig(const std::string& logicalPath, const LoadConfigList& loadConfigList,
                                         const std::string& lifecycle)
{
    for (size_t i = 0; i < loadConfigList.Size(); ++i) {
        const auto& loadConfig = loadConfigList.GetLoadConfig(i);
        if (loadConfig.Match(logicalPath, lifecycle)) {
            return loadConfig;
        }
    }
    return loadConfigList.GetDefaultLoadConfig();
}

// refer to EntryTableBuilder::RewriteEntryMeta
void IndexFileDeployer::FillByOneEntryMeta(const EntryMeta& entryMeta, const LoadConfigList& loadConfigList,
                                           const std::shared_ptr<LifecycleTable>& lifecycleTable)
{
    AUTIL_LOG(DEBUG, "fill logicalPath[%s], physicalRoot[%s], physicalPath[%s], dir[%d]",
              entryMeta.GetLogicalPath().c_str(), entryMeta.GetPhysicalRoot().c_str(),
              entryMeta.GetPhysicalPath().c_str(), entryMeta.IsDir());
    // for compatible
    const std::string& logicalPath = entryMeta.IsDir() ? entryMeta.GetLogicalPath() + "/" : entryMeta.GetLogicalPath();
    const std::string& lifecycle = lifecycleTable ? lifecycleTable->GetLifecycle(logicalPath) : "";
    const LoadConfig& loadConfig = MatchLoadConfig(logicalPath, loadConfigList, lifecycle);
    if (loadConfig.IsRemote()) {
        // physicalRoot came from loadConfig or empty[from indexRoot from suez target, secondRoot]
        FillDeployIndexMeta(entryMeta, loadConfig.GetRemoteRootPath(), &_remoteRootMap, _remoteDeployIndexMetaVec);
    }
    if (loadConfig.NeedDeploy()) {
        // TODO: support brnachName from entryTable, need dp2 support
        //       support branch source root -> LOCAL/PARTITION/BRANCH1/ to avoid duplicate filename
        // physicalRoot came from entryTable without redirect, source offline index root
        FillDeployIndexMeta(entryMeta, loadConfig.GetLocalRootPath(), &_localRootMap, _localDeployIndexMetaVec);
    }
}

static void dedupDeployIndexMetaVec(DeployIndexMetaVec* deployIndexMetaVec)
{
    for (auto& deployIndexMeta : *deployIndexMetaVec) {
        deployIndexMeta->Dedup();
    }
}

FSResult<int32_t> IndexFileDeployer::GetLastVersionId(const string& physicalRoot)
{
    return EntryTableBuilder::GetLastVersion(physicalRoot);
}

FSResult<bool> IndexFileDeployer::IsDeployMetaExist(const string& physicalRoot, versionid_t versionId)
{
    string entryTableFileName = EntryTable::GetEntryTableFileName(versionId);
    string entryTablePath = util::PathUtil::JoinPath(physicalRoot, entryTableFileName);
    FSResult<bool> ret = FslibWrapper::IsExist(entryTablePath);
    if (!ret.OK()) {
        return ret;
    } else if (ret.Value()) {
        return {FSEC_OK, true};
    }
    string deployMetaFileName = DEPLOY_META_FILE_NAME_PREFIX + string(".") + std::to_string(versionId);
    auto deployMetaPath = util::PathUtil::JoinPath(physicalRoot, deployMetaFileName);
    return FslibWrapper::IsExist(deployMetaPath);
    // every index should has deploy meta now
}

ErrorCode IndexFileDeployer::FillDeployIndexMetaVec(versionid_t versionId, const string& rawPhysicalRoot,
                                                    const LoadConfigList& loadConfigList,
                                                    const std::shared_ptr<LifecycleTable>& lifecycleTable)
{
    string physicalRoot = util::PathUtil::NormalizePath(rawPhysicalRoot);
    EntryTableBuilder entryTableBuilder;
    std::shared_ptr<FileSystemOptions> fsOptions(new FileSystemOptions());
    fsOptions->isOffline = false;
    fsOptions->loadConfigList = loadConfigList;
    std::shared_ptr<EntryTable> entryTable = entryTableBuilder.CreateEntryTable("", physicalRoot, fsOptions);

    if (unlikely(versionId == INVALID_VERSIONID)) {
        auto ret = GetLastVersionId(physicalRoot);
        if (!ret.OK()) {
            return ret.Code();
        }
        versionId = ret.Value();
    }

    // load file list from rawPhysicalRoot by versionId
    ErrorCode ec = entryTableBuilder.MountVersion(physicalRoot, versionId, "", FSMT_READ_ONLY);
    if (ec != FSEC_OK) {
        AUTIL_LOG(WARN, "mount version[%d] failed, rootPath[%s]", versionId, physicalRoot.c_str());
        return ec;
    }

    // entry table file self does not in EntryTable
    if (entryTableBuilder.GetLastEntryTableEntryMeta()) {
        FillByOneEntryMeta(*entryTableBuilder.GetLastEntryTableEntryMeta(), loadConfigList, lifecycleTable);
    }

    // fill by entry meta
    std::vector<EntryMeta> entryMetas = entryTable->ListDir("", true);
    for (const auto& entryMeta : entryMetas) {
        if (entryMeta.IsInPackage()) {
            // OPT, dedup
            // dir -> package file meta, file -> package file data
            const string& physicalRoot = entryMeta.GetRawPhysicalRoot();
            const string& physicalPath = entryMeta.GetPhysicalPath();
            auto [ec, length] = entryTable->GetPackageFileLength(util::PathUtil::JoinPath(physicalRoot, physicalPath));
            RETURN_IF_FS_ERROR(ec, "GetPackageFileLength failed");
            // notice here use physicalPath (eg. package_file.__data__) to match load config
            EntryMeta fakePackageEntryMeta(physicalPath, physicalPath, &physicalRoot);
            fakePackageEntryMeta.SetLength(length);
            FillByOneEntryMeta(fakePackageEntryMeta, loadConfigList, lifecycleTable);

            const string physicalPackageMetaPath = util::PathUtil::JoinPath(
                util::PathUtil::GetParentDirPath(physicalPath), string(PACKAGE_FILE_PREFIX) + PACKAGE_FILE_META_SUFFIX);
            EntryMeta fakePackageMetaEntryMeta(physicalPackageMetaPath, physicalPackageMetaPath, &physicalRoot);
            auto [ec2, packageMetaLength] =
                entryTable->GetPackageFileLength(util::PathUtil::JoinPath(physicalRoot, physicalPackageMetaPath));
            RETURN_IF_FS_ERROR(ec2, "");
            fakePackageMetaEntryMeta.SetLength(packageMetaLength);
            FillByOneEntryMeta(fakePackageMetaEntryMeta, loadConfigList, lifecycleTable);

            // TODO: any logical file need dp, dp physical file
            // FillByOneEntryMeta(entryMeta)
        } else {
            FillByOneEntryMeta(entryMeta, loadConfigList, lifecycleTable);
        }
    }

    dedupDeployIndexMetaVec(_localDeployIndexMetaVec);
    dedupDeployIndexMetaVec(_remoteDeployIndexMetaVec);

    return FSEC_OK;
}
}} // namespace indexlib::file_system
