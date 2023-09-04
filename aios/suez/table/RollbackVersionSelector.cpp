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
#include "suez/table/RollbackVersionSelector.h"

#include <autil/Log.h>

#include "indexlib/base/Constant.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/framework/VersionLoader.h"
#include "suez/common/TablePathDefine.h"
#include "suez/table/VersionSynchronizer.h"

using namespace std;
using indexlibv2::INVALID_VERSIONID;
using indexlibv2::versionid_t;

AUTIL_DECLARE_AND_SETUP_LOGGER(suez, RollbackVersionSelector);

namespace suez {

RollbackVersionSelector::RollbackVersionSelector(VersionSynchronizer *versionSynchronizer,
                                                 const std::string &rawIndexRoot,
                                                 const PartitionId &pid)
    : _versionSynchronizer(versionSynchronizer)
    , _partitionRoot(TablePathDefine::constructIndexPath(rawIndexRoot, pid))
    , _pid(pid) {}

RollbackVersionSelector::~RollbackVersionSelector() {}

IncVersion RollbackVersionSelector::select(int64_t timestamp, IncVersion incVersion) const {
    auto [remoteStatus, isRemoteExist] = indexlib::file_system::FslibWrapper::IsExist(_partitionRoot).StatusWith();
    if (!remoteStatus.IsOK()) {
        AUTIL_LOG(ERROR, "check raw index root [%s] exist failed", _partitionRoot.c_str());
        return INVALID_VERSIONID;
    }
    if (!isRemoteExist) {
        AUTIL_LOG(ERROR, "remote partition root [%s] not exist", _partitionRoot.c_str());
        return INVALID_VERSIONID;
    }
    std::vector<TableVersion> versionSynced;
    std::vector<IncVersion> candidataVersions;
    if (timestamp > 0) {
        // select by timestamp
        if (!loadSyncedVersion(_versionSynchronizer, versionSynced)) {
            AUTIL_LOG(ERROR, "load published version failed");
            return INVALID_VERSIONID;
        }
        for (const auto &tableVersion : versionSynced) {
            auto versionId = tableVersion.getVersionId();
            auto commitTime = tableVersion.getVersionMeta().GetCommitTime();
            if (commitTime <= timestamp) {
                candidataVersions.push_back(versionId);
            }
        }
    } else {
        candidataVersions.push_back(incVersion);
    }
    std::sort(candidataVersions.begin(), candidataVersions.end());
    std::reverse(candidataVersions.begin(), candidataVersions.end());
    auto dir = indexlib::file_system::Directory::GetPhysicalDirectory(_partitionRoot)->GetIDirectory();
    for (const auto &versionId : candidataVersions) {
        auto [status, exist] = indexlibv2::framework::VersionLoader::HasVersion(dir, versionId);
        if (!status.IsOK()) {
            AUTIL_LOG(ERROR, "check version exist from [%s] failed, retry later", _partitionRoot.c_str());
            return INVALID_VERSIONID;
        }
        if (!exist) {
            AUTIL_LOG(INFO, "version [%d] not exist in index root, ignore", versionId);
            continue;
        }
        return versionId;
    }
    AUTIL_LOG(ERROR, "select rollback version from [%s] failed", _partitionRoot.c_str());
    return INVALID_VERSIONID;
}

bool RollbackVersionSelector::loadSyncedVersion(VersionSynchronizer *versionSynchronizer,
                                                std::vector<TableVersion> &versionSynced) const {

    return versionSynchronizer->getVersionList(_pid, versionSynced);
}

} // namespace suez
