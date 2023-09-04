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
#ifndef ISEARCH_BS_SYNCINDEXTASKCONTROLLER_H
#define ISEARCH_BS_SYNCINDEXTASKCONTROLLER_H

#include "build_service/admin/taskcontroller/DefaultTaskController.h"
#include "build_service/common/CheckpointResourceKeeper.h"
#include "build_service/common/IndexCheckpointAccessor.h"
#include "build_service/common_define.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/util/Log.h"

namespace build_service { namespace admin {

class SyncIndexTaskController : public DefaultTaskController
{
public:
    SyncIndexTaskController(const std::string& taskId, const std::string& taskName,
                            const TaskResourceManagerPtr& resMgr);
    ~SyncIndexTaskController();
    struct SyncIndexTaskConfig : public autil::legacy::Jsonizable {
        void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
        {
            json.Jsonize(config::MADROX_ADMIN_ADDRESS, madroxAddr, madroxAddr);         // "madrox_admin_address"
            json.Jsonize(config::BS_SYNC_INDEX_ROOT, remoteIndexPath, remoteIndexPath); // "sync_index_root"
            json.Jsonize("keep_version_count", keepVersionCount, size_t(1));            // "keep_version_count"
            json.Jsonize("prefer_sync_index", preferSyncIndex, true);                   // "prefer_sync_index"
        }
        std::string madroxAddr;
        std::string remoteIndexPath;
        size_t keepVersionCount;
        bool preferSyncIndex;
    };

public:
    bool doInit(const std::string& clusterName, const std::string& taskConfigPath,
                const std::string& initParam) override;
    bool operate(Nodes& nodes) override;
    bool updateConfig() override;
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

private:
    SyncIndexTaskController(const SyncIndexTaskController&);
    SyncIndexTaskController& operator=(const SyncIndexTaskController&);
    bool createNewTarget(config::TaskTarget* target,
                         const common::CheckpointResourceKeeper::CheckpointResourceInfoVec& currentResourceInfo);
    void tryUpdateTarget(const common::CheckpointResourceKeeper::CheckpointResourceInfoVec& currentResourceInfo,
                         TaskController::Node& node);
    bool updateIndexSavepoint(const std::vector<versionid_t>& reservedVersions);
    bool getTaskConfigValue(SyncIndexTaskConfig& config);

private:
    static constexpr const char* SYNC_INDEX = "syncIndex";
    static constexpr size_t TARGET_KEEP_COUNT = 30;
    std::string _cluster;
    std::string _madroxAddr;
    std::string _remoteIndexRoot;
    std::string _originalIndexRoot;
    std::string _syncResourceName;
    uint32_t _totalTargetIdx;
    common::IndexCheckpointAccessorPtr _indexCkpAccessor;
    common::CheckpointResourceKeeperPtr _keeper;
    size_t _keepVersionCount;
    versionid_t _targetVersion;
    bool _needNewTarget;
    bool _preferSyncIndex;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(SyncIndexTaskController);

}} // namespace build_service::admin

#endif // ISEARCH_BS_SYNCINDEXTASKCONTROLLER_H
