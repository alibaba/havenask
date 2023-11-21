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
#pragma once

#include <map>
#include <memory>
#include <stdint.h>
#include <string>
#include <utility>
#include <vector>

#include "autil/legacy/legacy_jsonizable.h"
#include "autil/legacy/legacy_jsonizable_dec.h"
#include "build_service/admin/CheckpointSynchronizer.h"
#include "build_service/admin/controlflow/TaskResourceManager.h"
#include "build_service/admin/taskcontroller/TaskController.h"
#include "build_service/common/IndexCheckpointAccessor.h"
#include "build_service/common/ResourceContainer.h"
#include "build_service/common/ResourceKeeperGuard.h"
#include "build_service/common_define.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/config/ConfigDefine.h"
#include "build_service/config/TaskConfig.h"
#include "build_service/config/TaskTarget.h"
#include "build_service/proto/Admin.pb.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/proto/BuildTaskCurrentInfo.h"
#include "build_service/proto/BuildTaskTargetInfo.h"
#include "build_service/proto/DataDescription.h"
#include "build_service/proto/Heartbeat.pb.h"
#include "indexlib/base/Constant.h"
#include "indexlib/base/Types.h"
#include "indexlib/indexlib.h"

namespace build_service { namespace admin {

class SingleBuilderTaskV2 : public TaskController
{
public:
    SingleBuilderTaskV2(const std::string& taskId, const std::string& taskName, const TaskResourceManagerPtr& resMgr);
    SingleBuilderTaskV2(const SingleBuilderTaskV2&);
    ~SingleBuilderTaskV2();

public:
    virtual bool operator==(const TaskController& other) const override
    {
        const SingleBuilderTaskV2* taskController = dynamic_cast<const SingleBuilderTaskV2*>(&other);
        if (!taskController) {
            return false;
        }
        return (*this == (*taskController));
    }
    virtual bool operator!=(const TaskController& other) const override { return !(*this == other); }

public:
    TaskController* clone() override;
    bool init(const std::string& clusterName, const std::string& taskConfigPath, const std::string& initParam) override;
    bool start(const KeyValueMap& kvMap) override;
    bool finish(const KeyValueMap& kvMap) override;
    void notifyStopped() override;
    bool operate(TaskController::Nodes& nodes) override;
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    bool updateConfig() override;
    const std::string& getTaskName() const override { return _taskName; }
    void supplementLableInfo(KeyValueMap& info) const override;
    std::string getUsingConfigPath() const override;
    std::string getTaskInfo() override;
    static bool convertTaskInfoToSingleClusterInfo(const proto::TaskInfo& taskInfo,
                                                   proto::SingleClusterInfo* singleClusterInfo);

private:
    struct BuilderNode {
        Node node;
        proto::BuildTaskCurrentInfo buildCheckpoint;
        proto::Range range;
        proto::BuildTaskTargetInfo taskInfo;
        void updateTargetDescription(const proto::DataDescription& builderDataDesc, const std::string& mergeConfig)
        {
            if (!mergeConfig.empty()) {
                taskInfo.specificMergeConfig = mergeConfig;
            }
            node.taskTarget.updateTargetDescription(config::BS_BUILD_TASK_TARGET, ToJsonString(taskInfo));
            node.taskTarget.updateTargetDescription(config::DATA_DESCRIPTION_KEY, ToJsonString(builderDataDesc));
        }
    };
    struct BuilderGroup {
        BuilderNode master;
        bool masterBuildFinished = false;
        std::vector<BuilderNode> slaves;
    };
    struct RollbackInfo {
        checkpointid_t rollbackCheckpointId = INVALID_CHECKPOINT_ID;
        uint64_t branchId = 0;
        std::map<std::string, versionid_t> rollbackVersionIdMapping;
    };

private:
    SingleBuilderTaskV2& operator=(const SingleBuilderTaskV2&);
    virtual bool operator==(const SingleBuilderTaskV2& other) const;
    virtual bool operator!=(const SingleBuilderTaskV2& other) const;
    bool parseImportParam(const KeyValueMap& param);
    bool processRollback(const KeyValueMap& param);
    bool prepareBuilderDataDescription(config::ResourceReaderPtr resourceReader, const KeyValueMap& kvMap,
                                       proto::DataDescription& ds);
    config::ResourceReaderPtr getConfigReader();
    bool needTriggerAlignVersion();
    void triggerAlignVersion();
    void prepareBuilders(std::vector<BuilderGroup>& builders, bool isStart) const;
    BuilderNode createBuilderNode(std::string roleName, uint32_t nodeId, uint32_t partitionCount, uint32_t parallelNum,
                                  uint32_t instanceIdx, bool isStart) const;
    void updateBuilderNodes(const TaskController::Nodes& nodes);
    void commitBuilderCheckpoint(const proto::BuildTaskCurrentInfo& checkpoint, const BuilderNode& builderNode,
                                 bool isMaster);
    void updateVersionProgress(const std::vector<proto::BuildTaskCurrentInfo>& checkpoint,
                               BuilderNode& builderNode) const;
    std::pair<bool, Node> getNode(uint32_t nodeId, const TaskController::Nodes& nodes);
    bool parseBuildCheckpoint(const std::string& statusDescription, proto::BuildTaskCurrentInfo& checkpoint);
    void updatePublicVersionForSlave(BuilderGroup& builder) const;
    void updateSlaveProgressForMaster(BuilderGroup& builder) const;
    void cleanBuilderCheckpoint(const std::vector<BuilderGroup>& builders, bool keepMasterCheckpoint) const;
    bool needPublishCheckpoint();
    void publishCheckpoint();
    static void convertTaskTargetToBuilderTarget(const proto::TaskTarget& taskTarget,
                                                 proto::BuilderTarget* builderTarget);
    std::string getBuildStepStr() const;
    bool updateKeepCheckpointCount(config::ResourceReaderPtr resourceReader);
    void registBrokerTopic();
    void updateRequestQueue();
    void reportBuildFreshness();
    std::shared_ptr<CheckpointSynchronizer> getCheckpointSynchronizer() const;
    bool checkNoNeedBuildDataFinish();

private:
    std::string _clusterName;
    uint32_t _partitionCount = 0;
    uint32_t _parallelNum = 0;
    std::string _configPath;
    RollbackInfo _rollbackInfo;
    versionid_t _alignVersionId = indexlibv2::INVALID_VERSIONID;
    int64_t _lastAlignVersionTimestamp = 0;
    int64_t _lastAlignSchemaId = config::INVALID_SCHEMAVERSION;
    int64_t _checkpointKeepCount = 1;
    proto::BuildStep _buildStep;
    proto::DataDescription _builderDataDesc;
    common::ResourceKeeperGuardPtr _swiftResourceGuard;
    std::vector<BuilderGroup> _builders;
    common::IndexCheckpointAccessorPtr _indexCkpAccessor;
    common::BuilderCheckpointAccessorPtr _builderCkpAccessor;
    int64_t _lastReportTimestamp = 0;
    bool _needSkipMerge = false;
    int32_t _batchId = -1;
    indexlib::versionid_t _importedVersionId = indexlibv2::INVALID_VERSIONID;
    std::string _specificMergeConfig;
    int64_t _schemaVersion;
    bool _needBuildData = true;

private:
    friend class SingleBuilderTaskV2Test;
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(SingleBuilderTaskV2);

}} // namespace build_service::admin
