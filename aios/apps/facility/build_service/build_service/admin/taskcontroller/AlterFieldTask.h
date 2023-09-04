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
#ifndef ISEARCH_BS_ALTERFIELDTASK_H
#define ISEARCH_BS_ALTERFIELDTASK_H

#include "build_service/admin/taskcontroller/SingleMergerTask.h"
#include "build_service/common_define.h"
#include "build_service/util/Log.h"

namespace build_service { namespace admin {

class AlterFieldTask : public SingleMergerTask
{
public:
    AlterFieldTask(const proto::BuildId& buildId, const std::string& clusterName, const TaskResourceManagerPtr& resMgr)
        : SingleMergerTask(buildId, clusterName, resMgr)
        , _canFinish(false)
    {
    }

    ~AlterFieldTask();

private:
    AlterFieldTask(const AlterFieldTask&);
    AlterFieldTask& operator=(const AlterFieldTask&);

public:
    static std::string getAlterFieldCheckpointId(const std::string& clusterName);
    static bool getOpsIdFromCheckpointName(const std::string& checkpointName, int64_t& opsId);
    bool init(proto::BuildStep buildStep);
    bool start(const KeyValueMap& kvMap) override;
    bool run(proto::WorkerNodes& workerNodes) override;
    bool finish(const KeyValueMap& kvMap) override;
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void doSupplementLableInfo(KeyValueMap& info) const override;
    std::string getTaskPhaseIdentifier() const override;

private:
    struct TargetCheckpoint : public autil::legacy::Jsonizable {
        int64_t versionId;
        std::string checkpointId;
        std::string checkpointName;
        TargetCheckpoint() : versionId(-1) {}
        void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
        {
            json.Jsonize("versionId", versionId, versionId);
            json.Jsonize("checkpointId", checkpointId, checkpointId);
            json.Jsonize("checkpointName", checkpointName, checkpointName);
        }
        bool operator==(const TargetCheckpoint& other) const
        {
            return versionId == other.versionId && checkpointId == other.checkpointId &&
                   checkpointName == other.checkpointName;
        }
        bool operator!=(const TargetCheckpoint& other) const { return !((*this) == other); }
    };

private:
    void createSavepoint(const TargetCheckpoint& checkpoint);
    void removeSavepoint(const TargetCheckpoint& checkpoint);
    void updateLatestVersionToAlterField();
    void updateCheckpoint();
    std::string getRoleName();
    proto::MergerTarget generateTargetStatus() const override;
    std::string getTaskIdentifier() const override;
    void endMerge(::google::protobuf::RepeatedPtrField<proto::IndexInfo>& indexInfos) override;
    bool isOpIdDisable() const;

private:
    TargetCheckpoint _currentCheckpoint;
    TargetCheckpoint _nextCheckpoint;
    TargetCheckpoint _finishCheckpoint;
    std::string _opsId;
    bool _canFinish;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(AlterFieldTask);

}} // namespace build_service::admin

#endif // ISEARCH_BS_ALTERFIELDTASK_H
