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

#include <algorithm>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>

#include "autil/legacy/legacy_jsonizable_dec.h"
#include "build_service/admin/AdminTaskBase.h"
#include "build_service/admin/CounterCollector.h"
#include "build_service/admin/DefaultSlowNodeDetectStrategy.h"
#include "build_service/admin/FatalErrorDiscoverer.h"
#include "build_service/admin/NewSlowNodeDetectStrategy.h"
#include "build_service/admin/controlflow/TaskResourceManager.h"
#include "build_service/admin/taskcontroller/NodeStatusManager.h"
#include "build_service/admin/taskcontroller/TaskController.h"
#include "build_service/common/ResourceContainer.h"
#include "build_service/common_define.h"
#include "build_service/config/TaskConfig.h"
#include "build_service/proto/Admin.pb.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/proto/ErrorCollector.h"
#include "build_service/proto/TaskIdentifier.h"
#include "build_service/proto/WorkerNode.h"

namespace build_service { namespace admin {
class TaskMaintainer : public AdminTaskBase
{
public:
    TaskMaintainer(const proto::BuildId& buildId, const std::string& taskId, const std::string& taskName,
                   const TaskResourceManagerPtr& resMgr);
    // TaskMaintainer(const TaskMaintainer &);
    ~TaskMaintainer();

    /* public: */
    /*     //only for jsonize */
    /*     TaskMaintainer(); */
    /*     TaskMaintainer& operator=(const TaskMaintainer &); */

private:
    TaskMaintainer& operator=(const TaskMaintainer&);

public:
    bool init(const std::string& clusterName, const std::string& taskConfigPath, const std::string& initParam);
    bool run(proto::WorkerNodes& workerNodes) override;
    bool start(const KeyValueMap& kvMap) override;
    bool finish(const KeyValueMap& kvMap) override { return _taskController->finish(kvMap); }
    void notifyStopped() override;

    bool updateConfig() override;
    std::string getOriginalTaskId() const;
    void cleanZkTaskNodes(const std::string& zkDir);

    std::string getTaskName() const { return _taskName; }
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    bool operator==(const TaskMaintainer&) const;
    bool operator!=(const TaskMaintainer&) const;

    void collectTaskInfo(proto::TaskInfo* taskInfo, const proto::TaskNodes& taskNodes,
                         const CounterCollector::RoleCounterMap& roleCounterMap);
    void waitSuspended(proto::WorkerNodes& workerNodes) override;
    const std::string& getClusterName() const { return _clusterName; }

    std::string getTaskIdentifier();
    std::string getTaskPhaseIdentifier() const override;
    void doSupplementLableInfo(KeyValueMap& info) const override;
    const TaskControllerPtr& getController() const;
    void setBeeperTags(const beeper::EventTagsPtr beeperTags) override;

private:
    void getTaskIdentifier(proto::TaskIdentifier& identifier);
    bool run(proto::TaskNodes& workerNodes);
    bool loadFromConfig(const config::ResourceReaderPtr& resourceReader) override { return false; }
    config::ResourceReaderPtr getResourceReader() const;
    proto::TaskNodePtr createTaskNode(const TaskController::Node& node);
    proto::PartitionId createPartitionId(const TaskController::Node& node);
    void taskNodesToNodes(const proto::TaskNodes& taskNodes, TaskController::Nodes& nodes);
    void nodesToTaskNodes(const TaskController::Nodes& nodes, proto::TaskNodes& taskNodes);

    bool prepareSlowNodeDetector(const std::string& clusterName) override
    {
        return doPrepareSlowNodeDetector<proto::TaskNodes>(clusterName);
    }
    proto::TaskNodePtr GetTaskNode(const proto::TaskNodes& taskNodes, const TaskController::Node& node) const;
    bool existBackInfo(const proto::TaskNodes& workerNodes) const;

private:
    std::string _taskId;
    // std::string _configPath;
    std::string _taskConfigPath;
    std::string _taskName;
    std::string _clusterName;
    TaskControllerPtr _taskController;
    FatalErrorDiscovererPtr _fatalErrorDiscoverer;
    bool _legacyTaskIdentifyStr; // TODO: remove later

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(TaskMaintainer);

}} // namespace build_service::admin
