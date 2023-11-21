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

#include "autil/Lock.h"
#include "autil/legacy/jsonizable.h"
#include "build_service/admin/CounterCollector.h"
#include "build_service/admin/WorkerTable.h"
#include "build_service/admin/controlflow/TaskFlow.h"
#include "build_service/admin/taskcontroller/TaskMaintainer.h"
#include "build_service/common_define.h"
#include "build_service/proto/Admin.pb.h"

BS_DECLARE_REFERENCE_CLASS(admin, FlowFactory);
#include <map>
#include <string>
#include <vector>

#include "autil/Lock.h"
#include "autil/legacy/jsonizable.h"
#include "build_service/admin/CounterCollector.h"
#include "build_service/admin/WorkerTable.h"
#include "build_service/admin/controlflow/TaskFlow.h"
#include "build_service/admin/controlflow/TaskResourceManager.h"
#include "build_service/admin/taskcontroller/TaskMaintainer.h"
#include "build_service/common/ResourceContainer.h"
#include "build_service/common_define.h"
#include "build_service/proto/Admin.pb.h"

BS_DECLARE_REFERENCE_CLASS(admin, FlowFactory);

namespace build_service { namespace admin {

class TaskFlowManager
{
public:
    // rootPath = path1;path2;...
    TaskFlowManager(const std::string& rootPath, const TaskFactoryPtr& factory, const TaskResourceManagerPtr& resMgr);
    ~TaskFlowManager();

public:
    bool init(const std::string& graphFileName, const KeyValueMap& kvMap);

    bool loadSubGraph(const std::string& graphName, const std::string& graphFileName, const KeyValueMap& kvMap);

    void declareTaskParameter(const std::string& flowId, const std::string& taskId, const KeyValueMap& kvMap);

    void stepRun();

    TaskFlowPtr loadFlow(const std::string& fileName, const KeyValueMap& flowParam = KeyValueMap(),
                         bool runFlowAtOnce = true);

    TaskFlowPtr loadSimpleFlow(const std::string& kernalType, const std::string& taskId,
                               const KeyValueMap& kvMap = KeyValueMap(), bool runFlowAtOnce = true);

    TaskFlowPtr getFlow(const std::string& flowId);
    void getFlowIdByTag(const std::string& tag, std::vector<std::string>& flowIds);
    void getFlowByTag(const std::string& tag, std::vector<TaskFlowPtr>& flows);

    std::string serialize() const;
    bool deserialize(const std::string& str);

    void serialize(autil::legacy::Jsonizable::JsonWrapper& json) const;
    bool deserialize(autil::legacy::Jsonizable::JsonWrapper& json);

    std::vector<TaskBasePtr> getAllTask() const;
    std::vector<TaskFlowPtr> getAllFlow() const;

    bool stopFlow(const std::string& flowId);

    bool suspendFlow(const std::string& flowId);

    bool removeFlow(const std::string& flowId, bool clearTask);

    std::string getDotString(bool fillTask = false) const;

    void fillFlowSubGraphInfo(KeyValueMap& subGraphStringMap, /* key=flowId, value=flowDotString */
                              std::map<std::string, KeyValueMap>& taskDetailInfoMap /* key=taskId, value=taskInfoMap*/);

    std::string getDotStringForMatchingFlows(const std::string& flowParamKey, const std::string& flowParamValue,
                                             bool fillTask = false) const;

    void fillTaskFlowInfos(proto::GenerationInfo* generationInfo, const WorkerTable& workerTable,
                           const CounterCollector::RoleCounterMap& roleCounterMap) const;

public:
    const TaskResourceManagerPtr& getTaskResourceManager() const;
    const TaskFactoryPtr& getTaskFactory() const;
    const std::string& getRootPath() const { return _rootPath; }

    void setLogPrefix(const std::string& str);

    std::vector<TaskBasePtr> getOrphanTasks() const;

    TaskMaintainerPtr getTaskMaintainer(const TaskFlowPtr& flow) const;

public:
    void TEST_setTaskFactory(const TaskFactoryPtr& factory);

private:
    bool deserializeTasks(autil::legacy::Jsonizable::JsonWrapper& json);
    bool deserializeFlows(autil::legacy::Jsonizable::JsonWrapper& json);

    void fillFlowMetaInfos(const std::vector<TaskFlowPtr>& flows,
                           ::google::protobuf::RepeatedPtrField<proto::FlowMeta>* flowMetas) const;

    void fillTaskInfos(const std::vector<TaskBasePtr>& tasks,
                       ::google::protobuf::RepeatedPtrField<proto::FlowTaskMeta>* taskMetas) const;
    void fillTaskInfos(::google::protobuf::RepeatedPtrField<proto::TaskInfo>* taskInfos,
                       const std::vector<TaskBasePtr>& tasks, const WorkerTable& workerTable,
                       const CounterCollector::RoleCounterMap& roleCounterMap) const;
    void fillTaskFlowMetaInfos(const std::vector<TaskFlowPtr>& allFlows, const std::vector<TaskBasePtr>& orphanTasks,
                               proto::GenerationInfo* generationInfo) const;
    void getTasks(const std::vector<TaskFlowPtr>& flows, std::vector<TaskBasePtr>& activeTasks,
                  std::vector<TaskBasePtr>& stoppedTasks) const;

private:
    std::string _rootPath;
    TaskContainerPtr _taskContainer;
    FlowContainerPtr _flowContainer;
    FlowFactoryPtr _flowFactory;
    mutable autil::RecursiveThreadMutex _lock;

    std::string _logPrefix;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(TaskFlowManager);

}} // namespace build_service::admin
