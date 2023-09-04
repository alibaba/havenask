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
#ifndef ISEARCH_BS_BUILDSERVICETASK_H
#define ISEARCH_BS_BUILDSERVICETASK_H

#include "autil/legacy/jsonizable.h"
#include "build_service/admin/AdminTaskBase.h"
#include "build_service/admin/controlflow/TaskBase.h"
#include "build_service/admin/controlflow/TaskResourceManager.h"
#include "build_service/admin/taskcontroller/AlterFieldTask.h"
#include "build_service/admin/taskcontroller/BuilderTaskWrapper.h"
#include "build_service/admin/taskcontroller/SingleJobBuilderTask.h"
#include "build_service/admin/taskcontroller/SingleMergerTask.h"
#include "build_service/admin/taskcontroller/TaskMaintainer.h"
#include "build_service/common_define.h"
#include "build_service/proto/WorkerNode.h"
#include "build_service/util/Log.h"

namespace beeper {
class EventTags;
typedef std::shared_ptr<EventTags> EventTagsPtr;
} // namespace beeper

namespace build_service { namespace admin {

class BuildServiceTask : public TaskBase
{
public:
    struct NodeInfo {
        uint32_t totalCnt = 0;
        uint32_t finishCnt = 0;
    };

public:
    BuildServiceTask(const std::string& id, const std::string& type, const TaskResourceManagerPtr& resMgr)
        : TaskBase(id, type, resMgr)
        , _beeperTags(new beeper::EventTags)
        , _beeperReportTs(0)
        , _beeperReportInterval(300)
        , _batchMode(false)
    {
    }

    ~BuildServiceTask();

private:
    BuildServiceTask& operator=(const BuildServiceTask&);

public:
    bool doInit(const KeyValueMap& kvMap) override;
    bool doFinish(const KeyValueMap& kvMap) override;
    void doSyncTaskProperty(proto::WorkerNodes& workerNodes) override;
    bool isFinished(proto::WorkerNodes& workerNodes) override;
    TaskBase* clone() override;
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    bool doStart(const KeyValueMap& kvMap) override { return _taskImpl->start(kvMap); }
    bool doSuspend() override;
    bool doResume() override;
    bool waitSuspended(proto::WorkerNodes& workerNodes) override;
    bool updateConfig() override;

    AdminTaskBasePtr getTaskImpl() { return _taskImpl; }

    void clearWorkerZkNode(const std::string& generationDir) const;
    void doAccept(const TaskOptimizerPtr& optimizer, TaskOptimizer::OptimizeStep step) override;
    std::string getNodesIdentifier() const override;

private:
    AdminTaskBasePtr createProcessor(const proto::BuildId& buildId, const KeyValueMap& kvMap);
    TaskMaintainerPtr createTask(const proto::BuildId& buildId, const KeyValueMap& kvMap);
    bool getInitParam(const KeyValueMap& kvMap, proto::BuildStep& buildStep, std::string& clusterName);
    BuilderTaskWrapperPtr createBuilder(const proto::BuildId& buildId, const KeyValueMap& kvMap);
    AlterFieldTaskPtr createAlterField(const proto::BuildId& buildId, const KeyValueMap& kvMap);
    SingleMergerTaskPtr createMerger(const proto::BuildId& buildId, const KeyValueMap& kvMap);
    SingleJobBuilderTaskPtr createJobBuilder(const proto::BuildId& buildId, const KeyValueMap& kvMap);

    bool getBuildStep(const KeyValueMap& kvMap, proto::BuildStep& buildStep);
    void setTaskStatus(TaskStatus stat) override;

    void supplementLableInfo(KeyValueMap& info) const override;
    void createGlobalTag(const proto::BuildId& buildId, const std::string& taskId, const KeyValueMap& kvMap,
                         beeper::EventTagsPtr& beeperTags);
    bool initResource(const KeyValueMap& kvMap);
    bool fillResourceKeepers(const std::vector<std::string>& resourceNames);

public:
    static const std::string PROCESSOR;
    static const std::string BUILDER;
    static const std::string MERGER;
    static const std::string JOB_BUILDER;
    static const std::string ALTER_FIELD;

private:
    std::string _clusterName;
    AdminTaskBasePtr _taskImpl;
    proto::BuildStep _buildStep;
    beeper::EventTagsPtr _beeperTags;
    volatile int64_t _beeperReportTs;
    int64_t _beeperReportInterval; // second
    bool _batchMode;
    NodeInfo _nodeInfo;
    std::vector<common::ResourceKeeperGuardPtr> _resourceKeepers;
    std::string _taskPhaseId;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(BuildServiceTask);

}} // namespace build_service::admin

#endif // ISEARCH_BS_BUILDSERVICETASK_H
