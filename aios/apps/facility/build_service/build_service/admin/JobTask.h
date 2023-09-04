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
#ifndef ISEARCH_BS_JOBTASK_H
#define ISEARCH_BS_JOBTASK_H

#include "build_service/admin/FatalErrorDiscoverer.h"
#include "build_service/admin/GenerationTask.h"
#include "build_service/admin/taskcontroller/SingleMergerTask.h"
#include "build_service/common_define.h"
#include "build_service/util/Log.h"

namespace build_service { namespace admin {

class JobTask : public GenerationTask
{
public:
    enum JobStatus { JS_BUILD, JS_MERGE, JS_FINISHED };

public:
    JobTask(const proto::BuildId& buildId, cm_basic::ZkWrapper* zkWrapper);
    JobTask(const JobTask& other);
    ~JobTask();

private:
    JobTask& operator=(const JobTask&);

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

    bool loadFromString(const std::string& statusString, const std::string& generationDir) override;
    void makeDecision(WorkerTable& workerTable) override;
    // JobTask *clone() const override;
    bool restart(const std::string& statusString, const std::string& generationDir) override;
    void fillGenerationInfo(proto::GenerationInfo* generationInfo, const WorkerTable* workerTable, bool fillTaskHistory,
                            bool fillFlowInfo, bool fillEffectiveIndexInfo,
                            const CounterCollector::RoleCounterMap& roleCounterMap) override;

    bool ValidateAndWriteTaskSignature(const std::string& generationZkRoot, bool validateExistingIndex,
                                       std::string* errorMsg) const override
    {
        return true;
    };

private:
    bool doLoadFromConfig(const std::string& configPath, const std::string& generationDir,
                          const proto::DataDescriptions& dataDescriptions,
                          proto::BuildStep buildStep = proto::BUILD_STEP_FULL) override;
    bool doUpdateConfig(const std::string& configPath) override;
    bool doSuspendBuild(const std::string& flowId, std::string& errorMsg) override;
    bool doResumeBuild(const std::string& flowId, std::string& errorMsg) override;
    bool doRollBack(const std::string& clusterName, const std::string& generationZkRoot, versionid_t versionId,
                    int64_t startTimestamp, std::string& errorMsg) override;
    bool doCreateVersion(const std::string& clusterName, const std::string& mergeConfigName) override;
    bool doStartTask(int64_t taskId, const std::string& taskName, const std::string& taskConfigPath,
                     const std::string& clusterName, const std::string& taskParam,
                     GenerationTaskBase::StartTaskResponse* response) override;
    bool doStopTask(int64_t taskId, std::string& errorMsg) override;
    bool doSwitchBuild() override;
    bool cleanVersions(const std::string& clusterName, versionid_t version) override;
    bool doDeleteIndex() const override { return true; }
    bool doDeleteTmpBuilderIndex() const override { return true; };
    std::vector<std::string> getClusterNames() const override;
    bool checkFullBuildFinish() const override;

private:
    bool setupRawFileOrLineData(const config::ResourceReaderPtr& resourceReader);
    std::string getBuildStepString(GenerationStep gs) const;
    bool handleDataDescriptions(const proto::DataDescriptions& dataDescriptions);
    bool checkDescriptionsValid();
    using GenerationTask::prepareGeneration;
    void prepareGeneration();
    bool writeRealtimeInfoToIndex();
    void doMakeDecision(WorkerTable& workerTable);
    void cleanup(WorkerTable& workerTable);

    TaskFlowPtr getFlowByTag(const std::string& flowTag) const;

    void fillJobInfo(proto::JobInfo* jobInfo, const TaskFlowPtr& flow, const std::string& taskId,
                     const WorkerTable& workerTable, const CounterCollector::RoleCounterMap& roleCounterMap) const;

private:
    std::string _clusterName;
    proto::DataDescriptions _dataDescriptions;
    std::string _mergeConfigName;
    bool _needMerge;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(JobTask);

}} // namespace build_service::admin

#endif // ISEARCH_BS_JOBTASK_H
