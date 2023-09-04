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

#include "build_service/admin/GenerationTaskBase.h"
#include "build_service/common_define.h"
#include "build_service/util/Log.h"

namespace build_service { namespace admin {

class GeneralGenerationTask : public GenerationTaskBase
{
public:
    GeneralGenerationTask(const proto::BuildId& buildId, cm_basic::ZkWrapper* zkWrapper);
    ~GeneralGenerationTask();

public:
    GeneralGenerationTask(const GeneralGenerationTask&);
    GeneralGenerationTask& operator=(const GeneralGenerationTask&);

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

public:
    bool loadFromConfig(const std::string& configPath, const std::string& generationDir,
                        const std::string& dataDescriptionKvs,
                        proto::BuildStep buildStep = proto::BUILD_STEP_FULL) override;
    bool fatalErrorAutoStop() override { return false; }

    bool loadFromString(const std::string& statusString, const std::string& generationDir) override;

    void makeDecision(WorkerTable& workerTable) override;

    bool restart(const std::string& statusString, const std::string& generationDir) override;

    void fillGenerationInfo(proto::GenerationInfo* generationInfo, const WorkerTable* workerTable, bool fillTaskHistory,
                            bool fillFlowInfo, bool fillEffectiveIndexInfo,
                            const CounterCollector::RoleCounterMap& roleCounterMap) override;
    bool validateConfig(const std::string& configPath, std::string& errorMsg, bool firstTime) override { return true; }

    bool getTotalRemainIndexSize(int64_t& totalSize) const override { return false; }

    bool printGraph(bool fillTaskInfo, std::string& graphString) override
    {
        graphString = _taskFlowManager->getDotString(fillTaskInfo);
        return true;
    }

    bool printGraphForCluster(const std::string& clusterName, bool fillTaskInfo, std::string& graphString) override
    {
        return false;
    }

    bool importBuild(const std::string& configPath, const std::string& generationDir,
                     const std::string& dataDescriptionKvs, indexlib::versionid_t importedVersionId,
                     proto::StartBuildResponse* response) override
    {
        return false;
    }

    bool cleanVersions(const std::string& clusterName, versionid_t version) override { return true; }

    std::vector<std::string> getClusterNames() const override { return {}; }
    bool ValidateAndWriteTaskSignature(const std::string& generationZkRoot, bool validateExistingIndex,
                                       std::string* errorMsg) const override
    {
        return true;
    }

    bool updateConfig(const std::string& configPath) override;

private:
    bool isAllFlowFinished();
    bool doLoadFromConfig(const std::string& configPath, const std::string& generationDir,
                          const proto::DataDescriptions& dataDescriptions,
                          proto::BuildStep buildStep = proto::BUILD_STEP_FULL) override
    {
        assert(false);
        return false;
    }
    bool doSuspendBuild(const std::string& flowId, std::string& errorMsg) override
    {
        assert(false);
        return false;
    }
    bool doResumeBuild(const std::string& flowId, std::string& errorMsg) override
    {
        assert(false);
        return false;
    }
    bool doGetBulkloadInfo(const std::string& clusterName, const std::string& bulkloadId,
                           const ::google::protobuf::RepeatedPtrField<proto::Range>& ranges, std::string* resultStr,
                           std::string* errorMsg) const override
    {
        assert(false);
        return false;
    }
    bool doRollBack(const std::string& clusterName, const std::string& generationZkRoot, versionid_t versionId,
                    int64_t startTimestamp, std::string& errorMsg) override
    {
        assert(false);
        return false;
    }
    bool doRollBackCheckpoint(const std::string& clusterName, const std::string& generationZkRoot,
                              checkpointid_t checkpointId,
                              const ::google::protobuf::RepeatedPtrField<proto::Range>& ranges,
                              std::string& errorMsg) override
    {
        assert(false);
        return false;
    }

    bool doUpdateConfig(const std::string& configPath) override
    {
        assert(false);
        return false;
    }
    bool doCreateVersion(const std::string& clusterName, const std::string& mergeConfigName) override { return false; }
    bool doStartTask(int64_t taskId, const std::string& taskName, const std::string& taskConfigPath,
                     const std::string& clusterName, const std::string& taskParam,
                     GenerationTaskBase::StartTaskResponse* response) override;

    bool doStopTask(int64_t taskId, std::string& errorMsg) override;

    bool doSwitchBuild() override { return true; }
    bool doDeleteIndex() const override { return true; }
    bool doDeleteTmpBuilderIndex() const override { return true; }

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(GeneralGenerationTask);

}} // namespace build_service::admin
