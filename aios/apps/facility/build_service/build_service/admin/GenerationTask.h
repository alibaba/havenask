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

#include <memory>
#include <set>
#include <stdint.h>
#include <string>
#include <vector>

#include "aios/apps/facility/cm2/cm_basic/util/zk_wrapper.h"
#include "autil/legacy/legacy_jsonizable_dec.h"
#include "build_service/admin/ClusterCheckpointSynchronizerCreator.h"
#include "build_service/admin/ConfigValidator.h"
#include "build_service/admin/CounterCollector.h"
#include "build_service/admin/GenerationTaskBase.h"
#include "build_service/admin/LegacyCheckpointListSyncronizer.h"
#include "build_service/admin/WorkerTable.h"
#include "build_service/admin/controlflow/TaskFactory.h"
#include "build_service/admin/controlflow/TaskFlow.h"
#include "build_service/admin/taskcontroller/ProcessorTask.h"
#include "build_service/common/ResourceContainer.h"
#include "build_service/common_define.h"
#include "build_service/config/TaskConfig.h"
#include "build_service/proto/Admin.pb.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/proto/DataDescription.h"
#include "build_service/proto/DataDescriptions.h"
#include "build_service/proto/Heartbeat.pb.h"
#include "build_service/proto/WorkerNode.h"
#include "indexlib/base/Types.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/framework/VersionCoord.h"
#include "indexlib/indexlib.h"

namespace build_service { namespace admin {

class BrokerTopicKeeper;

BS_TYPEDEF_PTR(BrokerTopicKeeper);

class CheckpointSynchronizer;

class GenerationTask : public GenerationTaskBase
{
public:
    GenerationTask(const proto::BuildId& buildId, cm_basic::ZkWrapper* zkWrapper);
    virtual ~GenerationTask();

private:
    GenerationTask& operator=(const GenerationTask&);

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    proto::BuildStep getBuildStep() const override;

private:
    std::vector<ProcessorTaskPtr> getProcessorTask();

public:
    bool fatalErrorAutoStop() override;
    void makeDecision(WorkerTable& workerTable) override;
    bool loadFromString(const std::string& statusString, const std::string& generationDir) override;
    bool restart(const std::string& statusString, const std::string& generationDir) override;
    // virtual GenerationTask *clone() const;
    void fillGenerationInfo(proto::GenerationInfo* generationInfo, const WorkerTable* workerTable, bool fillTaskHistory,
                            bool fillFlowInfo, bool fillEffectiveIndexInfo,
                            const CounterCollector::RoleCounterMap& roleCounterMap) override;

    bool printGraph(bool fillTaskInfo, std::string& graphString) override;

    bool printGraphForCluster(const std::string& clusterName, bool fillTaskInfo, std::string& graphString) override;

    int64_t getMaxLastVersionTimestamp() const override;
    bool cleanReservedCheckpoints(const std::string& clusterName, versionid_t versionId) override;

    bool getTotalRemainIndexSize(int64_t& totalSize) const override;
    std::vector<std::string> getClusterNames() const override;

    bool CleanTaskSignature() const;
    bool ValidateAndWriteTaskSignature(const std::string& generationZkRoot, bool validateExistingIndex,
                                       std::string* errorMsg) const override;

    bool importBuild(const std::string& configPath, const std::string& generationDir,
                     const std::string& dataDescriptionKvs,
                     const GenerationTaskBase::ImportedVersionIdMap& importedVersionIdMap,
                     proto::StartBuildResponse* response) override;

public:
    // For test
    bool TEST_prepareFakeIndex() const;
    void TEST_setForceBuildWithTabletV2Mode() { _forceUseTabletV2Build = true; }

private:
    bool publishAndSaveCheckpoint(const config::ResourceReaderPtr& resourceReader, const std::string& cluster,
                                  indexlibv2::versionid_t versionId);

    std::string getOpsInfos(const indexlib::config::IndexPartitionSchemaPtr& newSchema, const std::string& clusterName);
    bool checkReAddIndex(const std::string& clusterName,
                         const std::shared_ptr<indexlibv2::config::TabletSchema>& newTabletSchema,
                         const ::google::protobuf::RepeatedPtrField<proto::IndexInfo>& indexInfos);
    std::string getEffectiveIndexInfo(const std::string& clusterName,
                                      const std::shared_ptr<indexlibv2::config::TabletSchema>& latestSchema,
                                      const ::google::protobuf::RepeatedPtrField<proto::IndexInfo>& indexInfos) const;

    bool isTaskInStoppingFlow(const std::vector<std::string>& stopFlows, const std::string& taskId);

    ConfigValidator::SchemaUpdateStatus checkUpdateSchema(const config::ResourceReaderPtr& resourceReader,
                                                          const std::string& clusterName);

    bool doLoadFromConfig(const std::string& configPath, const std::string& generationDir,
                          const proto::DataDescriptions& dataDescriptions,
                          proto::BuildStep buildStep = proto::BUILD_STEP_FULL) override;
    // command
    bool doUpdateConfig(const std::string& configPath) override;
    bool doSuspendBuild(const std::string& flowId, std::string& errorMsg) override;
    bool doResumeBuild(const std::string& flowId, std::string& errorMsg) override;
    bool doGetBulkloadInfo(const std::string& clusterName, const std::string& bulkloadId,
                           const ::google::protobuf::RepeatedPtrField<proto::Range>& rangeVec, std::string* resultStr,
                           std::string* errorMsg) const override;
    bool doBulkload(const std::string& clusterName, const std::string& bulkloadId,
                    const ::google::protobuf::RepeatedPtrField<proto::ExternalFiles>& externalFiles,
                    const std::string& options, const std::string& action, std::string* errorMsg) override;
    bool doRollBack(const std::string& clusterName, const std::string& generationZkRoot, versionid_t versionId,
                    int64_t startTimestamp, std::string& errorMsg) override;
    bool doRollBackCheckpoint(const std::string& clusterName, const std::string& generationZkRoot,
                              checkpointid_t checkpointId,
                              const ::google::protobuf::RepeatedPtrField<proto::Range>& ranges,
                              std::string& errorMsg) override;

    bool commitVersion(const std::string& clusterName, const proto::Range& range, const std::string& versionMetaStr,
                       bool instantPublish, std::string& errorMsg) override;
    bool getCommittedVersions(const std::string& clusterName, const proto::Range& range, uint32_t limit,
                              std::string& result, std::string& errorMsg) override;

    bool markCheckpoint(const std::string& clusterName, versionid_t versionId, bool reserveFlag,
                        std::string& errorMsg) override;
    bool createSavepoint(const std::string& clusterName, checkpointid_t checkpointId, const std::string& comment,
                         std::string& savepointStr, std::string& errorMsg) override;
    bool removeSavepoint(const std::string& clusterName, checkpointid_t checkpointId, std::string& errorMsg) override;
    bool getReservedVersions(const std::string& clusterName, const proto::Range& range,
                             std::set<indexlibv2::framework::VersionCoord>* reservedVersions) const override;
    bool listCheckpoint(const std::string& clusterName, bool savepointOnly, uint32_t offset, uint32_t limit,
                        std::string* resultStr) const override;
    bool getCheckpoint(const std::string& clusterName, checkpointid_t checkpointId,
                       std::string* resultStr) const override;

    bool cleanVersions(const std::string& clusterName, versionid_t version) override;

    bool doCreateVersion(const std::string& clusterName, const std::string& mergeConfigName) override;
    bool doStartTask(int64_t taskId, const std::string& taskName, const std::string& taskConfigPath,
                     const std::string& clusterName, const std::string& taskParam,
                     GenerationTaskBase::StartTaskResponse* response) override;
    bool doStopTask(int64_t taskId, std::string& errorMsg) override;
    bool doSwitchBuild() override;
    bool checkFullBuildFinish() const override;

    bool doDeleteIndex() const override;
    bool doDeleteTmpBuilderIndex() const override;

    void fillIndexInfoSummarys(proto::GenerationInfo* generationInfo) const;
    void fillIndexInfoSummary(proto::IndexInfoSummary* indexInfoSummary,
                              const ::google::protobuf::RepeatedPtrField<proto::IndexInfo>& indexInfos) const;
    void fillIndexInfos(bool isFull, ::google::protobuf::RepeatedPtrField<proto::IndexInfo>* indexInfos);
    void FillSingleBuilderInfoV2(proto::SingleClusterInfo* singleClusterInfo, const TaskFlowPtr& flow,
                                 const std::string& taskId, const WorkerTable& workerTable,
                                 const CounterCollector::RoleCounterMap& roleCounterMap) const;
    bool isLeaderFollowerMode() const { return _commitVersionCalled; }
    void getMinValue(int64_t& minValue, int64_t newValue, int64_t initValue) const
    {
        if (minValue == initValue || newValue < minValue) {
            minValue = newValue;
        }
    }

    void getMaxValue(int64_t& maxValue, int64_t newValue, int64_t initValue) const
    {
        if (maxValue == initValue || newValue > maxValue) {
            maxValue = newValue;
        }
    }
    bool getTaskTargetClusterName(std::string& clusterName, std::string& errorMsg);
    void getFlowIdsForRollback(const std::string& clusterName, std::string& needSuspendFlows,
                               std::string& newBuildFlowId, std::string& newMergeCrontabFlowId);

    bool doUpdateSchema(bool hasOperations, const std::vector<std::string>& chgSchemaClusters,
                        const KeyValueMap& schemaIdMap, const KeyValueMap& opsIdMap);

    bool innerUpdateConfig(const std::string& configPath);

protected:
    void fillFlows(std::vector<std::string>& flowIds, bool onlyActiveFlow, std::vector<TaskFlowPtr>& flows) const;

    void fillIndexInfosForCluster(const std::string& clusterName, bool isFull,
                                  ::google::protobuf::RepeatedPtrField<proto::IndexInfo>* indexInfos);

    void updateWorkerProtocolVersion(const proto::WorkerNodes& workerNodes);
    void checkFatalError(const WorkerTable& workerTable);
    bool getAllClusterNames(std::vector<std::string>& allClusters) const;
    int32_t getWorkerProtocolVersion();
    bool innerDeleteIndex(const std::string& indexRoot) const;
    virtual void prepareGeneration(WorkerTable& workerTable);
    bool writeRealtimeInfoToIndex();
    void stopGeneration();
    void doMakeDecision(WorkerTable& workerTable);
    std::string stepToString(GenerationStep step) const;
    bool isRollingBack() const;
    void fillTaskInfosForSingleClusters(proto::BuildInfo* buildInfo, const WorkerTable& workerTable,
                                        bool fillEffectiveIndexInfo,
                                        const CounterCollector::RoleCounterMap& roleCounterMap) const;

    void getTaskFlowsForSingleCluster(const std::string& clusterName, std::vector<TaskFlowPtr>& buildFlow,
                                      std::vector<TaskFlowPtr>& alterFieldFlow,
                                      std::vector<TaskFlowPtr>& mergeCrontabFlow) const;

    void getBatchTaskFlowForSingelCluster(const std::string& cluster, std::vector<TaskFlowPtr>& batchBuildFlow,
                                          std::vector<TaskFlowPtr>& batchMergeFlow) const;

    void getActiveFlows(const std::vector<std::string>& flowIds, std::vector<TaskFlowPtr>& buildFlow) const;

    void fillBuildTaskInfos(proto::SingleClusterInfo* singleClusterInfo, const std::vector<TaskFlowPtr>& buildFlow,
                            const std::vector<TaskFlowPtr>& alterFieldFlow, const WorkerTable& workerTable,
                            const CounterCollector::RoleCounterMap& roleCounterMap) const;
    void fillBatchTaskInfos(proto::SingleClusterInfo* singleClusterInfo, const std::vector<TaskFlowPtr>& batchBuildFlow,
                            const std::vector<TaskFlowPtr>& batchMergeFlow, const WorkerTable& workerTable,
                            const CounterCollector::RoleCounterMap& roleCounterMap) const;
    void fillPendingTaskInfos(proto::SingleClusterInfo* singleClusterInfo,
                              const std::vector<TaskFlowPtr>& mergeCrontabFlow) const;
    void fillCheckpointInfos(const std::string& clusterName, proto::SingleClusterInfo* singleClusterInfo) const;

    void FillSingleBuilderInfo(proto::SingleClusterInfo* singleClusterInfo, const TaskFlowPtr& flow,
                               const std::string& taskId, const WorkerTable& workerTable,
                               const CounterCollector::RoleCounterMap& roleCounterMap) const;

    void FillSingleMergerInfo(proto::SingleClusterInfo* singleClusterInfo, const TaskFlowPtr& flow,
                              const std::string& taskId, const WorkerTable& workerTable,
                              const CounterCollector::RoleCounterMap& roleCounterMap) const;

    bool doWriteRealtimeInfoForRealtimeJobMode(const std::string& clusterName, const proto::DataDescription& ds);
    bool doWriteRealtimeInfoForRealtimeServiceModeWithRawDoc(const std::string& clusterName,
                                                             const proto::DataDescription& ds);
    bool prepareCheckpointSynchronizer(const config::ResourceReaderPtr& resourceReader);
    bool prepareCheckpointSynchronizer(const config::ResourceReaderPtr& resourceReader,
                                       ClusterCheckpointSynchronizerCreator::Type type);
    bool getCheckpointSynchronizerType(const config::ResourceReaderPtr& resourceReader,
                                       ClusterCheckpointSynchronizerCreator::Type* type);

    bool getIncBuildParallelNum(const std::string& clusterName, uint32_t* incBuildParallelNum) const;
    bool removeIndexTaskMeta(const std::string& cluster, const std::vector<proto::Range>& ranges);

protected:
    void runTasks(WorkerTable& workerTable) override;

protected:
    // for test
    TaskBasePtr getTask(const std::string& flowId, const std::string& taskId);
    TaskFlowPtr getFlow(const std::string& flowId);
    bool updatePackageInfo(const config::ResourceReaderPtr& oldResourceReader,
                           const config::ResourceReaderPtr& newResourceReader);

protected:
    proto::DataDescription _realTimeDataDesc;
    proto::BuildStep _currentProcessorStep;
    bool _isFullBuildFinish;
    std::string _dataDescriptionsStr;
    LegacyCheckpointListSyncronizerPtr _legacyCheckpointSynchronizer;
    std::shared_ptr<CheckpointSynchronizer> _checkpointSynchronizer;
    mutable volatile int64_t _lastCheckErrorTs;
    bool _batchMode;
    bool _isImportedTask;
    bool _commitVersionCalled;
    GenerationTaskBase::ImportedVersionIdMap _importedVersionIdMap;
    static const int64_t INNER_CHECK_PERIOD_INTERVAL = 60; // 1 min
    bool _forceUseTabletV2Build = false;
    bool _isV2Build = false;

private:
    bool isV2Build(const config::ResourceReaderPtr& resourceReader, bool& hasError);

private:
    friend class GenerationTaskStateMachine;
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(GenerationTask);

}} // namespace build_service::admin
