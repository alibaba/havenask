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

#include <assert.h>
#include <map>
#include <memory>
#include <set>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "aios/apps/facility/cm2/cm_basic/util/zk_wrapper.h"
#include "autil/Lock.h"
#include "autil/legacy/json.h"
#include "autil/legacy/jsonizable.h"
#include "build_service/admin/CheckpointMetricReporter.h"
#include "build_service/admin/ConfigCleaner.h"
#include "build_service/admin/CounterCollector.h"
#include "build_service/admin/FatalErrorChecker.h"
#include "build_service/admin/FatalErrorMetricReporter.h"
#include "build_service/admin/FlowIdMaintainer.h"
#include "build_service/admin/SlowNodeMetricReporter.h"
#include "build_service/admin/TaskStatusMetricReporter.h"
#include "build_service/admin/WorkerTable.h"
#include "build_service/admin/catalog/CatalogPartitionIdentifier.h"
#include "build_service/admin/controlflow/TaskFactory.h"
#include "build_service/admin/controlflow/TaskFlowManager.h"
#include "build_service/admin/taskcontroller/TaskMaintainer.h"
#include "build_service/admin/taskcontroller/TaskOptimizerFactory.h"
#include "build_service/common/ResourceContainer.h"
#include "build_service/common_define.h"
#include "build_service/config/BuildServiceConfig.h"
#include "build_service/config/CounterConfig.h"
#include "build_service/config/MultiClusterRealtimeSchemaListKeeper.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/config/TaskConfig.h"
#include "build_service/proto/Admin.pb.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/proto/DataDescriptions.h"
#include "build_service/proto/ErrorCollector.h"
#include "build_service/util/SharedPtrGuard.h"
#include "indexlib/base/Types.h"
#include "indexlib/framework/VersionCoord.h"
#include "indexlib/indexlib.h"
#include "indexlib/table/BuiltinDefine.h"

namespace build_service { namespace admin {
class GenerationTaskBase : public autil::legacy::Jsonizable, public proto::ErrorCollector
{
public:
    enum GenerationStep {
        GENERATION_UNKNOWN,
        GENERATION_STARTING,
        GENERATION_STARTED,
        GENERATION_STOPPING,
        GENERATION_STOPPED,
        GENERATION_IDLE
    };
    enum TaskType {
        TT_SERVICE, // default
        TT_JOB,
        TT_CUSTOMIZED,
        TT_GENERAL,
        TT_UNKNOWN = 127,
    };

    struct StartTaskResponse {
        std::string message;
        bool duplicated = false;
        bool needRecover = true;
        StartTaskResponse() = default;
    };

    typedef std::vector<std::string> TargetRoleNames;
    // agentRole -> <agentIdentifier, [normal roles]>
    typedef std::map<std::string, std::pair<std::string, TargetRoleNames>> AgentRoleTargetMap;

    // clusterName->importedVersionId
    typedef std::map<std::string, indexlib::versionid_t> ImportedVersionIdMap;

public:
    GenerationTaskBase(const proto::BuildId& buildId, TaskType taskType, cm_basic::ZkWrapper* zkWrapper);
    // GenerationTaskBase(const GenerationTaskBase &other);
    virtual ~GenerationTaskBase();

private:
    GenerationTaskBase& operator=(const GenerationTaskBase&);

public:
    // static GenerationTaskBase *create(
    //     const proto::BuildId &buildId, bool jobMode, cm_basic::ZkWrapper *zkWrapper);

    static GenerationTaskBase* recover(const proto::BuildId& buildId, const std::string& jsonStr,
                                       cm_basic::ZkWrapper* zkWrapper);

public:
    virtual void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json);

public:
    virtual bool fatalErrorAutoStop() = 0;
    virtual bool loadFromConfig(const std::string& configPath, const std::string& generationDir,
                                const std::string& dataDescriptionKvs,
                                proto::BuildStep buildStep = proto::BUILD_STEP_FULL);
    virtual bool loadFromString(const std::string& statusString, const std::string& generationDir) = 0;
    virtual void makeDecision(WorkerTable& workerTable) = 0;
    // virtual GenerationTaskBase *clone() const = 0;
    virtual bool restart(const std::string& statusString, const std::string& generationDir) = 0;

    virtual void fillGenerationInfo(proto::GenerationInfo* generationInfo, const WorkerTable* workerTable,
                                    bool fillTaskHistory, bool fillFlowInfo, bool fillEffectiveIndexInfo,
                                    const CounterCollector::RoleCounterMap& roleCounterMap) = 0;

    virtual void fillTaskInfo(int64_t taskId, const WorkerTable& workerTable, proto::TaskInfo* taskInfo, bool& exist);
    virtual size_t getRunningTaskNum() const
    {
        assert(false);
        return 0;
    }
    virtual bool validateConfig(const std::string& configPath, std::string& errorMsg, bool firstTime);
    virtual bool getTotalRemainIndexSize(int64_t& totalSize) const = 0;

    bool callGraph(const std::string& graphName, const std::string& graphFileName, const KeyValueMap& graphKVMap);

    virtual bool printGraph(bool fillTaskInfo, std::string& graphString) = 0;
    virtual bool printGraphForCluster(const std::string& clusterName, bool fillTaskInfo, std::string& graphString) = 0;

    virtual bool checkFullBuildFinish() const { return false; }

    virtual bool importBuild(const std::string& configPath, const std::string& generationDir,
                             const std::string& dataDescriptionKvs, const ImportedVersionIdMap& importedVersionIdMap,
                             proto::StartBuildResponse* response) = 0;

    void setCheckpointMetricReporter(const CheckpointMetricReporterPtr& reporter);
    void setTaskStatusMetricReporter(const TaskStatusMetricReporterPtr& reporter);
    void SetSlowNodeMetricReporter(const SlowNodeMetricReporterPtr& reporter);
    void SetFatalErrorMetricReporter(const FatalErrorMetricReporterPtr& reporter);

    void setAgentRoleTargetMap(const std::shared_ptr<AgentRoleTargetMap>& map);
    std::shared_ptr<AgentRoleTargetMap> getAgentRoleTargetMap() const;
    void fillAgentRoleTargetInfo(proto::GenerationInfo* generationInfo);

    void setCatalogPartitionIdentifier(const std::shared_ptr<CatalogPartitionIdentifier>& identifier);
    std::shared_ptr<CatalogPartitionIdentifier> getCatalogPartitionIdentifier() const;

public:
    // for test
    // virtual bool operator==(const GenerationTaskBase &other) const = 0;
public:
    // command
    void stopBuild();
    bool suspendBuild(const std::string& flowId, std::string& errorMsg);
    bool resumeBuild(const std::string& flowId, std::string& errorMsg);
    bool rollBack(const std::string& clusterName, const std::string& generationZkRoot, versionid_t versionId,
                  int64_t startTimestamp, std::string& errorMsg);
    bool rollBackCheckpoint(const std::string& clusterName, const std::string& generationZkRoot,
                            checkpointid_t checkpointId,
                            const ::google::protobuf::RepeatedPtrField<proto::Range>& ranges, std::string& errorMsg);
    bool getBulkloadInfo(const std::string& clusterName, const std::string& bulkloadId,
                         const ::google::protobuf::RepeatedPtrField<proto::Range>& ranges, std::string* resultStr,
                         std::string* errorMsg) const;
    bool bulkload(const std::string& clusterName, const std::string& bulkloadId,
                  const ::google::protobuf::RepeatedPtrField<proto::ExternalFiles>& externalFiles,
                  const std::string& options, const std::string& action, std::string* errorMsg);
    virtual bool markCheckpoint(const std::string& clusterName, versionid_t versionId, bool reserveFlag,
                                std::string& errorMsg)
    {
        errorMsg = "unsupported operation";
        return false;
    }
    virtual bool commitVersion(const std::string& clusterName, const proto::Range& range,
                               const std::string& versionMetaStr, bool instantPublish, std::string& errorMsg)
    {
        assert(false);
        return false;
    }
    virtual bool getCommittedVersions(const std::string& clusterName, const proto::Range& range, uint32_t limit,
                                      std::string& result, std::string& errorMsg)
    {
        assert(false);
        return false;
    }

    virtual bool createSavepoint(const std::string& clusterName, checkpointid_t checkpointId,
                                 const std::string& comment, std::string& savepointStr, std::string& errorMsg)
    {
        assert(false);
        return false;
    }
    virtual bool removeSavepoint(const std::string& clusterName, checkpointid_t checkpointId, std::string& errorMsg)
    {
        assert(false);
        return false;
    }
    virtual bool getReservedVersions(const std::string& clusterName, const proto::Range& range,
                                     std::set<indexlibv2::framework::VersionCoord>* reservedVersions) const
    {
        assert(false);
        return false;
    }
    virtual bool listCheckpoint(const std::string& clusterName, bool savepointOnly, uint32_t offset, uint32_t limit,
                                std::string* result) const
    {
        assert(false);
        return false;
    }
    virtual bool getCheckpoint(const std::string& clusterName, checkpointid_t checkpointId, std::string* result) const
    {
        assert(false);
        return false;
    }

    virtual bool updateConfig(const std::string& configPath);
    bool createVersion(const std::string& clusterName, const std::string& mergeConfigName);
    bool startTask(int64_t taskId, const std::string& taskName, const std::string& taskConfigPath,
                   const std::string& cluserName, const std::string& taskParam, StartTaskResponse* response);
    bool stopTask(int64_t taskId, std::string& errorMsg);
    bool switchBuild();
    virtual bool cleanVersions(const std::string& clusterName, versionid_t version) = 0;

public:
    TaskType getType() const { return _taskType; }
    bool isIndexDeleted() const { return _indexDeleted; }
    void deleteIndex();
    bool deleteTmpBuilderIndex() const;
    const config::CounterConfig& getCounterConfig() const { return _counterConfig; }
    virtual int64_t getMaxLastVersionTimestamp() const { return -1; }
    virtual bool cleanReservedCheckpoints(const std::string& clusterName, versionid_t versionId) { return true; }

    virtual proto::BuildStep getBuildStep() const { return proto::BUILD_STEP_FULL; }

    const std::string& getIndexRoot() const { return _indexRoot; }
    virtual std::vector<std::string> getClusterNames() const = 0;
    virtual bool ValidateAndWriteTaskSignature(const std::string& generationZkRoot, bool validateExistingIndex,
                                               std::string* errorMsg) const = 0;
    config::ResourceReaderPtr GetLatestResourceReader() const;
    std::string getFatalErrorString() const;

protected:
    // bool equals(const GenerationTaskBase &other) const;
private:
    void init();
    virtual bool doLoadFromConfig(const std::string& configPath, const std::string& generationDir,
                                  const proto::DataDescriptions& dataDescriptions,
                                  proto::BuildStep buildStep = proto::BUILD_STEP_FULL) = 0;
    virtual bool doSuspendBuild(const std::string& flowId, std::string& errorMsg) = 0;
    virtual bool doResumeBuild(const std::string& flowId, std::string& errorMsg) = 0;
    virtual bool doGetBulkloadInfo(const std::string& clusterName, const std::string& bulkloadId,
                                   const ::google::protobuf::RepeatedPtrField<proto::Range>& ranges,
                                   std::string* resultStr, std::string* errorMsg) const = 0;
    virtual bool doBulkload(const std::string& clusterName, const std::string& bulkloadId,
                            const ::google::protobuf::RepeatedPtrField<proto::ExternalFiles>& externalFiles,
                            const std::string& options, const std::string& action, std::string* errorMsg) = 0;
    virtual bool doRollBack(const std::string& clusterName, const std::string& generationZkRoot, versionid_t versionId,
                            int64_t startTimestamp, std::string& errorMsg) = 0;
    virtual bool doRollBackCheckpoint(const std::string& clusterName, const std::string& generationZkRoot,
                                      checkpointid_t checkpointId,
                                      const ::google::protobuf::RepeatedPtrField<proto::Range>& ranges,
                                      std::string& errorMsg) = 0;
    virtual bool doUpdateConfig(const std::string& configPath) = 0;
    virtual bool doCreateVersion(const std::string& clusterName, const std::string& mergeConfigName) = 0;
    virtual bool doStartTask(int64_t taskId, const std::string& taskName, const std::string& taskConfigPath,
                             const std::string& clusterName, const std::string& taskParam,
                             StartTaskResponse* response) = 0;
    virtual bool doStopTask(int64_t taskId, std::string& errorMsg) = 0;
    virtual bool doSwitchBuild() = 0;
    virtual bool doDeleteIndex() const = 0;
    virtual bool doDeleteTmpBuilderIndex() const = 0;

protected:
    virtual bool readIndexRoot(const std::string& configPath, std::string& indexRoot, std::string& fullBuilderTmpRoot);
    bool isSuspending();
    bool checkIndexRoot(const std::string& configPath);
    bool isPackageInfoUpdated(const std::string& oldConfigPath, const std::string& newConfigPath, bool& updated);
    bool getAllClusterNames(config::ResourceReader& resourceReader, std::vector<std::string>& clusterNames) const;

protected:
    // todo move to graph generation
    virtual void runTasks(WorkerTable& workerTable);
    std::string stepToString(GenerationStep step) const;

public:
    void setDebugMode(const std::string& debugMode) { _debugMode = debugMode; }
    std::string getDebugMode() const { return _debugMode; }
    const proto::BuildId& getBuildId() const { return _buildId; }
    const std::string getHeartbeatType() const { return _heartbeatType; }
    std::string getConfigPath() const;
    void setConfigPath(const std::string& path);

    bool isStarting() const { return _step == GENERATION_STARTING; }
    bool isStopping() const { return _step == GENERATION_STOPPING; }
    bool isStopped() const { return _step == GENERATION_STOPPED; }

    int64_t getStartTimeStamp() const { return _startTimeStamp; }
    int64_t getStopTimeStamp() const { return _stopTimeStamp; }
    GenerationStep getGenerationStep() const { return _step; }
    const TaskFlowManagerPtr& getTaskFlowManager() const { return _taskFlowManager; }

protected:
    virtual std::shared_ptr<config::MultiClusterRealtimeSchemaListKeeper> getSchemaListKeeper() const;
    bool isActive() const;
    bool loadBuildServiceConfig(const std::string& configPath, config::BuildServiceConfig& buildServiceConfig);
    bool readHeartbeatType(const std::string& configPath);
    bool checkCounterConfig(const std::string& configPath);
    bool parseDataDescriptions(const std::string& dataDescriptionKvs, proto::DataDescriptions& dataDescriptions);
    bool loadDataDescriptions(const std::string& configPath, proto::DataDescriptions& dataDescriptions);
    bool doWriteRealtimeInfoToIndex(const std::string& clusterName, autil::legacy::json::JsonMap realtimeInfoJsonMap);
    TaskMaintainerPtr getUserTask(int64_t taskId);

    static std::string convertTableType(const std::string& tableType)
    {
        if (tableType == indexlib::table::TABLE_TYPE_NORMAL) {
            return "index";
        } else if (tableType == indexlib::table::TABLE_TYPE_RAWFILE) {
            return "rawfile";
        } else if (tableType == indexlib::table::TABLE_TYPE_LINEDATA) {
            return "linedata";
        }
        return tableType;
    }
    static std::string getUserTaskTag(int64_t taskId) { return std::string("TaskId:") + std::to_string(taskId); }

protected:
    // status for self
    proto::BuildId _buildId;
    TaskType _taskType;
    cm_basic::ZkWrapper* _zkWrapper;
    GenerationStep _step;

    mutable autil::ThreadMutex _configMutex;
    std::string _configPath;
    std::string _debugMode;
    std::string _heartbeatType;
    bool _indexDeleted;
    std::string _indexRoot;
    int64_t _startTimeStamp;
    int64_t _stopTimeStamp;
    config::CounterConfig _counterConfig;
    std::string _fullBuilderTmpRoot;
    mutable FatalErrorChecker _fatalErrorChecker;
    std::unordered_set<std::string> _realTimeInfoState;
    std::string _generationDir;
    CheckpointMetricReporterPtr _checkpointMetricReporter;
    TaskStatusMetricReporterPtr _taskStatusMetricReporter;
    SlowNodeMetricReporterPtr _slowNodeMetricReporter;
    FatalErrorMetricReporterPtr _fatalErrorMetricReporter;

    TaskFactoryPtr _taskFactory;
    TaskFlowManagerPtr _taskFlowManager;
    std::string _luaPathId;
    TaskResourceManagerPtr _resourceManager;
    ConfigCleanerPtr _configCleaner;
    FlowIdMaintainerPtr _flowCleaner;
    TaskOptimizerFactoryPtr _optimizerFactory;
    std::map<std::string, int64_t> _callGraphHistory;
    util::SharedPtrGuard<AgentRoleTargetMap> _agentRoleTargetMapGuard;
    std::shared_ptr<CatalogPartitionIdentifier> _catalogPartitionIdentifier;
    mutable std::shared_ptr<config::MultiClusterRealtimeSchemaListKeeper> _schemaListKeeper;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(GenerationTaskBase);
}} // namespace build_service::admin
