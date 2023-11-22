#pragma once

#include <set>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <utility>
#include <vector>

#include "aios/apps/facility/cm2/cm_basic/util/zk_wrapper.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "build_service/admin/GenerationTask.h"
#include "build_service/admin/GenerationTaskBase.h"
#include "build_service/admin/WorkerTable.h"
#include "build_service/admin/controlflow/TaskBase.h"
#include "build_service/admin/controlflow/TaskFlow.h"
#include "build_service/common/ResourceContainer.h"
#include "build_service/admin/taskcontroller/BuildServiceTask.h"
#include "build_service/admin/taskcontroller/SingleMergerTask.h"
#include "build_service/admin/test/BuilderV2Node.h"
#include "build_service/common_define.h"
#include "build_service/proto/Admin.pb.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/proto/DataDescriptions.h"
#include "build_service/proto/Heartbeat.pb.h"
#include "build_service/proto/WorkerNode.h"
#include "indexlib/base/Types.h"
#include "indexlib/indexlib.h"

namespace build_service { namespace admin {

class GenerationTaskStateMachine
{
public:
    GenerationTaskStateMachine(bool isJobTask = false, bool autoRestart = false);
    GenerationTaskStateMachine(const std::string& tableName, WorkerTable* workerTable, GenerationTask* gTask);
    virtual ~GenerationTaskStateMachine();

private:
    GenerationTaskStateMachine(const GenerationTaskStateMachine&);
    GenerationTaskStateMachine& operator=(const GenerationTaskStateMachine&);

public: // action
    bool init(const std::string& tableName, const std::string& configPath, const std::string& rootDir,
              proto::DataDescriptions dataDescriptions = proto::DataDescriptions(),
              proto::BuildStep buildStep = proto::BUILD_STEP_FULL, bool useV2Build = false);
    bool initFromString(const std::string& rootDir, const std::string& tableName, const std::string& jsonStr);
    bool updateConfig(const std::string& configPath)
    {
        std::vector<proto::ErrorInfo> errorInfos;
        return updateConfig(configPath, errorInfos);
    }

    void cloneTask()
    {
        std::string generationStr = ToJsonString(*_generationTask);
        auto tmp = _generationTask;
        _generationTask = NULL;
        prepareFakeGenerationTask(_tableName, _rootDir);
        FromJsonString(*_generationTask, generationStr);
        delete tmp;
    }

    bool updateConfig(const std::string& configPath, std::vector<proto::ErrorInfo>& errorInfos)
    {
        std::string generationStr = ToJsonString(*_generationTask);
        if (!_generationTask->updateConfig(configPath)) {
            _generationTask->transferErrorInfos(errorInfos);
            delete _generationTask;
            prepareFakeGenerationTask(_tableName, _rootDir);
            FromJsonString(*_generationTask, generationStr);
            return false;
        }
        return true;
    }
    bool startTask(int64_t taskId, const std::string& taskName, const std::string& taskConfigPath,
                   const std::string& clusterName, const std::string& taskParam, std::string& errorMsg);
    bool stopTask(int64_t taskId, std::string& errorMsg);
    bool switchBuild(bool createFullVersion = true, versionid_t fullVersion = 0, int64_t indexSize = 0,
                     bool finishProcessor = true);
    void makeDecision(size_t decisionTimes = 1);
    bool createVersion(const std::string& clusterName, const std::string& mergeConfigName);
    bool waitCreateVersion(const std::string& clusterName, versionid_t targetVersion, int64_t waitTimes = 15,
                           int64_t indexSize = 0);
    bool waitCreateVersionV2(const std::string& clusterName, versionid_t& createdVersion, int64_t waitTimes = 15);

    bool waitAlterField(const std::string& clusterName, int64_t rollBackVersion, int64_t alterFieldVersion);

    bool suspendBuild(std::string& errorMsg);
    bool suspendBuild(const std::string& clusterName, std::string& errorMsg);

    bool rollBack(const std::string& cluster, versionid_t targetVersion, std::string& errorMsg,
                  int64_t startTimestamp = -1);
    bool rollBackCheckpoint(const std::string& cluster, checkpointid_t checkpointId, std::string& errorMsg,
                            int64_t startTimestamp = -1);

    bool markCheckpoint(const std::string& clusterName, versionid_t versionId, bool reserveFlag, std::string& errorMsg);
    bool cleanVersions(const std::string& clusterName, versionid_t version);
    bool cleanReservedCheckpoints(const std::string& clusterName, versionid_t version);
    void stopBuild() { _generationTask->stopBuild(); }
    bool getTotalRemainIndexSize(int64_t& totalSize) const
    {
        return _generationTask->getTotalRemainIndexSize(totalSize);
    }

    bool callGraph(const std::string& graphName, const std::string& graphFileName, const KeyValueMap& graphKVMap);

public: // util
    void finishProcessor();
    void suspendProcessor();
    void suspendBuilder(const std::string& clusterName);
    void suspendMerger(const std::string& clusterName);
    void suspendTask(const std::string& clusterName, const std::string& taskName);
    bool resumeBuild(const std::string& clusterName, std::string& errorMsg);

    std::string getZkRoot() { return _generationDir; }

    bool fatalErrorAutoStop() { return _generationTask->fatalErrorAutoStop(); }
    int32_t getWorkerProtocolVersion() { return _generationTask->getWorkerProtocolVersion(); }

public:
    // interface for check
    WorkerTable* getWorkerTable() { return _workerTable; }
    proto::WorkerNodes getWorkerNodes(const std::string& clusterName, proto::RoleType roleType,
                                      std::string taskId = std::string(""));
    static proto::WorkerNodes getWorkerNodes(const std::string& clusterName, proto::RoleType roleType,
                                             WorkerTable* workerTable, std::string taskId = std::string(""));
    std::vector<BuilderV2Node> getBuilderV2Nodes(std::string clusterName = "");

public:
    bool checkTaskZkStatus(const std::string& flowTag, const std::string& taskId, const std::string& nodePath,
                           const std::string& result);
    void getGenerationInfo(proto::GenerationInfo* generationInfo, bool fillEffectiveIndexInfo = false);
    bool checkBrokerTopicExist(const std::string& topicName);

    GenerationTaskBase::GenerationStep getStep() const { return _generationTask->getGenerationStep(); }

    bool getCheckpointInfos(const std::string& clusterName, versionid_t version,
                            ::google::protobuf::RepeatedPtrField<proto::CheckpointInfo>& checkpoints);

    bool getIndexInfo(bool isFull, const std::string& clusterName,
                      ::google::protobuf::RepeatedPtrField<proto::IndexInfo>& indexInfos);
    std::set<versionid_t> getReservedIndexCheckpoint(const std::string& clusterName);

    void getCheckpointInfos(const std::string& ckpId, std::vector<std::pair<std::string, std::string>>& checkpoints);

    void addBuilderCheckpoint(const std::string& clusterName, const proto::BuilderCheckpoint& builderCheckpoint);

    void setWorkerProtocolVersion(int32_t workerProtocolVersion, const std::string& configPath)
    {
        setWorkerProtocolVersion(_workerTable, workerProtocolVersion, configPath);
    }

    static void setWorkerProtocolVersion(WorkerTable* workerTable, int32_t workerProtocolVersion,
                                         const std::string& configPath);

    bool getSchemaPatch(const std::string& clusterName, std::string& patchInfo);
    // TODO: delete
    bool getZkStatus(const std::string& nodePath, std::string& result);
    bool checkZkStatus(const std::string& nodePath, const std::string& result);
    void getTaskInfo(int64_t taskId, proto::TaskInfo& taskInfo, bool& exist);
    TaskFlowPtr getFlow(const std::string& flowId);
    TaskFlowPtr getFlowByTag(const std::string& flowTag);
    proto::BuildId getBuildId();
    static void finishTasks(proto::WorkerNodes& nodes, std::string status = "");
    versionid_t getPublishedVersion(const std::string& clusterName);

private:
    static void finishTasks(proto::TaskNodes& taskNodes, std::string status = "");
    static void finishBuilder(const std::string& clusterName, WorkerTable* workerTable, const std::string& status = "",
                              bool needStopEndBuilder = true);
    static void finishBuilderV2(const std::string& clusterName, WorkerTable* workerTable);
    static void finishProcessor(WorkerTable* workerTable);
    static void finishTask(const std::string& taskId, WorkerTable* workerTable, std::string status = "");

    static proto::ProcessorNodes getProcessorNodes(WorkerTable* workerTable, const std::string& processorId);
    static proto::BuilderNodes getBuilderNodes(WorkerTable* workerTable, std::string clusterName = "");
    static std::vector<BuilderV2Node> getBuilderV2Nodes(WorkerTable* workerTable, std::string clusterName = "");
    static proto::MergerNodes getMergerNodes(WorkerTable* workerTable, std::string clusterName = "");
    static proto::TaskNodes getTaskNodes(WorkerTable* workerTable, const std::string& taskId,
                                         std::string clusterName = "");

    proto::TaskNodes getTaskNodes(const std::string& taskId, std::string clusterName = "");

    bool innerGetZkStatus(const std::string& content, const std::string& nodePath, std::string& result);

    bool innerCheckStatusEqual(const std::string& actual, const std::string& result);
    TaskBasePtr getTask(const std::string& flowTag, const std::string& taskId);

private:
    void finishMerger(const std::string& clusterName, versionid_t targetVersion, int64_t indexSize = 0,
                      std::string taskId = "");
    void finishBuilder(const std::string& clusterName, const std::string& status = "", bool needStopEndBuilder = true);
    void finishTasks(const std::string& taskName, const std::string& clusterName, const std::string& status = "");

    void finishJobBuilder();

    bool switchProcessorToInc();
    virtual void prepareFakeGenerationTask(const std::string& tableName, const std::string& rootDir);
    // void triggerMerge(const std::string& clusterName,
    //                   const std::string& mergeConfig);

public:
    // TODO delete
    bool createVersion(const std::string& clusterName, const std::string& mergeConfigName, versionid_t startVersion,
                       versionid_t endVersion);

private:
    cm_basic::ZkWrapper* _zkWrapper;
    GenerationTask* _generationTask;
    WorkerTable* _workerTable;
    std::string _generationDir;
    std::string _topicDir;
    std::string _tableName;
    std::string _rootDir;
    bool _needDeletePointer;
    bool _isJobTask;
    bool _autoRestart;
    bool _useV2Build;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(GenerationTaskStateMachine);

}} // namespace build_service::admin
