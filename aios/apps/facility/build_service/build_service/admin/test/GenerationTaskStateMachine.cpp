#include "build_service/admin/test/GenerationTaskStateMachine.h"

#include "autil/StringUtil.h"
#include "build_service/admin/CheckpointCreator.h"
#include "build_service/admin/taskcontroller/MergeCrontabTask.h"
#include "build_service/admin/test/FakeGenerationTask.h"
#include "build_service/admin/test/FakeJobTask.h"
#include "build_service/common/BuilderCheckpointAccessor.h"
#include "build_service/common/CheckpointAccessor.h"
#include "build_service/common/IndexCheckpointAccessor.h"
#include "build_service/common/PathDefine.h"
#include "build_service/config/CLIOptionNames.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/proto/ProcessorTaskIdentifier.h"
#include "build_service/proto/test/ProtoCreator.h"
#include "build_service/worker/WorkerStateHandler.h"
#include "fslib/util/FileUtil.h"

using namespace std;
using namespace build_service::proto;
using namespace build_service::common;
using namespace build_service::worker;
using namespace build_service::util;
using namespace build_service::config;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace autil;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, GenerationTaskStateMachine);

GenerationTaskStateMachine::GenerationTaskStateMachine(bool isJobTask, bool autoRestart)
    : _zkWrapper(NULL)
    , _generationTask(NULL)
    , _workerTable(NULL)
    , _needDeletePointer(true)
    , _isJobTask(isJobTask)
    , _autoRestart(autoRestart)
{
}

GenerationTaskStateMachine::GenerationTaskStateMachine(const string& tableName, WorkerTable* workerTable,
                                                       GenerationTask* gTask)
    : _zkWrapper(NULL)
    , _generationTask(gTask)
    , _workerTable(workerTable)
    , _tableName(tableName)
    , _rootDir(gTask->_indexRoot)
    , _needDeletePointer(false)
    , _autoRestart(false)
{
    _generationDir = PathDefine::getGenerationZkRoot(gTask->_indexRoot, gTask->_buildId);
    JobTask* task = dynamic_cast<JobTask*>(gTask);
    _isJobTask = (task != NULL);
}

GenerationTaskStateMachine::~GenerationTaskStateMachine()
{
    if (_zkWrapper) {
        delete _zkWrapper;
    }
    if (!_needDeletePointer) {
        return;
    }
    if (_generationTask) {
        delete _generationTask;
    }

    if (_workerTable) {
        delete _workerTable;
    }
}

bool GenerationTaskStateMachine::checkBrokerTopicExist(const string& topicName)
{
    bool exist;
    if (!fslib::util::FileUtil::isExist(fslib::util::FileUtil::joinFilePath(_topicDir, topicName), exist)) {
        assert(false);
        return false;
    }
    return exist;
}

bool GenerationTaskStateMachine::initFromString(const string& rootDir, const string& tableName, const string& jsonStr)
{
    prepareFakeGenerationTask(tableName, rootDir);
    _workerTable = new WorkerTable(_generationDir, _zkWrapper);
    return _generationTask->loadFromString(jsonStr, _generationDir);
}

bool GenerationTaskStateMachine::getSchemaPatch(const std::string& clusterName, std::string& patchInfo)
{
    IndexCheckpointAccessorPtr checkpointAccessor =
        CheckpointCreator::createIndexCheckpointAccessor(_generationTask->_resourceManager);
    return checkpointAccessor->getSchemaChangedStandard(clusterName, patchInfo);
}

void GenerationTaskStateMachine::setWorkerProtocolVersion(WorkerTable* workerTable, int32_t workerProtocolVersion,
                                                          const std::string& configPath)
{
    struct WorkerProtocolSetter {
        WorkerProtocolSetter(int32_t workerProtocolVersion, const std::string& configPath)
            : _workerProtocolVersion(workerProtocolVersion)
            , _configPath(configPath)
        {
        }
        ~WorkerProtocolSetter() {}
        bool operator()(WorkerNodeBasePtr node)
        {
            ProcessorNodePtr processorNode = DYNAMIC_POINTER_CAST(ProcessorNode, node);
            if (processorNode) {
                ProcessorCurrent processorCurrent;
                string processorCurrentStr;
                processorCurrent.set_protocolversion(_workerProtocolVersion);
                processorCurrent.set_configpath(_configPath);
                processorCurrent.SerializeToString(&processorCurrentStr);
                node->setCurrentStatusStr(processorCurrentStr, "");
                return true;
            }
            BuilderNodePtr builderNode = DYNAMIC_POINTER_CAST(BuilderNode, node);
            if (builderNode) {
                BuilderCurrent builderCurrent;
                string builderCurrentStr;
                builderCurrent.set_protocolversion(_workerProtocolVersion);
                builderCurrent.set_configpath(_configPath);
                builderCurrent.SerializeToString(&builderCurrentStr);
                node->setCurrentStatusStr(builderCurrentStr, "");
                return true;
            }
            MergerNodePtr mergerNode = DYNAMIC_POINTER_CAST(MergerNode, node);
            if (mergerNode) {
                MergerCurrent mergerCurrent;
                string mergerCurrentStr;
                mergerCurrent.set_protocolversion(_workerProtocolVersion);
                mergerCurrent.set_configpath(_configPath);
                mergerCurrent.SerializeToString(&mergerCurrentStr);
                node->setCurrentStatusStr(mergerCurrentStr, "");
                return true;
            }
            TaskNodePtr taskNode = DYNAMIC_POINTER_CAST(TaskNode, node);
            if (taskNode) {
                TaskCurrent taskCurrent;
                string taskCurrentStr;
                taskCurrent.set_protocolversion(_workerProtocolVersion);
                taskCurrent.set_configpath(_configPath);
                taskCurrent.SerializeToString(&taskCurrentStr);
                node->setCurrentStatusStr(taskCurrentStr, "");
                return true;
            }
            return false;
        }
        int32_t _workerProtocolVersion;
        std::string _configPath;
    };
    WorkerProtocolSetter setter(workerProtocolVersion, configPath);
    workerTable->forEachActiveNode(setter);
}

void GenerationTaskStateMachine::prepareFakeGenerationTask(const std::string& tableName, const std::string& rootDir)
{
    DELETE_AND_SET_NULL(_zkWrapper);
    _zkWrapper = new cm_basic::ZkWrapper();
    BuildId buildId = ProtoCreator::createBuildId(tableName, 1, "app");
    string indexRoot = rootDir;
    _topicDir = rootDir;
    _generationDir = PathDefine::getGenerationZkRoot(rootDir, buildId);
    bool exist = false;
    fslib::util::FileUtil::isExist(_generationDir, exist);
    if (!exist) {
        fslib::util::FileUtil::mkDir(_generationDir, true);
    }

    if (_isJobTask) {
        _generationTask = new FakeJobTask(indexRoot, buildId, _zkWrapper);
    } else {
        _generationTask = new FakeGenerationTask(indexRoot, buildId, _zkWrapper);
    }
}

bool GenerationTaskStateMachine::init(const std::string& tableName, const std::string& configPath,
                                      const std::string& rootDir, DataDescriptions dataDescriptions,
                                      proto::BuildStep buildStep)
{
    _tableName = tableName;
    _rootDir = rootDir;
    prepareFakeGenerationTask(tableName, rootDir);
    assert(!_workerTable); // @init not reentrant
    _workerTable = new WorkerTable(_generationDir, _zkWrapper);
    if (dataDescriptions.size() == 0) {
        DataDescription dataDescription;
        dataDescription[READ_SRC] = "swift";
        dataDescription[READ_SRC_TYPE] = "swift";
        dataDescriptions.push_back(dataDescription);
    }
    bool ret = _generationTask->GenerationTaskBase::loadFromConfig(
        configPath, _generationDir, ToJsonString(dataDescriptions.toVector()), buildStep);
    return ret;
}

void GenerationTaskStateMachine::makeDecision(size_t decisionTimes)
{
    ConfigReaderAccessorPtr configAccessor;
    _generationTask->_resourceManager->getResource(configAccessor);
    ResourceReaderPtr reader = configAccessor->getLatestConfig();
    for (size_t i = 0; i < decisionTimes; i++) {
        _generationTask->makeDecision(*_workerTable);
        setWorkerProtocolVersion(WorkerStateHandler::PROTOCOL_VERSION, reader->getConfigPath());
    }
    if (_autoRestart) {
        cloneTask();
        _workerTable->clearAllNodes();
        for (size_t i = 0; i < decisionTimes; i++) {
            _generationTask->makeDecision(*_workerTable);
            setWorkerProtocolVersion(WorkerStateHandler::PROTOCOL_VERSION, reader->getConfigPath());
        }
    }
}

void GenerationTaskStateMachine::finishTasks(WorkerNodes& nodes, string status)
{
    TaskNodes taskNodes;
    taskNodes.reserve(nodes.size());
    for (auto& node : nodes) {
        TaskNodePtr taskNode = DYNAMIC_POINTER_CAST(TaskNode, node);
        taskNodes.push_back(taskNode);
    }
    finishTasks(taskNodes, status);
    nodes.clear();
    nodes.reserve(taskNodes.size());
    for (auto& node : taskNodes) {
        nodes.push_back(node);
    }
}

void GenerationTaskStateMachine::finishTasks(TaskNodes& tasksNodes, string status)
{
    for (size_t i = 0; i < tasksNodes.size(); i++) {
        auto current = tasksNodes[i]->getCurrentStatus();
        auto target = tasksNodes[i]->getTargetStatus();
        *current.mutable_reachedtarget() = target;
        current.set_statusdescription(status);
        tasksNodes[i]->setCurrentStatus(current);
    }
}

void GenerationTaskStateMachine::finishTasks(const string& taskName, const string& clusterName, const string& status)
{
    auto taskNodes = getWorkerNodes(clusterName, ROLE_TASK, taskName);
    finishTasks(taskNodes, status);
}

bool GenerationTaskStateMachine::waitCreateVersion(const string& clusterName, versionid_t targetVersion,
                                                   int64_t waitTimes, int64_t indexSize)
{
    makeDecision(2);
    for (int64_t i = 0; i < waitTimes; i++) {
        proto::GenerationInfo generationInfo;
        getGenerationInfo(&generationInfo);
        auto indexInfos = generationInfo.indexinfos();
        for (int64_t i = 0; i < indexInfos.size(); i++) {
            if (clusterName == indexInfos.Get(i).clustername() &&
                indexInfos.Get(i).indexversion() == (uint32_t)targetVersion) {
                return true;
            }
        }
        finishBuilder(clusterName, _workerTable);
        finishMerger(clusterName, targetVersion, indexSize);
        makeDecision();
    }
    return false;
}

bool GenerationTaskStateMachine::createVersion(const string& clusterName, const string& mergeConfigName)
{
    sleep(1);
    std::string generationStr = ToJsonString(*_generationTask);
    if (_generationTask->GenerationTaskBase::createVersion(clusterName, mergeConfigName)) {
        return true;
    }
    delete _generationTask;
    prepareFakeGenerationTask(_tableName, _rootDir);
    FromJsonString(*_generationTask, generationStr);
    return false;
}

bool GenerationTaskStateMachine::startTask(int64_t taskId, const string& taskName, const string& taskConfigPath,
                                           const string& clusterName, const string& taskParam, string& errorMsg)
{
    std::string generationStr = ToJsonString(*_generationTask);
    GenerationTaskBase::StartTaskResponse response;
    if (_generationTask->GenerationTaskBase::startTask(taskId, taskName, taskConfigPath, clusterName, taskParam,
                                                       &response)) {
        errorMsg = response.message;
        return true;
    }
    errorMsg = response.message;
    BS_LOG(ERROR, "%s", errorMsg.c_str());
    delete _generationTask;
    prepareFakeGenerationTask(_tableName, _rootDir);
    FromJsonString(*_generationTask, generationStr);
    return false;
}

bool GenerationTaskStateMachine::stopTask(int64_t taskId, std::string& errorMsg)
{
    std::string generationStr = ToJsonString(*_generationTask);
    if (_generationTask->GenerationTaskBase::stopTask(taskId, errorMsg)) {
        return true;
    }
    delete _generationTask;
    prepareFakeGenerationTask(_tableName, _rootDir);
    FromJsonString(*_generationTask, generationStr);
    return false;
}

bool GenerationTaskStateMachine::markCheckpoint(const string& clusterName, versionid_t versionId, bool reserveFlag,
                                                string& errorMsg)
{
    return _generationTask->markCheckpoint(clusterName, versionId, reserveFlag, errorMsg);
}

bool GenerationTaskStateMachine::cleanVersions(const string& clusterName, versionid_t version)
{
    return _generationTask->cleanVersions(clusterName, version);
}

bool GenerationTaskStateMachine::cleanReservedCheckpoints(const string& clusterName, versionid_t version)
{
    return _generationTask->cleanReservedCheckpoints(clusterName, version);
}

bool GenerationTaskStateMachine::switchBuild(bool createFullVersion, versionid_t fullVersion, int64_t indexSize,
                                             bool finishProcessor)
{
    std::string generationStr = ToJsonString(*_generationTask);
    if (!_generationTask->switchBuild()) {
        delete _generationTask;
        prepareFakeGenerationTask(_tableName, _rootDir);
        FromJsonString(*_generationTask, generationStr);
        return false;
    }
    if (finishProcessor) {
        if (!switchProcessorToInc()) {
            return false;
        }
    }

    if (!createFullVersion) {
        return true;
    }

    if (!finishProcessor) {
        return false;
    }
    GenerationInfo generationInfo;
    getGenerationInfo(&generationInfo);
    auto clusterInfos = generationInfo.buildinfo().clusterinfos();
    for (int64_t i = 0; i < clusterInfos.size(); i++) {
        waitCreateVersion(clusterInfos.Get(i).clustername(), fullVersion, 100, indexSize);
    }
    return true;
}

void GenerationTaskStateMachine::getGenerationInfo(proto::GenerationInfo* generationInfo, bool fillEffectiveIndexInfo)
{
    CounterCollector::RoleCounterMap counterMap;
    _generationTask->fillGenerationInfo(generationInfo, _workerTable, false, true, fillEffectiveIndexInfo, counterMap);
}

void GenerationTaskStateMachine::getTaskInfo(int64_t taskId, proto::TaskInfo& taskInfo, bool& exist)
{
    _generationTask->fillTaskInfo(taskId, *_workerTable, &taskInfo, exist);
}

bool GenerationTaskStateMachine::suspendBuild(std::string& errorMsg) { return suspendBuild("", errorMsg); }

bool GenerationTaskStateMachine::suspendBuild(const string& clusterName, string& errorMsg)
{
    WorkerNodes nodes = _workerTable->getActiveNodes();
    for (auto& node : nodes) {
        std::string currentStr;
        if (node->getRoleType() == ROLE_PROCESSOR) {
            ProcessorNode* workerNode = (ProcessorNode*)node.get();
            auto status = workerNode->getCurrentStatus();
            status.set_protocolversion(1);
            status.set_configpath(_generationTask->getConfigPath());
            status.SerializeToString(&currentStr);
        } else if (node->getRoleType() == ROLE_BUILDER) {
            BuilderNode* workerNode = (BuilderNode*)node.get();
            auto status = workerNode->getCurrentStatus();
            status.set_protocolversion(1);
            status.set_configpath(_generationTask->getConfigPath());
            status.SerializeToString(&currentStr);
        } else if (node->getRoleType() == ROLE_MERGER) {
            MergerNode* workerNode = (MergerNode*)node.get();
            auto status = workerNode->getCurrentStatus();
            status.set_protocolversion(1);
            status.set_configpath(_generationTask->getConfigPath());
            status.SerializeToString(&currentStr);
        } else if (node->getRoleType() == ROLE_TASK) {
            TaskNode* taskNode = (TaskNode*)node.get();
            auto status = taskNode->getCurrentStatus();
            status.set_protocolversion(1);
            status.set_configpath(_generationTask->getConfigPath());
            status.SerializeToString(&currentStr);
        }
        node->setCurrentStatusStr(currentStr, "");
    }
    makeDecision();
    return _generationTask->suspendBuild(clusterName, errorMsg);
}

bool GenerationTaskStateMachine::checkTaskZkStatus(const std::string& flowTag, const std::string& taskId,
                                                   const std::string& nodePath, const std::string& result)
{
    TaskBasePtr task = getTask(flowTag, taskId);
    if (!task) {
        return false;
    }
    string jsonStr = ToJsonString(*(task.get()));

    string value;
    if (!innerGetZkStatus(jsonStr, nodePath, value)) {
        return false;
    }
    return innerCheckStatusEqual(value, result);
}

bool GenerationTaskStateMachine::resumeBuild(const string& clusterName, string& errorMsg)
{
    makeDecision();
    return _generationTask->resumeBuild(clusterName, errorMsg);
}

bool GenerationTaskStateMachine::rollBack(const std::string& clusterName, versionid_t targetVersion,
                                          std::string& errorMsg, int64_t startTimestamp)
{
    return _generationTask->rollBack(clusterName, _generationDir, targetVersion, startTimestamp, errorMsg);
}

bool GenerationTaskStateMachine::rollBackCheckpoint(const std::string& clusterName, checkpointid_t checkpointId,
                                                    std::string& errorMsg, int64_t startTimestamp)
{
    return _generationTask->rollBackCheckpoint(clusterName, _generationDir, checkpointId, {}, errorMsg);
}

WorkerNodes GenerationTaskStateMachine::getWorkerNodes(const string& clusterName, proto::RoleType roleType,
                                                       string taskId)
{
    return getWorkerNodes(clusterName, roleType, _workerTable, taskId);
}

WorkerNodes GenerationTaskStateMachine::getWorkerNodes(const string& clusterName, proto::RoleType roleType,
                                                       WorkerTable* workerTable, string taskId)
{
    WorkerNodes workerNodes;
    WorkerNodes nodes = workerTable->getActiveNodes();
    for (auto& node : nodes) {
        if (node->getRoleType() == roleType) {
            if (roleType == ROLE_PROCESSOR) {
                if (taskId.empty()) {
                    workerNodes.push_back(node);
                    continue;
                }
                ProcessorTaskIdentifier identifier;
                identifier.fromString(node->getPartitionId().taskid());
                if (identifier.getTaskId() == taskId || (identifier.getTaskId() == "" && taskId == "0")) {
                    workerNodes.push_back(node);
                    continue;
                }
                continue;
            }
            if (node->getPartitionId().clusternames(0) == clusterName) {
                if (taskId.empty()) {
                    workerNodes.push_back(node);
                    continue;
                }
                TaskIdentifier identifier;
                identifier.fromString(node->getPartitionId().taskid());
                if (identifier.getTaskId() == taskId) {
                    workerNodes.push_back(node);
                    continue;
                }
                continue;
            }
        }
    }
    return workerNodes;
}

bool GenerationTaskStateMachine::switchProcessorToInc()
{
    for (int64_t i = 0; i < 100; ++i) {
        finishProcessor(_workerTable);
        makeDecision();
        GenerationInfo generationInfo;
        getGenerationInfo(&generationInfo);
        if (generationInfo.buildstep() == "incremental") {
            return true;
        }
    }
    return false;
}

void GenerationTaskStateMachine::suspendProcessor()
{
    auto activeWorkers = _workerTable->getActiveNodes();
    for (auto& node : activeWorkers) {
        if (node->getRoleType() == ROLE_PROCESSOR) {
            ProcessorNode* processorNode = (ProcessorNode*)node.get();
            auto target = processorNode->getTargetStatus();
            if (target.suspendtask()) {
                auto current = processorNode->getCurrentStatus();
                auto target = processorNode->getTargetStatus();
                target.set_suspendtask(true);
                current.set_issuspended(true);
                *current.mutable_targetstatus() = target;
                processorNode->setTargetStatus(target);
                processorNode->setCurrentStatus(current);
            }
        }
    }
}

void GenerationTaskStateMachine::suspendBuilder(const string& clusterName)
{
    auto activeWorkers = _workerTable->getActiveNodes();
    for (auto& node : activeWorkers) {
        if (node->getRoleType() == ROLE_BUILDER && node->getPartitionId().clusternames(0) == clusterName) {
            BuilderNode* builderNode = (BuilderNode*)node.get();
            auto current = builderNode->getCurrentStatus();
            auto target = builderNode->getTargetStatus();
            target.set_suspendtask(true);
            current.set_issuspended(true);
            *current.mutable_targetstatus() = target;
            builderNode->setTargetStatus(target);
            builderNode->setCurrentStatus(current);
        }
    }
}

void GenerationTaskStateMachine::suspendMerger(const string& clusterName)
{
    auto activeWorkers = _workerTable->getActiveNodes();
    for (auto& node : activeWorkers) {
        if (node->getRoleType() == ROLE_MERGER && node->getPartitionId().clusternames(0) == clusterName) {
            MergerNode* mergerNode = (MergerNode*)node.get();
            auto current = mergerNode->getCurrentStatus();
            auto target = mergerNode->getTargetStatus();
            target.set_suspendtask(true);
            current.set_issuspended(true);
            *current.mutable_targetstatus() = target;
            mergerNode->setTargetStatus(target);
            mergerNode->setCurrentStatus(current);
        }
    }
}

void GenerationTaskStateMachine::suspendTask(const std::string& clusterName, const std::string& taskName)
{
    auto activeWorkers = _workerTable->getActiveNodes();
    for (auto& node : activeWorkers) {
        if (node->getRoleType() == ROLE_TASK && node->getPartitionId().clusternames(0) == clusterName) {
            string taskId = node->getPartitionId().taskid();
            TaskIdentifier identifier;
            identifier.fromString(taskId);
            string innerTaskName;
            identifier.getTaskName(innerTaskName);
            if (innerTaskName == taskName) {
                TaskNode* taskNode = (TaskNode*)node.get();
                auto current = taskNode->getCurrentStatus();
                current.set_issuspended(true);
                taskNode->setCurrentStatus(current);
            }
        }
    }
}

ProcessorNodes GenerationTaskStateMachine::getProcessorNodes(WorkerTable* workerTable, const std::string& processorId)
{
    ProcessorNodes processorNodes;
    WorkerNodes workerNodes = workerTable->getActiveNodes();
    for (size_t i = 0; i < workerNodes.size(); i++) {
        if (workerNodes[i]->getRoleType() == ROLE_PROCESSOR &&
            workerNodes[i]->getPartitionId().taskid() == processorId) {
            processorNodes.push_back(DYNAMIC_POINTER_CAST(ProcessorNode, workerNodes[i]));
        }
    }
    return processorNodes;
}

TaskNodes GenerationTaskStateMachine::getTaskNodes(WorkerTable* workerTable, const string& taskId, string clusterName)
{
    TaskNodes nodes;
    WorkerNodes workerNodes = getWorkerNodes(clusterName, ROLE_TASK, workerTable, taskId);
    nodes.reserve(workerNodes.size());
    for (size_t i = 0; i < workerNodes.size(); i++) {
        nodes.push_back(DYNAMIC_POINTER_CAST(TaskNode, workerNodes[i]));
    }
    return nodes;
}

TaskNodes GenerationTaskStateMachine::getTaskNodes(const string& taskId, string clusterName)
{
    return getTaskNodes(_workerTable, taskId, clusterName);
}

BuilderNodes GenerationTaskStateMachine::getBuilderNodes(WorkerTable* workerTable, std::string clusterName)
{
    BuilderNodes builderNodes;
    WorkerNodes workerNodes = workerTable->getActiveNodes();
    for (size_t i = 0; i < workerNodes.size(); i++) {
        if (workerNodes[i]->getRoleType() != proto::RoleType::ROLE_BUILDER) {
            continue;
        }
        if (!clusterName.empty() && workerNodes[i]->getPartitionId().clusternames(0) != clusterName) {
            continue;
        }
        if (workerNodes[i]->getRoleType() == ROLE_BUILDER) {
            builderNodes.push_back(DYNAMIC_POINTER_CAST(BuilderNode, workerNodes[i]));
        }
    }
    return builderNodes;
}

MergerNodes GenerationTaskStateMachine::getMergerNodes(WorkerTable* workerTable, std::string clusterName)
{
    MergerNodes mergerNodes;
    WorkerNodes workerNodes = workerTable->getActiveNodes();
    for (size_t i = 0; i < workerNodes.size(); i++) {
        if (workerNodes[i]->getRoleType() != proto::RoleType::ROLE_MERGER) {
            continue;
        }
        if (!clusterName.empty() && workerNodes[i]->getPartitionId().clusternames(0) != clusterName) {
            continue;
        }
        if (workerNodes[i]->getRoleType() == ROLE_MERGER) {
            mergerNodes.push_back(DYNAMIC_POINTER_CAST(MergerNode, workerNodes[i]));
        }
    }
    return mergerNodes;
}

void GenerationTaskStateMachine::finishProcessor(WorkerTable* workerTable)
{
    WorkerNodes workerNodes = workerTable->getActiveNodes();
    for (size_t i = 0; i < workerNodes.size(); i++) {
        if (workerNodes[i]->getRoleType() == ROLE_PROCESSOR) {
            workerNodes[i]->setFinished(true);
        }
    }
}

void GenerationTaskStateMachine::finishProcessor()
{
    WorkerNodes workerNodes = _workerTable->getActiveNodes();
    for (size_t i = 0; i < workerNodes.size(); i++) {
        if (workerNodes[i]->getRoleType() == ROLE_PROCESSOR) {
            workerNodes[i]->setFinished(true);
        }
    }
}

void GenerationTaskStateMachine::finishBuilder(const string& clusterName, WorkerTable* workerTable,
                                               const string& status, bool needStopEndBuilder)
{
    string newStatus = status;
    if (status.empty()) {
        // compatity for old UT
        KeyValueMap taskStatus;
        taskStatus[BS_ENDBUILD_VERSION] = "1";
        newStatus = ToJsonString(taskStatus);
    }
    auto activeWorkers = workerTable->getActiveNodes();
    for (auto& node : activeWorkers) {
        if (node->getRoleType() == ROLE_BUILDER) {
            if (node->getPartitionId().clusternames(0) == clusterName) {
                node->setFinished(true);
            }
        }
    }
    auto iter = workerTable->getWorkerNodesMap().begin();
    while (iter != workerTable->getWorkerNodesMap().end()) {
        if (iter->first.find("incBuilder") == string::npos) {
            iter++;
            continue;
        }
        WorkerNodes nodes = iter->second;
        TaskNodes taskNodes;
        for (auto& node : nodes) {
            if (node->getPartitionId().clusternames(0) != clusterName) {
                continue;
            }
            TaskNodePtr tNode = DYNAMIC_POINTER_CAST(TaskNode, node);
            if (!tNode) {
                continue;
            }
            taskNodes.push_back(tNode);
        }
        if (needStopEndBuilder) {
            finishTasks(taskNodes, newStatus);
        }
        iter++;
    }
}

void GenerationTaskStateMachine::finishJobBuilder()
{
    auto activeWorkers = _workerTable->getActiveNodes();
    for (auto& node : activeWorkers) {
        if (node->getRoleType() == ROLE_JOB) {
            node->setFinished(true);
        }
    }
}

void GenerationTaskStateMachine::finishBuilder(const string& clusterName, const string& status, bool needStopEndBuilder)
{
    finishBuilder(clusterName, _workerTable, status, needStopEndBuilder);
}

void GenerationTaskStateMachine::finishMerger(const string& clusterName, versionid_t targetVersion, int64_t indexSize,
                                              string taskId)
{
    auto activeWorkers = _workerTable->getActiveNodes();
    for (auto& workerNode : activeWorkers) {
        if (workerNode->getRoleType() == ROLE_MERGER) {
            if (workerNode->getPartitionId().clusternames(0) == clusterName) {
                if (taskId != "") {
                    TaskIdentifier identifier;
                    identifier.fromString(workerNode->getPartitionId().taskid());
                    if (identifier.getTaskId() != taskId) {
                        continue;
                    }
                } else {
                    if (workerNode->getPartitionId().taskid() != "") {
                        continue;
                    }
                }
                workerNode->setFinished(true);
                MergerNode* node = (MergerNode*)workerNode.get();
                MergerCurrent current = node->getCurrentStatus();
                string currentStr;
                auto builderCkpAccessor =
                    CheckpointCreator::createBuilderCheckpointAccessor(_generationTask->_resourceManager);
                BuilderCheckpoint builderCheckpoint;
                builderCkpAccessor->getLatestCheckpoint(clusterName, builderCheckpoint);
                current.set_targetversionid(targetVersion);
                current.mutable_indexinfo()->set_clustername(clusterName);
                current.mutable_indexinfo()->set_indexversion(targetVersion);
                current.mutable_indexinfo()->set_totalremainindexsize(indexSize);
                current.mutable_indexinfo()->set_versiontimestamp(builderCheckpoint.versiontimestamp());
                current.SerializeToString(&currentStr);
                node->setCurrentStatusStr(currentStr, "");
                node->setFinished(true);
            }
        }
    }
}

bool GenerationTaskStateMachine::getZkStatus(const string& nodePath, string& result)
{
    string content = ToJsonString(*_generationTask);
    return innerGetZkStatus(content, nodePath, result);
}

bool GenerationTaskStateMachine::checkZkStatus(const string& nodePath, const string& result)
{
    string actual;
    if (!getZkStatus(nodePath, actual)) {
        return false;
    }
    return innerCheckStatusEqual(actual, result);
}

bool GenerationTaskStateMachine::createVersion(const string& clusterName, const string& mergeConfigName,
                                               versionid_t startVersion, versionid_t endVersion)
{
    for (versionid_t i = startVersion; i <= endVersion; i++) {
        if (!createVersion(clusterName, mergeConfigName)) {
            return false;
        }
        if (!waitCreateVersion(clusterName, i)) {
            return false;
        }
        makeDecision(2);
    }
    return true;
}

TaskFlowPtr GenerationTaskStateMachine::getFlow(const string& flowId) { return _generationTask->getFlow(flowId); }

bool GenerationTaskStateMachine::waitAlterField(const std::string& clusterName, int64_t rollBackVersion,
                                                int64_t alterFieldVersion)
{
    suspendBuilder(clusterName);
    suspendMerger(clusterName);
    suspendTask(clusterName, "drop_building_index");
    makeDecision(5);
    auto builderNodes = getWorkerNodes(clusterName, ROLE_BUILDER);
    if (builderNodes.size() > 0) {
        return false;
    }
    auto mergerNodes = getWorkerNodes(clusterName, ROLE_MERGER);
    if (mergerNodes.size() > 0) {
        return false;
    }
    auto taskNodes = getWorkerNodes(clusterName, ROLE_TASK);
    if (taskNodes.size() != 1) {
        return false;
    }
    map<string, string> taskStatus;
    taskStatus[BS_ROLLBACK_TARGET_VERSION] = StringUtil::toString(rollBackVersion);
    finishTasks(taskNodes, ToJsonString(taskStatus));
    makeDecision(5);
    taskNodes = getWorkerNodes(clusterName, ROLE_TASK);
    if (taskNodes.size() > 0) {
        return false;
    }
    return waitCreateVersion(clusterName, alterFieldVersion);
}

// CheckpointAccessorPtr GenerationTaskStateMachine::getCheckpointAccessor()
// {
//     CheckpointAccessorPtr checkpointAccessor;
//     _generationTask->_resourceManager->getResource(checkpointAccessor);
//     return checkpointAccessor;
// }

void GenerationTaskStateMachine::finishTask(const string& taskId, WorkerTable* workerTable, string status)
{
    auto& workerNodes = workerTable->getWorkerNodes(taskId);
    TaskNodes taskNodes;
    taskNodes.reserve(workerNodes.size());
    for (const auto& workerNode : workerNodes) {
        taskNodes.push_back(DYNAMIC_POINTER_CAST(TaskNode, workerNode));
    }
    finishTasks(taskNodes, status);
}

bool GenerationTaskStateMachine::innerGetZkStatus(const string& content, const string& nodePath, string& result)
{
    if (nodePath.empty()) {
        result = content;
        return true;
    }
    return ResourceReader::getJsonString(content, nodePath, result);
}

bool GenerationTaskStateMachine::innerCheckStatusEqual(const string& actual, const string& result)
{
    string ret;
    // StringUtil::replace(actual, '\\/', '/');
    for (size_t i = 0; i < actual.length(); i++) {
        if (actual[i] == '\\' || actual[i] == '\"') {
            continue;
        }
        ret += actual[i];
    }
    if (ret != result) {
        cout << "expected: [" << result.length() << "]" << result << ", actual get [" << ret.length() << "]" << ret
             << endl;
        return false;
    }
    return true;
}

TaskFlowPtr GenerationTaskStateMachine::getFlowByTag(const string& flowTag)
{
    TaskFlowManagerPtr flowMgr = _generationTask->_taskFlowManager;
    std::vector<std::string> flowIds;
    flowMgr->getFlowIdByTag(flowTag, flowIds);
    if (flowIds.size() != 1) {
        return TaskFlowPtr();
    }
    return flowMgr->getFlow(flowIds[0]);
}

TaskBasePtr GenerationTaskStateMachine::getTask(const string& flowTag, const string& taskId)
{
    TaskFlowManagerPtr flowMgr = _generationTask->_taskFlowManager;
    std::vector<std::string> tempFlowIds;
    std::vector<std::string> flowIds;
    flowMgr->getFlowIdByTag(flowTag, tempFlowIds);
    for (auto& tempFlowId : tempFlowIds) {
        if (flowMgr->getFlow(tempFlowId)->getStatus() == TaskFlow::TF_STATUS::tf_finish ||
            flowMgr->getFlow(tempFlowId)->getStatus() == TaskFlow::TF_STATUS::tf_stopped) {
            continue;
        }
        flowIds.push_back(tempFlowId);
    }
    if (flowIds.size() != 1) {
        return TaskBasePtr();
    }
    auto flow = flowMgr->getFlow(flowIds[0]);
    if (!flow) {
        return TaskBasePtr();
    }
    return flow->getTask(taskId);
}

bool GenerationTaskStateMachine::callGraph(const string& graphName, const string& graphFileName,
                                           const KeyValueMap& graphKVMap)
{
    KeyValueMap kvMap = graphKVMap;
    kvMap["buildId"] = ProtoUtil::buildIdToStr(_generationTask->_buildId);
    return _generationTask->callGraph(graphName, graphFileName, kvMap);
}

bool GenerationTaskStateMachine::getIndexInfo(bool isFull, const std::string& clusterName,
                                              ::google::protobuf::RepeatedPtrField<proto::IndexInfo>& indexInfos)
{
    IndexCheckpointAccessorPtr checkpointAccessor =
        CheckpointCreator::createIndexCheckpointAccessor(_generationTask->_resourceManager);
    return checkpointAccessor->getIndexInfo(isFull, clusterName, indexInfos);
}

std::set<versionid_t> GenerationTaskStateMachine::getReservedIndexCheckpoint(const std::string& clusterName)
{
    IndexCheckpointAccessorPtr checkpointAccessor =
        CheckpointCreator::createIndexCheckpointAccessor(_generationTask->_resourceManager);
    return checkpointAccessor->getReservedVersions(clusterName);
}

bool GenerationTaskStateMachine::getCheckpointInfos(const std::string& clusterName, versionid_t version,
                                                    ::google::protobuf::RepeatedPtrField<CheckpointInfo>& checkpoints)
{
    checkpoints.Clear();
    proto::GenerationInfo generationInfo;
    getGenerationInfo(&generationInfo);
    bool ret = false;
    for (auto clusterInfo : generationInfo.buildinfo().clusterinfos()) {
        if (clusterInfo.clustername() == clusterName) {
            for (auto checkpoint : clusterInfo.checkpointinfos()) {
                if (checkpoint.versionid() == version) {
                    *checkpoints.Add() = checkpoint;
                    ret = true;
                }
            }
        }
    }
    return ret;
}
proto::BuildId GenerationTaskStateMachine::getBuildId() { return _generationTask->_buildId; }

void GenerationTaskStateMachine::getCheckpointInfos(const std::string& ckpId, vector<pair<string, string>>& checkpoints)
{
    CheckpointAccessorPtr checkpointAccessor;
    _generationTask->_resourceManager->getResource(checkpointAccessor);
    checkpointAccessor->listCheckpoints(ckpId, checkpoints);
}

void GenerationTaskStateMachine::addBuilderCheckpoint(const string& clusterName,
                                                      const proto::BuilderCheckpoint& builderCheckpoint)
{
    BuilderCheckpointAccessorPtr checkpointAccessor =
        CheckpointCreator::createBuilderCheckpointAccessor(_generationTask->_resourceManager);
    checkpointAccessor->addCheckpoint(clusterName, builderCheckpoint);
}

}} // namespace build_service::admin
