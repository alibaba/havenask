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
#include <ext/alloc_traits.h>
#include <map>
#include <memory>
#include <stdint.h>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "alog/Logger.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/jsonizable.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "build_service/admin/CounterCollector.h"
#include "build_service/admin/SlowNodeDetector.h"
#include "build_service/admin/SlowNodeHandleStrategy.h"
#include "build_service/admin/SlowNodeMetricReporter.h"
#include "build_service/admin/controlflow/TaskResourceManager.h"
#include "build_service/admin/taskcontroller/NodeStatusManager.h"
#include "build_service/common/BeeperCollectorDefine.h"
#include "build_service/common/ResourceContainer.h"
#include "build_service/common_define.h"
#include "build_service/config/ConfigReaderAccessor.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/config/SlowNodeDetectConfig.h"
#include "build_service/proto/Admin.pb.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/proto/ErrorCollector.h"
#include "build_service/proto/ProtoUtil.h"
#include "build_service/proto/RoleNameGenerator.h"
#include "build_service/proto/WorkerNode.h"
#include "build_service/proto/WorkerNodeCreator.h"

namespace build_service { namespace admin {

class AdminTaskBase : public autil::legacy::Jsonizable, public proto::ErrorCollector
{
public:
    enum TaskStatus { TASK_NORMAL = 0, TASK_SUSPENDING, TASK_SUSPENDED, TASK_STOPPED, TASK_FINISHED };

public:
    AdminTaskBase(const proto::BuildId& buildId, const TaskResourceManagerPtr& resMgr);
    virtual ~AdminTaskBase() {}

protected:
    AdminTaskBase(); // for jsonize

private:
    AdminTaskBase& operator=(const AdminTaskBase&);

protected:
    virtual bool loadFromConfig(const config::ResourceReaderPtr& resourceReader)
    {
        assert(false);
        return false;
    };

public:
    virtual const std::map<std::string, std::string>& GetPropertyMap() const { return _propertyMap; }

    virtual void SetProperty(const std::string& key, const std::string& value) { _propertyMap[key] = value; }

    virtual bool GetProperty(const std::string& key, std::string& value) const
    {
        auto it = _propertyMap.find(key);
        if (it == _propertyMap.end()) {
            return false;
        }
        value = it->second;
        return true;
    }

    virtual void SetPropertyMap(const std::map<std::string, std::string>& propertyMap) { _propertyMap = propertyMap; }

    std::string getTaskStatusString() const
    {
        switch (_taskStatus) {
        case TASK_NORMAL:
            return "normal";
        case TASK_SUSPENDING:
            return "suspending";
        case TASK_SUSPENDED:
            return "suspended";
        case TASK_STOPPED:
            return "stopped";
        case TASK_FINISHED:
            return "finished";
        default:
            return "unknown";
        }
    }
    void supplementLableInfo(KeyValueMap& info) const
    {
        info["taskStatus"] = getTaskStatusString();
        doSupplementLableInfo(info);
    }

    virtual std::string getTaskPhaseIdentifier() const { return std::string("unknown"); }

    virtual bool getTaskRunningTime(int64_t& intervalInMicroSec) const
    {
        if (_taskStatus != TASK_NORMAL) {
            return false;
        }
        if (_nodesStartTimestamp < 0) {
            return false;
        }
        intervalInMicroSec = autil::TimeUtility::currentTimeInMicroSeconds() - _nodesStartTimestamp;
        return true;
    }

    virtual void notifyStopped() { _taskStatus = TASK_STOPPED; }

    virtual TaskStatus getTaskStatus() { return _taskStatus; }

    TaskResourceManagerPtr getTaskResourceManager() { return _resourceManager; }

    virtual bool finish(const KeyValueMap& kvMap) = 0;

    virtual bool start(const KeyValueMap& kvMap)
    {
        if (!updateConfig()) {
            BS_LOG(ERROR, "update config failed");
            return false;
        }
        _taskStatus = TASK_NORMAL;
        return true;
    }
    const std::string& getConfigPath() const { return _configPath; }

    virtual config::ResourceReaderPtr getConfigReader() const
    {
        config::ConfigReaderAccessorPtr readerAccessor;
        _resourceManager->getResource(readerAccessor);
        auto reader = readerAccessor->getConfig(_configPath);
        if (!reader) {
            reader.reset(new config::ResourceReader(_configPath));
            reader->init();
        }
        return reader;
    }

    virtual void waitSuspended(proto::WorkerNodes& workerNodes) = 0;
    virtual void doSupplementLableInfo(KeyValueMap& info) const {}
    virtual bool suspendTask(bool forceSuspend)
    {
        if (forceSuspend) {
            _taskStatus = TASK_SUSPENDED;
            return true;
        }
        if (_taskStatus == TASK_SUSPENDED) {
            return false;
        }
        if (!_hasCreateNodes) {
            _taskStatus = TASK_SUSPENDED;
            return true;
        }
        _taskStatus = TASK_SUSPENDING;
        return true;
    }
    virtual bool updateConfig()
    {
        assert(false);
        return false;
    }
    virtual bool run(proto::WorkerNodes& workerNodes)
    {
        assert(false);
        return false;
    }
    virtual bool resumeTask()
    {
        if (_taskStatus == TASK_SUSPENDING) {
            return false;
        }
        _taskStatus = TASK_NORMAL;
        return true;
    }

    virtual bool isTaskSuspended() const { return _taskStatus == TASK_SUSPENDED; }

    template <typename Node>
    inline static bool isSuspended(Node& workerNode);

    template <typename RoleInfoType, typename NodesType>
    static void fillPartitionInfos(RoleInfoType* roleInfo, const NodesType& nodes,
                                   const CounterCollector::RoleCounterMap& roleCounterMap);

    // check all role group finished, and set finished to worker node if need
    template <typename Nodes>
    static bool checkAndSetFinished(const taskcontroller::NodeStatusManagerPtr& nodeStatusManager, Nodes& workerNodes,
                                    SlowNodeHandleStrategy::Metric* metric = nullptr);

    bool getSlowNodeDetectConfig(const config::ResourceReaderPtr& resourceReader, const std::string& clusterName,
                                 config::SlowNodeDetectConfig& slowNodeDetectConfig);

    bool initSlowNodeDetect(const config::ResourceReaderPtr& resourceReader, const std::string& clusterName = "");

    proto::BuildId getBuildId() const { return _buildId; }

    static std::string getDateFormatString(int64_t timeInSecond);

    bool hasCreateNodes() const { return _hasCreateNodes; }

    virtual void setBeeperTags(const beeper::EventTagsPtr beeperTags);

private:
    void deleteProperty(const std::string& key) { _propertyMap.erase(key); }
    void ReportSlowNodeMetrics(const std::string& clusterName, const SlowNodeHandleStrategy::Metric& metric);

protected:
    void saveBackupInfo()
    {
        std::string backupInfo = _nodeStatusManager->SerializeBackInfo();
        if (backupInfo != "{}") {
            SetProperty("backup_node_info", _nodeStatusManager->SerializeBackInfo());
        } else {
            deleteProperty("backup_node_info");
        }
    }

    void clearBackupInfo()
    {
        std::string info;
        if (GetProperty("backup_node_info", info)) {
            SetProperty("backup_node_info", "");
            BS_LOG(INFO, "clear backup info [%s].", info.c_str());
        }
    }

    template <typename Nodes>
    void recoverFromBackInfo(Nodes& activeNodes);

    // before call detectSlowNodes, you should call nodeStatusManager->Update() to sync nodeStatus
    template <typename Nodes>
    bool detectSlowNodes(Nodes& nodes, const std::string& clusterName = "", bool simpleHandle = false);

    void doClearFullWorkerZkNode(const std::string& generationDir, const proto::RoleType roleType,
                                 const std::string& clusterName = "") const;

    void intervalBeeperReport(const std::string& beeperId, std::string format, ...);

    virtual bool prepareSlowNodeDetector(const std::string& clusterName) { return false; }

    template <typename Nodes>
    bool doPrepareSlowNodeDetector(const std::string& clusterName);

protected:
    // to create nodes
    proto::BuildId _buildId;
    SlowNodeDetectorPtr _slowNodeDetector;
    bool _slowNodeDetect;
    int64_t _nodesStartTimestamp;
    TaskStatus _taskStatus;
    TaskResourceManagerPtr _resourceManager;
    std::shared_ptr<taskcontroller::NodeStatusManager> _nodeStatusManager;
    std::string _configPath;
    bool _hasCreateNodes;
    beeper::EventTagsPtr _beeperTags;
    volatile int64_t _beeperReportTs;
    SlowNodeHandleStrategy::Metric _reportedSlowNodeMetric;
    std::map<std::string, std::string> _propertyMap;

private:
    int64_t _beeperReportInterval; // second
    static int64_t DEFAULT_BEEPER_REPORT_INTERVAL;

    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(AdminTaskBase);

template <typename RoleInfoType, typename NodesType>
void AdminTaskBase::fillPartitionInfos(RoleInfoType* roleInfo, const NodesType& nodes,
                                       const CounterCollector::RoleCounterMap& roleCounterMap)
{
    for (const auto& node : nodes) {
        auto partitionInfo = node->getPartitionInfo();
        bool ignoreBackupId = node->getRoleType() == proto::ROLE_PROCESSOR ? true : false;
        std::string counterFileName;
        if (proto::ProtoUtil::partitionIdToCounterFileName(partitionInfo.pid(), counterFileName, ignoreBackupId)) {
            auto it = roleCounterMap.find(counterFileName);
            if (it != roleCounterMap.end()) {
                partitionInfo.mutable_counteritems()->CopyFrom(it->second.counteritem());
            }
        }
        *roleInfo->add_partitioninfos() = partitionInfo;
    }
}

template <typename Nodes>
void AdminTaskBase::recoverFromBackInfo(Nodes& activeNodes)
{
    std::string backupInfoStr;
    bool hasBackupNode = GetProperty("backup_node_info", backupInfoStr);
    if (!hasBackupNode) {
        return;
    }
    const auto backInfo = _nodeStatusManager->DeserializeBackInfo(backupInfoStr);
    Nodes tempNodes;
    for (const auto& node : activeNodes) {
        std::string roleGroupId;
        proto::ProtoUtil::partitionIdToRoleGroupId(node->getPartitionId(), roleGroupId);
        auto iter = backInfo.find(roleGroupId);
        bool isAppendNode = true;
        if (iter != backInfo.end()) {
            for (const auto& backupId : iter->second) {
                if (_slowNodeDetect && _slowNodeDetector) {
                    ++(_slowNodeDetector->getMetric().slowNodeBackupCreateTimes);
                }
                auto backupNode = proto::WorkerNodeCreator<
                    proto::WorkerNodes2WorkerTypeTraits<Nodes>::WorkerRoleType>::createBackupNode(node, backupId);
                tempNodes.emplace_back(backupNode);
            }
            if (node->getPartitionId().role() == proto::ROLE_PROCESSOR) {
                // if a processorNodes has backup node, main node has been stopped already.
                isAppendNode = false;
            }
        }
        if (isAppendNode) {
            tempNodes.emplace_back(node);
        }
    }
    activeNodes.swap(tempNodes);
    BS_LOG(INFO, "recoverFromBackInfo:%s", backupInfoStr.c_str());
}

// return true means slowNodeDetector exist
template <typename Nodes>
bool AdminTaskBase::detectSlowNodes(Nodes& nodes, const std::string& clusterName, bool simpleHandle)
{
    if (!_slowNodeDetect) {
        return false;
    }
    if (!_slowNodeDetector) {
        prepareSlowNodeDetector(clusterName);
    }
    if (_slowNodeDetector) {
        _nodeStatusManager->Update(nodes);
        SlowNodeMetricReporterPtr reporter;
        _resourceManager->getResource(reporter);
        _slowNodeDetector->detectAndHandleSlowNodes<Nodes>(
            reporter, _nodeStatusManager, &nodes, autil::TimeUtility::currentTimeInMicroSeconds(), simpleHandle);
        const SlowNodeHandleStrategy::Metric& metric = _slowNodeDetector->getMetric();
        ReportSlowNodeMetrics(clusterName, metric);
        if (_nodeStatusManager->GetAllNodes().empty() || metric == _reportedSlowNodeMetric) {
            return true;
        }
        _reportedSlowNodeMetric = metric;
        intervalBeeperReport(GENERATION_STATUS_COLLECTOR_NAME,
                             "cluster [%s] role [%s] slow node: slowNodeKillTimes[%ld], deadNodeKillTimes[%ld], "
                             "restartNodeKillTimes[%ld], slowNodeBackupCreateTimes[%ld], "
                             "effectiveBackupNodeCount[%ld], effectiveBackupNodeRate[%lf]",
                             clusterName.c_str(), _nodeStatusManager->GetAllNodes()[0]->roleName.c_str(),
                             metric.slowNodeKillTimes, metric.deadNodeKillTimes, metric.restartNodeKillTimes,
                             metric.slowNodeBackupCreateTimes, metric.effectiveBackupNodeCount,
                             metric.effectiveBackupNodeRate());
        return true; // run a slow node detect loop
    }
    return false; // no slowNodeDetector, don't run detect loop
}

template <typename Nodes>
bool AdminTaskBase::doPrepareSlowNodeDetector(const std::string& clusterName)
{
    // if clusterName is empty, use global config, otherwise use cluster config
    auto resourceReader = getConfigReader();
    config::SlowNodeDetectConfig slowNodeDetectConfig;
    if (!getSlowNodeDetectConfig(resourceReader, clusterName, slowNodeDetectConfig)) {
        BS_LOG(ERROR, "get slowNodeDetectConfig from %s failed", resourceReader->getOriginalConfigPath().c_str());
        return false;
    }
    _slowNodeDetect = slowNodeDetectConfig.enableSlowDetect;
    if (_slowNodeDetect) {
        auto detector = std::make_shared<SlowNodeDetector>();
        if (detector->Init<Nodes>(slowNodeDetectConfig, _nodesStartTimestamp)) {
            _slowNodeDetector = detector;
            BS_LOG(INFO, "init slowNodeDetectConfig[%s] for cluster[%s]", ToJsonString(slowNodeDetectConfig).c_str(),
                   clusterName.c_str());
        }
    }
    return true;
}

template <typename Node>
inline bool AdminTaskBase::isSuspended(Node& workerNode)
{
    const auto& current = workerNode->getCurrentStatus();
    const auto& target = workerNode->getTargetStatus();
    if (!(target.has_suspendtask() && target.suspendtask() && current.has_issuspended() && current.issuspended())) {
        return false;
    }
    return true;
}

template <typename Nodes>
bool AdminTaskBase::checkAndSetFinished(const taskcontroller::NodeStatusManagerPtr& nodeStatusManager,
                                        Nodes& workerNodes, SlowNodeHandleStrategy::Metric* metric)
{
    taskcontroller::NodeStatusManager::RoleNames finishedNodes, finishedGroups;
    bool finished = nodeStatusManager->CheckFinished(workerNodes, &finishedNodes, &finishedGroups);
    for (const auto& node : workerNodes) {
        const proto::PartitionId& pid = node->getPartitionId();
        bool isBackup = pid.has_backupid() && pid.backupid() != 0;
        std::string roleName = proto::RoleNameGenerator::generateRoleName(node->getPartitionId());
        std::string roleGroupName = proto::RoleNameGenerator::generateRoleGroupName(node->getPartitionId());
        if (finishedNodes.find(roleName) != finishedNodes.end()) {
            if (!node->isFinished() && isBackup && metric) {
                // main node is not finished but backup node is.
                if (finishedNodes.find(roleGroupName) == finishedNodes.end()) {
                    ++(metric->effectiveBackupNodeCount);
                }
            }
            node->setFinished(true);
        } else if (isSuspended(node)) {
            node->setSuspended(true);
        }
    }
    return finished;
}

}} // namespace build_service::admin
