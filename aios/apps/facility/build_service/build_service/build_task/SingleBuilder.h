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

#include "build_service/config/TaskTarget.h"
#include "build_service/proto/BuildTaskTargetInfo.h"
#include "build_service/proto/ErrorCollector.h"
#include "build_service/task_base/RestartIntervalController.h"
#include "build_service/task_base/Task.h"
#include "build_service/util/Log.h"
#include "build_service/workflow/BuildFlow.h"
#include "build_service/workflow/FlowFactory.h"
#include "indexlib/util/TaskScheduler.h"
#include "indexlib/util/counter/CounterMap.h"

namespace indexlibv2::framework {
class ITablet;
}

namespace build_service::build_task {

class SingleBuilder : public proto::ErrorCollector
{
public:
    enum class CommandType { START_BUILD, STOP_BUILD, NONE, INVALID_PARAM };

    SingleBuilder(const task_base::Task::TaskInitParam& initParam,
                  const std::shared_ptr<indexlibv2::framework::ITablet>& tablet)
        : _taskInitParam(initParam)
        , _tablet(tablet)
        , _hasFatalError(false)
        , _isDone(false)
        , _latestOpenedPublishedVersion(indexlibv2::INVALID_VERSIONID)
        , _updateLocatorTaskId(indexlib::util::TaskScheduler::INVALID_TASK_ID)
        , _startTimestamp(0)
    {
    }
    virtual ~SingleBuilder();

    bool handleTarget(const config::TaskTarget& target);
    bool isDone() const;
    std::shared_ptr<indexlib::util::CounterMap> getCounterMap() const { return _counterMap; }
    bool hasFatalError() const;
    void updateOpenedPublishedVersion(versionid_t versionId) { _latestOpenedPublishedVersion.store(versionId); }
    // virtual for test
    virtual versionid_t getOpenedPublishedVersion() const { return _latestOpenedPublishedVersion.load(); }
    static proto::PartitionId getPartitionRange(const task_base::Task::TaskInitParam& initParam);

private:
    bool startBuildFlow(KeyValueMap& kvMap);
    bool prepareIntervalTask(const KeyValueMap& kvMap);
    common::ResourceKeeperMap createResources(KeyValueMap& kvMap) const;
    void reset();
    bool parseTargetDescription(const config::TaskTarget& target, KeyValueMap& kvMap);
    bool startBuild(const config::TaskTarget& target);
    bool stopBuild(const config::TaskTarget& target);
    CommandType getCommandType(const config::TaskTarget& target);
    void collectErrorInfos() const;
    bool getTimestamp(const KeyValueMap& kvMap, const std::string& key, int64_t& timestamp) const;
    bool isProcessedDocDataDesc(const KeyValueMap& kvMap) const;
    proto::PartitionId getPartitionRange() const;

private:
    static constexpr int64_t UPDATE_LOCATOR_INTERVAL = 60 * 1000 * 1000; // 60s

    mutable std::mutex _lock;
    const task_base::Task::TaskInitParam _taskInitParam;
    std::shared_ptr<indexlibv2::framework::ITablet> _tablet;
    std::shared_ptr<indexlib::util::CounterMap> _counterMap;
    std::shared_ptr<indexlib::util::TaskScheduler> _taskScheduler;
    std::unique_ptr<workflow::BuildFlow> _buildFlow;
    std::unique_ptr<workflow::FlowFactory> _brokerFactory;
    std::unique_ptr<task_base::RestartIntervalController> _restartIntervalController;
    std::atomic<bool> _hasFatalError;
    std::atomic<bool> _isDone;
    std::atomic<versionid_t> _latestOpenedPublishedVersion;
    int32_t _updateLocatorTaskId;
    int64_t _startTimestamp;
    config::TaskTarget _currentFinishTarget;
    std::string _buildStep = "";

private:
    BS_LOG_DECLARE();
};

} // namespace build_service::build_task
