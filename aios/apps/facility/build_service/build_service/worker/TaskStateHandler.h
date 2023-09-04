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
#ifndef ISEARCH_BS_TASKSTATEHANDLER_H
#define ISEARCH_BS_TASKSTATEHANDLER_H

#include "autil/Lock.h"
#include "build_service/common_define.h"
#include "build_service/plugin/PlugInManager.h"
#include "build_service/task_base/BuildInTaskFactory.h"
#include "build_service/task_base/TaskFactory.h"
#include "build_service/util/Log.h"
#include "build_service/worker/WorkerStateHandler.h"

namespace build_service { namespace worker {

class TaskStateHandler : public WorkerStateHandler
{
public:
    TaskStateHandler(const proto::PartitionId& pid, indexlib::util::MetricProviderPtr metricProvider,
                     const proto::LongAddress& address, const std::string& appZkRoot = "",
                     const std::string& adminServiceName = "", const std::string& epochId = "");

    ~TaskStateHandler();

private:
    TaskStateHandler(const TaskStateHandler&);
    TaskStateHandler& operator=(const TaskStateHandler&);

public:
    bool init() override;
    void doHandleTargetState(const std::string& state, bool hasResourceUpdated) override;
    bool needSuspendTask(const std::string& state) override;
    bool needRestartProcess(const std::string& state) override;
    void getCurrentState(std::string& state) override;
    bool hasFatalError() override { return false; }

public:
    void __TEST_setReusePluginMananger(const plugin::PlugInManagerPtr& plugInManager)
    {
        _reusePluginManager = true;
        _plugInManager = plugInManager;
    }

private:
    void syncCounter();
    task_base::TaskFactory* getTaskFactory(const std::string& moduleName);
    bool checkTargetTimestamp(const proto::TaskTarget& target);
    task_base::TaskPtr createTask(const config::TaskTarget& taskTarget);
    bool getCounterConfig(const config::ResourceReaderPtr& resourceReader, config::CounterConfigPtr& counterConfig);
    void fillCurrentState(proto::TaskCurrent& current);
    void fillWorkerStatus(task_base::Task* task, proto::TaskCurrent& current);
    void fillProgressStatus(proto::WorkerStatus status, proto::TaskCurrent& current);
    bool fillInstanceInfo(const proto::PartitionId _pid, const config::TaskTarget& taskTarget,
                          task_base::Task::InstanceInfo& instanceInfo);
    bool prepareInputs(const std::map<std::string, config::TaskInputConfig>& inputConfigs,
                       task_base::InputCreatorMap& inputMap);
    bool prepareOutputs(const std::map<std::string, config::TaskOutputConfig>& outputConfigs,
                        task_base::OutputCreatorMap& outputMap);

    io::InputCreatorPtr createInputCreator(const config::TaskInputConfig& inputConfig);
    io::OutputCreatorPtr createOutputCreator(const config::TaskOutputConfig& outputConfig);
    bool checkUpdateConfig(const proto::TaskTarget& target);

private:
    mutable autil::RecursiveThreadMutex _lock;
    plugin::PlugInManagerPtr _plugInManager;
    config::ResourceReaderPtr _resourceReader;
    proto::TaskCurrent _current;
    proto::TaskTarget _currentTarget;
    task_base::TaskPtr _task;
    config::CounterConfigPtr _counterConfig;
    common::CounterSynchronizerPtr _counterSynchronizer;
    int64_t _startTimestamp;
    bool _reusePluginManager;
    task_base::BuildInTaskFactory _buildInTaskFactory;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(TaskStateHandler);

}} // namespace build_service::worker

#endif // ISEARCH_BS_TASKSTATEHANDLER_H
