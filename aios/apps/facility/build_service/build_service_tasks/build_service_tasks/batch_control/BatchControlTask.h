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

#include <stdint.h>
#include <string>

#include "build_service/common_define.h"
#include "build_service/config/TaskTarget.h"
#include "build_service/proto/DataDescription.h"
#include "build_service/task_base/Task.h"
#include "build_service/util/Log.h"
#include "build_service_tasks/batch_control/BatchControlWorker.h"
#include "indexlib/util/counter/CounterMap.h"
#include "swift/client/SwiftClient.h"
#include "swift/client/SwiftReader.h"

namespace build_service { namespace task_base {

class BatchControlTask : public Task
{
public:
    BatchControlTask();
    ~BatchControlTask();

public:
    static const std::string TASK_NAME;

private:
    BatchControlTask(const BatchControlTask&);
    BatchControlTask& operator=(const BatchControlTask&);

public:
    bool init(TaskInitParam& initParam) override;
    bool handleTarget(const config::TaskTarget& target) override;
    bool isDone(const config::TaskTarget& target) override { return false; }
    indexlib::util::CounterMapPtr getCounterMap() override { return indexlib::util::CounterMapPtr(); }
    std::string getTaskStatus() override { return ""; }
    bool hasFatalError() override { return false; }

private:
    virtual bool initSwiftReader(const std::string& topicName, const std::string& swiftRoot);

private:
    bool _hasStarted;
    TaskInitParam _taskInitParam;
    BatchControlWorkerPtr _worker;
    config::TaskTarget _currentFinishTarget;
    swift::client::SwiftClient* _client;
    swift::client::SwiftReader* _swiftReader;
    std::string _adminAddress;
    int32_t _port;
    proto::DataDescription _dataDesc;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(BatchControlTask);

}} // namespace build_service::task_base
