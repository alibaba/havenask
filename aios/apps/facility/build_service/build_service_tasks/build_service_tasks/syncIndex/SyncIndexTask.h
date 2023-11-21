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
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <vector>

#include "aios/autil/autil/Lock.h"
#include "autil/LoopThread.h"
#include "build_service/common_define.h"
#include "build_service/config/TaskTarget.h"
#include "build_service/task_base/Task.h"
#include "build_service/util/Log.h"
#include "indexlib/base/Types.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/counter/CounterMap.h"

namespace build_service { namespace task_base {

class MadroxChannel;

class SyncIndexTask : public Task
{
public:
    SyncIndexTask();
    ~SyncIndexTask() = default;

    SyncIndexTask(const SyncIndexTask&) = delete;
    SyncIndexTask& operator=(const SyncIndexTask&) = delete;

private:
    enum SyncStatus {
        SS_DONE = 0,
        SS_FAILED = 1,
        SS_RETRY = 2,
        SS_WAIT = 3,
    };

public:
    static const std::string TASK_NAME;
    static constexpr uint32_t RPC_RETRY_INTERVAL_S = 5; // 5s
    static constexpr size_t GS_RETRY_LIMIT = 2;

public:
    bool init(Task::TaskInitParam& initParam) override;
    bool handleTarget(const config::TaskTarget& target) override;
    bool isDone(const config::TaskTarget& target) override;
    indexlib::util::CounterMapPtr getCounterMap() override;
    std::string getTaskStatus() override;
    bool hasFatalError() override { return _hasFatalError; }

private:
    bool checkTarget(const KeyValueMap& kvMap, std::vector<versionid_t>& targetVersions) const;
    SyncStatus isAllSynced(const std::vector<versionid_t>& targetVersions);
    std::vector<std::string> getDeployMetas(const std::vector<versionid_t>& targetVersions);
    bool cloneIndexRpcBlock(const std::vector<versionid_t>& targetVersions);
    void updateLoop();

private:
    autil::ThreadMutex _mtx;
    autil::ThreadMutex _reachTargetMtx;
    autil::LoopThreadPtr _updateThread;
    config::TaskTarget _currentTarget;
    config::TaskTarget _reachedTarget;
    Task::TaskInitParam _taskInitParam;
    std::shared_ptr<MadroxChannel> _madroxChannel;
    std::string _madroxRoot;
    std::string _remoteIndexRoot;
    std::string _sourceIndexRoot;
    uint32_t _partitionCount;
    bool _hasFatalError;
    bool _requestPosted;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(SyncIndexTask);

}} // namespace build_service::task_base
