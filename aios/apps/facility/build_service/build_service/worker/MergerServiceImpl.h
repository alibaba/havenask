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

#include <atomic>
#include <stdint.h>
#include <string>

#include "aios/autil/autil/Lock.h"
#include "build_service/common/ResourceContainer.h"
#include "build_service/common_define.h"
#include "build_service/config/BuildServiceConfig.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/proto/Heartbeat.pb.h"
#include "build_service/task_base/JobConfig.h"
#include "build_service/task_base/MergeTask.h"
#include "build_service/task_base/TaskBase.h"
#include "build_service/worker/WorkerStateHandler.h"

namespace build_service { namespace worker {

// todo: start merge in another thread to avoid blocking the heartbeat thread
class MergerServiceImpl : public WorkerStateHandler
{
public:
    MergerServiceImpl(const proto::PartitionId& pid, indexlib::util::MetricProviderPtr metricProvider,
                      const proto::LongAddress& address, const std::string& zkRoot, const std::string& appZkRoot = "",
                      const std::string& adminServiceName = "", const std::string& epochId = "");
    ~MergerServiceImpl();

private:
    MergerServiceImpl(const MergerServiceImpl&);
    MergerServiceImpl& operator=(const MergerServiceImpl&);

public:
    bool init() override;
    void doHandleTargetState(const std::string& state, bool hasResourceUpdated) override;
    bool needSuspendTask(const std::string& state) override;
    bool needRestartProcess(const std::string& state) override;
    void getCurrentState(std::string& state) override;
    bool hasFatalError() override;

protected:
    virtual task_base::MergeTask* createMergeTask();

private:
    bool merge(const proto::MergerTarget& target); // only for ut
    void doMergeTarget(const proto::MergerTarget& target);
    bool mergeIndex(const proto::MergerTarget& target, const std::string& localConfigPath);
    bool mergeNewField(const proto::MergerTarget& target, const std::string& localConfigPath);
    uint32_t calculateInstanceId(const proto::PartitionId& pid, uint32_t partitionCount, uint32_t mergeParallelNum);
    bool calculateMergerInstanceId(const proto::MergerTarget& target, uint32_t& instanceId);
    bool readCheckpoint(proto::MergerCurrent& checkpoint);
    bool commitCheckpoint(const proto::MergerCurrent& checkpoint);
    // virtual for test
    virtual bool fillIndexInfo(const std::string& clusterName, const proto::Range& range, const std::string& indexRoot,
                               proto::IndexInfo* indexInfo);
    // only for test
    void trySleep(const config::ResourceReader& resourceReader, const std::string& clusterName,
                  const std::string& mergeConfigName);
    void fillProgressStatus(proto::MergerCurrent& current);
    // virtual for test
    virtual bool getServiceConfig(config::ResourceReader& resourceReader, config::BuildServiceConfig& serviceConfig);
    bool prepareKVMap(const proto::MergerTarget& target, const config::BuildServiceConfig& serviceConfig,
                      const std::string& localConfigPath, KeyValueMap& kvMap);
    void updateMetricTagsHandler(const proto::MergerTarget& target);

    bool parseReservedVersions(const proto::MergerTarget& target, KeyValueMap& kvMap);
    bool parseTargetDescription(const proto::MergerTarget& target, KeyValueMap& kvMap);
    bool checkUpdateConfig(const proto::MergerTarget& target);
    bool isMultiPartMerge(const KeyValueMap& kvMap, const task_base::JobConfig& jobConfig);
    void cleanUselessResource(const proto::MergerTarget& target);

private:
    std::string _zkRoot;
    mutable proto::MergerCurrent _current;
    std::string _checkpointFilePath;
    std::atomic<bool> _hasCachedCheckpoint;
    proto::MergerCurrent _checkpoint;
    task_base::TaskBase* _task;
    int64_t _startTimestamp;
    mutable autil::RecursiveThreadMutex _lock; // attention lock code should use less time
private:
    static const int64_t COMMIT_CHECKPOINT_RETRY_INTERVAL = 1 * 1000 * 1000; // 1s
    static const int64_t MAX_ZK_RETRY = 20;

private:
    BS_LOG_DECLARE();
};

}} // namespace build_service::worker
