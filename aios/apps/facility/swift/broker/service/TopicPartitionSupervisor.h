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

#include <map>
#include <memory>
#include <stdint.h>
#include <utility>
#include <vector>

#include "autil/Lock.h"
#include "autil/Log.h"
#include "autil/LoopThread.h"
#include "autil/ThreadPool.h"
#include "swift/broker/service/BrokerPartition.h"
#include "swift/common/Common.h"
#include "swift/heartbeat/ThreadSafeTaskStatus.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/util/ZkDataAccessor.h"

namespace swift {
namespace config {
class BrokerConfig;
} // namespace config
namespace monitor {

class BrokerMetricsReporter;
} // namespace monitor
namespace network {
class SwiftRpcChannelManager;
} // namespace network

namespace heartbeat {
class ZkConnectionStatus;
} // namespace heartbeat
namespace protocol {
class HeartbeatInfo;
} // namespace protocol
namespace util {
class BlockPool;
class PermissionCenter;
} // namespace util

namespace service {

class TopicPartitionSupervisor {
public:
    // topicPartitionId -> TopicPartition
    typedef std::map<protocol::PartitionId, std::pair<heartbeat::ThreadSafeTaskStatusPtr, BrokerPartitionPtr>>
        BrokerPartitionMap;

    TopicPartitionSupervisor(config::BrokerConfig *brokerConfig,
                             heartbeat::ZkConnectionStatus *status,
                             monitor::BrokerMetricsReporter *metricsReporter);
    // virtual for test
    virtual ~TopicPartitionSupervisor();

public:
    void setChannelManager(const std::shared_ptr<network::SwiftRpcChannelManager> &channelManager);

    // control command.
    protocol::ErrorCode loadBrokerPartition(const protocol::TaskInfo &taskInfo);
    void loadBrokerPartition(const std::vector<protocol::TaskInfo> &taskInfoVec);

    protocol::ErrorCode unLoadBrokerPartition(const protocol::PartitionId &topicPartId);
    void unLoadBrokerPartition(const std::vector<protocol::PartitionId> &partIdVec);

    // heartbeat.
    void getPartitionStatus(protocol::HeartbeatInfo &info);
    // virtual for test
    virtual void getAllPartitions(std::vector<protocol::PartitionId> &partitionIds);
    bool needRejectServing() const;
    virtual BrokerPartitionPtr getBrokerPartition(const protocol::PartitionId &partitionId);
    void getPartitionInMetrics(uint32_t interval,
                               google::protobuf::RepeatedPtrField<protocol::PartitionInMetric> *partMetrics,
                               int64_t &rejectTime);
    void amonReport();
    void start(util::ZkDataAccessorPtr zkDataAccessor = nullptr);
    void stop();
    void releaseLongPolling();
    void stopSecurityModeThread();

private:
    void setBrokerPartition(const protocol::PartitionId &partitionId, const BrokerPartitionPtr &borkerPartition);
    void recycleBuffer();
    void recycleWriteCache(int64_t maxUnusedBlock, int64_t minReserveBlock);
    void recycleFileCache();
    void recycleFile();
    void recycleObsoleteReader();
    void commitMessage();
    void delExpiredFile();
    std::vector<BrokerPartitionPtr> getToDelFilePartition();
    void loadBrokerPartitionMutilThread(const std::vector<protocol::TaskInfo> &taskInfoVec);
    void unLoadBrokerPartitionMutilThread(const std::vector<protocol::PartitionId> &partIdVec);
    void getRecycleFileThreshold(int64_t &threshold, const std::vector<int64_t> &fileBlocks);

private:
    TopicPartitionSupervisor(const TopicPartitionSupervisor &);
    TopicPartitionSupervisor &operator=(const TopicPartitionSupervisor &);

public:
    config::BrokerConfig *_brokerConfig;
    BrokerPartitionMap _brokerPartitionMap;
    heartbeat::ZkConnectionStatus *_status;
    //    bool _status;
    autil::ReadWriteLock _brokerPartitionMapMutex;
    monitor::BrokerMetricsReporter *_metricsReporter;

    util::BlockPool *_centerBlockPool;
    util::BlockPool *_fileCachePool;
    util::PermissionCenter *_permissionCenter;

    autil::LoopThreadPtr _recycleBufferThread;
    autil::LoopThreadPtr _commitMessageThread;
    autil::LoopThreadPtr _delExpiredFileThread;
    autil::ThreadPoolPtr _commitThreadPool;
    autil::ReadWriteLock _commitThreadPoolMutex;
    autil::ThreadPoolPtr _loadThreadPool;
    autil::ThreadPoolPtr _unloadThreadPool;
    autil::ThreadMutex _unloadMutex;
    bool _isStop;
    int64_t _delCount;
    bool _firstLoad = true;
    util::ZkDataAccessorPtr _zkDataAccessor;
    std::shared_ptr<network::SwiftRpcChannelManager> _channelManager;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(TopicPartitionSupervisor);

} // namespace service
} // namespace swift
