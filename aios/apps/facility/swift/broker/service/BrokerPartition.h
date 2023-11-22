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

#include <limits>
#include <list>
#include <memory>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <utility>
#include <vector>

#include "autil/Lock.h"
#include "autil/Log.h"
#include "swift/broker/storage/MessageGroup.h"
#include "swift/common/Common.h"
#include "swift/common/SingleTopicWriterController.h"
#include "swift/heartbeat/ThreadSafeTaskStatus.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/util/ZkDataAccessor.h"

namespace autil {
class ZlibCompressor;
} // namespace autil

namespace kmonitor {
class MetricsTags;
}

namespace swift {
namespace common {
class RangeUtil;
} // namespace common

namespace config {
class BrokerConfig;
class PartitionConfig;
} // namespace config

namespace filter {
class FieldFilter;
class MessageFilter;
} // namespace filter

namespace monitor {
class BrokerMetricsReporter;
struct LongPollingMetricsCollector;
struct PartitionMetricsCollector;
struct ReadMetricsCollector;
} // namespace monitor

namespace protocol {
class ConsumptionRequest;
class FBMessageWriter;
class Message;
class MessageIdRequest;
class MessageIdResponse;
class MessageResponse;
class ProductionRequest;

namespace flat {
struct Message;
} // namespace flat
} // namespace protocol

namespace util {
class BlockPool;
class ConsumptionLogClosure;
class PermissionCenter;
class ProductionLogClosure;
class TimeoutChecker;

typedef std::shared_ptr<ConsumptionLogClosure> ConsumptionLogClosurePtr;
} // namespace util

namespace network {
class SwiftRpcChannelManager;
}
namespace storage {
class MessageBrain;
} // namespace storage

namespace service {

class MirrorPartition;

class BrokerPartition {
public:
    typedef std::list<std::pair<int64_t, util::ConsumptionLogClosurePtr>> ReadRequestQueue;

public:
    BrokerPartition(const protocol::TaskInfo &taskInfo,
                    monitor::BrokerMetricsReporter *metricsReporter = nullptr,
                    util::ZkDataAccessorPtr zkDataAccessor = nullptr);
    BrokerPartition(const heartbeat::ThreadSafeTaskStatusPtr &taskInfo,
                    monitor::BrokerMetricsReporter *metricsReporter = nullptr,
                    util::ZkDataAccessorPtr zkDataAccessor = nullptr);
    // virtual for test
    virtual ~BrokerPartition();

private:
    BrokerPartition(const BrokerPartition &);
    BrokerPartition &operator=(const BrokerPartition &);

public:
    bool init(const config::BrokerConfig &brokerConfig,
              util::BlockPool *centerBlockPool,
              util::BlockPool *fileCachePool,
              util::PermissionCenter *permissionCenter,
              const std::shared_ptr<network::SwiftRpcChannelManager> &channelManager = nullptr);
    void stopSecurityModeThread();
    // virtual for test
    virtual protocol::ErrorCode getMessage(const protocol::ConsumptionRequest *request,
                                           protocol::MessageResponse *response,
                                           const std::string *srcIpPort = NULL);
    protocol::ErrorCode sendMessage(util::ProductionLogClosure *closure);
    void addReplicationMessage(protocol::ProductionRequest *request, protocol::MessageResponse *response);
    protocol::ErrorCode getMaxMessageId(const protocol::MessageIdRequest *request,
                                        protocol::MessageIdResponse *response);
    protocol::ErrorCode getMinMessageIdByTime(const protocol::MessageIdRequest *request,
                                              protocol::MessageIdResponse *response);
    // virtual for test
    virtual protocol::PartitionId getPartitionId() const;
    virtual protocol::PartitionStatus getPartitionStatus() const;
    void setPartitionStatus(protocol::PartitionStatus status);
    heartbeat::ThreadSafeTaskStatusPtr getTaskStatus() const;
    // virtual for test
    virtual void collectMetrics(monitor::PartitionMetricsCollector &collector);
    void collectInMetric(uint32_t interval, protocol::PartitionInMetric *partInMetric);
    protocol::TopicMode getTopicMode();
    storage::RecycleInfo getRecycleInfo();
    void recycleBuffer();
    void recycleFileCache(int64_t metaThreshold, int64_t dataThreshold);
    void recycleFile();
    void recycleObsoleteReader();
    void delExpiredFile();
    void syncDfsUsedSize();
    bool needCommitMessage();
    bool isCommitting() { return _isCommitting; }
    void setCommitting(bool flag) { _isCommitting = flag; }
    void commitMessage();
    int64_t getCommittedMsgId() const;
    void setForceUnload(bool flag);
    int64_t getFileCacheBlockCount();
    bool getFileBlockDis(std::vector<int64_t> &metaDisBlocks, std::vector<int64_t> &dataDisBlocks);
    bool sealed() { return _taskStatus->sealed(); }
    int64_t getSessionId() { return _sessionId; }
    template <typename RequestType>
    bool sessionChanged(const RequestType *request) {
        if (request->has_sessionid()) {
            int64_t sessionId = request->sessionid();
            if (sessionId != -1 && sessionId != _sessionId) {
                return true;
            }
        }
        return false;
    }
    int64_t addLongPolling(int64_t expireTime, const util::ConsumptionLogClosurePtr &closure, int64_t startId);
    // virtual for test
    virtual size_t
    stealTimeoutPolling(bool timeBack, int64_t currentTime, int64_t maxHoldTime, ReadRequestQueue &timeoutReqs);
    void stealNeedActivePolling(ReadRequestQueue &pollingQueue, uint64_t compressPayload, int64_t maxMsgId = -1);
    void stealAllPolling(ReadRequestQueue &pollingQueue);
    void reportLongPollingQps();
    void reportLongPollingMetrics(monitor::LongPollingMetricsCollector &collector);
    bool needRequestPolling(int64_t maxMsgId);
    // virtual for test
    virtual bool isEnableLongPolling() const { return _enableLongPolling; }

private:
    std::string getPartitionDir(const std::string &rootDir, const protocol::PartitionId &partId);
    void preparePartitionConfig(const config::BrokerConfig &brokerConfig, config::PartitionConfig &partitionConfig);
    util::TimeoutChecker *createTimeoutChecker();
    void adjustRequestFilter(const protocol::ConsumptionRequest *request);
    void decompressMessage(protocol::MessageResponse *response, monitor::ReadMetricsCollector &collector);
    void compressResponse(protocol::MessageResponse *response, monitor::ReadMetricsCollector &collector);
    void unpackMergedMessage(const protocol::ConsumptionRequest *request,
                             protocol::MessageResponse *response,
                             monitor::ReadMetricsCollector &collector);
    void unpackMergedPbMessage(autil::ZlibCompressor *compressor,
                               const filter::MessageFilter *metaFilter,
                               filter::FieldFilter *fieldfilter,
                               protocol::MessageResponse *response);
    void unpackMergedFbMessage(autil::ZlibCompressor *compressor,
                               const filter::MessageFilter *metaFilter,
                               filter::FieldFilter *fieldfilter,
                               protocol::MessageResponse *response);
    void unpackPbMessage(autil::ZlibCompressor *compressor,
                         protocol::Message *msg,
                         const filter::MessageFilter *metaFilter,
                         filter::FieldFilter *fieldfilter,
                         protocol::MessageResponse *response);
    void unpackFbMessage(autil::ZlibCompressor *compressor,
                         const protocol::flat::Message *msg,
                         const filter::MessageFilter *metaFilter,
                         filter::FieldFilter *fieldfilter,
                         protocol::FBMessageWriter *writer);
    bool startMirrorPartition(const std::string &mirrorZkRoot,
                              const std::shared_ptr<network::SwiftRpcChannelManager> &channelManager);
    // virtual for test
    virtual std::unique_ptr<MirrorPartition> createMirrorPartition(const protocol::PartitionId &pid) const;

private:
    storage::MessageBrain *_messageBrain;
    heartbeat::ThreadSafeTaskStatusPtr _taskStatus;
    monitor::BrokerMetricsReporter *_metricsReporter;
    std::string _topicName;
    uint32_t _partitionId;
    volatile bool _isCommitting;
    int64_t _requestTimeout;
    int64_t _sessionId;
    common::RangeUtil *_rangeUtil;
    bool _needFieldFilter;
    bool _enableLongPolling;
    autil::ReadWriteLock _pollingLock;
    ReadRequestQueue _pollingQueue;
    int64_t _pollingMaxMsgId = 0;
    int64_t _pollingMinStartId = std::numeric_limits<int64_t>::max();
    std::shared_ptr<kmonitor::MetricsTags> _metricsTags;
    common::SingleTopicWriterControllerPtr _writerVersionController;
    util::ZkDataAccessorPtr _zkDataAccessor;
    std::unique_ptr<MirrorPartition> _mirror;

private:
    friend class BrokerPartitionTest;
    friend class TransporterImplTest;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(BrokerPartition);

} // namespace service
} // namespace swift
