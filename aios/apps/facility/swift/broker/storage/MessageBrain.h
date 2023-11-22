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

#include "autil/Lock.h"
#include "autil/Log.h"
#include "autil/Thread.h"
#include "swift/broker/storage/InnerPartMetric.h"
#include "swift/broker/storage/MessageGroup.h"
#include "swift/broker/storage/RequestItemQueue.h"
#include "swift/common/Common.h"
#include "swift/heartbeat/ThreadSafeTaskStatus.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/util/ReaderRec.h"

namespace kmonitor {
class MetricsTags;
}
namespace swift {
namespace storage {
class FlowControl;
} // namespace storage

namespace config {
class PartitionConfig;
} // namespace config
namespace monitor {
class BrokerMetricsReporter;
struct PartitionMetricsCollector;
struct ReadMetricsCollector;
struct WriteMetricsCollector;

typedef std::shared_ptr<ReadMetricsCollector> ReadMetricsCollectorPtr;
typedef std::shared_ptr<WriteMetricsCollector> WriteMetricsCollectorPtr;

} // namespace monitor
namespace protocol {
class ConsumptionRequest;
class MessageIdRequest;
class MessageIdResponse;
class MessageResponse;
class ProductionRequest;
} // namespace protocol
namespace storage {
class CommitManager;
class FileManager;
class MessageCommitter;
} // namespace storage
namespace util {
class BlockPool;
class PermissionCenter;
class ProductionLogClosure;
class TimeoutChecker;
} // namespace util
} // namespace swift

namespace swift {
namespace storage {
class FsMessageReader;

class MessageBrain {
public:
    typedef RequestItemInternal<protocol::ProductionRequest, protocol::MessageResponse> WriteRequestItem;
    typedef RequestItemQueue<protocol::ProductionRequest, protocol::MessageResponse> WriteRequestItemQueue;
    static const int64_t DEFAULT_TIMETOUT_LIMIT = 35 * 1000 * 1000;      // 35 s
    static const int64_t DEFAULT_RETRY_COMMIT_INTERVAL = 200 * 1000;     // 200 ms
    static const int64_t DEFAULT_WAITTIME_FOR_SECURITYMODE = 200 * 1000; // 200 ms
public:
    MessageBrain(int64_t sessionId = -1);
    virtual ~MessageBrain();

private:
    MessageBrain(const MessageBrain &);
    MessageBrain &operator=(const MessageBrain &);

public:
    protocol::ErrorCode
    init(const config::PartitionConfig &config,
         util::BlockPool *centerPool,
         util::BlockPool *fileCachePool,
         util::PermissionCenter *permissionCenter,
         monitor::BrokerMetricsReporter *metricsReporter,
         const heartbeat::ThreadSafeTaskStatusPtr &taskStatus = heartbeat::ThreadSafeTaskStatusPtr(),
         int32_t oneFileFdNum = 1,
         int64_t fileReserveTime = 600,
         bool enableFastRecover = false);
    void stopSecurityModeThread();
    // virtual for test
    virtual protocol::ErrorCode getMessage(const protocol::ConsumptionRequest *request,
                                           protocol::MessageResponse *response,
                                           util::TimeoutChecker *timeoutChecker,
                                           const std::string *srcIpPort,
                                           monitor::ReadMetricsCollector &collector);
    protocol::ErrorCode addMessage(util::ProductionLogClosure *closure, monitor::WriteMetricsCollectorPtr &collector);
    protocol::ErrorCode getMaxMessageId(const protocol::MessageIdRequest *request,
                                        protocol::MessageIdResponse *response);
    protocol::ErrorCode getMinMessageIdByTime(const protocol::MessageIdRequest *request,
                                              protocol::MessageIdResponse *response,
                                              util::TimeoutChecker *timeoutChecker);
    bool needCommitMessage();
    protocol::ErrorCode commitMessage();
    int64_t getCommittedMsgId() const;

    RecycleInfo getRecycleInfo();
    void recycleBuffer(int64_t blockCount = 0);
    void recycleFileCache(int64_t metaThreshold, int64_t dataThreshold);
    void delExpiredFile();
    void syncDfsUsedSize();

    protocol::TopicMode getTopicMode();
    void setForceUnload(bool flag);
    int64_t getFileCacheBlockCount();
    void recycleFile();
    void recycleObsoleteReader();
    bool getFileBlockDis(std::vector<int64_t> &metaDisBlocks, std::vector<int64_t> &dataDisBlocks);
    bool hasSealError();

    MessageCommitter *getMessageCommitter() { return _messageCommitter; }

public:
    // for metrics report
    void getPartitionMetrics(monitor::PartitionMetricsCollector &collector) const;
    void getPartitionInMetric(uint32_t interval, protocol::PartitionInMetric *partInMetric);

private:
    void backgroundCommitForSecurityMode();
    void commitForSecurityMode();
    protocol::ErrorCode checkProductionRequest(const protocol::ProductionRequest *request,
                                               int64_t &totalSize,
                                               int64_t &msgCount,
                                               int64_t &msgCountWithMerged,
                                               monitor::WriteMetricsCollector &collector);
    bool checkSecurityModeCommitCondition();
    protocol::ErrorCode doAddMessage(const protocol::ProductionRequest *request,
                                     protocol::MessageResponse *response,
                                     int64_t msgCount,
                                     int64_t totalMsgSize,
                                     monitor::WriteMetricsCollectorPtr &collector);
    protocol::ErrorCode getFieldFilterMessage(const protocol::ConsumptionRequest *request,
                                              protocol::MessageResponse *response,
                                              util::TimeoutChecker *timeoutChecker,
                                              util::ReaderInfoPtr &readerInfo,
                                              monitor::ReadMetricsCollector &collector);
    protocol::ErrorCode doGetMessage(const protocol::ConsumptionRequest *request,
                                     protocol::MessageResponse *response,
                                     util::TimeoutChecker *timeoutChecker,
                                     util::ReaderInfoPtr &readerInfo,
                                     monitor::ReadMetricsCollector &collector);
    void checkTimeOutWriteRequest(std::vector<WriteRequestItem *> &requestItemVec);
    void addWriteRequestToMemory(std::vector<WriteRequestItem *> &requestItemVec);
    void commitAllMessage();
    void doneRunForProductRequest(util::ProductionLogClosure *closure,
                                  monitor::WriteMetricsCollectorPtr &collector,
                                  protocol::ErrorCode ec = protocol::ERROR_NONE);
    size_t calNeedBlockCount(size_t msgCount, int64_t totalDataSize);
    bool messageIdValidInMemory(const protocol::ConsumptionRequest *request);
    bool messageIdValid(const protocol::ConsumptionRequest *request,
                        util::TimeoutChecker *timeoutChecker,
                        monitor::ReadMetricsCollector *collector = NULL);
    protocol::ErrorCode getMinMessageIdByTime(int64_t timestamp,
                                              protocol::MessageIdResponse *response,
                                              util::TimeoutChecker *timeoutChecker,
                                              bool applyFileRead = true);
    void updateReaderInfo(protocol::ErrorCode ec,
                          const protocol::ConsumptionRequest *request,
                          const std::string *srcIpPort,
                          const protocol::MessageResponse *response,
                          util::ReaderInfoPtr &reader);
    util::ReaderDes getReaderDescription(const protocol::ConsumptionRequest *request, const std::string &srcIpPort);

private:
    util::BlockPool *_partitionBlockPool;
    FileManager *_fileManager;
    FsMessageReader *_fsMessageReader;
    MessageGroup *_messageGroup;
    MessageCommitter *_messageCommitter;
    monitor::BrokerMetricsReporter *_metricsReporter;
    util::PermissionCenter *_permissionCenter;
    autil::ThreadMutex _addMsgLock;
    heartbeat::ThreadSafeTaskStatusPtr _taskStatus;
    CommitManager *_commitManager;
    int64_t _keepTime;
    int64_t _maxMessageSize;
    int64_t _minBufferSize;
    int64_t _maxBufferSize;
    int64_t _blockSize;
    bool _memoryOnly;
    bool _isStopped;
    bool _needFieldFilter;
    bool _compressMsg;
    bool _compressThres;
    double _recyclePercent;
    bool _checkFieldFilterMsg;
    int64_t _sessionId;
    int64_t _maxCommitIntervalForMemoryPrefer;
    int64_t _maxCommitIntervalWhenDelay;
    protocol::TopicMode _topicMode;
    protocol::PartitionId _partitionId;
    autil::ThreadPtr _backGroundThreadForSecurityMode;
    int64_t _maxDataSizeForSecurityCommit;
    int64_t _maxWaitTimeForSecurityCommit;
    mutable autil::ThreadCond _writeRequestCond;
    WriteRequestItemQueue _writeRequestItemQueue;
    mutable autil::ThreadMutex _recycleMutex;
    bool _forceUnload;
    util::ReaderInfoMap _readerInfoMap;
    autil::ReadWriteLock _readerRW;
    int64_t _obsoleteReaderInterval;
    int64_t _obsoleteReaderMetricInterval;
    int64_t _lastReadMsgTimeStamp;
    int64_t _lastReadTime;
    InnerPartMetricStat _metricStat;
    InnerPartMetric *_innerPartMetric;
    int64_t _maxReadSizeSec;
    FlowControl *_flowCtrol;
    bool _enableFastRecover;
    std::shared_ptr<kmonitor::MetricsTags> _metricsTags;

private:
    AUTIL_LOG_DECLARE();
    friend class MessageBrainTest;
};

SWIFT_TYPEDEF_PTR(MessageBrain);

} // namespace storage
} // namespace swift
