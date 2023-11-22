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
#include <functional>
#include <stddef.h>
#include <stdint.h>
#include <utility>
#include <vector>

#include "autil/Lock.h"
#include "autil/Log.h"
#include "autil/ThreadPool.h"
#include "kmonitor/client/core/MetricsTags.h"
#include "swift/client/BufferSizeLimiter.h"
#include "swift/client/MessageInfo.h"
#include "swift/client/MessageWriteBuffer.h"
#include "swift/client/RangeUtil.h"
#include "swift/client/SwiftWriterConfig.h"
#include "swift/client/TransportClosure.h"
#include "swift/common/Common.h"
#include "swift/network/SwiftAdminAdapter.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/util/BlockPool.h"

namespace swift {
namespace common {
class MessageInfo;
} // namespace common

namespace protocol {
class MessageResponse;
} // namespace protocol

namespace monitor {
class ClientMetricsReporter;
struct ClientMetricsCollector;
} // namespace monitor

namespace client {
template <swift::client::TransportRequestType EnumType>
class SwiftTransportAdapter;

class SingleSwiftWriter {
public:
    static int64_t INVALID_CHECK_POINT_ID;

public:
    SingleSwiftWriter(network::SwiftAdminAdapterPtr adminAdapter,
                      network::SwiftRpcChannelManagerPtr channelManager,
                      uint32_t partitionId,
                      const SwiftWriterConfig &config,
                      util::BlockPoolPtr blockPool,
                      autil::ThreadPoolPtr mergeThreadPool,
                      BufferSizeLimiterPtr limiter = BufferSizeLimiterPtr(),
                      monitor::ClientMetricsReporter *reporter = nullptr,
                      int64_t topicVersion = -1,
                      int64_t sessionId = -1);
    ~SingleSwiftWriter();

private:
    SingleSwiftWriter(const SingleSwiftWriter &);
    SingleSwiftWriter &operator=(const SingleSwiftWriter &);

public:
    protocol::ErrorCode write(MessageInfo &msgInfo, bool isSynchronous = false);
    bool getCommittedCheckpointId(int64_t &checkpoint);
    bool getMaxCheckpointId(int64_t &checkpoint);
    bool isBufferEmpty();
    bool isSent();
    int64_t getLastRefreshTime() const;
    // background thread
    protocol::ErrorCode sendRequest(int64_t now, bool force);
    void clearBuffer();
    // statistics
    size_t getUnsendMessageCount();
    size_t getUnsendMessageSize();
    size_t getUncommittedMsgCount();
    size_t getUncommittedMsgSize();
    void setTopicChangeCallBack(const std::function<protocol::ErrorCode(int64_t)> &func);
    void setCommittedCallBack(const std::function<void(const std::vector<std::pair<int64_t, int64_t>> &)> &func);
    void setErrorCallBack(const std::function<void(const protocol::ErrorCode &)> &func);
    void updateRangeUtil(const RangeUtilPtr rangeUtil);
    bool stealUnsendMessage(std::vector<common::MessageInfo> &msgInfoVec);

private:
    void setCheckpointId(int64_t checkpointId);
    bool postSendMessageRequest(int64_t now,
                                monitor::ClientMetricsCollector &collector,
                                int64_t timeout = common::DEFAULT_POST_TIMEOUT); // 30s
    void handleSendMessageResponse();
    protocol::ErrorCode getLastErrorCode() const;
    void setLastErrorCode(protocol::ErrorCode);
    void setLastRefreshTime(int64_t refreshTime);
    bool needRetreat(int64_t currentTime);
    int64_t calculateCommittedCount(const protocol::MessageResponse &response);
    bool needDetectCommitProgress(int64_t now);
    void sendSyncRequest(int64_t now, uint32_t retryTimes, monitor::ClientMetricsCollector &collector);
    protocol::ErrorCode addMsgFail();
    size_t getOneRequestSendBytes();

private:
    mutable autil::ThreadMutex _mutex;
    mutable autil::ThreadMutex _synchronousMutex;
    SwiftTransportAdapter<TRT_SENDMESSAGE> *_transportAdapter;
    uint32_t _partitionId;
    SwiftWriterConfig _config;
    int64_t _lastSendRequestTime;
    int64_t _checkpointId;
    int64_t _refreshTime;
    int64_t _retreatNextTimestamp;
    protocol::ErrorCode _lastErrorCode;
    MessageWriteBuffer _writeBuffer;
    int64_t _sessionId;
    protocol::ClientVersionInfo _clientVersionInfo;
    std::function<protocol::ErrorCode(int64_t)> _topicChangeFunc;
    std::function<void(std::vector<std::pair<int64_t, int64_t>> &)> _committedFunc;
    std::function<void(const protocol::ErrorCode &)> _errorFunc;
    util::BlockPoolPtr _blockPool;
    int64_t _lastTopicVersion;
    uint64_t _requestSeq = 0;
    std::atomic<bool> _hasVersionError;
    monitor::ClientMetricsReporter *_metricsReporter;
    kmonitor::MetricsTags _metricsTags;

private:
    friend class SwiftWriterImplMock;
    friend class SwiftClientFactory;
    friend class FakeClientHelper;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(SingleSwiftWriter);

} // namespace client
} // namespace swift
