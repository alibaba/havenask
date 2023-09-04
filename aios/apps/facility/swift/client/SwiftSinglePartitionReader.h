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

#include <functional>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <utility>
#include <vector>

#include "autil/Lock.h"
#include "autil/Log.h"
#include "autil/ThreadPool.h"
#include "autil/TimeUtility.h"
#include "kmonitor/client/core/MetricsTags.h"
#include "swift/client/MessageReadBuffer.h"
#include "swift/client/SwiftPartitionStatus.h"
#include "swift/client/SwiftReaderConfig.h"
#include "swift/client/SwiftTransportAdapter.h"
#include "swift/client/TransportClosure.h"
#include "swift/common/Common.h"
#include "swift/network/SwiftAdminAdapter.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/protocol/TraceMessage.pb.h"

namespace swift {

namespace protocol {
class MessageResponse;
class Messages;
} // namespace protocol
namespace monitor {
class ClientMetricsReporter;
struct ClientMetricsCollector;
} // namespace monitor

namespace client {
class Notifier;

struct SeekByTimestampRecord {
    protocol::ErrorCode lastErrorCode;
    int64_t lastSeekTime;
};

class SwiftSinglePartitionReader {
public:
    static const int64_t SEEKBYTIMESTAMP_ERROR_INTERVAL = 1 * 1000 * 1000; // 1 s
public:
    SwiftSinglePartitionReader(network::SwiftAdminAdapterPtr adminAdapter,
                               network::SwiftRpcChannelManagerPtr channelManager,
                               uint32_t partitionId,
                               const SwiftReaderConfig &config,
                               int64_t topicVersion = -1,
                               Notifier *notifier = NULL,
                               monitor::ClientMetricsReporter *reporter = nullptr);
    virtual ~SwiftSinglePartitionReader();

private:
    SwiftSinglePartitionReader(const SwiftSinglePartitionReader &);
    SwiftSinglePartitionReader &operator=(const SwiftSinglePartitionReader &);

public:
    bool exceedTimestampLimit();
    virtual bool tryRead(protocol::Messages &msg);
    virtual int64_t tryFillBuffer(int64_t currentTime = autil::TimeUtility::currentTime());
    virtual void checkCurrentError(protocol::ErrorCode &errorCode, std::string &errorMsg) const;
    virtual protocol::ErrorCode reportFatalError(bool resetLastError = true);
    virtual int64_t getUnReadMsgCount();
    virtual bool hasUnReadMsg();
    virtual protocol::ErrorCode seekByMessageId(int64_t msgId);
    virtual protocol::ErrorCode seekByTimestamp(int64_t timestamp);
    virtual int64_t getFirstMsgTimestamp();
    virtual int64_t getNextMsgTimestamp();
    virtual std::pair<int64_t, int32_t> getCheckpointTimestamp();
    virtual SwiftPartitionStatus getSwiftPartitionStatus(int64_t timestamp);
    void setRequiredFieldNames(const std::vector<std::string> &fieldNames);
    std::vector<std::string> getRequiredFieldNames();
    void setFieldFilterDesc(const std::string &fieldFilterDesc);
    std::string getFieldFilterDesc();
    void setTimestampLimit(int64_t timestampLimit);
    int64_t getTimestampLimit() const;
    int64_t getLastMsgTimestamp();
    void setCallBackFunc(const std::function<protocol::ErrorCode(int64_t)> &func);
    bool updateCommittedCheckpoint(int64_t checkpoint);
    void setDecompressThreadPool(autil::ThreadPoolPtr decompressThreadPool);
    uint32_t getPartitionId() const { return _partitionId; }
    bool sealedTopicReadFinish() { return _sealedTopicReadFinish; }
    void setTopicLongPollingEnabled(bool enable) { _isTopicLongPollingEnabled = enable; }
    std::pair<uint32_t, uint32_t> getFilterRange() const;
    void setFilterRange(uint32_t from, uint32_t to);

public:
    // for test
    const SwiftReaderConfig &getReaderConfig() { return _config; }
    protocol::ErrorCode getLastErrorCode() const { return _lastErrorCode; }
    int64_t getNextMsgId() { return _nextMsgId; }
    void resetTransportAdapter(SwiftTransportAdapter<TRT_GETMESSAGE> *transportAdapter) {
        delete _transportAdapter;
        _transportAdapter = transportAdapter;
    }
    void resetMaxMsgIdTransportAdapter(SwiftTransportAdapter<TRT_GETMAXMESSAGEID> *transportAdapter) {
        delete _maxMsgIdTransportAdapter;
        _maxMsgIdTransportAdapter = transportAdapter;
    }
    void resetMsgIdTransportAdapter(SwiftTransportAdapter<TRT_GETMINMESSAGEIDBYTIME> *transportAdapter) {
        delete _msgIdTransportAdapter;
        _msgIdTransportAdapter = transportAdapter;
    }
    void setNextTimestamp(int64_t timestamp) { _nextTimestamp = timestamp; }
    void setSeekTimestamp(int64_t timestamp) { _seekTimestamp = timestamp; }
    int64_t tryFillBuffer(int64_t currentTime, bool force, bool &isSent);

private:
    protocol::ErrorCode handleGetMessageResponse(swift::monitor::ClientMetricsCollector &collector);
    protocol::ErrorCode handleGetMessageIdResponse();
    protocol::ErrorCode postGetMessageRequest(swift::monitor::ClientMetricsCollector &collector);
    template <typename ResponseType>
    protocol::ErrorCode fillPartitionInfo(int64_t refreshTime, const ResponseType &response);
    protocol::ErrorCode getMinMessageIdByTime(int64_t timestamp, int64_t &msgid, int64_t &msgTime);
    void checkErrorCode(protocol::ErrorCode ec);
    void resetLastErrorCode();
    protocol::ErrorCode hasFatalError(protocol::ErrorCode ec) const;
    bool isValidateResponse(protocol::MessageResponse *response, protocol::ErrorCode ec) const;
    void clear();
    protocol::ErrorCode doSeekByTimestamp(int64_t timestamp);
    bool mayWaitForRetry(protocol::ErrorCode ec);

private:
    network::SwiftAdminAdapterPtr _adminAdapter;
    uint32_t _partitionId;
    std::vector<std::string> _requiredFieldNames;
    std::string _filedFilterDesc;
    SwiftReaderConfig _config;

    MessageReadBuffer _buffer;

    autil::ReadWriteLock _partStatusLock;
    SwiftPartitionStatus _partitionStatus;
    int64_t _lastRefreshMsgIdTime;
    SwiftTransportAdapter<TRT_GETMESSAGE> *_transportAdapter;
    SwiftTransportAdapter<TRT_GETMAXMESSAGEID> *_maxMsgIdTransportAdapter;
    SwiftTransportAdapter<TRT_GETMINMESSAGEIDBYTIME> *_msgIdTransportAdapter;
    int64_t _nextMsgId;
    int64_t _nextTimestamp;
    int64_t _seekTimestamp;
    int64_t _lastSuccessResponseTime;
    int64_t _lastReportErrorTime;
    uint64_t _curRequestUuid = 0;
    uint64_t _requestSeq = 0;
    protocol::ErrorCode _lastErrorCode;
    protocol::ErrorCode _errorCodeForCheck;
    SeekByTimestampRecord _seekByTimestampRecord;
    int64_t _timestampLimit;
    int64_t _lastMsgTimestamp;
    int32_t _lastMsgOffsetInMerge = 0;
    int64_t _lastCheckpointTimestamp;
    protocol::ClientVersionInfo _clientVersionInfo;
    protocol::ReaderInfo _readerInfo;
    std::function<protocol::ErrorCode(int64_t)> _func;
    int64_t _sessionId;
    int64_t _clientCommittedCheckpoint; // for client update checkpoint
    autil::ThreadMutex _mutex;
    int64_t _lastTopicVersion;
    autil::ThreadPoolPtr _decompressThreadPool;
    bool _sealedTopicReadFinish;
    bool _isTopicLongPollingEnabled;
    monitor::ClientMetricsReporter *_metricsReporter;
    kmonitor::MetricsTags _metricsTags;
    bool _checkpointReflushMode = true;
    uint32_t _rangeFrom = 0;
    uint32_t _rangeTo = 65535;

private:
    friend class SwiftSinglePartitionReaderTest;
    friend class SwiftReaderImplTest;
    friend class SwiftClientFactory;
    friend class FakeSwiftSinglePartitionReader;
    friend class FakeClientHelper;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(SwiftSinglePartitionReader);

} // namespace client
} // namespace swift
