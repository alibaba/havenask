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

#include <stddef.h>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/Lock.h"
#include "autil/Log.h"
#include "autil/TimeUtility.h"
#include "swift/client/SwiftPartitionStatus.h"
#include "swift/client/SwiftReader.h"
#include "swift/client/SwiftReaderConfig.h"
#include "swift/common/Common.h"
#include "swift/network/SwiftAdminAdapter.h"
#include "swift/network/SwiftRpcChannelManager.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/protocol/SwiftMessage.pb.h"

namespace swift {

namespace common {
class SchemaReader;
} // namespace common

namespace protocol {
class TopicInfo;
} // namespace protocol

namespace monitor {
class ClientMetricsReporter;
} // namespace monitor

namespace client {

class Notifier;
class SwiftSinglePartitionReader;

class SwiftReaderImpl : public SwiftReader {
public:
public:
    SwiftReaderImpl(const network::SwiftAdminAdapterPtr &adminAdapter,
                    const network::SwiftRpcChannelManagerPtr &channelManager,
                    const protocol::TopicInfo &topicInfo,
                    Notifier *notifier = NULL,
                    std::string logicTopicName = std::string(),
                    monitor::ClientMetricsReporter *reporter = nullptr);
    ~SwiftReaderImpl();

private:
    SwiftReaderImpl(const SwiftReaderImpl &);
    SwiftReaderImpl &operator=(const SwiftReaderImpl &);

public:
    protocol::ErrorCode init(const SwiftReaderConfig &config) override;
    protocol::ErrorCode read(int64_t &timeStamp, protocol::Messages &msgs, int64_t timeout = 3 * 1000000) override;
    protocol::ErrorCode read(int64_t &timeStamp, protocol::Message &msg, int64_t timeout = 3 * 1000000) override;
    protocol::ErrorCode seekByTimestamp(int64_t timeStamp, bool force = false) override;
    protocol::ErrorCode seekByProgress(const protocol::ReaderProgress &progress, bool force = false) override;
    SwiftPartitionStatus getSwiftPartitionStatus() override;

    void checkCurrentError(protocol::ErrorCode &errorCode, std::string &errorMsg) const override;
    void setRequiredFieldNames(const std::vector<std::string> &fieldNames) override;
    void setFieldFilterDesc(const std::string &fieldFilterDesc) override;
    void setTimestampLimit(int64_t timestampLimit, int64_t &acceptTimestamp) override;
    std::vector<std::string> getRequiredFieldNames() override;
    bool updateCommittedCheckpoint(int64_t checkpoint) override;
    common::SchemaReader *getSchemaReader(const char *data, protocol::ErrorCode &ec) override;
    protocol::ErrorCode getReaderProgress(protocol::ReaderProgress &progress) const override;
    int64_t getCheckpointTimestamp() const override;

public:
    protocol::ErrorCode setTopicChanged(int64_t topicVersion);
    int64_t getMaxLastMsgTimestamp() const;
    bool isTopicChanged() { return _isTopicChanged; }
    const std::string getTopicName() const { return _config.topicName; }
    uint32_t getPartitionCount() const { return _partitionCount; }
    void resetTopicChanged() { _isTopicChanged = false; }
    int64_t getTopicVersion() const { return _topicVersion; }
    void setTopicLongPollingEnabled(bool enable);

public:
    protocol::ErrorCode seekByMessageId(int64_t msgId) override;
    int64_t getNextMsgTimestamp() const;

private:
    protocol::ErrorCode reportFatalError();
    int64_t getUnReadMsgCount() const;
    bool tryRead(protocol::Messages &msg);
    protocol::ErrorCode fillBuffer(int64_t timeout);
    int64_t getFirstMsgTimestamp() const;
    protocol::ErrorCode fillReaderStatus();
    protocol::ErrorCode checkTimestampLimit();
    protocol::ErrorCode doBatchRead(int64_t &timeStamp, protocol::Messages &msgs, int64_t timeout);
    void initReaders(const std::vector<uint32_t> &readPartVec, int32_t partCount);
    void clearReaders();
    int64_t tryFillBuffer(int64_t currentTime = autil::TimeUtility::currentTime());
    bool hasUnReadMsg() const;
    protocol::ErrorCode sealedTopicReadFinish(protocol::ErrorCode ec);
    int64_t getMinCheckpointTimestamp() const;

protected:
    network::SwiftAdminAdapterPtr _adminAdapter;
    std::string _logicTopicName;
    SwiftReaderConfig _config;

private:
    network::SwiftRpcChannelManagerPtr _channelManager;
    mutable autil::ThreadMutex _mutex;
    mutable autil::ThreadMutex _statusMutex;
    std::vector<SwiftSinglePartitionReader *> _readers;
    std::vector<bool> _exceedLimitVec;
    bool _timeLimitMode;
    std::vector<int64_t> _timestamps;
    int64_t _checkpointTimestamp;
    Notifier *_notifier;
    bool _needFillAll;
    uint32_t _lastReadIndex;
    std::vector<std::string> _fieldNames;
    std::string _fieldFilterDesc;
    protocol::Messages _messageBuffer;
    int32_t _bufferCursor;
    uint32_t _bufferReadIndex;
    bool _isBufferRead;
    volatile bool _isTopicChanged;
    uint32_t _partitionCount;
    bool _ownNotifier;
    int64_t _topicVersion;
    bool _isTopicLongPollingEnabled;
    monitor::ClientMetricsReporter *_reporter;

private:
    friend class SwiftReaderImplTest;
    friend class SwiftClientFactory;
    friend class FakeClientHelper;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(SwiftReaderImpl);

} // namespace client
} // namespace swift
