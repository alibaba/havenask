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
#include <stdint.h>
#include <string>
#include <unistd.h>
#include <vector>

#include "autil/CommonMacros.h"
#include "autil/Lock.h"
#include "autil/Log.h"
#include "autil/ThreadPool.h"
#include "swift/client/SwiftPartitionStatus.h"
#include "swift/client/SwiftReader.h"
#include "swift/client/SwiftReaderConfig.h"
#include "swift/client/SwiftReaderImpl.h"
#include "swift/client/trace/ReadMessageTracer.h"
#include "swift/common/Common.h"
#include "swift/common/SchemaReader.h"
#include "swift/network/SwiftAdminAdapter.h"
#include "swift/network/SwiftRpcChannelManager.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"

namespace swift {
namespace protocol {
class Message;
class Messages;
class ReaderProgress;
} // namespace protocol

namespace monitor {

class ClientMetricsReporter;
} // namespace monitor

namespace client {

class Notifier;

class SwiftReaderAdapter : public SwiftReader {
public:
    SwiftReaderAdapter(network::SwiftAdminAdapterPtr adminAdapter,
                       network::SwiftRpcChannelManagerPtr channelManager,
                       monitor::ClientMetricsReporter *reporter = nullptr);
    virtual ~SwiftReaderAdapter();

private:
    SwiftReaderAdapter(const SwiftReaderAdapter &);
    SwiftReaderAdapter &operator=(const SwiftReaderAdapter &);

public:
    protocol::ErrorCode init(const SwiftReaderConfig &config) override;
    protocol::ErrorCode read(int64_t &timeStamp, protocol::Messages &msgs, int64_t timeout = 3 * 1000000) override;
    protocol::ErrorCode read(int64_t &timeStamp, protocol::Message &msg, int64_t timeout = 3 * 1000000) override;
    protocol::ErrorCode seekByTimestamp(int64_t timeStamp, bool force = false) override;
    protocol::ErrorCode seekByProgress(const protocol::ReaderProgress &progress, bool force = false) override;
    void checkCurrentError(protocol::ErrorCode &errorCode, std::string &errorMsg) const override;
    SwiftPartitionStatus getSwiftPartitionStatus() override;
    void setRequiredFieldNames(const std::vector<std::string> &fieldNames) override;
    void setFieldFilterDesc(const std::string &fieldFilterDesc) override;
    void setTimestampLimit(int64_t timestampLimit, int64_t &acceptTimestamp) override;
    std::vector<std::string> getRequiredFieldNames() override;
    bool updateCommittedCheckpoint(int64_t checkpoint) override;
    void setDecompressThreadPool(const autil::ThreadPoolPtr &decompressThreadPool) override;
    common::SchemaReader *getSchemaReader(const char *data, protocol::ErrorCode &ec) override;
    protocol::ErrorCode getReaderProgress(protocol::ReaderProgress &progress) const override;
    int64_t getCheckpointTimestamp() const override;
    void setMessageTracer(ReadMessageTracerPtr msgTracer);
    void setNotifier(Notifier *notifier);

private:
    void setSwiftReader(SwiftReaderImplPtr swiftReader);
    SwiftReaderImplPtr getSwiftReader() const;
    protocol::ErrorCode doTopicChanged();
    template <typename T>
    protocol::ErrorCode readMsg(int64_t &timeStamp,
                                T &msg,
                                int64_t timeout = 3 * 1000000); // 3 s
    bool doSwitch();
    protocol::ErrorCode getNextPhysicTopicName(std::string &topicName);
    bool doGetNextPhysicTopicName(std::string &topicName);
    protocol::ErrorCode createNextPhysicReaderImpl(SwiftReaderImplPtr &retReaderImpl);
    protocol::ErrorCode createAndInitReaderImpl(const protocol::TopicInfo &tinfo, SwiftReaderImplPtr &retReaderImpl);
    protocol::ErrorCode seekPhysicTopic(int64_t timestamp);
    protocol::ErrorCode seekPhysicTopic(const protocol::ReaderProgress &progress);
    protocol::ErrorCode findPhysicName(int64_t timestamp, std::string &physicName);
    protocol::ErrorCode doFindPhysicName(int64_t timestamp, std::string &physicName, bool &isLastPhysic);
    protocol::ErrorCode updateReaderImpl(const std::string &topic);
    protocol::ErrorCode updateLogicTopicInfo();
    bool needResetReader(const protocol::TopicInfo &topicInfo, uint32_t partCnt);
    protocol::ErrorCode reseekNewReader(SwiftReaderImplPtr rawReader, SwiftReaderImplPtr newReader);
    bool validateReaderProgress(const protocol::ReaderProgress &readerProgress);

public:
    protocol::ErrorCode seekByMessageId(int64_t msgId) override;
    int64_t getNextMsgTimestamp() const;
    int64_t getMaxLastMsgTimestamp() const;
    protocol::ErrorCode getSchema(int32_t version, int32_t &retVersion, std::string &schema);
    const SwiftReaderConfig &getReaderConfig() const;
    bool checkAuthority(const protocol::TopicInfo &ti);

private:
    network::SwiftAdminAdapterPtr _adminAdapter;
    network::SwiftRpcChannelManagerPtr _channelManager;
    ReadMessageTracerPtr _msgTracer;
    SwiftReaderImplPtr _swiftReaderImpl;
    SwiftReaderConfig _readerConfig;
    int64_t _acceptTimestamp;
    mutable autil::ReadWriteLock _readerLock;
    autil::ThreadMutex _mutex;
    Notifier *_notifier;
    protocol::TopicInfo _topicInfo;
    monitor::ClientMetricsReporter *_reporter;

private:
    friend class SwiftClientFactory;
    friend class FakeClientHelper;

private:
    AUTIL_LOG_DECLARE();
};

template <typename T>
protocol::ErrorCode SwiftReaderAdapter::readMsg(int64_t &timeStamp, T &msg, int64_t timeout) {
    SwiftReaderImplPtr reader = getSwiftReader();
    if (unlikely(reader->isTopicChanged())) {
        protocol::ErrorCode ec = doTopicChanged();
        if (ec != protocol::ERROR_NONE) {
            AUTIL_LOG(WARN, "doTopicChanged error[%s]", ErrorCode_Name(ec).c_str());
            return ec;
        }
        reader = getSwiftReader();
    }
    auto ec = reader->read(timeStamp, msg, timeout);
    if (protocol::ERROR_SEALED_TOPIC_READ_FINISH == ec &&
        (protocol::TOPIC_TYPE_LOGIC_PHYSIC == _topicInfo.topictype() ||
         protocol::TOPIC_TYPE_LOGIC == _topicInfo.topictype())) {
        while (protocol::ERROR_SEALED_TOPIC_READ_FINISH == ec) {
            // assume logic topic cannot seal
            doSwitch();
            reader = getSwiftReader();
            ec = reader->read(timeStamp, msg, timeout);
            if (protocol::ERROR_NONE != ec) {
                usleep(500 * 1000);
                timeout -= 500 * 1000;
            }
            if (timeout <= 0) {
                if (protocol::ERROR_SEALED_TOPIC_READ_FINISH == ec) {
                    ec = protocol::ERROR_CLIENT_PHYSIC_TOPIC_SWITCH_NOT_READY;
                }
                break;
            }
        }
    }
    if (_msgTracer) {
        _msgTracer->tracingMsg(msg);
    }
    return ec;
}

SWIFT_TYPEDEF_PTR(SwiftReaderAdapter);

} // namespace client
} // namespace swift
