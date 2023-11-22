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

#include <list>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/Lock.h"
#include "autil/Log.h"
#include "autil/ThreadPool.h"
#include "swift/common/Common.h"
#include "swift/protocol/BrokerRequestResponse.pb.h"
#include "swift/protocol/FBMessageReader.h"
#include "swift/protocol/SwiftMessage.pb.h"
#include "swift/protocol/TraceMessage.pb.h"
#include "swift/util/Atomic.h"

namespace swift {
namespace common {
struct MergedMessageMeta;
} // namespace common
namespace filter {
class FieldFilter;
class MessageFilter;
} // namespace filter
namespace protocol {
namespace flat {
struct Message;
} // namespace flat
} // namespace protocol

namespace client {
class Notifier;

class SingleResponse {
private:
    const protocol::MessageResponse *_response;
    int64_t _readPos;
    int64_t _msgCount;
    int64_t _totalMsgCount;
    uint64_t _requestUuid;
    protocol::MessageFormat _msgFormat;
    protocol::FBMessageReader _reader;
    protocol::ReaderInfo _readerInfo;
    bool _needDecompress;
    Notifier *_notifier;

public:
    SingleResponse(const protocol::MessageResponse *response);
    bool init();
    ~SingleResponse();

public:
    bool read(protocol::Message &msg);

    uint64_t getRequestUuid() const { return _requestUuid; }
    int64_t getMsgCount() const { return _msgCount; }
    int64_t getTotalMsgCount() const { return _totalMsgCount; }
    int64_t getFirstMsgTimestamp() const;
    void setReaderInfo(const protocol::ReaderInfo &readerInfo);
    bool waitDecompressDone();
    bool needDecompress() { return _needDecompress; }
    Notifier *getNotifier() { return _notifier; }

private:
    void convertFB2PBMsg(const protocol::flat::Message &fbMsgbMsg, protocol::Message &msg);

private:
    AUTIL_LOG_DECLARE();
};

class MessageReadBuffer {
public:
    MessageReadBuffer();
    ~MessageReadBuffer();

private:
    MessageReadBuffer(const MessageReadBuffer &);
    MessageReadBuffer &operator=(const MessageReadBuffer &);

public:
    bool addResponse(protocol::MessageResponse *response);
    bool read(protocol::Message &msg);
    void clear();
    int64_t getUnReadMsgCount();
    int64_t seek(int64_t msgid);
    int64_t getFirstMsgTimestamp();
    void updateFilter(const protocol::Filter &filter,
                      const std::vector<std::string> &requiredFileds,
                      const std::string &filterDesc);
    void setTopicName(const std::string &topicName);
    void setPartitionId(uint32_t partitionId);
    void setDecompressThreadPool(autil::ThreadPoolPtr decompressThreadPool);
    void setReaderInfo(const protocol::ReaderInfo &readerInfo);

private:
    SingleResponse *getNextResponse();
    bool doRead(protocol::Message &msg);
    bool unpackMessage(const protocol::Message &msg, int32_t &totalCount);
    bool fillUnpackMessage(const protocol::Message &msg,
                           const common::MergedMessageMeta *metaVec,
                           uint16_t msgCount,
                           const char *dataPart,
                           size_t dataLen);
    bool fillMessage();

private:
    mutable autil::ThreadMutex _mutex;
    SingleResponse *_frontResponse;
    std::list<SingleResponse *> _responseList;
    swift::util::Atomic32 _unReadMsgCount;
    std::vector<protocol::Message> _unpackMsgVec;
    size_t _readOffset;
    swift::filter::MessageFilter *_metaFilter;
    swift::filter::FieldFilter *_fieldFilter;
    std::string _topicName;
    uint32_t _partitionId;
    autil::ThreadPoolPtr _decompressThreadPool;
    protocol::ReaderInfo _readerInfo;

private:
    friend class MessageReadBufferTest;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(MessageReadBuffer);

} // namespace client
} // namespace swift
