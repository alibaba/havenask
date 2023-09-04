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

#include "swift/common/FileMessageMeta.h"
#include "swift/common/MemoryMessage.h"
#include "swift/protocol/SwiftMessage.pb.h"
#include "swift/protocol/SwiftMessage_generated.h"

namespace swift {
namespace protocol {

template <typename T>
struct MessageAdapter {
    static int64_t getMsgId(const T &msg);
    static int64_t getTimestamp(const T &msg);
    static void fillMeta(const T &msg, common::FileMessageMeta &meta);
    static size_t fillData(const T &msg, std::string &buf);
    static uint32_t getPayload(const T &msg);
    static uint32_t getMask(const T &msg);
    static size_t getMsgLen(const T &msg);
};

template <>
struct MessageAdapter<common::MemoryMessage> {
    static int64_t getMsgId(const common::MemoryMessage &msg) { return msg.getMsgId(); }
    static int64_t getTimestamp(const common::MemoryMessage &msg) { return msg.getTimestamp(); }
    static void fillMeta(const common::MemoryMessage &msg, common::FileMessageMeta &meta) { meta = msg.getFileMeta(); }
    static size_t fillData(const common::MemoryMessage &msg, std::string &buf) {
        int64_t msgLen = msg.getLen();
        int64_t leftLen = msgLen;
        auto block = msg.getBlock();
        int64_t offset = msg.getOffset();
        while (leftLen > 0) {
            int64_t count = std::min(leftLen, block->getSize() - offset);
            buf.append(block->getBuffer() + offset, count);
            offset += count;
            leftLen -= count;
            if (offset == block->getSize()) {
                block = block->getNext();
                offset = 0;
            }
        }
        return msgLen;
    }
    static uint32_t getPayload(const common::MemoryMessage &msg) { return msg.getPayload(); }
    static uint32_t getMask(const common::MemoryMessage &msg) { return msg.getMaskPayload(); }
    static size_t getMsgLen(const common::MemoryMessage &msg) { return msg.getLen(); }
};

template <>
struct MessageAdapter<protocol::Message> {
    static int64_t getMsgId(const protocol::Message &msg) { return msg.msgid(); }
    static int64_t getTimestamp(const protocol::Message &msg) { return msg.timestamp(); }
    static void fillMeta(const protocol::Message &msg, common::FileMessageMeta &meta) {
        meta.timestamp = msg.timestamp();
        meta.isMerged = msg.merged();
        meta.isCompress = msg.compress();
        meta.maskPayload = msg.uint8maskpayload();
        meta.payload = msg.uint16payload();
    }
    static size_t fillData(const protocol::Message &msg, std::string &buf) {
        buf.append(msg.data());
        return msg.data().size();
    }
    static uint32_t getPayload(const protocol::Message &msg) { return msg.uint16payload(); }
    static uint32_t getMask(const protocol::Message &msg) { return msg.uint8maskpayload(); }
    static size_t getMsgLen(const protocol::Message &msg) { return msg.data().size(); }
};

template <>
struct MessageAdapter<protocol::flat::Message> {
    static int64_t getMsgId(const protocol::flat::Message &msg) { return msg.msgId(); }
    static int64_t getTimestamp(const protocol::flat::Message &msg) { return msg.timestamp(); }
    static void fillMeta(const protocol::flat::Message &msg, common::FileMessageMeta &meta) {
        meta.timestamp = msg.timestamp();
        meta.isMerged = msg.merged();
        meta.isCompress = msg.compress();
        meta.maskPayload = msg.uint8MaskPayload();
        meta.payload = msg.uint16Payload();
    }
    static size_t fillData(const protocol::flat::Message &msg, std::string &buf) {
        size_t len = msg.data()->size();
        buf.append(msg.data()->c_str(), len);
        return len;
    }
    static uint32_t getPayload(const protocol::flat::Message &msg) { return msg.uint16Payload(); }
    static uint32_t getMask(const protocol::flat::Message &msg) { return msg.uint8MaskPayload(); }
    static size_t getMsgLen(const protocol::flat::Message &msg) { return msg.data()->size(); }
};

template <typename ContainerType>
class MessageIterator {
public:
    using MsgType = typename ContainerType::value_type;
    using ConstIterator = typename ContainerType::const_iterator;

public:
    explicit MessageIterator(const ContainerType &container) : _cursor(container.begin()), _end(container.end()) {}

public:
    bool hasNext() const { return _cursor != _end; }

    const MsgType &next() { return *_cursor++; }

public:
    ConstIterator _cursor;
    ConstIterator _end;
};

// specialize for flat message
template <>
class MessageIterator<swift::protocol::flat::Messages> {
public:
    using MsgType = swift::protocol::flat::Message;

public:
    explicit MessageIterator(const swift::protocol::flat::Messages &msgs) : _msgs(msgs.msgs()), _cursor(0) {}

public:
    bool hasNext() const { return _msgs != nullptr && _cursor < _msgs->size(); }

    const MsgType &next() { return *_msgs->Get(_cursor++); }

private:
    const flatbuffers::Vector<flatbuffers::Offset<swift::protocol::flat::Message>> *_msgs;
    size_t _cursor = 0;
};

} // namespace protocol
} // namespace swift
