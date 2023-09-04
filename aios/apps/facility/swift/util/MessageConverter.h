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

#include <stdint.h>
#include <string>

#include "autil/Log.h"
#include "swift/common/Common.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/util/Block.h"

namespace swift {
namespace common {
class ClientMemoryMessage;
class MemoryMessage;
class MessageInfo;
} // namespace common
namespace protocol {
class FBMessageWriter;
class Message;
namespace flat {
struct Message;
} // namespace flat
} // namespace protocol
namespace util {
class BlockPool;
} // namespace util
} // namespace swift

namespace swift {
namespace util {

// convert between protocol::Message and MemoryMessage

class MessageConverter {
public:
    MessageConverter(BlockPool *partitionBlockPool);
    ~MessageConverter();

private:
    MessageConverter(const MessageConverter &);
    MessageConverter &operator=(const MessageConverter &);

public:
    protocol::ErrorCode encode(const protocol::Message &msg, common::MemoryMessage &memMsg);
    protocol::ErrorCode encode(const protocol::flat::Message &msg, common::MemoryMessage &memMsg);
    protocol::ErrorCode encode(const common::MessageInfo &msg, common::ClientMemoryMessage &memMsg);
    static uint32_t decode(const common::MemoryMessage &memMsg, protocol::Message &msg);
    static uint32_t decode(const common::MemoryMessage &memMsg, protocol::FBMessageWriter *writer);

    static uint32_t decode(const common::MemoryMessage &memMsg, protocol::Message &msg, std::string &buf);
    static uint32_t decode(const common::MemoryMessage &memMsg, protocol::FBMessageWriter *writer, std::string &buf);

    static uint32_t decode(const common::ClientMemoryMessage &memMsg, protocol::Message &msg, std::string &buf);
    static uint32_t
    decode(const common::ClientMemoryMessage &memMsg, protocol::FBMessageWriter *writer, std::string &buf);
    static uint32_t decode(const common::ClientMemoryMessage &memMsg, common::MessageInfo &msgInfo);
    static uint32_t decode(const protocol::Message &pbMsg, common::MessageInfo &msgInfo);

    int64_t remainDataSize();

private:
    protocol::ErrorCode prepareBlock(int64_t msgLen);
    void writeMsg(const char *data, int64_t dataLen);

private:
    BlockPtr _lastBlock;
    int64_t _lastBlockWritePos;
    BlockPool *_partitionBlockPool;
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(MessageConverter);

} // namespace util
} // namespace swift
