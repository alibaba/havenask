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
#include "swift/util/MessageConverter.h"

#include <algorithm>
#include <assert.h>
#include <iosfwd>
#include <memory>
#include <string.h>

#include "autil/TimeUtility.h"
#include "flatbuffers/flatbuffers.h"
#include "swift/common/MemoryMessage.h"
#include "swift/common/MessageInfo.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/protocol/FBMessageWriter.h"
#include "swift/protocol/SwiftMessage.pb.h"
#include "swift/protocol/SwiftMessage_generated.h"
#include "swift/util/BlockPool.h"

using namespace autil;
using namespace std;
namespace swift {
namespace util {
AUTIL_LOG_SETUP(swift, MessageConverter);

using namespace swift::protocol;
using namespace swift::common;

MessageConverter::MessageConverter(BlockPool *pool) : _lastBlockWritePos(0), _partitionBlockPool(pool) {}

MessageConverter::~MessageConverter() {}

ErrorCode MessageConverter::encode(const protocol::Message &msg, MemoryMessage &memMsg) {
    const string &data = msg.data();
    size_t msgLen = data.length();
    ErrorCode errCode = prepareBlock(msgLen);
    if (errCode != ERROR_NONE) {
        return errCode;
    }
    memMsg.setBlock(_lastBlock);
    memMsg.setOffset(_lastBlockWritePos);
    memMsg.setLen(msgLen);
    memMsg.setTimestamp(msg.timestamp());
    memMsg.setPayload(msg.uint16payload());
    memMsg.setMaskPayload(msg.uint8maskpayload());
    memMsg.setMsgId(msg.msgid());
    memMsg.setCompress(msg.compress());
    if (msg.merged()) {
        memMsg.setMerged(true);
        if (msgLen >= sizeof(uint16_t)) {
            memMsg.setMsgCount(*(const uint16_t *)(msg.data().c_str()));
        } else {
            memMsg.setMsgCount(1);
        }
    } else {
        memMsg.setMsgCount(1);
    }
    writeMsg(data.c_str(), msgLen);
    return ERROR_NONE;
}

ErrorCode MessageConverter::encode(const protocol::flat::Message &msg, MemoryMessage &memMsg) {
    size_t msgLen = msg.data()->size();
    ErrorCode errCode = prepareBlock(msgLen);
    if (errCode != ERROR_NONE) {
        return errCode;
    }
    memMsg.setBlock(_lastBlock);
    memMsg.setOffset(_lastBlockWritePos);
    memMsg.setLen(msgLen);
    memMsg.setTimestamp(msg.timestamp());
    memMsg.setPayload(msg.uint16Payload());
    memMsg.setMaskPayload(msg.uint8MaskPayload());
    memMsg.setMsgId(msg.msgId());
    memMsg.setCompress(msg.compress());
    if (msg.merged()) {
        memMsg.setMerged(true);
        if (msgLen >= sizeof(uint16_t)) {
            memMsg.setMsgCount(*(const uint16_t *)(msg.data()->c_str()));
        } else {
            memMsg.setMsgCount(1);
        }
    } else {
        memMsg.setMsgCount(1);
    }
    writeMsg(msg.data()->c_str(), msgLen);
    return ERROR_NONE;
}

ErrorCode MessageConverter::encode(const MessageInfo &msg, ClientMemoryMessage &memMsg) {
    size_t msgLen = msg.data.size();
    ErrorCode errCode = prepareBlock(msgLen);
    if (errCode != ERROR_NONE) {
        return errCode;
    }
    memMsg.block = _lastBlock;
    memMsg.offset = _lastBlockWritePos;
    memMsg.msgLen = msgLen;
    memMsg.compress = msg.compress;
    memMsg.isMerged = msg.isMerged;
    memMsg.uint8Payload = msg.uint8Payload;
    memMsg.uint16Payload = msg.uint16Payload;
    memMsg.checkpointId = msg.checkpointId;
    writeMsg(msg.data.c_str(), msgLen);
    return ERROR_NONE;
}

ErrorCode MessageConverter::prepareBlock(int64_t msgLen) {
    if (!_lastBlock) {
        _lastBlock = _partitionBlockPool->allocate();
        if (!_lastBlock) {
            return ERROR_BROKER_BUSY;
        }
        _lastBlockWritePos = 0;
    }
    msgLen -= (_lastBlock->getSize() - _lastBlockWritePos);
    assert(!_lastBlock->getNext());
    BlockPtr block = _lastBlock;
    while (msgLen > 0) {
        block->setNext(_partitionBlockPool->allocate());
        block = block->getNext();
        if (!block) {
            _lastBlock->setNext(BlockPtr());
            return ERROR_BROKER_BUSY;
        } else {
            msgLen -= block->getSize();
        }
    }
    return ERROR_NONE;
}

void MessageConverter::writeMsg(const char *data, int64_t dataLen) {
    int64_t leftLen = dataLen;
    do {
        assert(_lastBlock);
        int64_t blockRemain = _lastBlock->getSize() - _lastBlockWritePos;
        int64_t count = std::min(blockRemain, leftLen);
        ::memcpy(_lastBlock->getBuffer() + _lastBlockWritePos, data + dataLen - leftLen, count);
        leftLen -= count;
        _lastBlockWritePos += count;
        if (_lastBlockWritePos == _lastBlock->getSize()) {
            _lastBlock = _lastBlock->getNext();
            _lastBlockWritePos = 0;
        }
    } while (leftLen > 0);
}

uint32_t MessageConverter::decode(const MemoryMessage &memMsg, protocol::Message &msg) {
    string buffer;
    return decode(memMsg, msg, buffer);
}

uint32_t MessageConverter::decode(const MemoryMessage &memMsg, protocol::Message &msg, string &data) {
    msg.set_msgid(memMsg.getMsgId());
    msg.set_uint16payload(memMsg.getPayload());
    msg.set_uint8maskpayload(memMsg.getMaskPayload());
    msg.set_timestamp(memMsg.getTimestamp());
    msg.set_compress(memMsg.isCompress());
    msg.set_merged(memMsg.isMerged());
    BlockPtr block = memMsg.getBlock();
    int64_t len = memMsg.getLen();
    int64_t offset = memMsg.getOffset();
    data.clear();
    data.reserve(len);
    while (len > 0) {
        assert(block);
        int64_t count = std::min(len, block->getSize() - offset);
        data.append(block->getBuffer() + offset, count);
        len -= count;
        offset = 0;
        block = block->getNext();
    }
    msg.set_data(data);
    if (memMsg.isMerged() && data.size() >= 2) {
        return *(uint16_t *)(data.c_str());
    } else {
        return 1;
    }
}

uint32_t MessageConverter::decode(const MemoryMessage &memMsg, FBMessageWriter *writer) {
    string buffer;
    return decode(memMsg, writer, buffer);
}

uint32_t MessageConverter::decode(const MemoryMessage &memMsg, FBMessageWriter *writer, string &data) {
    BlockPtr block = memMsg.getBlock();
    int64_t len = memMsg.getLen();
    int64_t offset = memMsg.getOffset();
    data.clear();
    data.reserve(len);
    while (len > 0) {
        assert(block);
        int64_t count = std::min(len, block->getSize() - offset);
        data.append(block->getBuffer() + offset, count);
        len -= count;
        if (len > 0) {
            offset = 0;
            block = block->getNext();
        }
    }
    writer->addMessage(data.c_str(),
                       data.size(),
                       memMsg.getMsgId(),
                       memMsg.getTimestamp(),
                       memMsg.getPayload(),
                       memMsg.getMaskPayload(),
                       memMsg.isCompress(),
                       memMsg.isMerged());
    if (memMsg.isMerged() && data.size() >= 2) {
        return *(uint16_t *)(data.c_str());
    } else {
        return 1;
    }
}

uint32_t MessageConverter::decode(const ClientMemoryMessage &memMsg, protocol::Message &msg, string &data) {
    msg.set_uint16payload(memMsg.uint16Payload);
    msg.set_uint8maskpayload(memMsg.uint8Payload);
    msg.set_compress(memMsg.compress);
    msg.set_merged(memMsg.isMerged);
    BlockPtr block = memMsg.block;
    int64_t len = memMsg.msgLen;
    int64_t offset = memMsg.offset;
    data.clear();
    data.reserve(len);
    while (len > 0) {
        assert(block);
        int64_t count = std::min(len, block->getSize() - offset);
        data.append(block->getBuffer() + offset, count);
        len -= count;
        offset = 0;
        block = block->getNext();
    }
    msg.set_data(data);
    if (memMsg.isMerged && data.size() >= 2) {
        return *(uint16_t *)(data.c_str());
    } else {
        return 1;
    }
}

uint32_t MessageConverter::decode(const ClientMemoryMessage &memMsg, FBMessageWriter *writer, string &data) {
    BlockPtr block = memMsg.block;
    int64_t len = memMsg.msgLen;
    int64_t offset = memMsg.offset;
    data.clear();
    data.reserve(len);
    while (len > 0) {
        assert(block);
        int64_t count = std::min(len, block->getSize() - offset);
        data.append(block->getBuffer() + offset, count);
        len -= count;
        if (len > 0) {
            offset = 0;
            block = block->getNext();
        }
    }
    writer->addMessage(
        data.c_str(), data.size(), 0, 0, memMsg.uint16Payload, memMsg.uint8Payload, memMsg.compress, memMsg.isMerged);
    if (memMsg.isMerged && data.size() >= 2) {
        return *(uint16_t *)(data.c_str());
    } else {
        return 1;
    }
}

uint32_t MessageConverter::decode(const ClientMemoryMessage &memMsg, MessageInfo &msgInfo) {
    // Note: no info to resume pid, hashStr, dataType
    msgInfo.compress = memMsg.compress;
    msgInfo.isMerged = memMsg.isMerged;
    msgInfo.uint8Payload = memMsg.uint8Payload;
    msgInfo.uint16Payload = memMsg.uint16Payload;
    msgInfo.checkpointId = memMsg.checkpointId;

    BlockPtr block = memMsg.block;
    int64_t len = memMsg.msgLen;
    int64_t offset = memMsg.offset;
    string data;
    data.reserve(len);
    while (len > 0) {
        assert(block);
        int64_t count = std::min(len, block->getSize() - offset);
        data.append(block->getBuffer() + offset, count);
        len -= count;
        offset = 0;
        block = block->getNext();
    }
    msgInfo.data.swap(data);
    if (memMsg.isMerged && msgInfo.data.size() >= 2) {
        return *(uint16_t *)(msgInfo.data.c_str());
    } else {
        return 1;
    }
}

uint32_t MessageConverter::decode(const Message &pbMsg, MessageInfo &msgInfo) {
    // Note: no info to resume pid, checkpoint, hashStr
    msgInfo.compress = pbMsg.compress();
    msgInfo.isMerged = pbMsg.merged();
    msgInfo.uint8Payload = pbMsg.uint8maskpayload();
    msgInfo.uint16Payload = pbMsg.uint16payload();
    msgInfo.dataType = pbMsg.datatype();
    msgInfo.data = pbMsg.data();

    if (msgInfo.isMerged && msgInfo.data.size() >= 2) {
        return *(uint16_t *)(msgInfo.data.c_str());
    } else {
        return 1;
    }
}

int64_t MessageConverter::remainDataSize() {
    if (_lastBlock != NULL) {
        return _lastBlock->getSize() - _lastBlockWritePos;
    } else {
        return 0;
    }
}

} // namespace util
} // namespace swift
