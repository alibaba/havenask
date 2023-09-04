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
#include <vector>

#include "autil/Log.h"
#include "swift/common/FileMessageMeta.h"
#include "swift/util/Block.h"

namespace swift {
namespace common {

class MemoryMessage {
public:
    MemoryMessage() : _msgId(-1), _lenExtend(false), _len(0) {}
    ~MemoryMessage() {}

public:
    void setBlock(const util::BlockPtr &block) { _block = block; }
    void setOffset(uint64_t value) { _meta.endPos |= value; }
    void setLen(uint64_t value) {
        _len = value & 0x7fff;
        value = value >> 15;
        if (value > 0) {
            _lenExtend = true;
            _meta.msgLenHighPart = value;
        }
    }
    void setMsgId(int64_t msgId) { _msgId = msgId; }
    void setTimestamp(uint64_t timestamp) { _meta.timestamp = timestamp; }
    void setPayload(uint16_t payload) { _meta.payload = payload; }
    void setMaskPayload(uint8_t maskPayload) { _meta.maskPayload = maskPayload; }
    void setCompress(bool compress) { _meta.isCompress = compress; }
    void setMerged(bool merged) { _meta.isMerged = merged; }
    void setMsgCount(uint64_t count) { _meta.endPos |= count << 25; }
    int64_t getMsgId() const { return _msgId; }
    uint64_t getTimestamp() const { return _meta.timestamp; }
    uint64_t getLen() const {
        if (!_lenExtend) {
            return _len;
        } else {
            int64_t len = _meta.msgLenHighPart;
            len = (len << 15) | _len;
            return len;
        }
    }
    uint64_t getOffset() const { return _meta.endPos & 0x1ffffff; } // 25 bit
    uint16_t getPayload() const { return _meta.payload; }
    uint8_t getMaskPayload() const { return _meta.maskPayload; }
    const util::BlockPtr &getBlock() const { return _block; }
    bool isCompress() const { return _meta.isCompress; }
    bool isMerged() const { return _meta.isMerged; }
    uint16_t getMsgCount() const { return (_meta.endPos >> 25) & 1023; }
    FileMessageMeta getFileMeta() const {
        FileMessageMeta meta = _meta;
        meta.msgLenHighPart = 0;
        meta.endPos = 0;
        return meta;
    }

private:
    int64_t _msgId  : 48;
    bool _lenExtend : 1; // if len > 2^15, the high part in meta
    uint64_t _len   : 15;
    FileMessageMeta _meta;
    util::BlockPtr _block;

private:
    AUTIL_LOG_DECLARE();
};

const int64_t MEMORY_MESSAGE_META_SIZE = sizeof(MemoryMessage);
typedef std::vector<MemoryMessage> MemoryMessageVector;

class ClientMemoryMessage {
public:
    ClientMemoryMessage()
        : compress(false), isMerged(false), uint8Payload(0), uint16Payload(0), offset(0), msgLen(0), checkpointId(0) {}
    ~ClientMemoryMessage() {}

public:
    bool compress;
    bool isMerged;
    uint8_t uint8Payload;
    uint16_t uint16Payload;
    uint32_t offset;
    uint32_t msgLen;
    int64_t checkpointId;
    util::BlockPtr block;
};

} // namespace common
} // namespace swift
