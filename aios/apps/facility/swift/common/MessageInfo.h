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

#include "swift/protocol/SwiftMessage.pb.h"

namespace swift {
namespace common {
// old writer write MessageInfo
class MessageInfo {
public:
    MessageInfo()
        : compress(false)
        , isMerged(false)
        , uint8Payload(0)
        , uint16Payload(0)
        , pid(0)
        , checkpointId(0)
        , dataType(protocol::DATA_TYPE_UNKNOWN) {}
    MessageInfo(const std::string &dataStr, uint8_t mask, uint16_t payload, int64_t id)
        : compress(false)
        , isMerged(false)
        , uint8Payload(mask)
        , uint16Payload(payload)
        , pid(0)
        , checkpointId(id)
        , data(dataStr)
        , dataType(protocol::DATA_TYPE_UNKNOWN) {}
    ~MessageInfo() {}

public:
    void initAndSwapData(MessageInfo &msgInfo) {
        compress = msgInfo.compress;
        isMerged = msgInfo.isMerged;
        uint16Payload = msgInfo.uint16Payload;
        uint8Payload = msgInfo.uint8Payload;
        checkpointId = msgInfo.checkpointId;
        data.swap(msgInfo.data);
    }

public:
    bool compress : 1;
    bool isMerged : 1;
    uint8_t uint8Payload;
    uint16_t uint16Payload;
    uint32_t pid;
    int64_t checkpointId;
    std::string data;
    std::string hashStr;
    protocol::DataType dataType;
};

} // namespace common
} // namespace swift
