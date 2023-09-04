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
#include <vector>

#include "swift/common/MessageInfo.h"

namespace autil {
class ZlibCompressor;
} // namespace autil
namespace swift {
namespace protocol {
class WriteMessageInfo;
class WriteMessageInfoVec;
} // namespace protocol
} // namespace swift

namespace swift {
namespace util {

class MessageInfoUtil {
public:
    static common::MessageInfo constructMsgInfo(const protocol::WriteMessageInfo &pbMsgInfo);
    static std::vector<common::MessageInfo> constructMsgInfo(protocol::WriteMessageInfoVec &pbMsgInfoVec);
    static void constructMsgInfoSwapData(protocol::WriteMessageInfo &pbMsgInfo, common::MessageInfo &msgInfo);
    static void constructMsgInfoSwapData(protocol::WriteMessageInfoVec &pbMsgInfoVec,
                                         std::vector<common::MessageInfo> &msgInfos);

    static void mergeMessage(uint16_t hashId,
                             const std::vector<common::MessageInfo *> &msgVec,
                             uint64_t reserveLen,
                             common::MessageInfo &msgInfo);

    static void compressMessage(autil::ZlibCompressor *compressor, uint64_t threashold, common::MessageInfo &msgInfo);

    static common::MessageInfo constructMsgInfo(const std::string &data,
                                                int64_t checkpointId = -1,
                                                uint16_t uint16Payload = 0,
                                                uint8_t uint8Payload = 0,
                                                uint32_t pid = 0,
                                                const std::string &hashStr = "");
};

} // namespace util
} // namespace swift
