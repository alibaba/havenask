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
#include "swift/util/MessageInfoUtil.h"

#include <algorithm>
#include <cstddef>
#include <memory>

#include "autil/ZlibCompressor.h"
#include "swift/common/MessageMeta.h"
#include "swift/protocol/MessageCompressor.h"
#include "swift/protocol/SwiftMessage.pb.h"

using namespace autil;
using namespace std;
using namespace swift::protocol;
using namespace swift::common;

namespace swift {
namespace util {

MessageInfo MessageInfoUtil::constructMsgInfo(const protocol::WriteMessageInfo &pbMsgInfo) {
    MessageInfo msgInfo;
    msgInfo.compress = pbMsgInfo.compress();
    msgInfo.uint16Payload = (uint16_t)pbMsgInfo.uint16payload();
    msgInfo.uint8Payload = (uint8_t)pbMsgInfo.uint8payload();
    if (pbMsgInfo.has_pid()) {
        msgInfo.pid = pbMsgInfo.pid();
    }
    if (pbMsgInfo.has_checkpointid()) {
        msgInfo.checkpointId = pbMsgInfo.checkpointid();
    }
    if (pbMsgInfo.has_data()) {
        msgInfo.data = pbMsgInfo.data();
    }
    if (pbMsgInfo.has_hashstr()) {
        msgInfo.hashStr = pbMsgInfo.hashstr();
    }
    return msgInfo;
}

vector<MessageInfo> MessageInfoUtil::constructMsgInfo(protocol::WriteMessageInfoVec &pbMsgInfoVec) {
    vector<MessageInfo> messageInfoVec;
    for (int i = 0; i < pbMsgInfoVec.messageinfovec_size(); i++) {
        const protocol::WriteMessageInfo &pbMsgInfo = pbMsgInfoVec.messageinfovec(i);
        messageInfoVec.push_back(constructMsgInfo(pbMsgInfo));
    }
    return messageInfoVec;
}

void MessageInfoUtil::constructMsgInfoSwapData(protocol::WriteMessageInfo &pbMsgInfo, MessageInfo &msgInfo) {
    msgInfo.compress = pbMsgInfo.compress();
    msgInfo.uint16Payload = (uint16_t)pbMsgInfo.uint16payload();
    msgInfo.uint8Payload = (uint8_t)pbMsgInfo.uint8payload();
    msgInfo.dataType = pbMsgInfo.datatype();
    if (pbMsgInfo.has_pid()) {
        msgInfo.pid = pbMsgInfo.pid();
    }
    if (pbMsgInfo.has_checkpointid()) {
        msgInfo.checkpointId = pbMsgInfo.checkpointid();
    }
    if (pbMsgInfo.has_data()) {
        pbMsgInfo.mutable_data()->swap(msgInfo.data);
    }
    if (pbMsgInfo.has_hashstr()) {
        pbMsgInfo.mutable_hashstr()->swap(msgInfo.hashStr);
    }
}

void MessageInfoUtil::constructMsgInfoSwapData(protocol::WriteMessageInfoVec &pbMsgInfoVec,
                                               std::vector<MessageInfo> &msgInfos) {
    int msgCount = pbMsgInfoVec.messageinfovec_size();
    msgInfos.resize(msgCount);
    for (int i = 0; i < msgCount; i++) {
        protocol::WriteMessageInfo *pbMsgInfo = pbMsgInfoVec.mutable_messageinfovec(i);
        constructMsgInfoSwapData(*pbMsgInfo, msgInfos[i]);
    }
}

void MessageInfoUtil::mergeMessage(uint16_t hashId,
                                   const vector<MessageInfo *> &msgVec,
                                   uint64_t reserveLen,
                                   MessageInfo &msgInfo) {
    size_t count = msgVec.size();
    vector<common::MergedMessageMeta> metaVec(count);
    string allData;
    allData.reserve(reserveLen + sizeof(common::MergedMessageMeta) * count + sizeof(uint16_t));
    uint16_t countU16 = uint16_t(count);
    allData.append((char *)(&countU16), sizeof(uint16_t));
    uint8_t uint8payload = 0;
    int64_t endPos = 0;
    for (size_t i = 0; i < count; ++i) {
        metaVec[i].isCompress = msgVec[i]->compress;
        metaVec[i].maskPayload = msgVec[i]->uint8Payload;
        metaVec[i].payload = msgVec[i]->uint16Payload;
        endPos += msgVec[i]->data.size();
        metaVec[i].endPos = endPos;
        uint8payload |= msgVec[i]->uint8Payload;
    }
    allData.append((char *)(&metaVec[0]), sizeof(common::MergedMessageMeta) * count);
    for (size_t i = 0; i < count; ++i) {
        allData.append(msgVec[i]->data);
    }
    msgInfo.data.swap(allData);
    msgInfo.uint8Payload = uint8payload;
    msgInfo.uint16Payload = hashId;
    msgInfo.checkpointId = msgVec[0]->checkpointId;
    msgInfo.isMerged = true;
    msgInfo.compress = false;
}

void MessageInfoUtil::compressMessage(autil::ZlibCompressor *compressor, uint64_t threashold, MessageInfo &msgInfo) {
    if (compressor && !msgInfo.compress) {
        string compressedData;
        if (msgInfo.isMerged) {
            if (MessageCompressor::compressMergedMessage(
                    compressor, threashold, msgInfo.data.c_str(), msgInfo.data.size(), compressedData)) {
                msgInfo.compress = true;
                msgInfo.data.swap(compressedData);
            }
        } else {
            if ((uint64_t)msgInfo.data.size() > threashold) {
                compressor->reset();
                compressor->addDataToBufferIn(msgInfo.data);
                if (compressor->compress()) {
                    if (compressor->getBufferOutLen() < msgInfo.data.size()) {
                        msgInfo.compress = true;
                        msgInfo.data.clear();
                        msgInfo.data.append(compressor->getBufferOut(), compressor->getBufferOutLen());
                    }
                }
            }
        }
    }
}

MessageInfo MessageInfoUtil::constructMsgInfo(const string &data,
                                              int64_t checkpointId,
                                              uint16_t uint16Payload,
                                              uint8_t uint8Payload,
                                              uint32_t pid,
                                              const string &hashStr) {
    MessageInfo msgInfo;
    msgInfo.pid = pid;
    msgInfo.data = data;
    msgInfo.checkpointId = checkpointId;
    msgInfo.uint16Payload = uint16Payload;
    msgInfo.uint8Payload = uint8Payload;
    msgInfo.hashStr = hashStr;
    return msgInfo;
}

} // namespace util
} // namespace swift
