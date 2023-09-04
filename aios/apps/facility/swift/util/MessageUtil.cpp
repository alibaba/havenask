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
#include "swift/util/MessageUtil.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <utility>

#include "autil/TimeUtility.h"
#include "autil/ZlibCompressor.h"
#include "swift/common/FieldSchema.h"
#include "swift/common/MessageInfo.h"
#include "swift/common/MessageMeta.h"
#include "swift/common/RangeUtil.h"
#include "swift/filter/FieldFilter.h"
#include "swift/filter/MessageFilter.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/MessageCompressor.h"
#include "swift/protocol/SwiftMessage.pb.h"
#include "swift/util/MessageConverter.h"

using namespace swift::filter;
using namespace swift::common;
using namespace swift::protocol;
using namespace std;
using namespace autil;

namespace swift {
namespace util {
AUTIL_LOG_SETUP(swift, MessageUtil);

MessageUtil::MessageUtil() {}

MessageUtil::~MessageUtil() {}

bool MessageUtil::unpackMessage(ZlibCompressor *compressor,
                                const char *allData,
                                size_t allSize,
                                int64_t msgid,
                                int64_t timestamp,
                                const MessageFilter *metaFilter,
                                FieldFilter *fieldFilter,
                                std::vector<protocol::Message> &unpackMsgVec,
                                int32_t &totalCount) {
    string uncompressData;
    if (compressor) {
        compressor->reset();
        if (MessageCompressor::decompressMergedMessage(compressor, allData, allSize, uncompressData)) {
            allData = uncompressData.c_str();
            allSize = uncompressData.size();
        } else {
            return false;
        }
    }
    if (allSize < sizeof(uint16_t)) {
        return false;
    }
    uint16_t msgCount = *((const uint16_t *)allData);
    if (msgCount == 0) {
        return false;
    }
    const MergedMessageMeta *metaVec = (const MergedMessageMeta *)(allData + sizeof(uint16_t));
    size_t metaLen = msgCount * sizeof(MergedMessageMeta) + sizeof(uint16_t);
    if (allSize < metaLen) {
        return false;
    }
    const char *dataPart = allData + metaLen;
    size_t dataLen = allSize - metaLen;
    if (unpackMessage(msgid, timestamp, metaVec, msgCount, dataPart, dataLen, metaFilter, fieldFilter, unpackMsgVec)) {
        totalCount = msgCount;
        return true;
    } else {
        return false;
    }
}

bool MessageUtil::unpackMessage(int64_t msgid,
                                int64_t timestamp,
                                const MergedMessageMeta *metaVec,
                                uint16_t msgCount,
                                const char *dataPart,
                                size_t dataLen,
                                const MessageFilter *metaFilter,
                                FieldFilter *fieldFilter,
                                vector<Message> &msgVec) {
    uint64_t dataBeginPos = 0;
    int32_t offset = 0;
    msgVec.clear();
    msgVec.resize(msgCount);
    for (size_t i = 0; i < msgCount; ++i) {
        if (metaVec[i].endPos > dataLen) {
            msgVec.clear();
            return false;
        }
        if (metaFilter != NULL && !metaFilter->filter(metaVec[i])) {
            dataBeginPos = metaVec[i].endPos;
            continue;
        }
        Message &oneMsg = msgVec[offset];
        if (fieldFilter != NULL) {
            oneMsg.mutable_data()->clear();
            if (!fieldFilter->filter(
                    dataPart + dataBeginPos, metaVec[i].endPos - dataBeginPos, *oneMsg.mutable_data())) {
                dataBeginPos = metaVec[i].endPos;
                continue;
            }
        } else {
            oneMsg.set_data(dataPart + dataBeginPos, metaVec[i].endPos - dataBeginPos);
        }
        oneMsg.set_msgid(msgid);
        oneMsg.set_timestamp(timestamp);
        oneMsg.set_uint16payload(metaVec[i].payload);
        oneMsg.set_uint8maskpayload(metaVec[i].maskPayload);
        oneMsg.set_compress(metaVec[i].isCompress);
        oneMsg.set_merged(false);
        oneMsg.set_offsetinrawmsg(OFFSET_IN_MERGE_MSG_BASE + i);

        if (oneMsg.compress()) {
            string uncompressData;
            if (MessageCompressor::uncompressData(oneMsg.data(), uncompressData)) {
                oneMsg.set_data(uncompressData);
                oneMsg.set_compress(false);
            } else {
                AUTIL_LOG(ERROR,
                          "decompress one merged msg[%ld/%d %ld %ld %u %s] fail",
                          i,
                          msgCount,
                          msgid,
                          timestamp,
                          oneMsg.uint16payload(),
                          oneMsg.data().c_str());
            }
        }
        dataBeginPos = metaVec[i].endPos;
        offset++;
    }
    msgVec.resize(offset);
    return true;
}

bool MessageUtil::unpackMessage(const common::MessageInfo &inMsg, std::vector<common::MessageInfo> &outMsgs) {
    vector<protocol::Message> unpackMsgVec;
    int32_t totalCount = 0;
    bool ret = unpackMessage(NULL, inMsg.data.c_str(), inMsg.data.size(), -1, -1, NULL, NULL, unpackMsgVec, totalCount);
    if (!ret) {
        return false;
    }
    for (const auto &unMsg : unpackMsgVec) {
        MessageInfo info = inMsg; // reserve checkpoint
        MessageConverter::decode(unMsg, info);
        outMsgs.emplace_back(info);
    }
    return true;
}

void MessageUtil::mergeMessage(const std::vector<Message> &mmv,
                               common::RangeUtil *rangeUtil,
                               uint32_t mergeCount,
                               vector<Message> &mergedVec) {
    int64_t startId = 0;
    map<uint16_t, vector<Message>> msgMap;
    uint16_t hashId;
    for (size_t i = 0; i < mmv.size(); i++) {
        const Message &msg = mmv[i];
        hashId = rangeUtil->getMergeHashId(msg.uint16payload());
        vector<Message> &msgVec = msgMap[hashId];
        msgVec.push_back(msg);
        if (msgVec.size() >= mergeCount) {
            Message mergedMsg;
            doMergeMessage(hashId, msgVec, mergedMsg);
            mergedMsg.set_msgid(startId++);
            mergedMsg.set_timestamp(startId);
            mergedVec.push_back(mergedMsg);
            msgVec.clear();
        }
    }
    map<uint16_t, vector<Message>>::iterator iter = msgMap.begin();
    for (; iter != msgMap.end(); iter++) {
        std::vector<Message> &msgVec = iter->second;
        if (msgVec.empty()) {
            continue;
        }
        if (msgVec.size() == 1) {
            msgVec[0].set_msgid(startId++);
            msgVec[0].set_timestamp(startId);
            mergedVec.push_back(msgVec[0]);
        } else {
            Message mergedMsg;
            uint16_t hashId = iter->first;
            doMergeMessage(hashId, iter->second, mergedMsg);
            mergedMsg.set_msgid(startId++);
            mergedMsg.set_timestamp(startId);
            mergedVec.push_back(mergedMsg);
        }
        msgVec.clear();
    }
}

void MessageUtil::doMergeMessage(uint16_t hashId, const vector<Message> &msgVec, Message &msgInfo) {
    size_t count = msgVec.size();
    std::vector<common::MergedMessageMeta> metaVec(count);
    std::string allData;
    uint16_t countU16 = uint16_t(count);
    allData.append((char *)(&countU16), sizeof(uint16_t));
    uint8_t uint8payload = 0;
    int64_t endPos = 0;
    for (size_t i = 0; i < count; ++i) {
        metaVec[i].isCompress = msgVec[i].compress();
        metaVec[i].maskPayload = msgVec[i].uint8maskpayload();
        metaVec[i].payload = msgVec[i].uint16payload();
        endPos += msgVec[i].data().size();
        metaVec[i].endPos = endPos;
        uint8payload |= msgVec[i].uint8maskpayload();
    }
    allData.append((char *)(&metaVec[0]), sizeof(MergedMessageMeta) * count);
    for (size_t i = 0; i < count; ++i) {
        allData.append(msgVec[i].data());
    }
    msgInfo.set_data(allData);
    msgInfo.set_uint8maskpayload(uint8payload);
    msgInfo.set_uint16payload(hashId);
    msgInfo.set_merged(true);
    msgInfo.set_compress(false);
}

void MessageUtil::writeDataHead(MessageInfo &msgInfo) {
    SchemaHeader header;
    msgInfo.data.insert(0, sizeof(HeadMeta), 0);
    header.meta.checkval = static_cast<uint32_t>(std::hash<string>{}(msgInfo.data));
    header.meta.dataType = msgInfo.dataType;
    const char *buf = msgInfo.data.c_str();
    header.toBufHead(const_cast<char *>(buf));
}

bool MessageUtil::readDataHead(Message &msg) {
    if (msg.data().size() < sizeof(HeadMeta)) {
        return false;
    }

    SchemaHeader header;
    const char *data = msg.data().c_str();
    header.fromBufHead(data);

    uint32_t old = *(uint32_t *)(data);
    *(int32_t *)(data) = 0;
    uint32_t acutal = static_cast<uint32_t>(std::hash<string>{}(msg.data()));
    *(int32_t *)(data) = old;
    uint32_t dataCheck = SchemaHeader::getCheckval(acutal);
    if (dataCheck != header.meta.checkval) {
        return false;
    }

    msg.set_datatype(static_cast<protocol::DataType>(header.meta.dataType));
    if (protocol::DATA_TYPE_SCHEMA != header.meta.dataType) {
        const string &data = msg.data().substr(sizeof(HeadMeta));
        msg.set_data(data);
    }
    return true;
}

bool MessageUtil::needFilter(const protocol::PartitionId &partitionId, const protocol::Filter &filter) {
    return partitionId.from() < filter.from() || partitionId.to() > filter.to() || partitionId.to() == 0 ||
           filter.uint8filtermask() != 0 || filter.uint8maskresult() != 0;
}

} // namespace util
} // namespace swift
