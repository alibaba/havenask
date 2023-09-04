#pragma once

#include "autil/Log.h"
#include "swift/common/Common.h"
#include "swift/common/MemoryMessage.h"
#include "swift/common/MessageMeta.h"
#include "swift/common/RangeUtil.h"

using namespace swift::common;

namespace swift {
namespace storage {

class MemoryMessageUtil {
public:
    MemoryMessageUtil() {}
    ~MemoryMessageUtil() {}

private:
    MemoryMessageUtil(const MemoryMessageUtil &);
    MemoryMessageUtil &operator=(const MemoryMessageUtil &);

public:
    static void mergeMemoryMessage(const MemoryMessageVector &mmv,
                                   common::RangeUtil *rangeUtil,
                                   uint32_t mergeCount,
                                   MemoryMessageVector &mergedVec) {
        int64_t startId = 0;
        std::map<uint16_t, std::vector<MemoryMessage>> msgMap;
        uint16_t hashId;
        for (size_t i = 0; i < mmv.size(); i++) {
            const MemoryMessage &msg = mmv[i];
            hashId = rangeUtil->getMergeHashId(msg.getPayload());
            std::vector<MemoryMessage> &msgVec = msgMap[hashId];
            msgVec.push_back(msg);
            if (msgVec.size() >= mergeCount) {
                MemoryMessage mergedMsg;
                doMergeMessage(hashId, msgVec, mergedMsg);
                mergedMsg.setMsgId(startId++);
                mergedMsg.setTimestamp(startId);
                mergedVec.push_back(mergedMsg);
                msgVec.clear();
            }
        }
        std::map<uint16_t, std::vector<MemoryMessage>>::iterator iter = msgMap.begin();
        for (; iter != msgMap.end(); iter++) {
            std::vector<MemoryMessage> &msgVec = iter->second;
            if (msgVec.empty()) {
                continue;
            }
            if (msgVec.size() == 1) {
                msgVec[0].setMsgId(startId++);
                msgVec[0].setTimestamp(startId);
                mergedVec.push_back(msgVec[0]);
            } else {
                MemoryMessage mergedMsg;
                uint16_t hashId = iter->first;
                doMergeMessage(hashId, iter->second, mergedMsg);
                mergedMsg.setMsgId(startId++);
                mergedMsg.setTimestamp(startId);
                mergedVec.push_back(mergedMsg);
            }
            msgVec.clear();
        }
    }

    static void doMergeMessage(uint16_t hashId, const std::vector<MemoryMessage> &msgVec, MemoryMessage &msgInfo) {
        size_t count = msgVec.size();
        std::vector<common::MergedMessageMeta> metaVec(count);
        std::string allData;
        uint16_t countU16 = uint16_t(count);
        allData.append((char *)(&countU16), sizeof(uint16_t));
        uint8_t uint8payload = 0;
        int64_t endPos = 0;
        for (size_t i = 0; i < count; ++i) {
            metaVec[i].isCompress = msgVec[i].isCompress();
            metaVec[i].maskPayload = msgVec[i].getMaskPayload();
            metaVec[i].payload = msgVec[i].getPayload();
            endPos += msgVec[i].getLen();
            metaVec[i].endPos = endPos;
            uint8payload |= msgVec[i].getMaskPayload();
        }
        allData.append((char *)(&metaVec[0]), sizeof(common::MergedMessageMeta) * count);
        for (size_t i = 0; i < count; ++i) {
            allData.append(msgVec[i].getBlock()->getBuffer() + msgVec[i].getOffset(), msgVec[i].getLen());
        }
        util::BlockPtr block(new util::Block(allData.size(), NULL));
        memcpy(block->getBuffer(), allData.c_str(), allData.size());
        msgInfo.setBlock(block);
        msgInfo.setLen(allData.size());
        msgInfo.setOffset(0);
        msgInfo.setMaskPayload(uint8payload);
        msgInfo.setPayload(hashId);
        msgInfo.setMerged(true);
        msgInfo.setCompress(false);
    }

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(MemoryMessageUtil);

} // namespace storage
} // namespace swift
