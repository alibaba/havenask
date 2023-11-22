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
#include "swift/client/MessageWriteBuffer.h"

#include <assert.h>
#include <cstdint>
#include <iostream>
#include <memory>
#include <unistd.h>
#include <vector>

#include "autil/CommonMacros.h"
#include "autil/Singleton.h"
#include "autil/TimeUtility.h"
#include "autil/WorkItem.h"
#include "autil/ZlibCompressor.h"
#include "swift/client/MergeMessageWorkItem.h"
#include "swift/client/SwiftWriterConfig.h"
#include "swift/common/MemoryMessage.h"
#include "swift/common/MessageInfo.h"
#include "swift/common/RangeUtil.h"
#include "swift/common/ThreadBasedObjectPool.h"
#include "swift/protocol/BrokerRequestResponse.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/protocol/FBMessageReader.h"
#include "swift/protocol/MessageCompressor.h"
#include "swift/protocol/SwiftMessage.pb.h"
#include "swift/util/Block.h"
#include "swift/util/MessageConverter.h"
#include "swift/util/MessageInfoUtil.h"
#include "swift/util/MessageUtil.h"

namespace swift {
namespace protocol {
class FBMessageWriter;
} // namespace protocol
} // namespace swift

using namespace autil;
using namespace std;
using namespace swift::common;
using namespace swift::util;
using namespace swift::protocol;
using namespace swift::protocol::flat;
namespace swift {
namespace client {
AUTIL_LOG_SETUP(swift, MessageWriteBuffer);

static int FILL_CONVERT_MESSAGE_THRESHOLD = 512;
static int MAX_COMPRESSION_MESSAGE_USE_TIME_IN_US = 100000;

MessageWriteBuffer::MessageWriteBuffer(BlockPoolPtr pool,
                                       ThreadPoolPtr mergeThreadPool,
                                       BufferSizeLimiterPtr limiter,
                                       int64_t mergeThreshold)
    : _writeQueueSize(0)
    , _mergeThreshold(mergeThreshold)
    , _sendQueue(pool.get())
    , _inMerge(false)
    , _partitionId(0)
    , _unsendCursor(0)
    , _uncommittedSize(0)
    , _dataSize(0)
    , _canAdd(true)
    , _sealed(false)
    , _minCheckpointIdInMerge(-1)
    , _maxCheckpointIdInMerge(-1)
    , _ignoreUpdate(false)
    , _mergeMsg(false)
    , _mergeCount(SwiftWriterConfig::DEFAULT_MERGE_THRESHOLD_IN_COUNT)
    , _mergeSize(SwiftWriterConfig::DEFAULT_MERGE_THRESHOLD_IN_SIZE)
    , _compressThresholdInBytes(SwiftWriterConfig::DEFAULT_COMPRESS_THRESHOLD_IN_BYTES)
    , _limiter(limiter)
    , _mergeThreadPool(mergeThreadPool) {
    _msgConverter = new MessageConverter(pool.get());
}

MessageWriteBuffer::~MessageWriteBuffer() {
    while (_inMerge) {
        usleep(1000 * 100);
    }
    DELETE_AND_SET_NULL(_msgConverter);
    clear();
}

bool MessageWriteBuffer::addWriteMessage(MessageInfo &messageInfo) {
    if (_sealed) {
        return false;
    }
    if (messageInfo.compress || messageInfo.isMerged) {
        AUTIL_INTERVAL_LOG(100, INFO, "[%s %d] message is compressed or merged.", _topicName.c_str(), _partitionId);
        return false;
    }
    if (!_canAdd) {
        AUTIL_INTERVAL_LOG(100,
                           INFO,
                           "[%s %d] wait for merge. left merge count [%ld]",
                           _topicName.c_str(),
                           _partitionId,
                           getLeftToMergeCountSafe());
        return false;
    }
    if (_writeQueueSize >= _mergeThreshold) {
        if (!_inMerge && _mergeThreadPool) {
            WorkItem *workItem = new MergeMessageWorkItem(this);
            if (ThreadPool::ERROR_NONE != _mergeThreadPool->pushWorkItem(workItem)) {
                AUTIL_LOG(WARN, "merge thread pool queue full [%d].", (int)_mergeThreadPool->getItemCount());
                delete workItem;
            }
        }
    }
    size_t msgLen = messageInfo.data.size();
    if (_limiter && !_limiter->addSize(msgLen + sizeof(MessageInfo))) {
        AUTIL_INTERVAL_LOG(100,
                           INFO,
                           "[%s %d] write queue[%ld], data size [%ld] "
                           "reach limit [%ld], wait for merge.",
                           _topicName.c_str(),
                           _partitionId,
                           (int64_t)_writeQueue.size(),
                           _writeQueueSize,
                           _limiter->getMaxSize());
        return false;
    }
    ScopedLock lock(_writeQueueMutex);
    MessageInfo *msgInfo = new MessageInfo();
    msgInfo->initAndSwapData(messageInfo);
    msgInfo->hashStr.clear();
    _writeQueue.push_back(msgInfo);
    addDataSize(msgLen);
    _writeQueueSize = msgLen + _writeQueueSize;
    return true;
}

size_t MessageWriteBuffer::fillRequest(protocol::ProductionRequest &request, size_t byteCount) {
    if (getUnsendCountInSendQueue() <= FILL_CONVERT_MESSAGE_THRESHOLD) {
        convertMessage();
    }
    ScopedReadLock lock(_rwLock);
    uint64_t ret = 0;
    string buffer;
    for (int64_t i = _unsendCursor; i < _sendQueue.size(); i++) {
        const ClientMemoryMessage &msgInfo = _sendQueue[i];
        protocol::Message *message = request.add_msgs();
        MessageConverter::decode(msgInfo, *message, buffer);
        ret += msgInfo.msgLen;
        if (ret >= byteCount) {
            break;
        }
    }
    _ignoreUpdate = false;
    return ret;
}

size_t MessageWriteBuffer::fillRequest(FBMessageWriter *writer, size_t byteCount, int64_t &fillCount) {
    if (getUnsendCountInSendQueue() <= FILL_CONVERT_MESSAGE_THRESHOLD) {
        convertMessage();
    }
    ScopedReadLock lock(_rwLock);
    uint64_t ret = 0;
    string buffer;
    for (int64_t i = _unsendCursor; i < _sendQueue.size(); i++) {
        const ClientMemoryMessage &msgInfo = _sendQueue[i];
        MessageConverter::decode(msgInfo, writer, buffer);
        ret += msgInfo.msgLen;
        ++fillCount;
        if (ret >= byteCount) {
            break;
        }
    }
    _ignoreUpdate = false;
    return ret;
}

bool MessageWriteBuffer::getUnSendMsgs(std::vector<common::MessageInfo> &msgInfoVec) {
    for (int64_t i = _unsendCursor; i < _sendQueue.size(); i++) {
        const ClientMemoryMessage &memMsg = _sendQueue[i];
        MessageInfo msgInfo;
        MessageConverter::decode(memMsg, msgInfo);

        if (msgInfo.compress) {
            float ratio;
            MessageCompressor::decompressMessageInfo(msgInfo, ratio);
        }
        if (msgInfo.isMerged) {
            if (!MessageUtil::unpackMessage(msgInfo, msgInfoVec)) {
                AUTIL_LOG(ERROR, "unpackMessage[%s] fail", msgInfo.data.c_str());
                return false;
            }
        } else {
            msgInfoVec.emplace_back(msgInfo);
        }
    }
    return true;
}

void MessageWriteBuffer::updateBuffer(int64_t acceptedMsgCount, int64_t &checkPointId) {
    if (acceptedMsgCount <= 0) {
        return;
    }
    ScopedWriteLock lock(_rwLock);
    if (_ignoreUpdate) {
        AUTIL_LOG(INFO, "ignore update buffer");
        return;
    }
    updateSendBuffer(acceptedMsgCount);
    vector<int64_t> committedCp;
    checkPointId = popWriteMessage(acceptedMsgCount, committedCp);
    AUTIL_LOG(INFO,
              "[%s %d] accepted message count[%ld], checkPointId [%ld], first committedCp [%ld], sendQueue size [%ld], "
              "has sended [%ld]",
              _topicName.c_str(),
              _partitionId,
              acceptedMsgCount,
              checkPointId,
              committedCp.size() > 0 ? committedCp[0] : 0,
              _sendQueue.size(),
              _unsendCursor);
}

void MessageWriteBuffer::updateBuffer(int64_t acceptedMsgCount,
                                      int64_t committedId,
                                      int64_t accecptedMsgBeginId,
                                      int64_t &checkPointId,
                                      const vector<int64_t> &timestamps,
                                      vector<pair<int64_t, int64_t>> &committedCpTs) {
    checkPointId = -1;
    ScopedWriteLock lock(_rwLock);
    if (_ignoreUpdate) {
        AUTIL_LOG(INFO, "ignore update buffer");
        return;
    }
    updateSendBuffer(acceptedMsgCount);
    vector<int64_t> committedTs;
    int64_t committedCount =
        updateCommitBuffer(committedId, accecptedMsgBeginId, acceptedMsgCount, timestamps, committedTs);
    if (committedCount <= 0) {
        return;
    }
    vector<int64_t> committedCp;
    checkPointId = popWriteMessage(committedCount, committedCp);
    bool hasCommittedTs = (committedTs.size() > 0);
    if (hasCommittedTs) {
        assert(committedTs.size() == committedCp.size());
    }
    for (size_t i = 0; i < committedCp.size(); ++i) {
        committedCpTs.emplace_back(make_pair(committedCp[i], hasCommittedTs ? committedTs[i] : -1));
    }
    AUTIL_LOG(INFO,
              "[%s %d] accepted message count[%ld], checkPointId [%ld], first committedCp [%ld], send queue size "
              "[%ld], sended count [%ld]",
              _topicName.c_str(),
              _partitionId,
              acceptedMsgCount,
              checkPointId,
              committedCp.size() > 0 ? committedCp[0] : 0,
              _sendQueue.size(),
              _unsendCursor);
}

void MessageWriteBuffer::resetUnsendCursor() {
    ScopedWriteLock lock(_rwLock);
    _unsendCursor = 0;
    _uncommittedSize = 0;
    _uncommittedMsgs.clear();
}

bool MessageWriteBuffer::latestCheckPointId(int64_t &checkpoint) const {
    ScopedReadLock lock(_rwLock);
    if (_sendQueue.size() != 0) {
        checkpoint = _sendQueue.front().checkpointId;
        return true;
    }
    if (_minCheckpointIdInMerge != -1) {
        checkpoint = _minCheckpointIdInMerge;
        return true;
    }
    {
        ScopedLock writeLock(_writeQueueMutex);
        if (_writeQueue.size() != 0) {
            checkpoint = _writeQueue.front()->checkpointId;
            return true;
        }
    }
    return false;
}

bool MessageWriteBuffer::maxCheckPointId(int64_t &checkpoint) const {
    ScopedReadLock lock(_rwLock);
    {
        ScopedLock writeLock(_writeQueueMutex);
        if (_writeQueue.size() != 0) {
            checkpoint = _writeQueue.back()->checkpointId;
            return true;
        }
    }
    if (_maxCheckpointIdInMerge != -1) {
        checkpoint = _maxCheckpointIdInMerge;
        return true;
    }
    if (_sendQueue.size() != 0) {
        checkpoint = _sendQueue.back().checkpointId;
        return true;
    }
    return false;
}

void MessageWriteBuffer::clear() {
    ScopedWriteLock sendLock(_rwLock);
    ScopedLock writeLock(_writeQueueMutex);
    ScopedLock sizeLock(_dataSizeMutex);
    AUTIL_LOG(INFO,
              "will clear buffer msg count[writeQueue:%ld, "
              "toMergeQueue:%ld, sendQueue:%ld]",
              _writeQueue.size(),
              _toMergeQueue.size(),
              _sendQueue.size());
    for (size_t i = 0; i < _writeQueue.size(); i++) {
        if (_limiter) {
            _limiter->subSize(_writeQueue[i]->data.size() + sizeof(MessageInfo));
        }
        delete _writeQueue[i];
    }
    _writeQueue.clear();
    _writeQueueSize = 0;
    _sendQueue.clear();
    _unsendCursor = 0;
    _uncommittedMsgs.clear();
    _dataSize = 0;
    _uncommittedSize = 0;
    _ignoreUpdate = true;
    cleanMergeInfo();
}

size_t MessageWriteBuffer::getSize() {
    ScopedReadLock sendlock(_rwLock);
    ScopedLock lock(_dataSizeMutex);
    return _dataSize + (_writeQueue.size() + getLeftToMergeCount()) * sizeof(MessageInfo) +
           _sendQueue.size() * sizeof(ClientMemoryMessage);
}

size_t MessageWriteBuffer::getQueueSize() {
    ScopedReadLock lock(_rwLock);
    ScopedLock writeLock(_writeQueueMutex);
    return _writeQueue.size() + _sendQueue.size() + getLeftToMergeCount();
}

size_t MessageWriteBuffer::getUnsendCount() {
    ScopedReadLock lock(_rwLock);
    ScopedLock writeLock(_writeQueueMutex);
    return _writeQueue.size() + _sendQueue.size() + getLeftToMergeCount() - _unsendCursor;
}

void MessageWriteBuffer::getMsgInBufferInfo(size_t &inWriteCount,
                                            size_t &toSendCount,
                                            size_t &leftMergeCount,
                                            size_t &uncommittedCount) {
    ScopedReadLock lock(_rwLock);
    ScopedLock writeLock(_writeQueueMutex);
    inWriteCount = _writeQueue.size();
    leftMergeCount = getLeftToMergeCount();
    uncommittedCount = _unsendCursor;
    toSendCount = _sendQueue.size() - _unsendCursor;
}

size_t MessageWriteBuffer::getUnsendSize() { return _dataSize - _uncommittedSize; }

size_t MessageWriteBuffer::getUncommittedCount() { return _unsendCursor; }

size_t MessageWriteBuffer::getUncommittedSize() { return _uncommittedSize; }

void MessageWriteBuffer::updateMergeInfo(bool mergeMsg,
                                         uint32_t mergeCount,
                                         uint32_t mergeSize,
                                         uint64_t compressThresholdInBytes) {
    ScopedWriteLock lock(_rwLock);
    _mergeMsg = mergeMsg;
    _mergeCount = mergeCount;
    _mergeSize = mergeSize;
    _compressThresholdInBytes = compressThresholdInBytes;
}

void MessageWriteBuffer::updateRangeUtil(const RangeUtilPtr &rangeUtil) {
    ScopedWriteLock lock(_rwLock);
    _rangeUtil = rangeUtil;
}

void MessageWriteBuffer::setTopicName(const std::string &topicName) { _topicName = topicName; }

void MessageWriteBuffer::setPartitionId(uint32_t partitionId) { _partitionId = partitionId; }

void MessageWriteBuffer::setInMerge(bool flag) { _inMerge = flag; }

void MessageWriteBuffer::convertMessage() {
    ScopedWriteLock lock(_rwLock);
    if (!isLastConvertFinished()) {
        _canAdd = doConvertMessage();
        return;
    }
    {
        ScopedLock lock(_writeQueueMutex);
        if (_writeQueue.empty()) {
            return;
        }
        _toMergeQueue.swap(_writeQueue);
        _minCheckpointIdInMerge = _toMergeQueue.front()->checkpointId;
        _maxCheckpointIdInMerge = _toMergeQueue.back()->checkpointId;
        _writeQueueSize = 0;
    }
    _canAdd = doConvertMessage();
}

bool MessageWriteBuffer::compressAndAddMsgToSend(MessageInfo *msgInfo,
                                                 autil::ZlibCompressor *compressor,
                                                 bool compressFlag) {
    int64_t size = msgInfo->data.size();
    if (compressFlag) {
        MessageInfoUtil::compressMessage(compressor, _compressThresholdInBytes, *msgInfo);
    }
    int64_t reduceLen = size - msgInfo->data.size();
    if (reduceLen > 0) {
        subDataSize(reduceLen);
        if (_limiter) {
            _limiter->subSize(reduceLen);
        }
    }
    if (!addMsgToSend(*msgInfo)) {
        return false;
    }
    return true;
}

bool MessageWriteBuffer::addMsgToSend(const MessageInfo &sendMsg) {
    if (!_sendQueue.reserve()) {
        return false;
    }
    ClientMemoryMessage memMsg;
    if (ERROR_NONE != _msgConverter->encode(sendMsg, memMsg)) {
        return false;
    }
    _sendQueue.push_back(memMsg);
    return true;
}

void MessageWriteBuffer::updateSendBuffer(int64_t acceptedMsgCount) {
    if (acceptedMsgCount <= 0) {
        return;
    }
    int64_t newUnsendCursor = _unsendCursor + (size_t)acceptedMsgCount;
    for (int64_t i = _unsendCursor; i < newUnsendCursor && i < _sendQueue.size(); i++) {
        _uncommittedSize = _uncommittedSize + _sendQueue[i].msgLen;
    }
    _unsendCursor = newUnsendCursor;
}

int64_t MessageWriteBuffer::updateCommitBuffer(int64_t committedId,
                                               int64_t acceptedMsgBeginId,
                                               int64_t acceptedMsgCount,
                                               const vector<int64_t> &timestamps,
                                               vector<int64_t> &committedTs) {
    if (acceptedMsgCount > 0) {
        int64_t start = acceptedMsgBeginId;
        int64_t end = start + acceptedMsgCount - 1;
        _uncommittedMsgs.push_back(make_pair(make_pair(start, end), timestamps));
    }
    int64_t committedCount = 0;
    while (!_uncommittedMsgs.empty()) {
        auto &uids = _uncommittedMsgs.front();
        pair<int64_t, int64_t> &p = uids.first;
        if (committedId >= p.second) {
            committedCount += p.second - p.first + 1;
            committedTs.insert(committedTs.end(), uids.second.begin(), uids.second.end());
            _uncommittedMsgs.pop_front();
        } else if (committedId < p.first) {
            break;
        } else {
            int64_t count = committedId - p.first + 1;
            committedCount += count;
            p.first = committedId + 1;
            if (uids.second.size() >= count) {
                committedTs.insert(committedTs.end(), uids.second.begin(), uids.second.begin() + count);
                uids.second.erase(uids.second.begin(), uids.second.begin() + count);
            }
            break;
        }
    }
    return committedCount;
}

int64_t MessageWriteBuffer::popWriteMessage(int64_t count, vector<int64_t> &committedCp) {
    int64_t checkpointId = -1;
    while (count-- > 0 && !_sendQueue.empty()) {
        size_t size = _sendQueue.front().msgLen;
        subDataSize(size);
        _uncommittedSize = _uncommittedSize - size;
        checkpointId = _sendQueue.front().checkpointId;
        committedCp.emplace_back(checkpointId);
        _sendQueue.pop_front();
        _unsendCursor = _unsendCursor - 1;
    }
    return checkpointId;
}

size_t MessageWriteBuffer::getMetaSize() const {
    return (_writeQueue.size() + _sendQueue.size()) * sizeof(MessageInfo);
}

bool MessageWriteBuffer::isLastConvertFinished() { return _toMergeQueue.size() == 0 && _msgMap.size() == 0; }

size_t MessageWriteBuffer::getLeftToMergeCount() {
    size_t size = 0;
    map<uint16_t, vector<MessageInfo *>>::iterator iter = _msgMap.begin();
    for (; iter != _msgMap.end(); iter++) {
        size += iter->second.size();
    }
    return _toMergeQueue.size() + size;
}

size_t MessageWriteBuffer::getLeftToMergeCountSafe() {
    ScopedReadLock lock(_rwLock);
    return getLeftToMergeCount();
}

bool MessageWriteBuffer::doConvertMessage() {
    if (_mergeMsg && _rangeUtil != NULL) {
        return mergeMessage();
    }
    swift::common::ThreadBasedObjectPool<autil::ZlibCompressor> *compressorPool =
        autil::Singleton<swift::common::ThreadBasedObjectPool<autil::ZlibCompressor>,
                         protocol::ZlibCompressorInstantiation>::getInstance();
    autil::ZlibCompressor *compressor = compressorPool->getObject();
    compressor->reset();
    AUTIL_LOG(DEBUG, "begin convert message");
    bool compressFlag = true;
    int64_t begTime = TimeUtility::currentTime();
    while (!_toMergeQueue.empty()) {
        MessageInfo *msgInfo = _toMergeQueue.front();
        if (compressFlag && TimeUtility::currentTime() - begTime > MAX_COMPRESSION_MESSAGE_USE_TIME_IN_US) {
            compressFlag = false;
        }
        if (!compressAndAddMsgToSend(msgInfo, compressor, compressFlag)) {
            AUTIL_INTERVAL_LOG(100, INFO, "[%s %d] add msg to send fail", _topicName.c_str(), _partitionId);
            return false;
        }
        if (_limiter) {
            _limiter->subSize(msgInfo->data.size() + sizeof(MessageInfo));
        }
        delete msgInfo;
        _toMergeQueue.pop_front();
    }
    _maxCheckpointIdInMerge = _minCheckpointIdInMerge = -1;
    AUTIL_LOG(DEBUG, "end convert message use time %ld", TimeUtility::currentTime() - begTime);
    return true;
}

bool MessageWriteBuffer::mergeMessage() {
    swift::common::ThreadBasedObjectPool<autil::ZlibCompressor> *compressorPool =
        autil::Singleton<swift::common::ThreadBasedObjectPool<autil::ZlibCompressor>,
                         protocol::ZlibCompressorInstantiation>::getInstance();
    autil::ZlibCompressor *compressor = compressorPool->getObject();
    compressor->reset();
    uint16_t hashId;
    AUTIL_LOG(DEBUG, "begin merge message");
    bool compressFlag = true;
    int64_t begTime = TimeUtility::currentTime();
    while (!_toMergeQueue.empty()) {
        if (compressFlag && TimeUtility::currentTime() - begTime > MAX_COMPRESSION_MESSAGE_USE_TIME_IN_US) {
            compressFlag = false;
        }
        MessageInfo *msgInfo = _toMergeQueue.front();
        hashId = _rangeUtil->getMergeHashId(msgInfo->uint16Payload);
        vector<MessageInfo *> &msgVec = _msgMap[hashId];
        uint64_t &size = _sizeMap[hashId];
        if (msgVec.size() < _mergeCount) {
            msgVec.push_back(msgInfo);
            _toMergeQueue.pop_front();
            size += msgInfo->data.size();
        }
        if (msgVec.size() >= _mergeCount || size >= _mergeSize) {
            if (msgVec.size() == 1) {
                if (!compressAndAddMsgToSend(msgVec[0], compressor, compressFlag)) {
                    return false;
                }
            } else {
                MessageInfo mergedMsg;
                doMergeMessage(hashId, msgVec, mergedMsg, size, compressor);
                if (!addMsgToSend(mergedMsg)) {
                    return false;
                }
                subDataSize((int64_t)size - mergedMsg.data.size());
            }
            size = 0;
            cleanMsgs(msgVec);
        }
    }
    map<uint16_t, vector<MessageInfo *>>::iterator iter = _msgMap.begin();
    for (; iter != _msgMap.end(); iter++) {
        vector<MessageInfo *> &msgVec = iter->second;
        if (msgVec.size() == 1) {
            if (!compressAndAddMsgToSend(msgVec[0], compressor, compressFlag)) {
                return false;
            }
        } else if (msgVec.size() > 1) {
            MessageInfo mergedMsg;
            uint16_t hashId = iter->first;
            int64_t dataLen = _sizeMap[hashId];

            doMergeMessage(hashId, iter->second, mergedMsg, dataLen, compressor);
            if (!addMsgToSend(mergedMsg)) {
                return false;
            }
            subDataSize(dataLen - mergedMsg.data.size());
        }
        cleanMsgs(msgVec);
    }
    _msgMap.clear();
    _sizeMap.clear();
    _maxCheckpointIdInMerge = _minCheckpointIdInMerge = -1;
    AUTIL_LOG(DEBUG, "end merge message use time %ld", TimeUtility::currentTime() - begTime);
    return true;
}

void MessageWriteBuffer::doMergeMessage(uint16_t hashId,
                                        const vector<MessageInfo *> &msgVec,
                                        MessageInfo &msgInfo,
                                        uint64_t reserveLen,
                                        ZlibCompressor *compressor) {
    MessageInfoUtil::mergeMessage(hashId, msgVec, reserveLen, msgInfo);
    MessageInfoUtil::compressMessage(compressor, _compressThresholdInBytes, msgInfo);
    AUTIL_LOG(DEBUG, "[%s %d] merge [%d] messages", _topicName.c_str(), _partitionId, (int)msgVec.size());
}

void MessageWriteBuffer::cleanMsgs(vector<MessageInfo *> &msgVec) {
    for (size_t i = 0; i < msgVec.size(); i++) {
        if (msgVec[i] != NULL) {
            if (_limiter) {
                _limiter->subSize(msgVec[i]->data.size() + sizeof(MessageInfo));
            }
            delete msgVec[i];
            msgVec[i] = NULL;
        }
    }
    msgVec.clear();
}

void MessageWriteBuffer::cleanMergeInfo() {
    while (!_toMergeQueue.empty()) {
        MessageInfo *msg = _toMergeQueue.front();
        if (_limiter) {
            _limiter->subSize(msg->data.size() + sizeof(MessageInfo));
        }
        delete msg;
        _toMergeQueue.pop_front();
    }
    map<uint16_t, vector<MessageInfo *>>::iterator iter = _msgMap.begin();
    for (; iter != _msgMap.end(); iter++) {
        cleanMsgs(iter->second);
    }
    _msgMap.clear();
    _sizeMap.clear();
    _maxCheckpointIdInMerge = -1;
    _minCheckpointIdInMerge = -1;
}

void MessageWriteBuffer::addDataSize(int64_t size) {
    ScopedLock lock(_dataSizeMutex);
    _dataSize = _dataSize + size;
}

void MessageWriteBuffer::subDataSize(int64_t size) {
    ScopedLock lock(_dataSizeMutex);
    _dataSize = _dataSize - size;
}

int64_t MessageWriteBuffer::getUnsendCountInSendQueue() {
    ScopedReadLock lock(_rwLock);
    return _sendQueue.size() - _unsendCursor;
}

bool MessageWriteBuffer::stealUnsendMessage(vector<MessageInfo> &msgInfoVec) {
    {
        ScopedWriteLock lock(_rwLock);
        AUTIL_LOG(INFO,
                  "before steal, buffer msg count[writeQueue:%ld, "
                  "toMergeQueue:%ld, sendQueue:%ld]",
                  _writeQueue.size(),
                  _toMergeQueue.size(),
                  _sendQueue.size());
        // all queue empty, return
        if (_writeQueue.empty() && _toMergeQueue.empty() && _sendQueue.empty()) {
            return true;
        }
        while (_inMerge) {
            AUTIL_LOG(INFO, "wait merge thread finish");
            usleep(100 * 1000);
        }
        // 1. _toMergeQueue -> _sendQueue
        if (!doConvertMessage()) {
            AUTIL_LOG(ERROR, "flush merge queue, doConvertMessage failed");
            return false;
        }
        // 2. _writeQueue -> _toMergeQueue -> _sendQueue
        if (!_writeQueue.empty()) {
            _toMergeQueue.swap(_writeQueue);
            _writeQueueSize = 0;
            if (!doConvertMessage()) {
                AUTIL_LOG(ERROR, "flush write queue, doConvertMessage failed");
                return false;
            }
        }
        // 3. _sendQueue -> msgInfoVec
        if (!getUnSendMsgs(msgInfoVec)) {
            AUTIL_LOG(ERROR, "get unsend msgs from _sendQueue failed");
            return false;
        }
    }

    // 5. clear buffer
    DELETE_AND_SET_NULL(_msgConverter);
    clear();
    AUTIL_LOG(INFO, "steal finish, steal msg count[%ld]", msgInfoVec.size());
    return true;
}

} // namespace client
} // namespace swift
