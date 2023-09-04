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
#include "swift/broker/storage/MessageGroup.h"

#include <assert.h>
#include <cmath>
#include <cstddef>
#include <memory>
#include <string>
#include <vector>

#include "autil/CommonMacros.h"
#include "autil/Singleton.h"
#include "autil/TimeUtility.h"
#include "flatbuffers/flatbuffers.h"
#include "swift/broker/storage/MessageDeque.h"
#include "swift/broker/storage/TimestampAllocator.h"
#include "swift/common/ThreadBasedObjectPool.h"
#include "swift/filter/MessageFilter.h"
#include "swift/monitor/BrokerMetricsReporter.h"
#include "swift/protocol/BrokerRequestResponse.pb.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/protocol/FBMessageWriter.h"
#include "swift/protocol/MessageAdapter.h"
#include "swift/protocol/SwiftMessage.pb.h"
#include "swift/protocol/SwiftMessage_generated.h"
#include "swift/util/Block.h"
#include "swift/util/BlockPool.h"
#include "swift/util/FilterUtil.h"
#include "swift/util/MessageConverter.h"
#include "swift/util/MessageQueue.h"
#include "swift/util/MessageUtil.h"
#include "swift/util/ObjectBlock.h"

using namespace std;
using namespace autil;
namespace swift {
namespace storage {
AUTIL_LOG_SETUP(swift, MessageGroup);
using namespace swift::protocol;
using namespace swift::filter;
using namespace swift::util;
using namespace swift::common;
using namespace swift::monitor;
const int64_t SECOND_FOR_COMPUTE_RESERVE_BLOCK_COUNT = 30 * 1000 * 1000; // 30 second
const uint64_t MAX_MEMORY_MESSAGE_BUFFER_SIZE = 1 << 21;

MessageGroup::MessageGroup()
    : _timestampAllocator(nullptr)
    , _partitionBlockPool(nullptr)
    , _msgDeque(nullptr)
    , _msgConverter(nullptr)
    , _readNotCommittedMsg(true) {}

MessageGroup::~MessageGroup() {
    if (_msgDeque) {
        _msgDeque->clear();
        DELETE_AND_SET_NULL(_msgDeque);
    }
    if (_timestampAllocator) {
        DELETE_AND_SET_NULL(_timestampAllocator);
    }
    DELETE_AND_SET_NULL(_msgConverter);
}

ErrorCode MessageGroup::init(const PartitionId &partitionId,
                             BlockPool *pool,
                             const MessageResponse *response,
                             TopicMode topicMode,
                             bool readNotCommittedMsg,
                             int64_t timestampOffsetInUs) {
    _partitionId = partitionId;
    _partitionBlockPool = pool;
    _topicMode = topicMode;
    _timestampAllocator = new TimestampAllocator(timestampOffsetInUs);
    _msgConverter = new MessageConverter(_partitionBlockPool);
    _msgDeque = new MessageDeque(pool, _timestampAllocator);
    _readNotCommittedMsg = canReadNotCommittedMsg(readNotCommittedMsg, topicMode);
    AUTIL_LOG(INFO,
              "[%s %d %s] set readNotCommittedMsg %d, canReadNotCommittedMsg %d",
              _partitionId.topicname().c_str(),
              _partitionId.id(),
              TopicMode_Name(_topicMode).c_str(),
              readNotCommittedMsg,
              _readNotCommittedMsg);
    if (!response) {
        return ERROR_NONE;
    }
    // init from recover. every message should store in memory.
    int size = response->msgs_size();
    if (!_msgDeque->reserve(size)) {
        AUTIL_LOG(ERROR,
                  "partition [%s] memory is full, init message group failed!",
                  _partitionId.ShortDebugString().c_str());
        return ERROR_BROKER_BUSY;
    }

    for (int i = 0; i < size; ++i) {
        const protocol::Message &msg = response->msgs(i);
        MemoryMessage memMsg;
        ErrorCode errorCode = _msgConverter->encode(msg, memMsg);
        if (errorCode != ERROR_NONE) {
            return errorCode;
        }
        _msgDeque->pushBack(memMsg);
    }
    if (size > 0) {
        int64_t msgId = response->msgs(size - 1).msgid();
        _msgDeque->setCommittedMsgId(msgId);
        AUTIL_LOG(INFO,
                  "partition [%s] recover [%d] msg from file,"
                  " now committed msg id is [%ld]",
                  _partitionId.ShortDebugString().c_str(),
                  size,
                  msgId);
    }
    return ERROR_NONE;
}

bool MessageGroup::canRecycle() const {
    ScopedReadWriteLock lock(_rwLock, 'r');
    auto view = _msgDeque->createView(_readNotCommittedMsg);
    if (view.empty()) {
        return false;
    }
    int64_t committdId = _msgDeque->getCommittedMsgId();
    int64_t minCount = _partitionBlockPool->getMinBlockCount();
    int64_t useCount = _partitionBlockPool->getUsedBlockCount();
    if (useCount <= minCount * 3 / 4) { // use 75% min buffer
        return false;
    } else {
        return view[0].getMsgId() < committdId;
    }
}

void MessageGroup::tryRecycleFast(double recyclePercent, int64_t &actualRecycleSize, int64_t &actualRecycleCount) {
    vector<util::ObjectBlock<MemoryMessage> *> objectVec;
    int64_t startOffset = 0;
    actualRecycleCount = 0;
    actualRecycleSize = 0;
    {
        ScopedReadWriteLock lock(_rwLock, 'w');
        auto view = _msgDeque->createView(_readNotCommittedMsg);
        int64_t msgCount = view.size();
        int64_t metaBlockCount = _msgDeque->getMetaUsedBlockCount();
        int64_t recycleBlock = metaBlockCount * recyclePercent;
        if (recycleBlock < 0) {
            return;
        }
        int64_t recycleMsgCount = msgCount * recyclePercent;
        if (recycleMsgCount >= msgCount || recycleMsgCount <= 0) {
            return;
        }
        int64_t committdId = _msgDeque->getCommittedMsgId();
        const MemoryMessage &msg = view[recycleMsgCount - 1];
        if (msg.getMsgId() > committdId) {
            return;
        }
        _msgDeque->stealBlock(recycleMsgCount, startOffset, objectVec);
    }
    size_t blockCount = objectVec.size();
    if (blockCount <= 0) {
        return;
    }
    for (size_t i = 0; i < blockCount; i++) {
        ObjectBlock<MemoryMessage> &objBlock = *objectVec[i];
        int64_t objSize = objBlock.size();
        for (int64_t offset = startOffset; offset < objSize; ++offset) {
            actualRecycleSize += objBlock[offset].getLen();
            actualRecycleCount++;
            objBlock.destoryBefore(offset);
        }
        startOffset = 0;
        delete objectVec[i];
    }
    {
        ScopedReadWriteLock lock(_rwLock, 'w');
        _msgDeque->updateStealDataLen(actualRecycleSize);
    }
    actualRecycleSize += actualRecycleCount * MEMORY_MESSAGE_META_SIZE;
    int64_t duration = SECOND_FOR_COMPUTE_RESERVE_BLOCK_COUNT;
    size_t reserveCount = estimateReserveBlockCount(duration);
    _partitionBlockPool->setReserveBlockCount(reserveCount);
    _partitionBlockPool->freeUnusedBlock();
}

// recycle as much as possible
void MessageGroup::tryRecycle(int64_t recycleSize, int64_t &actualRecycleSize, int64_t &actualRecycleCount) {
    ScopedReadWriteLock lock(_rwLock, 'w');
    _msgDeque->recycleBySize(recycleSize, actualRecycleSize, actualRecycleCount);

    int64_t duration = SECOND_FOR_COMPUTE_RESERVE_BLOCK_COUNT;
    size_t reserveCount = estimateReserveBlockCount(duration);
    _partitionBlockPool->setReserveBlockCount(reserveCount);
    _partitionBlockPool->freeUnusedBlock();
}

void MessageGroup::tryRecycleByReaderInfo(int64_t minTimeStamp,
                                          int64_t &actualRecycleSize,
                                          int64_t &actualRecycleCount) {
    ScopedReadWriteLock lock(_rwLock, 'w');
    _msgDeque->recycleByTimestamp(minTimeStamp, actualRecycleSize, actualRecycleCount);

    int64_t duration = SECOND_FOR_COMPUTE_RESERVE_BLOCK_COUNT;
    size_t reserveCount = estimateReserveBlockCount(duration);
    _partitionBlockPool->setReserveBlockCount(reserveCount);
    _partitionBlockPool->freeUnusedBlock();
}

ErrorCode MessageGroup::addMessage(const protocol::ProductionRequest *request,
                                   protocol::MessageResponse *response,
                                   WriteMetricsCollector &collector) {
    MessageFormat mf = request->messageformat();
    WriteStat stat;
    ErrorCode ec = ERROR_NONE;
    size_t inputMsgCount = 0;
    bool replicationMode = request->has_replicationmode() && request->replicationmode();
    bool fillTsForResponse = request->has_needtimestamp() && request->needtimestamp();
    if (MF_PB == mf) {
        const auto &msgs = request->msgs();
        inputMsgCount = msgs.size();
        ec = addMessage(msgs, replicationMode, fillTsForResponse, response, stat);
    } else {
        const flat::Messages *msgs = flatbuffers::GetRoot<flat::Messages>(request->fbmsgs().c_str());
        if (!msgs) {
            return ERROR_BROKER_REQUEST_INVALID;
        }
        inputMsgCount = msgs->msgs()->size();
        ec = addMessage(*msgs, replicationMode, fillTsForResponse, response, stat);
    }

    // fill response
    response->set_messageformat(mf);
    response->set_acceptedmsgcount(stat.acceptedMsgCount);
    response->set_totalmsgcount(stat.acceptedTotalMsgCount);
    response->set_compresspayload(stat.compressedPayload);
    response->set_hasmergedmsg(stat.hasMergedMsg());
    if (stat.firstAcceptedMsgId >= 0) {
        response->set_acceptedmsgbeginid(stat.firstAcceptedMsgId);
    }
    response->mutable_tracetimeinfo()->add_processedtimes(TimeUtility::currentTime());

    // fill metric
    if (stat.acceptedMsgCount > 0) {
        collector.acceptedMsgCount = stat.acceptedMsgCount;
        collector.totalAcceptedMsgCount = stat.acceptedTotalMsgCount;
        collector.mergedAcceptedMsgCount = stat.acceptedTotalMsgCount - stat.acceptedMsgCount;
        collector.acceptedMsgSize = 1.0 * stat.acceptedDataSize / stat.acceptedMsgCount;
        collector.acceptedMsgsCountPerWriteRequest = stat.acceptedMsgCount;
        collector.acceptedMsgsSizePerWriteRequest = stat.acceptedDataSize;
        collector.totalAcceptedMsgsSize = stat.acceptedDataSize;
        if (MF_FB == mf) {
            collector.acceptedMsgCount_FB = stat.acceptedMsgCount;
        } else {
            collector.acceptedMsgCount_PB = stat.acceptedMsgCount;
        }
        if (stat.compressMsgCount > 0) {
            collector.acceptedCompressMsgCount = stat.compressMsgCount;
        }
        if (stat.acceptedMsgCount - stat.compressMsgCount > 0) {
            collector.acceptedUncompressMsgCount = stat.acceptedMsgCount - stat.compressMsgCount;
        }
    }

    if (ec != ERROR_NONE) {
        AUTIL_LOG(ERROR,
                  "partition [%s] memory is full, only accept [%d] messages,"
                  "total [%d] messages in request, used block[%ld], ec [%s].",
                  _partitionId.ShortDebugString().c_str(),
                  (int)stat.acceptedMsgCount,
                  (int)inputMsgCount,
                  _partitionBlockPool->getUsedBlockCount(),
                  ErrorCode_Name(ec).c_str());
    }

    return ec;
}

template <typename ContainerType>
bool MessageGroup::checkReplicationRequest(const ContainerType &data, int64_t lastMsgId, int64_t lastMsgTime) {
    MessageIterator<ContainerType> iter(data);
    using MsgType = typename MessageIterator<ContainerType>::MsgType;
    while (iter.hasNext()) {
        const auto &msg = iter.next();
        int64_t msgId = MessageAdapter<MsgType>::getMsgId(msg);
        int64_t msgTime = MessageAdapter<MsgType>::getTimestamp(msg);
        if (lastMsgId != -1 && (msgId != lastMsgId + 1 || msgTime <= lastMsgTime)) {
            AUTIL_LOG(WARN,
                      "msgId: %ld, msgTime: %ld, lastMsgId:%ld, lastMsgTime: %ld",
                      msgId,
                      msgTime,
                      lastMsgId,
                      lastMsgTime);
            return false;
        }
        lastMsgId = msgId;
        lastMsgTime = msgTime;
    }
    return true;
}

template <typename ContainerType>
protocol::ErrorCode MessageGroup::addMessage(const ContainerType &container,
                                             bool replicationMode,
                                             bool fillTsForResponse,
                                             protocol::MessageResponse *response,
                                             MessageGroup::WriteStat &stat) {
    ScopedReadWriteLock lock(_rwLock, 'w');
    MessageIterator<ContainerType> it(container);
    using MsgType = typename MessageIterator<ContainerType>::MsgType;
    if (replicationMode && !checkReplicationRequest(container,
                                                    _msgDeque->getLastReceivedMsgId(),
                                                    _msgDeque->getLastReceivedMsgTimestamp())) {
        AUTIL_LOG(WARN, "%s: replication request invalid", _partitionId.ShortDebugString().c_str());
        return protocol::ERROR_BROKER_REQUEST_INVALID;
    }
    protocol::ErrorCode ec = ERROR_NONE;
    while (it.hasNext()) {
        if (!_msgDeque->reserve()) {
            ec = protocol::ERROR_BROKER_BUSY;
            break;
        }
        MemoryMessage memMsg;
        const auto &msg = it.next();
        ec = _msgConverter->encode(msg, memMsg);
        if (ec != ERROR_NONE) {
            break;
        }
        int64_t msgId = -1;
        int64_t msgTime = -1;
        if (unlikely(replicationMode)) {
            msgId = MessageAdapter<MsgType>::getMsgId(msg);
            msgTime = MessageAdapter<MsgType>::getTimestamp(msg);
        } else {
            msgId = _msgDeque->getLastReceivedMsgId() + 1;
            msgTime = _timestampAllocator->getNextMsgTimestamp(_msgDeque->getLastReceivedMsgTimestamp());
        }
        memMsg.setMsgId(msgId);
        memMsg.setTimestamp(msgTime);
        _msgDeque->pushBack(memMsg);
        if (fillTsForResponse) {
            response->add_timestamps(memMsg.getTimestamp());
        }
        ++stat.acceptedMsgCount;
        stat.acceptedTotalMsgCount += memMsg.getMsgCount();
        stat.acceptedDataSize += MessageAdapter<MsgType>::getMsgLen(msg);
        if (memMsg.isCompress()) {
            ++stat.compressMsgCount;
        }
        stat.compressedPayload |= FilterUtil::compressPayload(_partitionId, memMsg.getPayload());
    }
    stat.firstAcceptedMsgId = _msgDeque->getLastReceivedMsgId() - stat.acceptedMsgCount + 1;
    return ec;
}

int64_t MessageGroup::getMsgCountInBuffer() const {
    ScopedReadWriteLock lock(_rwLock, 'r');
    auto view = _msgDeque->createView(_readNotCommittedMsg);
    return view.size();
}

int64_t MessageGroup::getLastReceivedMsgId() const {
    ScopedReadWriteLock lock(_rwLock, 'r');
    return _msgDeque->getLastReceivedMsgId();
}

int64_t MessageGroup::getDataSize() const {
    ScopedReadWriteLock lock(_rwLock, 'r');
    return _msgDeque->getDataSize();
}

int64_t MessageGroup::getMetaSize() const {
    ScopedReadWriteLock lock(_rwLock, 'r');
    return _msgDeque->getMetaSize();
}

int64_t MessageGroup::getCommitDelay() const {
    ScopedReadWriteLock lock(_rwLock, 'r');
    return getIsMemOnly() ? 0 : _msgDeque->getCommitDelay();
}

int64_t MessageGroup::getLeftToBeCommittedDataSize() const {
    ScopedReadWriteLock lock(_rwLock, 'r');
    return getIsMemOnly() ? 0 : _msgDeque->getLeftToBeCommittedDataSize();
}

int64_t MessageGroup::getLastAcceptedMsgTimeStamp() const {
    ScopedReadWriteLock lock(_rwLock, 'r');
    return _msgDeque->getLastAcceptedMsgTimeStamp();
}

int64_t MessageGroup::getLastAvailableMsgTimestamp() const {
    ScopedReadWriteLock lock(_rwLock, 'r');
    auto view = _msgDeque->createView(_readNotCommittedMsg);
    return view.getLastMsgTimestamp();
}

int64_t MessageGroup::getLastAvailableMsgId() const {
    ScopedReadWriteLock lock(_rwLock, 'r');
    auto view = _msgDeque->createView(_readNotCommittedMsg);
    return view.getLastMsgId();
}

void MessageGroup::getMemoryMessage(int64_t startId, size_t count, MemoryMessageVector &vec) const {
    vec.clear();
    vec.reserve(count);
    ScopedReadWriteLock lock(_rwLock, 'r');
    _msgDeque->getMemoryMessage(startId, count, vec);
}

protocol::ErrorCode MessageGroup::getMessage(const ConsumptionRequest *request,
                                             bool memMode,
                                             MessageResponse *response,
                                             ReaderInfoPtr readerInfo,
                                             ReadMetricsCollector &collector) {
    if (readerInfo) {
        readerInfo->setReadFile(false);
    }
    int64_t startId = request->startid();
    int64_t count = request->count();
    int64_t maxTotalSize = request->maxtotalsize();
    bool filterFlag = MessageUtil::needFilter(_partitionId, request->filter());
    ThreadBasedObjectPool<MemoryMessageVector> *vecPool =
        Singleton<ThreadBasedObjectPool<MemoryMessageVector>>::getInstance();
    MemoryMessageVector *mVec = vecPool->getObject();
    mVec->clear();
    int64_t readCount = 0;
    {
        bool readNotCommittedMsg =
            request->has_readcommittedmsg() ? !request->readcommittedmsg() : _readNotCommittedMsg;
        ScopedReadWriteLock lock(_rwLock, 'r');
        auto view = _msgDeque->createView(readNotCommittedMsg);
        if (view.empty()) {
            response->set_nextmsgid(0);
            response->set_nexttimestamp(view.getLastMsgTimestamp());
            return ERROR_BROKER_NO_DATA;
        }
        assert(count >= 0);
        response->set_maxmsgid(view.getLastMsgId());
        response->set_maxtimestamp(view.getLastMsgTimestamp());

        const MemoryMessage &oldestMsg = view[0];
        int64_t oldestMsgId = oldestMsg.getMsgId();
        if (startId < oldestMsgId && !memMode) {
            // to avoid no message in dfs. missing the lastest file or something like that.
            response->set_nextmsgid(oldestMsgId);
            response->set_nexttimestamp(oldestMsg.getTimestamp());
            return ERROR_BROKER_NO_DATA_IN_MEM;
        }
        int64_t beginPos = startId - oldestMsgId > 0 ? startId - oldestMsgId : 0;
        int64_t msgCount = view.size();
        int64_t i = beginPos;
        int64_t totalSize = 0;
        if (filterFlag) {
            MessageFilter filter(request->filter());
            for (; i < msgCount; ++i) {
                const MemoryMessage &memMsg = view[i];
                if (filter.filter(memMsg)) {
                    if (readCount >= count) {
                        break;
                    }
                    totalSize += memMsg.getLen();
                    if (totalSize > maxTotalSize && readCount != 0) {
                        break;
                    }
                    mVec->push_back(memMsg);
                    readCount += memMsg.getMsgCount();
                }
            }
            for (; i < msgCount; ++i) {
                const MemoryMessage &memMsg = view[i];
                if (filter.filter(memMsg)) {
                    response->set_nextmsgid(memMsg.getMsgId());
                    response->set_nexttimestamp(memMsg.getTimestamp());
                    break;
                }
            }
        } else {
            for (; i < msgCount; ++i) {
                const MemoryMessage &memMsg = view[i];
                if (readCount >= count) {
                    break;
                }
                totalSize += memMsg.getLen();
                if (totalSize > maxTotalSize && readCount != 0) {
                    break;
                }
                mVec->push_back(memMsg);
                readCount += memMsg.getMsgCount();
            }
            if (i < msgCount) {
                const MemoryMessage &memMsg = view[i];
                response->set_nextmsgid(memMsg.getMsgId());
                response->set_nexttimestamp(memMsg.getTimestamp());
            }
        }
        if (i >= msgCount) {
            response->set_nextmsgid(view.getNextMsgIdAfterLast());
            response->set_nexttimestamp(view.getNextMsgTimestampAfterLast());
        }
    }
    int64_t returnedMsgsSizePerReadRequest = 0;
    size_t msgCount = mVec->size();
    string decodeBuf;
    bool hasMergedMsg = false;
    int64_t totalMsgCount = 0;
    if (request->versioninfo().supportfb()) {
        ThreadBasedObjectPool<FBMessageWriter> *objectPool =
            Singleton<ThreadBasedObjectPool<FBMessageWriter>>::getInstance();
        FBMessageWriter *writer = objectPool->getObject();
        writer->reset();
        for (size_t i = 0; i < msgCount; ++i) {
            const MemoryMessage &memMsg = (*mVec)[i];
            hasMergedMsg |= memMsg.isMerged();
            returnedMsgsSizePerReadRequest += memMsg.getLen();
            totalMsgCount += MessageConverter::decode(memMsg, writer, decodeBuf);
        }
        writer->finish();
        response->set_messageformat(MF_FB);
        response->set_fbmsgs(writer->data(), writer->size());
        writer->reset();
    } else {
        for (size_t i = 0; i < msgCount; ++i) {
            const MemoryMessage &memMsg = (*mVec)[i];
            hasMergedMsg |= memMsg.isMerged();
            protocol::Message *msg = response->add_msgs();
            returnedMsgsSizePerReadRequest += memMsg.getLen();
            totalMsgCount += MessageConverter::decode(memMsg, *msg, decodeBuf);
        }
        response->set_messageformat(MF_PB);
    }
    assert(totalMsgCount == readCount);
    if (totalMsgCount != readCount) {
        AUTIL_LOG(ERROR,
                  "partition [%s] read error, read count [%ld], decode count [%ld]!",
                  _partitionId.ShortDebugString().c_str(),
                  readCount,
                  totalMsgCount);
    }
    response->set_hasmergedmsg(hasMergedMsg);
    response->set_totalmsgcount(totalMsgCount);
    collector.msgReadQpsFromMemory = msgCount;
    collector.msgReadQpsWithMergedFromMemory = totalMsgCount;
    collector.msgReadRateFromMemory = returnedMsgsSizePerReadRequest;
    if (readerInfo) {
        readerInfo->dataInfo->updateRate(returnedMsgsSizePerReadRequest);
    }
    mVec->clear();
    if (mVec->capacity() * sizeof(MemoryMessage) > MAX_MEMORY_MESSAGE_BUFFER_SIZE) {
        AUTIL_LOG(INFO,
                  "memory message buffer [%ld] large than [%ld], release space.",
                  mVec->capacity() * sizeof(MemoryMessage),
                  MAX_MEMORY_MESSAGE_BUFFER_SIZE);
        MemoryMessageVector mmv;
        mmv.swap(*mVec);
    }
    return ERROR_NONE;
}

protocol::ErrorCode MessageGroup::getMaxMessageId(MessageIdResponse *response) {
    ScopedReadWriteLock lock(_rwLock, 'r');
    auto view = _msgDeque->createView(_readNotCommittedMsg);
    if (view.empty()) {
        return ERROR_BROKER_NO_DATA;
    }
    int64_t msgId = view.getLastMsgId();
    int64_t timestamp = view.getLastMsgTimestamp();
    response->set_msgid(msgId);
    response->set_timestamp(timestamp);
    response->set_maxmsgid(msgId);
    response->set_maxtimestamp(timestamp);
    return ERROR_NONE;
}

protocol::ErrorCode
MessageGroup::getMinMessageIdByTime(int64_t timestamp, MessageIdResponse *response, bool &isMinInMemory) {
    ScopedReadWriteLock lock(_rwLock, 'r');
    auto view = _msgDeque->createView(_readNotCommittedMsg);
    if (view.empty()) {
        return ERROR_BROKER_NO_DATA;
    }
    auto it = view.findByTimestamp(timestamp);

    // the timestamp is refering to the min message id in memory
    // so we may have to check the messages stored in data_root.
    response->set_maxmsgid(view.getLastMsgId());
    response->set_maxtimestamp(view.getLastMsgTimestamp());
    isMinInMemory = (it == view.begin());
    if (it == view.end()) {
        response->set_msgid(view.getLastMsgId());
        response->set_timestamp(view.getLastMsgTimestamp());
        return ERROR_BROKER_TIMESTAMP_TOO_LATEST;
    } else {
        response->set_msgid((*it).getMsgId());
        response->set_timestamp((*it).getTimestamp());
    }
    return ERROR_NONE;
}

void MessageGroup::setCommittedId(int64_t id) {
    autil::ScopedReadWriteLock lock(_rwLock, 'w');
    _msgDeque->setCommittedMsgId(id);
}

int64_t MessageGroup::getCommittedId() const {
    autil::ScopedReadWriteLock lock(_rwLock, 'r');
    return _msgDeque->getCommittedMsgId();
}

int64_t MessageGroup::getCommittedTimestamp() const {
    autil::ScopedReadWriteLock lock(_rwLock, 'r');
    return _msgDeque->getCommittedTimestamp();
}

int64_t MessageGroup::getRemainMetaCount() const {
    autil::ScopedReadWriteLock lock(_rwLock, 'r');
    return _msgDeque->remainMetaCount();
}

int64_t MessageGroup::getRemainDataSize() const {
    autil::ScopedReadWriteLock lock(_rwLock, 'r');
    return _msgConverter->remainDataSize();
}

int64_t MessageGroup::getMetaCountInOneBlock() const {
    autil::ScopedReadWriteLock lock(_rwLock, 'r');
    return _msgDeque->msgCountInOneBlock();
}

RecycleInfo MessageGroup::getRecycleInfo() const {
    autil::ScopedReadWriteLock lock(_rwLock, 'r');
    RecycleInfo info;
    auto view = _msgDeque->createView(_readNotCommittedMsg);
    if (!view.empty()) {
        info.firstMsgTimestamp = view[0].getTimestamp();
        info.lastMsgTimestamp = _msgDeque->getLastAcceptedMsgTimeStamp();
    }
    return info;
}

int64_t MessageGroup::estimateReserveBlockCount(int64_t duration) {
    // with lock held by caller
    auto view = _msgDeque->createView(_readNotCommittedMsg);
    if (view.empty()) {
        return 1;
    } else {
        int64_t begTime = view[0].getTimestamp();
        int64_t endTime = autil::TimeUtility::currentTime();
        int64_t totalDuration = endTime - begTime + 1;
        if (duration >= totalDuration) {
            totalDuration = duration;
        }
        double ratio = (double)duration / (totalDuration);
        int64_t usedBlock = _partitionBlockPool->getUsedBlockCount();
        int64_t reserveCount = (int64_t)std::ceil(usedBlock * ratio);
        return reserveCount;
    }
}

bool MessageGroup::messageIdValid(int64_t msgId, uint64_t msgTime, bool &valid) const {
    valid = false;
    autil::ScopedReadWriteLock lock(_rwLock, 'r');
    auto view = _msgDeque->createView(_readNotCommittedMsg);
    if (view.empty()) {
        valid = msgId == 0;
        return true;
    }
    const MemoryMessage &oldestMsg = view[0];
    int64_t pos = msgId - 1 - oldestMsg.getMsgId();
    if (pos < 0 || pos >= view.size()) {
        if (pos + 1 == 0) {
            // memory only
            valid = oldestMsg.getMsgId() == msgId && oldestMsg.getTimestamp() == msgTime;
            return valid;
        }
        return false;
    } else {
        valid = view[pos].getTimestamp() < msgTime;
        return true;
    }
}

int64_t MessageGroup::findMessageId(uint64_t msgTime) {
    autil::ScopedReadWriteLock lock(_rwLock, 'r');
    auto view = _msgDeque->createView(_readNotCommittedMsg);
    if (view.empty()) {
        return -1;
    }
    auto it = view.findByTimestamp(msgTime);
    if (it == view.end()) {
        return view.getLastMsgId();
    } else if (it->getTimestamp() == msgTime) {
        return it->getMsgId();
    } else {
        return it->getMsgId() - 1;
    }
}

bool MessageGroup::hasMsgInRange(uint16_t from, uint16_t to, int64_t begId, int64_t endId) const {
    autil::ScopedReadWriteLock lock(_rwLock, 'r');
    if (!_msgDeque) {
        return false;
    }
    auto view = _msgDeque->createView(_readNotCommittedMsg);
    if (view.empty()) {
        return false;
    }
    const MemoryMessage &oldestMsg = view[0];
    int64_t begPos = begId - oldestMsg.getMsgId();
    int64_t endPos = endId - oldestMsg.getMsgId();
    int64_t queueSize = view.size();
    uint16_t payload;
    int64_t i = begPos >= 0 ? begPos : 0;
    for (; i <= endPos && i < queueSize; i++) {
        const MemoryMessage &memMsg = view[i];
        payload = memMsg.getPayload();
        if (payload >= from && payload <= to) {
            return true;
        }
    }
    return false;
}

bool MessageGroup::canReadNotCommittedMsg(bool readNotCommittedMsg, TopicMode topicMode) {
    if (TOPIC_MODE_SECURITY == topicMode) {
        return false;
    } else if (TOPIC_MODE_MEMORY_ONLY == topicMode || TOPIC_MODE_MEMORY_PREFER == topicMode) {
        return true;
    } else {
        return readNotCommittedMsg;
    }
}

} // namespace storage
} // namespace swift
