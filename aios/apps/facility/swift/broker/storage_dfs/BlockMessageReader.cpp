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
#include "swift/broker/storage_dfs/BlockMessageReader.h"

#include <algorithm>
#include <assert.h>
#include <iosfwd>
#include <memory>
#include <string.h>
#include <string>

#include "autil/Singleton.h"
#include "flatbuffers/stl_emulation.h"
#include "swift/broker/storage_dfs/FileCache.h"
#include "swift/broker/storage_dfs/FileMessageMetaVec.h"
#include "swift/common/FileMessageMeta.h"
#include "swift/common/ThreadBasedObjectPool.h"
#include "swift/protocol/BrokerRequestResponse.pb.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/protocol/FBMessageWriter.h"
#include "swift/protocol/SwiftMessage.pb.h"

using namespace std;
using namespace flatbuffers;

using namespace swift::protocol;
using namespace swift::util;
using namespace swift::filter;
using namespace swift::common;
using namespace swift::monitor;

namespace swift {
namespace storage {
AUTIL_LOG_SETUP(swift, BlockMessageReader);

BlockMessageReader::BlockMessageReader(const FilePairPtr &filePair,
                                       FileCache *fileCache,
                                       TimeoutChecker *timeoutChecker,
                                       ReaderInfo *readerInfo,
                                       ReaderInfoMap *readerInfoMap,
                                       ReadMetricsCollector *collector) {
    _filePair = filePair;
    _fileCache = fileCache;
    _timeoutChecker = timeoutChecker;
    _bufferOffset = -1;
    _bufferDataSize = -1;
    _readerInfo = readerInfo;
    _readerInfoMap = readerInfoMap;
    _readMetricsCollector = collector;
}

BlockMessageReader::~BlockMessageReader() {}

ErrorCode BlockMessageReader::fillMessage(const ConsumptionRequest *request, MessageResponse *response) {
    int64_t begMsgId = _filePair->beginMessageId;
    int64_t endMsgId = _filePair->getEndMessageId();
    int64_t startId = request->startid();
    assert(startId >= begMsgId && startId < endMsgId);
    int64_t cursor = startId - begMsgId;
    MessageFilter filter(request->filter());
    int64_t maxReadCount = request->count();
    int64_t maxReadSize = request->maxtotalsize();
    FileMessageMetaVec fileMetaVec;
    ErrorCode ec = fileMetaVec.init(_fileCache,
                                    _filePair->metaFileName,
                                    _filePair->isAppending(),
                                    cursor,
                                    endMsgId - begMsgId,
                                    _timeoutChecker,
                                    _readerInfo,
                                    _readerInfoMap,
                                    _readMetricsCollector);
    if (ec != ERROR_NONE) {
        response->set_nextmsgid(startId);
        response->clear_nexttimestamp();
        _blockReadInfo.metaFail = true;
        AUTIL_LOG(WARN, "init meta file [%s] fail", _filePair->metaFileName.c_str());
        return ec;
    }
    fileMetaVec.getReadInfo(_blockReadInfo.dfsMetaSize, _blockReadInfo.dfsMetaSizeFromCache);
    if (!request->versioninfo().supportfb()) {
        ec = doFillMessagePB(fileMetaVec, filter, maxReadCount, maxReadSize, response, cursor);
    } else {
        ec = doFillMessageFB(fileMetaVec, filter, maxReadCount, maxReadSize, response, cursor);
    }
    FileMessageMeta meta;
    for (; cursor < fileMetaVec.end(); ++cursor) {
        fileMetaVec.fillMeta(cursor, meta);
        if (filter.filter(meta)) {
            break;
        }
    }
    response->set_nextmsgid(cursor + begMsgId);
    if (cursor < fileMetaVec.end()) {
        fileMetaVec.fillMeta(cursor, meta);
        response->set_nexttimestamp(meta.timestamp);
    } else {
        fileMetaVec.fillMeta(fileMetaVec.end() - 1, meta);
        response->set_nexttimestamp(meta.timestamp + 1);
    }
    return ec;
}

ErrorCode BlockMessageReader::doFillMessagePB(const FileMessageMetaVec &fileMetaVec,
                                              const MessageFilter &filter,
                                              int64_t maxReadCount,
                                              int64_t maxReadSize,
                                              MessageResponse *response,
                                              int64_t &cursor) {
    int64_t readedSize = 0;
    int32_t readedCount = 0;
    ErrorCode ec = ERROR_NONE;
    string dataStr;
    response->set_messageformat(MF_PB);
    int64_t begMsgId = _filePair->beginMessageId;
    FileMessageMeta meta;
    bool hasMergedMsg = false;
    for (; cursor < fileMetaVec.end() && readedCount < maxReadCount; ++cursor) {
        fileMetaVec.fillMeta(cursor, meta);
        if (!filter.filter(meta)) {
            continue;
        }
        // can read
        int64_t msgLen = fileMetaVec.getMsgLen(cursor);
        assert(msgLen >= 0);
        if (readedSize + msgLen > maxReadSize && readedCount != 0) {
            break;
        }
        dataStr.resize(msgLen);
        char *buffer = const_cast<char *>(dataStr.data());
        int64_t fileBeginOffset = fileMetaVec.getMsgBegin(cursor);
        ec = readFile(fileBeginOffset, msgLen, buffer);
        if (ec != ERROR_NONE) {
            AUTIL_LOG(WARN,
                      "read file[%s] failed, begin pos[%ld], len[%ld], ec[%s]",
                      _filePair->dataFileName.c_str(),
                      fileBeginOffset,
                      msgLen,
                      ErrorCode_Name(ec).c_str());
            break;
        }
        Message *msg = response->add_msgs();
        msg->set_msgid(begMsgId + cursor);
        msg->set_timestamp(meta.timestamp);
        msg->set_uint16payload(meta.payload);
        msg->set_uint8maskpayload(meta.maskPayload);
        msg->set_compress(meta.isCompress);
        msg->set_merged(meta.isMerged);
        if (meta.isMerged && dataStr.size() >= sizeof(uint16_t)) {
            hasMergedMsg |= meta.isMerged;
            readedCount += *(const uint16_t *)(dataStr.c_str());
        } else {
            readedCount++;
        }
        std::string *msgDataStr = msg->mutable_data();
        msgDataStr->swap(dataStr);
        _blockReadInfo.messageCount++;
        readedSize += msgLen;
        _blockReadInfo.messageDataSize += msgLen;
    }
    _blockReadInfo.mergedCount += readedCount;
    response->set_totalmsgcount(readedCount);
    response->set_hasmergedmsg(hasMergedMsg);
    return ec;
}

ErrorCode BlockMessageReader::doFillMessageFB(const FileMessageMetaVec &fileMetaVec,
                                              const MessageFilter &filter,
                                              int64_t maxReadCount,
                                              int64_t maxReadSize,
                                              MessageResponse *response,
                                              int64_t &cursor) {
    int64_t readedSize = 0;
    int32_t readedCount = 0;
    ErrorCode ec = ERROR_NONE;
    int64_t begMsgId = _filePair->beginMessageId;
    string dataStr;
    response->set_messageformat(MF_FB);
    ThreadBasedObjectPool<FBMessageWriter> *objectPool =
        autil::Singleton<ThreadBasedObjectPool<FBMessageWriter>>::getInstance();
    FBMessageWriter *writer = objectPool->getObject();
    writer->reset();
    FileMessageMeta meta;
    bool hasMergedMsg = false;
    for (; cursor < (int64_t)fileMetaVec.end() && readedCount < maxReadCount; ++cursor) {
        fileMetaVec.fillMeta(cursor, meta);
        if (!filter.filter(meta)) {
            continue;
        }
        // can read
        int64_t msgLen = fileMetaVec.getMsgLen(cursor);
        assert(msgLen >= 0);
        if (readedSize + msgLen > maxReadSize && readedCount != 0) {
            break;
        }
        dataStr.resize(msgLen);
        char *buffer = const_cast<char *>(dataStr.data());
        int64_t fileBeginOffset = fileMetaVec.getMsgBegin(cursor);
        ec = readFile(fileBeginOffset, msgLen, buffer);
        if (ec != ERROR_NONE) {
            AUTIL_LOG(WARN,
                      "read file[%s] failed, begin pos[%ld], len[%ld], ec[%s]",
                      _filePair->dataFileName.c_str(),
                      fileBeginOffset,
                      msgLen,
                      ErrorCode_Name(ec).c_str());
            break;
        }
        if (meta.isMerged && dataStr.size() >= sizeof(uint16_t)) {
            hasMergedMsg |= meta.isMerged;
            readedCount += *(const uint16_t *)(dataStr.c_str());
        } else {
            readedCount++;
        }
        writer->addMessage(
            dataStr, begMsgId + cursor, meta.timestamp, meta.payload, meta.maskPayload, meta.isCompress, meta.isMerged);
        _blockReadInfo.messageCount++;
        readedSize += msgLen;
        _blockReadInfo.messageDataSize += msgLen;
    }
    writer->finish();
    response->set_fbmsgs(writer->data(), writer->size());
    _blockReadInfo.mergedCount += readedCount;
    response->set_hasmergedmsg(hasMergedMsg);
    response->set_totalmsgcount(readedCount);
    writer->reset();
    return ec;
}

bool BlockMessageReader::hasDataInBuffer(int64_t startOffset) {
    return _dataBlock != NULL && startOffset >= _bufferOffset && startOffset < _bufferOffset + _bufferDataSize;
}

ErrorCode BlockMessageReader::readFile(int64_t beginPos, int64_t len, char *buf) {
    while (len > 0) {
        int64_t endPos = beginPos + len;
        if (!hasDataInBuffer(beginPos)) {
            int64_t fileBlockSize = _fileCache->getCacheBlockSize();
            _bufferOffset = beginPos / fileBlockSize * fileBlockSize;
            bool isFromCache = false;
            ErrorCode errorCode = _fileCache->readBlock(_filePair->dataFileName,
                                                        _bufferOffset,
                                                        _filePair->isAppending(),
                                                        _timeoutChecker,
                                                        _dataBlock,
                                                        isFromCache,
                                                        _readerInfo,
                                                        _readerInfoMap,
                                                        _readMetricsCollector);
            if (errorCode != ERROR_NONE) {
                AUTIL_LOG(WARN,
                          "read block in[%s] failed, begin[%ld], blockSize[%ld], ec[%s]",
                          _filePair->dataFileName.c_str(),
                          _bufferOffset,
                          fileBlockSize,
                          ErrorCode_Name(errorCode).c_str());
                _blockReadInfo.dataFail = true;
                _dataBlock.reset();
                if (errorCode != ERROR_BROKER_BUSY) {
                    _fileCache->setBadFile(_filePair->dataFileName);
                }
                return errorCode;
            }
            _bufferDataSize = _dataBlock->getActualSize();
            _blockReadInfo.dfsDataSize += _bufferDataSize;
            if (isFromCache) {
                _blockReadInfo.dfsDataSizeFromCache += _bufferDataSize;
            }
            if (!hasDataInBuffer(beginPos)) {
                _fileCache->removeBlockItem(_filePair->dataFileName, _bufferOffset);
                _fileCache->setBadFile(_filePair->dataFileName);
                AUTIL_LOG(WARN,
                          "data not in block, file[%s], begin[%ld], some data lost",
                          _filePair->dataFileName.c_str(),
                          beginPos);
                return ERROR_BROKER_SOME_MESSAGE_LOST;
            }
        }
        int64_t copyBeginPos = beginPos - _bufferOffset;
        int64_t copyEndPos = min(endPos - _bufferOffset, _bufferDataSize);
        int64_t needCopySize = copyEndPos - copyBeginPos;
        memcpy(buf, _dataBlock->getBuffer() + copyBeginPos, needCopySize);
        buf += needCopySize;
        len -= needCopySize;
        assert(len >= 0);
        beginPos += needCopySize;
    }
    return protocol::ERROR_NONE;
}

} // namespace storage
} // namespace swift
