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
#include "swift/broker/storage_dfs/FileMessageMetaVec.h"

#include <algorithm>
#include <cstddef>
#include <memory>

#include "swift/broker/storage_dfs/FileCache.h"
#include "swift/common/Common.h"
#include "swift/common/FileMessageMeta.h"
#include "swift/protocol/ErrCode.pb.h"

namespace swift {
namespace util {
class TimeoutChecker;
} // namespace util
} // namespace swift

using namespace swift::util;
using namespace swift::common;
using namespace swift::monitor;
using namespace swift::protocol;
using namespace std;

namespace swift {
namespace storage {
AUTIL_LOG_SETUP(swift, FileMessageMetaVec);

FileMessageMetaVec::FileMessageMetaVec() {
    _fileMetaVec = NULL;
    _start = 0;
    _end = -1;
    _firstMsgDataStart = 0;
    _dfsReadSize = 0;
    _fromCacheSize = 0;
}

FileMessageMetaVec::~FileMessageMetaVec() {}

ErrorCode FileMessageMetaVec::init(FileCache *fileCache,
                                   const string &fileName,
                                   bool isAppending,
                                   int64_t begPos,
                                   int64_t endPos,
                                   TimeoutChecker *timeoutChecker,
                                   ReaderInfo *readerInfo,
                                   const ReaderInfoMap *readerInfoMap,
                                   ReadMetricsCollector *collector,
                                   bool useLen) {
    int64_t metaOffset = begPos * FILE_MESSAGE_META_SIZE;
    bool isFromCache = false;
    ErrorCode errorCode = fileCache->readBlock(fileName,
                                               metaOffset,
                                               isAppending,
                                               timeoutChecker,
                                               _metaBlock,
                                               isFromCache,
                                               readerInfo,
                                               readerInfoMap,
                                               collector);
    if (errorCode != ERROR_NONE) {
        AUTIL_LOG(WARN,
                  "init file meta fail, file [%s], begPos [%ld], error code[%s]",
                  fileName.c_str(),
                  begPos,
                  ErrorCode_Name(errorCode).c_str());
        if (errorCode != ERROR_BROKER_BUSY) {
            fileCache->setBadFile(fileName);
        }
        return errorCode;
    }
    int64_t readLen = _metaBlock->getActualSize();
    if (readLen == 0) {
        AUTIL_LOG(WARN, "init file meta fail, file [%s], begPos [%ld], endPos [%ld]", fileName.c_str(), begPos, endPos);
        fileCache->setBadFile(fileName);
        return ERROR_BROKER_SOME_MESSAGE_LOST;
    }
    _dfsReadSize += readLen;
    if (isFromCache) {
        _fromCacheSize += readLen;
    }
    _fileMetaVec = (FileMessageMeta *)_metaBlock->getBuffer();
    int64_t blockSize = fileCache->getCacheBlockSize();
    int64_t msgCountInOneBlock = blockSize / FILE_MESSAGE_META_SIZE;
    _start = begPos / msgCountInOneBlock * msgCountInOneBlock; //得到meta所在的block起始位置消息id
    _end = _start + (readLen / FILE_MESSAGE_META_SIZE);        //得到这个block结束位置消息id
    int64_t expectEnd = (begPos + msgCountInOneBlock) / msgCountInOneBlock * msgCountInOneBlock;
    expectEnd = min(expectEnd, endPos);
    if (expectEnd > _end) {
        AUTIL_LOG(WARN,
                  "end msgid expect [%ld] actual [%ld], file [%s] "
                  "begin [%ld] end [%ld], some data loss.",
                  expectEnd,
                  _end,
                  fileName.c_str(),
                  begPos,
                  endPos);
        fileCache->removeBlockItem(fileName, metaOffset);
        fileCache->setBadFile(fileName);
        return ERROR_BROKER_SOME_MESSAGE_LOST;
    } else if (expectEnd < _end) {
        AUTIL_LOG(INFO,
                  "end msgid expect [%ld] actual [%ld], file [%s] "
                  "begin [%ld] end [%ld].",
                  expectEnd,
                  _end,
                  fileName.c_str(),
                  begPos,
                  endPos);
        _end = expectEnd;
    }
    if (useLen && begPos / msgCountInOneBlock != 0 && begPos % msgCountInOneBlock == 0) {
        FileMessageMetaVec preMetaVec;
        int64_t preBlockOffset = _start - 1;
        errorCode = preMetaVec.init(
            fileCache, fileName, isAppending, preBlockOffset, endPos, timeoutChecker, readerInfo, readerInfoMap);
        if (errorCode != ERROR_NONE) {
            return errorCode;
        }
        FileMessageMeta meta;
        preMetaVec.fillMeta(_start - 1, meta);
        _firstMsgDataStart = meta.endPos;
        int64_t dfsReadSize, cacheReadSize;
        preMetaVec.getReadInfo(dfsReadSize, cacheReadSize);
        _fromCacheSize += cacheReadSize;
        _dfsReadSize += dfsReadSize;
    }
    return ERROR_NONE;
}

bool FileMessageMetaVec::fillMeta(int64_t index, FileMessageMeta &meta) const {
    if (index >= _end || index < _start) {
        return false;
    }
    meta = _fileMetaVec[index - _start];
    return true;
}

int64_t FileMessageMetaVec::getMsgBegin(int64_t index) const {
    if (index >= _end || index < _start) {
        return -1;
    }
    if (index == _start) {
        return _firstMsgDataStart;
    } else {
        return _fileMetaVec[index - _start - 1].endPos;
    }
}

int64_t FileMessageMetaVec::getMsgLen(int64_t index) const {
    if (index >= _end || index < _start) {
        return -1;
    }
    if (index == _start) {
        return _fileMetaVec[index - _start].endPos - _firstMsgDataStart;
    } else {
        return _fileMetaVec[index - _start].endPos - _fileMetaVec[index - _start - 1].endPos;
    }
}

void FileMessageMetaVec::getReadInfo(int64_t &dfsReadSize, int64_t &fromCacheSize) const {
    dfsReadSize = _dfsReadSize;
    fromCacheSize = _fromCacheSize;
}

} // namespace storage
} // namespace swift
