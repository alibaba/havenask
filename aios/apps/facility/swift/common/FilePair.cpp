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
#include "swift/common/FilePair.h"

#include <algorithm>
#include <assert.h>
#include <cstddef>
#include <memory>
#include <sys/types.h>

#include "autil/TimeUtility.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/File.h"
#include "fslib/fs/FileSystem.h"
#include "swift/common/FileMessageMeta.h"
#include "swift/protocol/ErrCode.pb.h"

using namespace swift::protocol;
using namespace swift::common;
using namespace autil;
using namespace fslib::fs;
using namespace std;

namespace swift {
namespace storage {
AUTIL_LOG_SETUP(swift, FilePair);

FilePair::FilePair(const string &dataFileName,
                   const string &metaFileName,
                   const int64_t beginMessageId,
                   const int64_t beginTimestamp,
                   bool isAppending)
    : dataFileName(dataFileName)
    , metaFileName(metaFileName)
    , beginMessageId(beginMessageId)
    , beginTimestamp(beginTimestamp)
    , _endMessageId(-1)
    , _isAppending(isAppending) {}

int64_t FilePair::getMessageCount() {
    int64_t endId = getEndMessageId();
    if (endId >= beginMessageId) {
        return endId - beginMessageId;
    } else {
        return 0;
    }
}

FilePairPtr FilePair::getNext() const {
    ScopedLock lock(_mutex);
    return _next;
}

void FilePair::setNext(const FilePairPtr &pair) {
    ScopedLock lock(_mutex);
    _next = pair;
}

protocol::ErrorCode FilePair::getEndMessageId(int64_t &msgId) {
    ScopedLock lock(_mutex);
    if (_endMessageId != -1) {
        msgId = _endMessageId;
        return ERROR_NONE;
    }
    ErrorCode ec = updateEndMessageId();
    if (ec != ERROR_NONE || _endMessageId == -1) {
        msgId = beginMessageId;
    } else {
        msgId = _endMessageId;
    }
    return ec;
}

int64_t FilePair::getEndMessageId() {
    int64_t msgId;
    getEndMessageId(msgId);
    return msgId;
}

void FilePair::setEndMessageId(int64_t msgId) {
    ScopedLock lock(_mutex);
    _endMessageId = msgId;
}

int64_t FilePair::getMetaLength() {
    int64_t msgCount = getMessageCount();
    return msgCount * FILE_MESSAGE_META_SIZE;
}

int64_t FilePair::getDataLength() {
    auto endMessageId = getEndMessageId();
    common::FileMessageMeta messageMeta;
    auto ec = getMessageMetaByMessageId(endMessageId - 1, messageMeta);
    if (ec != ERROR_NONE) {
        AUTIL_LOG(WARN, "get message meta failed, error[%s]", ErrorCode_Name(ec).c_str());
        return -1;
    }
    return messageMeta.endPos;
}

ErrorCode FilePair::updateEndMessageId() {
    int64_t msgCnt;
    ErrorCode ec;
    if (_next == NULL) {
        ec = getMessageCountByMetaContent(msgCnt);
        if (ec != ERROR_NONE) {
            _endMessageId = -1;
        } else {
            _endMessageId = beginMessageId + msgCnt;
        }
        return ec;
    }

    ec = getMessageCountByMetaLength(msgCnt);
    if (ec != ERROR_NONE) {
        _endMessageId = -1;
        return ec;
    }
    if (beginMessageId + msgCnt != _next->beginMessageId) {
        ec = getMessageCountByMetaContent(msgCnt);
        if (ec != ERROR_NONE) {
            _endMessageId = -1;
            return ec;
        }
        if (beginMessageId + msgCnt < _next->beginMessageId) {
            _endMessageId = beginMessageId + msgCnt;
        } else {
            _endMessageId = _next->beginMessageId;
        }
    } else {
        _endMessageId = beginMessageId + msgCnt;
    }
    return ERROR_NONE;
}

protocol::ErrorCode FilePair::getMessageCountByMetaLength(int64_t &msgCnt) {
    fslib::FileMeta fileMeta;
    msgCnt = -1;
    fslib::ErrorCode ec = FileSystem::getFileMeta(metaFileName, fileMeta);
    if (ec != fslib::EC_OK) {
        AUTIL_LOG(ERROR, "get file meta [%s] failed", metaFileName.c_str());
        AUTIL_LOG(ERROR, "fslib errorcode [%d], errormsg [%s]", ec, fslib::fs::FileSystem::getErrorString(ec).c_str());
        return ERROR_BROKER_SOME_MESSAGE_LOST;
    } else {
        msgCnt = fileMeta.fileLength / FILE_MESSAGE_META_SIZE;
        return ERROR_NONE;
    }
}

protocol::ErrorCode FilePair::getMessageCountByMetaContent(int64_t &msgCnt) {
    fslib::FileMeta fileMeta;
    msgCnt = -1;
    fslib::ErrorCode ec = FileSystem::getFileMeta(metaFileName, fileMeta);
    if (ec != fslib::EC_OK) {
        AUTIL_LOG(ERROR, "get file meta [%s] failed", metaFileName.c_str());
        AUTIL_LOG(ERROR, "fslib errorcode [%d], errormsg [%s]", ec, fslib::fs::FileSystem::getErrorString(ec).c_str());
        return ERROR_BROKER_SOME_MESSAGE_LOST;
    }
    File *metaFile = FileSystem::openFile(metaFileName, fslib::READ);
    unique_ptr<File> auto_metaFile(metaFile);
    if (!metaFile || !metaFile->isOpened()) {
        AUTIL_LOG(ERROR, "open file meta [%s] failed", metaFileName.c_str());
        return ERROR_BROKER_SOME_MESSAGE_LOST;
    }
    AUTIL_LOG(INFO, "open file meta [%s] success!", metaFileName.c_str());
    int64_t maxBlockSize = 512 * 1024 * 1024; // 512M
    int64_t actualSeekPos = 0;
    ErrorCode errorCode = seekFslibFile(metaFileName, metaFile, fileMeta.fileLength + maxBlockSize, actualSeekPos);
    if (ERROR_NONE != errorCode) {
        return errorCode;
    }
    msgCnt = actualSeekPos / FILE_MESSAGE_META_SIZE;
    return ERROR_NONE;
}

protocol::ErrorCode FilePair::getMessageMetaByMessageId(int64_t messageId, FileMessageMeta &meta) {
    auto endMessageId = getEndMessageId();
    if (messageId < beginMessageId || messageId >= endMessageId) {
        return ERROR_BROKER_SOME_MESSAGE_LOST;
    }

    int64_t metaOffset = (messageId - beginMessageId) * FILE_MESSAGE_META_SIZE;
    File *metaFile = FileSystem::openFile(metaFileName, fslib::READ, false);
    std::unique_ptr<File> metaFileUniquePtr(metaFile);
    if (!metaFile || !metaFile->isOpened()) {
        AUTIL_LOG(ERROR, "open file meta [%s] failed", metaFileName.c_str());
        return ERROR_BROKER_SOME_MESSAGE_LOST;
    }
    char buffer[FILE_MESSAGE_META_SIZE] = {0};
    auto readLen = metaFile->pread(buffer, FILE_MESSAGE_META_SIZE, metaOffset);
    if (readLen != FILE_MESSAGE_META_SIZE) {
        auto ec = metaFile->getLastError();
        AUTIL_LOG(ERROR,
                  "read meta file failed. file[%s] offset[%ld] error[%d %s]",
                  metaFileName.c_str(),
                  metaOffset,
                  ec,
                  FileSystem::getErrorString(ec).c_str());
        return ERROR_BROKER_SOME_MESSAGE_LOST;
    }
    common::FileMessageMeta *fileMeta = (FileMessageMeta *)buffer;
    meta = fileMeta[0];
    return ERROR_NONE;
}

protocol::ErrorCode
FilePair::seekFslibFile(const string &fileName, File *file, int64_t seekPos, int64_t &actualSeekPos) {
    assert(file && file->isOpened());
    actualSeekPos = 0;

    fslib::FileMeta fileMeta;
    fslib::ErrorCode fsErrorCode = FileSystem::getFileMeta(fileName, fileMeta);
    if (fsErrorCode != fslib::EC_OK) {
        AUTIL_LOG(ERROR, "get file meta [%s] failed", fileName.c_str());
        AUTIL_LOG(ERROR,
                  "fslib errorcode [%d], errormsg [%s]",
                  fsErrorCode,
                  fslib::fs::FileSystem::getErrorString(fsErrorCode).c_str());
        return protocol::ERROR_BROKER_FS_OPERATE;
    }
    if (seekPos > fileMeta.fileLength) {
        fsErrorCode = file->seek(fileMeta.fileLength, fslib::FILE_SEEK_SET);
    } else {
        fsErrorCode = file->seek(seekPos, fslib::FILE_SEEK_SET);
    }
    if (fsErrorCode != fslib::EC_OK) {
        AUTIL_LOG(ERROR, "seek file [%s] failed, seek offset [%ld] ", fileName.c_str(), seekPos);
        AUTIL_LOG(ERROR,
                  "fslib errorcode [%d], errormsg [%s]",
                  fsErrorCode,
                  fslib::fs::FileSystem::getErrorString(fsErrorCode).c_str());
        return protocol::ERROR_BROKER_FS_OPERATE;
    }
    actualSeekPos = file->tell();
    if (actualSeekPos >= seekPos) {
        return protocol::ERROR_NONE;
    }

    vector<char> tmpBuf;
    int64_t expectReadLen = seekPos - actualSeekPos;
    int64_t buffSize = expectReadLen > DEFAULT_READ_BUF_SIZE ? DEFAULT_READ_BUF_SIZE : expectReadLen;
    tmpBuf.resize(buffSize);
    while (actualSeekPos < seekPos) {
        if (file->isEof()) {
            AUTIL_LOG(INFO,
                      "seek inputFile [%s], expectSeekPos [%ld], "
                      "actualSeekPos[%ld].",
                      fileName.c_str(),
                      seekPos,
                      actualSeekPos);
            return protocol::ERROR_NONE;
        }

        int64_t needReadLen = seekPos - actualSeekPos;
        needReadLen = needReadLen > buffSize ? buffSize : needReadLen;
        ssize_t readLen = file->read(&(*tmpBuf.begin()), needReadLen);
        if (readLen == -1) {
            fsErrorCode = file->getLastError();
            AUTIL_LOG(ERROR,
                      "fslib errorcode [%d], errormsg [%s]",
                      fsErrorCode,
                      fslib::fs::FileSystem::getErrorString(fsErrorCode).c_str());
            return protocol::ERROR_BROKER_READ_FILE;
        }
        actualSeekPos += readLen;
        tmpBuf.clear();
    }
    return protocol::ERROR_NONE;
}

} // namespace storage
} // namespace swift
