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
#include "swift/broker/storage_dfs/MessageCommitter.h"

#include <algorithm>
#include <assert.h>
#include <memory>
#include <sstream>
#include <stddef.h>
#include <sys/types.h>

#include "autil/CommonMacros.h"
#include "autil/TimeUtility.h"
#include "ext/alloc_traits.h"
#include "fslib/common/common_type.h"
#include "fslib/fs/File.h"
#include "fslib/fs/FileSystem.h"
#include "swift/broker/storage_dfs/FileManager.h"
#include "swift/common/FileMessageMeta.h"
#include "swift/config/PartitionConfig.h"
#include "swift/monitor/BrokerMetricsReporter.h"
#include "swift/monitor/MetricsCommon.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/protocol/MessageAdapter.h"
#include "swift/util/Block.h"
#include "swift/util/PanguInlineFileUtil.h"

namespace swift {
namespace protocol {
class Message;
namespace flat {
struct Messages;
} // namespace flat
} // namespace protocol
} // namespace swift

using namespace autil;
using namespace std;
using namespace fslib::fs;

using namespace swift::protocol;
using namespace swift::monitor;
using namespace swift::util;
using namespace swift::common;

namespace swift {
namespace storage {
AUTIL_LOG_SETUP(swift, MessageCommitter);
uint64_t MAX_COMMIT_BUFFER_SIZE = 1 << 21;

MessageCommitter::MessageCommitter(const PartitionId &partitionId,
                                   const config::PartitionConfig &config,
                                   FileManager *fileManager,
                                   monitor::BrokerMetricsReporter *metricsReporter,
                                   bool enableFastRecover)
    : _partitionId(partitionId)
    , _dataRoot(config.getDataRoot())
    , _commitFileIsReaded(false)
    , _maxCommitSize(config.getMaxCommitSize())
    , _maxCommitInterval(config.getMaxCommitInterval())
    , _maxFileSize(config.getMaxFileSize())
    , _minFileSize(config.getMinFileSize())
    , _maxFileSplitTime(config.getMaxFileSplitInterval())
    , _closeNoWriteFileInterval(config.getCloseNoWriteFileInterval())
    , _minChangeFileForDfsErrorTime(config.getMinChangeFileForDfsErrorTime())
    , _maxCommitTimeAsError(config.getMaxCommitTimeAsError())
    , _lastCommitTimeAsError(-1)
    , _fileManager(fileManager)
    , _committedId(_fileManager->getLastMessageId())
    , _writedId(_committedId)
    , _writedSize(0)
    , _committedSize(0)
    , _fileCreateTime(TimeUtility::currentTime())
    , _lastCommitTime(_fileCreateTime)
    , _fileMessageCount(0)
    , _writedSizeForReport(0)
    , _maxMessageCountInOneFile(config.getMaxMessageCountInOneFile())
    , _writeFailDataCount(0)
    , _writeFailMetaCount(0)
    , _dataOutputFile(NULL)
    , _metaOutputFile(NULL)
    , _metricsReporter(metricsReporter)
    , _writeBufferSize(0)
    , _enableFastRecover(enableFastRecover)
    , _canRefeshVersion(true)
    , _hasSealError(false) {
    _tags.AddTag("topic", _partitionId.topicname());
    _tags.AddTag("partition", intToString(_partitionId.id()));
    AUTIL_LOG(INFO,
              "parition [%s] init committer, _dataRoot[%s], _maxCommitSize[%fMB], "
              "_maxCommitInterval[%fms], _maxFileSize[%fMB], _minFileSize[%fMB],"
              "_maxFileSplitTime[%fms], _minChangeFileForDfsErrorTime[%fms], "
              "_maxCommitTimeAsError[%fms], _closeNoWriteFileInterval[%fms]",
              _partitionId.ShortDebugString().c_str(),
              _dataRoot.c_str(),
              _maxCommitSize / 1024.0 / 1024.0,
              _maxCommitInterval / 1000.0,
              _maxFileSize / 1024.0 / 1024.0,
              _minFileSize / 1024.0 / 1024.0,
              _maxFileSplitTime / 1000.0,
              _minChangeFileForDfsErrorTime / 1000.0,
              _maxCommitTimeAsError / 1000.0,
              _closeNoWriteFileInterval / 1000.0);
}

MessageCommitter::~MessageCommitter() {
    if (!_hasSealError) {
        commitFile();
        closeFile();
    }
}

void MessageCommitter::updateCommittedId(int64_t newCommittedId) {
    commitFile();
    closeFile();
    if (newCommittedId > _committedId) {
        _committedId = newCommittedId;
        _writedId = _committedId;
    }
}

ErrorCode MessageCommitter::write(const MemoryMessageVector &vec) { return writeMessages(vec); }

protocol::ErrorCode
MessageCommitter::write(const ::google::protobuf::RepeatedPtrField<::swift::protocol::Message> &msgs) {
    return writeMessages(msgs);
}

protocol::ErrorCode MessageCommitter::write(const ::swift::protocol::flat::Messages &msgs) {
    return writeMessages(msgs);
}

ErrorCode MessageCommitter::maybeCommit(int64_t curTime) {
    bool needCommit = needCommitFile(curTime);
    bool needClose = needCloseFile(curTime);
    if (needCommit || needClose) {
        ErrorCode ec = commitFile();
        if (ec != ERROR_NONE) {
            closeFile();
            AUTIL_LOG(ERROR, "partiton [%s] commit file fail", _partitionId.ShortDebugString().c_str());
            return ec;
        }
    }
    return needClose ? closeFile() : ERROR_NONE;
}

bool MessageCommitter::needCommitFile(int64_t curTime) {

    if (_committedId >= _writedId) {
        return false;
    }
    if (_meta.size() >= MAX_COMMIT_BUFFER_SIZE) {
        return true;
    }
    if (curTime - _lastCommitTime >= _maxCommitInterval) {
        return true;
    } else {
        int64_t metaSize = (_writedId - _committedId) * MEMORY_MESSAGE_META_SIZE;
        return (metaSize + _writedSize - _committedSize) >= _maxCommitSize;
    }
}

bool MessageCommitter::needCloseFile(int64_t curTime) {
    return _dataOutputFile && ((curTime - _fileCreateTime > _maxFileSplitTime) || (_writedSize > _maxFileSize) ||
                               (_fileMessageCount >= _maxMessageCountInOneFile) ||
                               (curTime - _lastCommitTime >= _closeNoWriteFileInterval) ||
                               (_commitFileIsReaded && (curTime - _fileCreateTime >= 2 * _closeNoWriteFileInterval ||
                                                        _committedSize > _minFileSize)));
}

ErrorCode MessageCommitter::commitFile() {
    if (!_dataOutputFile || _committedId >= _writedId) {
        return ERROR_NONE;
    }
    CommitFileMetricsCollector collector(_partitionId.topicname(), _partitionId.id());

    int64_t begTime = TimeUtility::currentTime();
    if (_dataOutputFile) {
        ErrorCode erc = writeDataBuffer();
        if (erc != ERROR_NONE) {
            AUTIL_LOG(WARN, "write data file[%s] failed", _dataOutputFile->getFileName());
            resetBuffer();
            return ERROR_BROKER_COMMIT_FILE;
        }
        fslib::ErrorCode ec = fslib::EC_OK;
        FUNC_GET_LATENCY(ec, _dataOutputFile->flush(), collector.dfsFlushDataLatency);
        if (ec != fslib::EC_OK) {
            setSealError(ec);
            AUTIL_LOG(WARN,
                      "flush data file[%s] failed, fslib ec[%d], errormsg[%s]",
                      _dataOutputFile->getFileName(),
                      ec,
                      FileSystem::getErrorString(ec).c_str());
            resetBuffer();
            return ERROR_BROKER_COMMIT_FILE;
        }
    }
    int64_t committedMetaSize = 0;
    if (_metaOutputFile) {
        int64_t totalLen = _meta.size();
        int64_t leftLen = totalLen;
        while (leftLen > 0) {
            ssize_t writeLen = -1;
            FUNC_REPORT_LATENCY(writeLen,
                                _metaOutputFile->write(&_meta[0] + totalLen - leftLen, leftLen),
                                reportDfsWriteMetaLatency,
                                _tags);
            if (writeLen == -1) {
                resetBuffer();
                fslib::ErrorCode ec = _metaOutputFile->getLastError();
                setSealError(ec);
                AUTIL_LOG(WARN,
                          "write meta file[%s] fail, fslib ec[%d], errormsg[%s]",
                          _metaOutputFile->getFileName(),
                          ec,
                          FileSystem::getErrorString(ec).c_str());
                return ERROR_BROKER_WRITE_FILE;
            }
            leftLen -= writeLen;
        }
        resetBuffer();
        fslib::ErrorCode ec = fslib::EC_OK;
        FUNC_GET_LATENCY(ec, _metaOutputFile->flush(), collector.dfsFlushMetaLatency);
        if (ec != fslib::EC_OK) {
            setSealError(ec);
            AUTIL_LOG(WARN,
                      "flush meta file[%s] failed, fslib ec[%d], errormsg[%s]",
                      _metaOutputFile->getFileName(),
                      ec,
                      FileSystem::getErrorString(ec).c_str());
            return ERROR_BROKER_COMMIT_FILE;
        }
        committedMetaSize = totalLen - leftLen;
        if (_metricsReporter) {
            _metricsReporter->reportDFSWriteRate(committedMetaSize, &_tags);
        }
    }
    int64_t endTime = TimeUtility::currentTime();
    int64_t commitLatency = endTime - begTime;
    collector.dfsCommitLatency = commitLatency / 1000.0;
    collector.dfsCommitSize = _writedSize - _committedSize + committedMetaSize;
    ++collector.dfsCommitQps;
    collector.dfsCommitInterval = (endTime - _lastCommitTime) / 1000.0;
    if (_metricsReporter) {
        _metricsReporter->reportCommitFileMetrics(collector);
    }

    _committedId = _writedId;
    _committedSize = _writedSize;
    _lastCommitTime = endTime;
    if (_nowFilePair) {
        _nowFilePair->setEndMessageId(_committedId + 1);
    }
    if (commitLatency > _maxCommitTimeAsError && endTime - _lastCommitTimeAsError > _minChangeFileForDfsErrorTime) {
        AUTIL_LOG(WARN,
                  "partiton[%s] commit latency too long[%f ms], last commit as error[%f ms]",
                  _partitionId.ShortDebugString().c_str(),
                  commitLatency / 1000.0,
                  _lastCommitTimeAsError / 1000.0);
        _lastCommitTimeAsError = endTime;
        return ERROR_BROKER_COMMIT_FILE;
    }

    return ERROR_NONE;
}

ErrorCode MessageCommitter::closeFile() {
    ErrorCode ret = ERROR_NONE;
    if (_dataOutputFile) {
        fslib::ErrorCode ec = _dataOutputFile->close();
        if (ec != fslib::EC_OK) {
            AUTIL_LOG(WARN,
                      "close data file[%s] failed, fslib ec[%d], errormsg [%s]",
                      _dataOutputFile->getFileName(),
                      ec,
                      FileSystem::getErrorString(ec).c_str());
            ret = ERROR_BROKER_CLOSE_FILE;
            _writeFailDataCount++;
        } else {
            AUTIL_LOG(INFO, "close data file[%s] success", _dataOutputFile->getFileName());
        }
    }
    if (_metaOutputFile) {
        fslib::ErrorCode ec = _metaOutputFile->close();
        if (ec != fslib::EC_OK) {
            AUTIL_LOG(WARN,
                      "close meta file[%s] failed, fslib ec[%d], errormsg [%s]",
                      _metaOutputFile->getFileName(),
                      ec,
                      FileSystem::getErrorString(ec).c_str());
            ret = ERROR_BROKER_CLOSE_FILE;
            _writeFailMetaCount++;
        } else {
            AUTIL_LOG(INFO, "close meta file[%s] success", _metaOutputFile->getFileName());
        }
    }

    resetBuffer();
    DELETE_AND_SET_NULL(_dataOutputFile);
    DELETE_AND_SET_NULL(_metaOutputFile);
    _writedSize = 0;
    _committedSize = 0;
    _fileMessageCount = 0;
    _writedId = _committedId;
    _commitFileIsReaded = false;
    return ret;
}

ErrorCode MessageCommitter::openFile(int64_t msgId, int64_t timestamp) {
    _nowFilePair = _fileManager->createNewFilePair(msgId, timestamp);

    assert(!_dataOutputFile);
    assert(!_metaOutputFile);

    const string &dataFileName = _nowFilePair->dataFileName;
    const string &metaFileName = _nowFilePair->metaFileName;

    fslib::ErrorCode ec = fslib::EC_OK;
    _dataOutputFile = openFileForWrite(dataFileName, ec);
    setSealError(ec, true);
    if (!_dataOutputFile || !_dataOutputFile->isOpened()) {
        DELETE_AND_SET_NULL(_dataOutputFile);
        AUTIL_LOG(ERROR, "open data file[%s] failed!", dataFileName.c_str());
        return ERROR_BROKER_OPEN_FILE;
    }
    AUTIL_LOG(INFO, "open data file[%s] success!", dataFileName.c_str());
    _metaOutputFile = openFileForWrite(metaFileName, ec);
    setSealError(ec, true);
    if (!_metaOutputFile || !_metaOutputFile->isOpened()) {
        DELETE_AND_SET_NULL(_dataOutputFile);
        DELETE_AND_SET_NULL(_metaOutputFile);
        AUTIL_LOG(INFO, "open meta file[%s] failed!", metaFileName.c_str());
        return ERROR_BROKER_OPEN_FILE;
    }
    AUTIL_LOG(INFO, "open meta file[%s] success!", metaFileName.c_str());
    _writedSize = 0;
    _committedSize = 0;
    _fileCreateTime = TimeUtility::currentTime();
    _lastCommitTime = _fileCreateTime;
    _writedId = _committedId;

    return ERROR_NONE;
}

template <typename ContainerType>
protocol::ErrorCode MessageCommitter::writeMessages(const ContainerType &msgs) {
    // try commit before write every time, to get more space for writing.
    int64_t curTime = TimeUtility::currentTime();
    _writedSizeForReport = 0;
    ErrorCode errorCode = ERROR_NONE;
    MessageIterator<ContainerType> iterator(msgs);
    while (iterator.hasNext()) {
        errorCode = maybeCommit(curTime);
        if (errorCode != ERROR_NONE) {
            AUTIL_LOG(ERROR, "partiton [%s] commit fail", _partitionId.ShortDebugString().c_str());
            break;
        }
        const auto &msg = iterator.next();
        errorCode = write(msg);
        _fileMessageCount++;
        if (errorCode != ERROR_NONE) {
            AUTIL_LOG(ERROR, "partiton [%s] write fail", _partitionId.ShortDebugString().c_str());
            closeFile();
            break;
        }
    }
    errorCode = writeDataBuffer(errorCode);
    if (errorCode != ERROR_NONE) {
        AUTIL_LOG(ERROR, "partiton [%s] write fail", _partitionId.ShortDebugString().c_str());
        closeFile();
    }
    if (_metricsReporter) {
        _metricsReporter->reportDFSWriteRate(_writedSizeForReport, &_tags);
    }
    if (ERROR_NONE != errorCode) {
        return errorCode;
    }
    // even vec size is zero, we follow up this
    // to trigger commit
    return maybeCommit(curTime);
}

template <typename T>
ErrorCode MessageCommitter::write(const T &msg) {
    // do not assert for case.
    // assert(_writedId == msg.getMsgId() - 1);
    ErrorCode errorCode = ERROR_NONE;
    if (!_dataOutputFile) {
        errorCode = openFile(MessageAdapter<T>::getMsgId(msg), MessageAdapter<T>::getTimestamp(msg));
    }
    if (errorCode != ERROR_NONE) {
        AUTIL_LOG(ERROR, "partiton[%s] open new file fail", _partitionId.ShortDebugString().c_str());
        return errorCode;
    }
    errorCode = writeSingleMessage(msg);
    if (errorCode != ERROR_NONE) {
        AUTIL_LOG(ERROR, "partition[%s] write data fail", _partitionId.ShortDebugString().c_str());
        return errorCode;
    }
    return ERROR_NONE;
}

template <typename T>
protocol::ErrorCode MessageCommitter::writeSingleMessage(const T &msg) {
    size_t msgLen = MessageAdapter<T>::fillData(msg, _dataBuffer);
    FileMessageMeta meta;
    MessageAdapter<T>::fillMeta(msg, meta);
    meta.endPos = _writedSize + msgLen;
    char *buf = (char *)(&meta);
    _meta.insert(_meta.end(), buf, buf + FILE_MESSAGE_META_SIZE);
    _writedSize += msgLen;
    _writedSizeForReport += msgLen;
    _writedId = MessageAdapter<T>::getMsgId(msg);
    return ERROR_NONE;
}

ErrorCode MessageCommitter::writeDataBuffer(ErrorCode emptyError) {
    if (_dataBuffer.empty()) {
        return emptyError;
    }
    assert(_dataOutputFile);
    int64_t leftLen = _dataBuffer.size();
    while (leftLen > 0) {
        ssize_t writeLen = -1;
        FUNC_REPORT_LATENCY(writeLen,
                            _dataOutputFile->write(_dataBuffer.c_str(), _dataBuffer.length()),
                            reportDfsWriteDataLatency,
                            _tags);
        if (writeLen == -1) {
            resetBuffer();
            fslib::ErrorCode ec = _dataOutputFile->getLastError();
            setSealError(ec);
            AUTIL_LOG(WARN,
                      "write data file[%s] fail, fslib error[%d], errormsg[%s]",
                      _dataOutputFile->getFileName(),
                      ec,
                      FileSystem::getErrorString(ec).c_str());
            return ERROR_BROKER_WRITE_FILE;
        }
        leftLen -= writeLen;
    }
    _dataBuffer.clear();
    return ERROR_NONE;
}

void MessageCommitter::resetBuffer() {
    if (_meta.capacity() > MAX_COMMIT_BUFFER_SIZE) {
        AUTIL_MASSIVE_LOG(INFO,
                          "partiton[%s] meta buffer[%ld] large than[%ld], release space",
                          _partitionId.ShortDebugString().c_str(),
                          _meta.capacity(),
                          MAX_COMMIT_BUFFER_SIZE);
        vector<char> tmp;
        _meta.swap(tmp);
    }
    _meta.clear();
    if (_dataBuffer.capacity() > MAX_COMMIT_BUFFER_SIZE) {
        AUTIL_MASSIVE_LOG(INFO,
                          "partiton[%s] data buffer[%ld] large than[%ld], release space",
                          _partitionId.ShortDebugString().c_str(),
                          _dataBuffer.capacity(),
                          MAX_COMMIT_BUFFER_SIZE);
        string tmp;
        _dataBuffer.swap(tmp);
    }
    _dataBuffer.clear();
    _writeBufferSize = _meta.capacity() + _dataBuffer.capacity();
}

File *MessageCommitter::openFileForWrite(const string &fileName, fslib::ErrorCode &ec) {
    if (_enableFastRecover) {
        const string &path = _fileManager->getInlineFilePath();
        const auto &inlineVersion = _fileManager->getInlineVersion();
        File *retFile = PanguInlineFileUtil::openFileForWrite(fileName, path, inlineVersion.serialize(), ec);
        if (fslib::EC_OK == ec) {
            AUTIL_LOG(INFO,
                      "open file for write success, path[%s], inline[%s]",
                      path.c_str(),
                      inlineVersion.toDebugString().c_str());
        } else {
            AUTIL_LOG(ERROR,
                      "open file for write failed, path[%s], inline[%s], error[%d %s]",
                      path.c_str(),
                      inlineVersion.toDebugString().c_str(),
                      ec,
                      FileSystem::getErrorString(ec).c_str());
        }
        return retFile;
    } else {
        ec = fslib::EC_OK;
        return FileSystem::openFile(fileName, fslib::WRITE);
    }
}

void MessageCommitter::setSealError(fslib::ErrorCode ec, bool tryRefreshVersion) {
    if (fslib::EC_PERMISSION == ec || fslib::EC_PANGU_USER_DENIED == ec) {
        if (tryRefreshVersion && _canRefeshVersion) {
            AUTIL_LOG(INFO, "try refresh inline version");
            ErrorCode ec = _fileManager->refreshVersion();
            if (ERROR_BROKER_INLINE_VERSION_INVALID == ec) { // no more retry
                _canRefeshVersion = false;
                _hasSealError = true;
            } else if (ERROR_NONE == ec) {
                _hasSealError = false;
            } else {
                AUTIL_LOG(WARN, "set hasSealError");
                _hasSealError = true;
            }
        } else {
            AUTIL_LOG(WARN, "set hasSealError");
            _hasSealError = true;
        }
    }
}

} // namespace storage
} // namespace swift
