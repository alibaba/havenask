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

#include <algorithm>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/Log.h"
#include "swift/common/Common.h"
#include "swift/protocol/Common.pb.h"

namespace swift {
namespace config {
static const int64_t MIN_PARTITION_BUFFER_SIZE = 1 << 20;                 // 1M
static const int64_t MIN_BLOCK_SIZE = 1 << 12;                            // 4k
static const int64_t MIN_MAX_COMMIT_SIZE = 1 << 20;                       // 1M
static const int64_t MIN_MAX_COMMIT_INTERVAL = 1000;                      // 1ms
static const int64_t MIN_CLOSE_NO_WRITE_FILE_INTERVAL = 60 * 1000 * 1000; // 1MIN

static const int64_t MIN_MAX_FILE_SIZE = 1 << 26;                               // 64M
static const int64_t MIN_MAX_FILE_SPLIT_INTERVAL = 60 * 1000 * 1000;            // 1min
static const int64_t MIN_MIN_CHANGE_FILE_FOR_DFS_ERROR_TIME = 60 * 1000 * 1000; // 1min
static const int64_t MIN_MAX_COMMIT_TIME_AS_ERROR = 1000 * 1000;                // 1s

static const int64_t MIN_OBSOLETE_FILE_TIME_INTERVAL = 600 * 1000 * 1000; // 10min
static const int32_t MIN_RESERVED_FILE_COUNT = 5;
static const int32_t MAX_WAIT_TIME_FOR_SECURITY_COMMIT = 50 * 1000; // 50ms
static const int32_t MAX_DATA_SIZE_FOR_SECURITY_COMMIT = 1 << 20;   // 1M

class PartitionConfig {
public:
    PartitionConfig();
    ~PartitionConfig();

public:
    void setDataRoot(const std::string &dataRoot) { _dataRoot = dataRoot; }
    const std::string &getDataRoot() const { return _dataRoot; }

    void setPartitionMinBufferSize(int64_t value) {
        if (!_configUnlimited) {
            _partitionMinBufferSize = std::max(value, MIN_PARTITION_BUFFER_SIZE);
        } else {
            _partitionMinBufferSize = value;
        }
    }
    void setPartitionMaxBufferSize(int64_t value) {
        if (!_configUnlimited) {
            _partitionMaxBufferSize = std::max(value, MIN_PARTITION_BUFFER_SIZE);
        } else {
            _partitionMaxBufferSize = value;
        }
    }
    void setBlockSize(int64_t value) {
        if (!_configUnlimited) {
            _blockSize = std::max(value, MIN_BLOCK_SIZE);
        } else {
            _blockSize = value;
        }
    }
    void setMaxCommitSize(int64_t value) {
        if (!_configUnlimited) {
            _maxCommitSize = std::max(value, MIN_MAX_COMMIT_SIZE);
        } else {
            _maxCommitSize = value;
        }
    }
    void setMaxCommitInterval(int64_t value) {
        if (!_configUnlimited) {
            _maxCommitInterval = std::max(value, MIN_MAX_COMMIT_INTERVAL);
        } else {
            _maxCommitInterval = value;
        }
    }
    void setMaxCommitIntervalForMemoryPreferTopic(int64_t value) {
        if (!_configUnlimited) {
            _maxCommitIntervalForMemoryPreferTopic = std::max(value, MIN_MAX_COMMIT_INTERVAL);
        } else {
            _maxCommitIntervalForMemoryPreferTopic = value;
        }
    }
    void setMaxCommitIntervalWhenDelay(int64_t value) {
        if (!_configUnlimited) {
            _maxCommitIntervalWhenDelay = std::max(value, MIN_MAX_COMMIT_INTERVAL);
        } else {
            _maxCommitIntervalWhenDelay = value;
        }
    }

    void setMaxFileSize(int64_t value) {
        if (!_configUnlimited) {
            _maxFileSize = std::max(value, MIN_MAX_FILE_SIZE);
        } else {
            _maxFileSize = value;
        }
    }

    void setMinFileSize(int64_t value) { _minFileSize = value; }

    void setMaxFileSplitInterval(int64_t value) {
        if (!_configUnlimited) {
            _maxFileSplitTime = std::max(value, MIN_MAX_FILE_SPLIT_INTERVAL);
        } else {
            _maxFileSplitTime = value;
        }
    }
    void setCloseNoWriteFileInterval(int64_t value) {
        if (!_configUnlimited) {
            _closeNoWriteFileInterval = std::max(value, MIN_CLOSE_NO_WRITE_FILE_INTERVAL);
        } else {
            _closeNoWriteFileInterval = value;
        }
    }

    void setMinChangeFileForDfsErrorTime(int64_t value) {
        if (!_configUnlimited) {
            _minChangeFileForDfsErrorTime = std::max(value, MIN_MIN_CHANGE_FILE_FOR_DFS_ERROR_TIME);
        } else {
            _minChangeFileForDfsErrorTime = value;
        }
    }
    void setMaxCommitTimeAsError(int64_t value) {
        if (!_configUnlimited) {
            _maxCommitTimeAsError = std::max(value, MIN_MAX_COMMIT_TIME_AS_ERROR);
        } else {
            _maxCommitTimeAsError = value;
        }
    }

    void setConfigUnlimited(bool value) { _configUnlimited = value; }
    void setTopicMode(protocol::TopicMode topicMode) { _topicMode = topicMode; }
    void setNeedFieldFilter(bool needFieldFilter) { _needFieldFilter = needFieldFilter; }
    void setObsoleteFileTimeInterval(int64_t timeIntervalHour) {
        if (!_configUnlimited) {
            _obsoleteFileTimeInterval = std::max(timeIntervalHour, MIN_OBSOLETE_FILE_TIME_INTERVAL);
        } else {
            _obsoleteFileTimeInterval = timeIntervalHour;
        }
    }
    void setReservedFileCount(int32_t reservedFileCount) {
        if (!_configUnlimited) {
            _reservedFileCount = std::max(reservedFileCount, MIN_RESERVED_FILE_COUNT);
        } else {
            _reservedFileCount = reservedFileCount;
        }
    }
    void setDelObsoleteFileInterval(int64_t timeIntervalSec) { _delObsoleteFileInterval = timeIntervalSec; }
    void setCandidateObsoleteFileInterval(int64_t timeIntervalSec) { _candidateObsoleteFileInterval = timeIntervalSec; }
    int64_t getCandidateObsoleteFileInterval() const { return _candidateObsoleteFileInterval; }
    int64_t getDelObsoleteFileInterval() const { return _delObsoleteFileInterval; }
    int32_t getReservedFileCount() const { return _reservedFileCount; }
    int64_t getObsoleteFileTimeInterval() const { return _obsoleteFileTimeInterval; }
    protocol::TopicMode getTopicMode() const { return _topicMode; }
    bool needFieldFilter() const { return _needFieldFilter; }
    bool getConfigUnlimited() const { return _configUnlimited; }
    int64_t getPartitionMinBufferSize() const { return _partitionMinBufferSize; }
    int64_t getPartitionMaxBufferSize() const { return _partitionMaxBufferSize; }
    int64_t getBlockSize() const { return _blockSize; }
    int64_t getMaxCommitSize() const { return _maxCommitSize; }
    int64_t getMaxCommitInterval() const { return _maxCommitInterval; }
    int64_t getMaxCommitIntervalForMemoryPreferTopic() const { return _maxCommitIntervalForMemoryPreferTopic; }
    int64_t getMaxCommitIntervalWhenDelay() const { return _maxCommitIntervalWhenDelay; }
    int64_t getMaxFileSize() const { return _maxFileSize; }
    int64_t getMinFileSize() const { return _minFileSize; }
    int64_t getMaxFileSplitInterval() const { return _maxFileSplitTime; }
    int64_t getCloseNoWriteFileInterval() const { return _closeNoWriteFileInterval; }
    int64_t getMinChangeFileForDfsErrorTime() const { return _minChangeFileForDfsErrorTime; }
    int64_t getMaxCommitTimeAsError() const { return _maxCommitTimeAsError; }
    void setMaxMessageCountInOneFile(int64_t maxMessageCountInOneFile) {
        _maxMessageCountInOneFile = maxMessageCountInOneFile;
    }
    int64_t getMaxMessageCountInOneFile() const { return _maxMessageCountInOneFile; }
    void setCacheMetaCount(uint32_t cacheMetaCount) { _cacheMetaCount = cacheMetaCount; }
    uint32_t getCacheMetaCount() const { return _cacheMetaCount; }

    void setMaxWaitTimeForSecurityCommit(int64_t waitTime) { _maxWaitTimeForSecurityCommit = waitTime; }
    int64_t getMaxWaitTimeForSecurityCommit() const { return _maxWaitTimeForSecurityCommit; }
    void setMaxDataSizeForSecurityCommit(int64_t dataSize) { _maxDataSizeForSecurityCommit = dataSize; }
    int64_t getMaxDataSizeForSecurityCommit() const { return _maxDataSizeForSecurityCommit; }
    void setCompressMsg(bool compressMsg) { _compressMsg = compressMsg; }
    bool compressMsg() const { return _compressMsg; }
    void setCompressThres(int thres) { _compressThres = thres; }
    int getCompressThres() const { return _compressThres; }
    void setExtendDataRoots(const std::vector<std::string> &dataRoot) { _extendDataRoots = dataRoot; }
    const std::vector<std::string> &getExtendDataRoots() const { return _extendDataRoots; }
    void setRecyclePercent(double percent) { _recyclePercent = percent; }
    double getRecyclePercent() const { return _recyclePercent; }
    void setCheckFieldFilterMsg(bool flag) { _checkFieldFilterMsg = flag; }
    bool checkFieldFilterMsg() const { return _checkFieldFilterMsg; }
    void setObsoleteReaderInterval(int64_t timeInterval) { _obsoleteReaderInterval = timeInterval; }
    int64_t getObsoleteReaderInterval() const { return _obsoleteReaderInterval; }
    void setObsoleteReaderMetricInterval(int64_t timeInterval) { _obsoleteReaderMetricInterval = timeInterval; }
    int64_t getObsoleteReaderMetricInterval() const { return _obsoleteReaderMetricInterval; }
    void setMaxReadSizeSec(int64_t sizeMB) { _maxReadSizeSec = sizeMB << 20; }
    int64_t getMaxReadSizeSec() const { return _maxReadSizeSec; }
    void setEnableLongPolling(bool isEnable) { _enableLongPolling = isEnable; }
    bool enableLongPolling() const { return _enableLongPolling; }
    void setReadNotCommittedMsg(bool isRead) { _readNotCommittedMsg = isRead; }
    bool readNotCommittedMsg() const { return _readNotCommittedMsg; }
    void setTimestampOffsetInUs(int64_t offset) { _timestampOffsetInUs = offset; }
    int64_t getTimestampOffsetInUs() const { return _timestampOffsetInUs; }

private:
    std::string _dataRoot;
    int64_t _partitionMinBufferSize;
    int64_t _partitionMaxBufferSize;
    int64_t _blockSize;
    int64_t _maxCommitSize;                         // byte
    int64_t _maxCommitInterval;                     // us
    int64_t _maxCommitIntervalForMemoryPreferTopic; // us
    int64_t _maxCommitIntervalWhenDelay;            // us
    int64_t _maxFileSize;                           // byte
    int64_t _minFileSize;                           // byte
    int64_t _maxFileSplitTime;                      // us
    int64_t _closeNoWriteFileInterval;              // us
    int64_t _minChangeFileForDfsErrorTime;          // us
    int64_t _maxCommitTimeAsError;                  // us

    bool _configUnlimited;
    protocol::TopicMode _topicMode;
    bool _needFieldFilter;
    int64_t _obsoleteFileTimeInterval;
    int32_t _reservedFileCount;
    int64_t _delObsoleteFileInterval;
    int64_t _candidateObsoleteFileInterval;
    int64_t _maxMessageCountInOneFile;
    uint32_t _cacheMetaCount;
    int64_t _maxWaitTimeForSecurityCommit;
    int64_t _maxDataSizeForSecurityCommit;
    bool _compressMsg;
    int _compressThres;
    std::vector<std::string> _extendDataRoots;
    double _recyclePercent;
    bool _checkFieldFilterMsg;
    int64_t _obsoleteReaderInterval;
    int64_t _obsoleteReaderMetricInterval;
    int64_t _maxReadSizeSec;
    bool _enableLongPolling;
    bool _readNotCommittedMsg;
    int64_t _timestampOffsetInUs;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(PartitionConfig);

} // namespace config
} // namespace swift
