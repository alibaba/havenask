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
#include "swift/config/BrokerConfig.h"

#include <assert.h>
#include <sstream>
#include <stddef.h>
#include <stdint.h>
#include <string>

#include "linux/sysinfo.h"
#include "swift/config/ConfigDefine.h"
#include "swift/config/PartitionConfig.h"
#include "sys/sysinfo.h"

namespace swift {
namespace config {
AUTIL_LOG_SETUP(swift, BrokerConfig);

BrokerConfig::BrokerConfig()
    : amonitorPort(DEFAULT_AMONITOR_PORT)
    , queueSize(DEFAULT_QUEUE_SIZE)
    , cacheMetaCount(DEFAULT_CACHE_META_COUNT)
    , concurrentReadFileLimit(DEFAULT_CONCURRENT_READ_FILE_LIMIT)
    , maxPermissionWaitTime(DEFAULT_MAX_PERMISSION_WAIT_TIME)
    , maxMessageCountInOneFile(DEFAULT_MAX_MESSAGE_COUNT_IN_ONE_FILE)
    , reservedFileCount(DEFAULT_RESERVED_FILE_COUNT)
    , fileBufferUseRatio(DEFAULT_FILE_BUFFER_USE_RATIO)
    , fileMetaBufferUseRatio(DEFAULT_FILE_META_BUFFER_USE_RATIO)
    , commitThreadLoopIntervalMs(DEFAULT_COMMIT_THREAD_LOOP_INTERVAL_MS)
    , dfsCommitInterval(DEFAULT_DFS_COMMIT_INTERVAL)
    , dfsCommitIntervalForMemoryPreferTopic(DEFAULT_DFS_COMMIT_INTERVAL_FOR_MEMORY_PREFER_TOPIC)
    , dfsCommitIntervalWhenDelay(DEFAULT_DFS_COMMIT_INTERVAL_WHEN_DELAY)
    , dfsCommitThreadNum(DEFAULT_DFS_COMMIT_THREAD_NUM)
    , dfsCommitQueueSize(DEFAULT_DFS_COMMIT_QUEUE_SIZE)
    , dfsFileSplitTime(DEFAULT_DFS_FILE_SPLIT_TIME)
    , closeNoWriteFileTime(DEFAULT_CLOSE_NO_WRITE_FILE_TIME)
    , minChangeFileForDfsErrorTime(DEFAULT_FILE_CHANGE_FOR_DFS_ERROR)
    , maxCommitTimeAsError(DEFAULT_COMMIT_INTERVAL_AS_ERROR)
    , leaderLeaseTime(DEFAULT_LEADER_LEASE_TIME)
    , leaderLoopInterval(DEFAULT_LEADER_LOOP_INTERVAL)
    , loadThreadNum(DEFAULT_LOAD_THREAD_NUM)
    , loadQueueSize(DEFAULT_LOAD_QUEUE_SIZE)
    , unloadThreadNum(DEFAULT_UNLOAD_THREAD_NUM)
    , unloadQueueSize(DEFAULT_UNLOAD_QUEUE_SIZE)
    , recyclePercent(DEFAULT_RECYCLE_PERCENT)
    , supportMergeMsg(DEFAULT_SUPPORT_MERGE_MSG)
    , supportFb(DEFAULT_SUPPORT_FB)
    , checkFieldFilterMsg(DEFAULT_CHECK_FIELD_FILTER_MSG)
    , httpThreadNum(1)
    , httpQueueSize(100)
    , readQueueSize(DEFAULT_READ_QUEUE_SIZE)
    , statusReportIntervalSec(10)
    , clearPollingThreadNum(DEFAULT_CLEAR_POLLING_THREAD_NUM)
    , clearPollingQueueSize(DEFAULT_CLEAR_POLLING_QUEUE_SIZE)
    , enableFastRecover(false)
    , randomDelObsoleteFileInterval(true)
    , useRecommendPort(true)
    , localMode(false)
    , readNotCommittedMsg(true)
    , maxTopicAclSyncIntervalUs(10000000)
    , _threadNum(DEFAULT_THREAD_NUM)
    , _reportMetricThreadNum(0)
    , _partitionMinBufferSize(DEFAULT_PARTITION_MIN_BUFFER_SIZE)
    , _partitionMaxBufferSize(DEFAULT_PARTITION_MAX_BUFFER_SIZE)
    , _bufferBlockSize(DEFAULT_BUFFER_BLOCK_SIZE)
    , _bufferMinReserveSize(DEFAULT_BUFFER_MIN_RESERVE_SIZE)
    , _dfsCommitBlockSize(DEFAULT_DFS_COMMIT_BLOCK_SIZE)
    , _dfsFileSize(DEFAULT_DFS_FILE_SIZE)
    , _dfsFileMinSize(DEFAULT_DFS_FILE_MIN_SIZE)
    , _obsoleteFileTimeInterval(DEFAULT_OBSOLETE_FILE_TIME_INTERVAL)
    , _delObsoleteFileInterval(DEFAULT_DEL_OBSOLETE_FILE_INTERVAL)
    , _candidateObsoleteFileInterval(DEFAULT_CANDIDATE_OBSOLETE_FILE_INTERVAL)
    , _requestTimeout(DEFAULT_REQUEST_TIMEOUT)
    , _oneFileFdNum(DEFAULT_ONE_FILE_FD_NUM)
    , _cacheFileReserveTime(DEFAULT_CACHE_FILE_RESERVE_TIME)
    , _cacheBlockReserveTime(DEFAULT_CACHE_BLOCK_RESERVE_TIME)
    , _obsoleteReaderInterval(DEFAULT_OBSOLETE_READER_INTERVAL)
    , _obsoleteReaderMetricInterval(DEFAULT_OBSOLETE_METRIC_INTERVAL)
    , _configUnlimited(false)
    , _logSampleCount(DEFAULT_LOG_SAMPLE_COUNT)
    , _closeForceLog(true)
    , _maxGetMessageSizeKb(0)
    , _holdNoDataRequestTimeMs(0)
    , _noDataRequestNotfiyIntervalMs(DEFAULT_NO_DATA_REQUEST_NOTFIY_INTERVAL_MS)
    , _maxReadSizeSec(DEFAULT_MAX_READ_SIZE_MB_SEC)
    , _timestampOffsetInUs(0) {
    maxReadThreadNum = int(DEFAULT_THREAD_NUM * DEFAULT_READ_THREAD_RATIO) + 1;
    maxWriteThreadNum = int(DEFAULT_THREAD_NUM * DEFAULT_WRITE_THREAD_RATIO) + 1;
    _totalBufferSize = (int64_t)(getPhyMemSize() * 0.5); // default use half system memory
}

BrokerConfig::~BrokerConfig() {}

bool BrokerConfig::validate() {
    if (maxReadThreadNum <= concurrentReadFileLimit) {
        AUTIL_LOG(ERROR,
                  "%s[%d] should larger than %s[%d]",
                  MAX_READ_THREAD_NUM,
                  maxReadThreadNum,
                  CONCURRENT_READ_FILE_LIMIT,
                  concurrentReadFileLimit);
        return false;
    }
    if (0 == statusReportIntervalSec || statusReportIntervalSec > 150) {
        AUTIL_LOG(ERROR, "%s[%d] should less than 150 and not 0", STATUS_REPORT_INTERVAL_SEC, statusReportIntervalSec);
        return false;
    }
    return true;
}

std::string BrokerConfig::getApplicationId() { return userName + "_" + serviceName; }

void BrokerConfig::getDefaultPartitionConfig(PartitionConfig &partitionConfig) const {
    partitionConfig.setConfigUnlimited(_configUnlimited);
    partitionConfig.setDataRoot(dfsRoot);
    partitionConfig.setPartitionMinBufferSize(_partitionMinBufferSize);
    partitionConfig.setPartitionMaxBufferSize(_partitionMaxBufferSize);
    partitionConfig.setBlockSize(_bufferBlockSize);
    partitionConfig.setMaxCommitSize(_dfsCommitBlockSize);
    partitionConfig.setMaxCommitInterval(int64_t(dfsCommitInterval * 1000000));
    partitionConfig.setMaxCommitIntervalForMemoryPreferTopic(int64_t(dfsCommitIntervalForMemoryPreferTopic * 1000000));
    partitionConfig.setMaxCommitIntervalWhenDelay(int64_t(dfsCommitIntervalWhenDelay * 1000000));
    partitionConfig.setMaxFileSize(_dfsFileSize);
    partitionConfig.setMinFileSize(_dfsFileMinSize);
    partitionConfig.setMaxFileSplitInterval(int64_t(dfsFileSplitTime * 1000000ll));
    partitionConfig.setCloseNoWriteFileInterval(int64_t(closeNoWriteFileTime * 1000000ll));
    partitionConfig.setMinChangeFileForDfsErrorTime(int64_t(minChangeFileForDfsErrorTime * 1000000ll));
    partitionConfig.setMaxCommitTimeAsError(int64_t(maxCommitTimeAsError * 1000ll));
    partitionConfig.setObsoleteFileTimeInterval(_obsoleteFileTimeInterval);
    partitionConfig.setReservedFileCount(reservedFileCount);
    partitionConfig.setDelObsoleteFileInterval(_delObsoleteFileInterval);
    partitionConfig.setCandidateObsoleteFileInterval(_candidateObsoleteFileInterval);
    partitionConfig.setMaxMessageCountInOneFile(maxMessageCountInOneFile);
    partitionConfig.setCacheMetaCount(cacheMetaCount);
    partitionConfig.setRecyclePercent(recyclePercent);
    partitionConfig.setCheckFieldFilterMsg(checkFieldFilterMsg);
    partitionConfig.setObsoleteReaderInterval(_obsoleteReaderInterval);
    partitionConfig.setObsoleteReaderMetricInterval(_obsoleteReaderMetricInterval);
    partitionConfig.setMaxReadSizeSec(_maxReadSizeSec);
    partitionConfig.setReadNotCommittedMsg(readNotCommittedMsg);
    partitionConfig.setTimestampOffsetInUs(_timestampOffsetInUs);
}

void BrokerConfig::setThreadNum(int64_t value) {
    _threadNum = value;
    maxReadThreadNum = int(value * DEFAULT_READ_THREAD_RATIO) + 1;
    maxWriteThreadNum = int(value * DEFAULT_WRITE_THREAD_RATIO) + 1;
}

int BrokerConfig::getThreadNum() const { return _threadNum; }

void BrokerConfig::setReportMetricThreadNum(int64_t value) { _reportMetricThreadNum = value; }
int BrokerConfig::getReportMetricThreadNum() const { return _reportMetricThreadNum; }

void BrokerConfig::setPartitionMinBufferSize(double value) { _partitionMinBufferSize = int64_t(value * 1024 * 1024); }
int64_t BrokerConfig::getPartitionMinBufferSize() const { return _partitionMinBufferSize; }

void BrokerConfig::setPartitionMaxBufferSize(double value) { _partitionMaxBufferSize = int64_t(value * 1024 * 1024); }
int64_t BrokerConfig::getPartitionMaxBufferSize() const { return _partitionMaxBufferSize; }

void BrokerConfig::setTotalBufferSize(double value) {
    if (value < 1.0) {
        _totalBufferSize = int64_t(getPhyMemSize() * value);
    } else {
        _totalBufferSize = int64_t(value * 1024 * 1024);
    }
}
int64_t BrokerConfig::getTotalBufferSize() const { return _totalBufferSize; }
int64_t BrokerConfig::getTotalFileBufferSize() const { return (int64_t)(_totalBufferSize * fileBufferUseRatio); }

int64_t BrokerConfig::getBrokerFileBufferSize() const {
    return (int64_t)((_totalBufferSize * fileBufferUseRatio) * (1 - fileMetaBufferUseRatio));
}
int64_t BrokerConfig::getBrokerFileMetaBufferSize() const {
    return (int64_t)((_totalBufferSize * fileBufferUseRatio) * fileMetaBufferUseRatio);
}
int64_t BrokerConfig::getBrokerMessageBufferSize() const {
    return (int64_t)(_totalBufferSize * (1 - fileBufferUseRatio));
}

void BrokerConfig::setBufferBlockSize(double value) {
    _bufferBlockSize = int64_t(value * 1024 * 1024);
    if (_bufferBlockSize > MAX_BUFFER_BLOCK_SIZE) {
        _bufferBlockSize = MAX_BUFFER_BLOCK_SIZE; // for memory message offset limit 22 bit
    }
}
int64_t BrokerConfig::getBufferBlockSize() const { return _bufferBlockSize; }

int64_t BrokerConfig::getBufferMinReserveSize() {
    if (_bufferMinReserveSize <= 0) {
        return int64_t(getBrokerMessageBufferSize() * DEFAULT_BUFFER_MIN_RESERVE_RATIO);
    } else {
        return _bufferMinReserveSize;
    }
}
void BrokerConfig::setBufferMinReserveSize(double size) { _bufferMinReserveSize = int64_t(size * 1024 * 1024); }

void BrokerConfig::setDfsCommitBlockSize(double value) { _dfsCommitBlockSize = int64_t(value * 1024 * 1024); }

int64_t BrokerConfig::getDfsCommitBlockSize() const { return _dfsCommitBlockSize; }

void BrokerConfig::setDfsFileSize(double value) { _dfsFileSize = int64_t(value * 1024 * 1024); }

int64_t BrokerConfig::getDfsFileSize() const { return _dfsFileSize; }

void BrokerConfig::setDfsFileMinSize(double value) { _dfsFileMinSize = int64_t(value * 1024 * 1024); }

int64_t BrokerConfig::getDfsFileMinSize() const { return _dfsFileMinSize; }

void BrokerConfig::setConfigUnlimited(std::string value) {
    if (value == "true") {
        _configUnlimited = true;
    }
}

bool BrokerConfig::getConfigUnlimited() const { return _configUnlimited; }

void BrokerConfig::setObsoleteFileTimeIntervalSec(int64_t timeIntervalSec) {
    _obsoleteFileTimeInterval = timeIntervalSec * 1000 * 1000;
}

void BrokerConfig::setObsoleteFileTimeIntervalHour(int64_t timeIntervalHour) {
    _obsoleteFileTimeInterval = timeIntervalHour * 3600 * 1000 * 1000;
}

int64_t BrokerConfig::getObsoleteFileTimeInterval() const { return _obsoleteFileTimeInterval; }

void BrokerConfig::setDelObsoleteFileIntervalSec(int64_t timeIntervalSec) {

    _delObsoleteFileInterval = timeIntervalSec * 1000 * 1000;
}

void BrokerConfig::addDelObsoleteFileIntervalSec(int64_t timeIntervalSec) {
    _delObsoleteFileInterval += timeIntervalSec * 1000 * 1000;
}

int64_t BrokerConfig::getDelObsoleteFileInterval() const { return _delObsoleteFileInterval; }

void BrokerConfig::setCandidateObsoleteFileIntervalSec(int64_t timeIntervalSec) {
    _candidateObsoleteFileInterval = timeIntervalSec * 1000 * 1000;
}

int64_t BrokerConfig::getCandidateObsoleteFileInterval() const { return _candidateObsoleteFileInterval; }

void BrokerConfig::setRequestTimeoutSec(int64_t timeoutSec) { _requestTimeout = (int64_t)timeoutSec * 1000 * 1000; }

int64_t BrokerConfig::getRequestTimeout() const { return _requestTimeout; }

void BrokerConfig::setOneFileFdNum(uint32_t fdNum) { _oneFileFdNum = fdNum; }

uint32_t BrokerConfig::getOneFileFdNum() const { return _oneFileFdNum; }

void BrokerConfig::setCacheFileReserveTime(int64_t timeSec) { _cacheFileReserveTime = timeSec; }

int64_t BrokerConfig::getCacheFileReserveTime() const { return _cacheFileReserveTime; }

void BrokerConfig::setCacheBlockReserveTime(int64_t timeSec) { _cacheBlockReserveTime = timeSec; }

int64_t BrokerConfig::getCacheBlockReserveTime() const { return _cacheBlockReserveTime; }

void BrokerConfig::setObsoleteReaderIntervalSec(int64_t timeIntervalSec) {
    _obsoleteReaderInterval = timeIntervalSec * 1000 * 1000;
}

int64_t BrokerConfig::getObsoleteReaderInterval() const { return _obsoleteReaderInterval; }

void BrokerConfig::setObsoleteReaderMetricIntervalSec(int64_t timeIntervalSec) {
    _obsoleteReaderMetricInterval = timeIntervalSec * 1000 * 1000;
}

int64_t BrokerConfig::getObsoleteReaderMetricInterval() const { return _obsoleteReaderMetricInterval; }

void BrokerConfig::setCommitThreadLoopIntervalMs(int32_t intervalMs) { commitThreadLoopIntervalMs = intervalMs; }

int32_t BrokerConfig::getCommitThreadLoopIntervalMs() const { return commitThreadLoopIntervalMs; }

void BrokerConfig::setLogSampleCount(uint32_t count) { _logSampleCount = count; }

uint32_t BrokerConfig::getLogSampleCount() const { return _logSampleCount; }

void BrokerConfig::setCloseForceLog(bool flag) { _closeForceLog = flag; }

bool BrokerConfig::getCloseForceLog() const { return _closeForceLog; }

void BrokerConfig::setMaxGetMessageSizeKb(size_t sizeKb) { _maxGetMessageSizeKb = sizeKb; }

size_t BrokerConfig::getMaxGetMessageSizeKb() const { return _maxGetMessageSizeKb; }

void BrokerConfig::setHoldNoDataRequestTimeMs(size_t timeMs) { _holdNoDataRequestTimeMs = timeMs; }
size_t BrokerConfig::getHoldNoDataRequestTimeMs() const { return _holdNoDataRequestTimeMs; }

void BrokerConfig::setNoDataRequestNotfiyIntervalMs(size_t timeMs) { _noDataRequestNotfiyIntervalMs = timeMs; }
size_t BrokerConfig::getNoDataRequestNotfiyIntervalMs() const { return _noDataRequestNotfiyIntervalMs; }

void BrokerConfig::setMaxReadSizeSec(int32_t sizeMB) { _maxReadSizeSec = sizeMB; }

int32_t BrokerConfig::getMaxReadSizeSec() const { return _maxReadSizeSec; }
void BrokerConfig::setTimestampOffsetInUs(int64_t offset) { _timestampOffsetInUs = offset; }
int64_t BrokerConfig::getTimestampOffsetInUs() const { return _timestampOffsetInUs; }

size_t BrokerConfig::getPhyMemSize() {
    struct sysinfo s_info;
    int error = 0;
    (void)(error);
    error = sysinfo(&s_info);
    assert(error == 0);
    return s_info.totalram;
}

std::string BrokerConfig::getConfigStr() {
    std::ostringstream oss;
    oss << "zkRoot:" << zkRoot << " userName:" << userName << " serviceName:" << serviceName << " dfsRoot:" << dfsRoot
        << " queueSize:" << queueSize << " maxReadThreadNum:" << maxReadThreadNum
        << " maxWriteThreadNum:" << maxWriteThreadNum << " cacheMetaCount:" << cacheMetaCount
        << " concurrentReadFileLimit:" << concurrentReadFileLimit << " maxPermissionWaitTime:" << maxPermissionWaitTime
        << " maxMessageCountInOneFile:" << maxMessageCountInOneFile << " reservedFileCount:" << reservedFileCount
        << " fileBufferUseRatio:" << fileBufferUseRatio << " fileMetaBufferUseRatio:" << fileMetaBufferUseRatio
        << " commitThreadLoopIntervalMs:" << commitThreadLoopIntervalMs << " dfsCommitInterval:" << dfsCommitInterval
        << " dfsCommitIntervalForMemoryPreferTopic:" << dfsCommitIntervalForMemoryPreferTopic
        << " dfsCommitIntervalWhenDelay:" << dfsCommitIntervalWhenDelay << " dfsCommitThreadNum:" << dfsCommitThreadNum
        << " dfsCommitQueueSize:" << dfsCommitQueueSize << " dfsFileSplitTime:" << dfsFileSplitTime
        << " closeNoWriteFileTime:" << closeNoWriteFileTime
        << " minChangeFileForDfsErrorTime:" << minChangeFileForDfsErrorTime
        << " maxCommitTimeAsError:" << maxCommitTimeAsError << " leaderLeaseTime:" << leaderLeaseTime
        << " leaderLoopInterval:" << leaderLoopInterval << " loadThreadNum:" << loadThreadNum
        << " loadQueueSize:" << loadQueueSize << " unloadThreadNum:" << unloadThreadNum
        << " unloadQueueSize:" << unloadQueueSize << " recyclePercent:" << recyclePercent
        << " checkFieldFilterMsg:" << checkFieldFilterMsg << " httpThreadNum:" << httpThreadNum
        << " httpQueueSize:" << httpQueueSize << " readQueueSize:" << readQueueSize
        << " statusReportIntervalSec:" << statusReportIntervalSec << " threadNum:" << _threadNum
        << " enableFastRecover:" << enableFastRecover << " partitionMinBufferSize:" << _partitionMinBufferSize
        << " partitionMaxBufferSize:" << _partitionMaxBufferSize << " totalBufferSize:" << _totalBufferSize
        << " bufferBlockSize:" << _bufferBlockSize << " bufferMinReserveSize:" << _bufferMinReserveSize
        << " dfsCommitBlockSize:" << _dfsCommitBlockSize << " dfsFileSize:" << _dfsFileSize
        << " dfsFileMinSize:" << _dfsFileMinSize << " obsoleteFileTimeInterval:" << _obsoleteFileTimeInterval
        << " delObsoleteFileInterval:" << _delObsoleteFileInterval
        << " randomDelObsoleteFileInterval:" << randomDelObsoleteFileInterval
        << " candidateObsoleteFileInterval:" << _candidateObsoleteFileInterval << " requestTimeout:" << _requestTimeout
        << " oneFileFdNum:" << _oneFileFdNum << " cacheFileReserveTime:" << _cacheFileReserveTime
        << " cacheBlockReserveTime:" << _cacheBlockReserveTime << " obsoleteReaderInterval:" << _obsoleteReaderInterval
        << " obsoleteReaderMetricInterval:" << _obsoleteReaderMetricInterval << " maxReadSizeSec:" << _maxReadSizeSec
        << " configUnlimited:" << _configUnlimited << " localMode:" << localMode << " mirrorZkRoot:" << mirrorZkRoot
        << " readNotCommittedMsg:" << readNotCommittedMsg << " timestampOffsetInUs:" << _timestampOffsetInUs
        << " maxTopicAclSyncIntervalUs:" << maxTopicAclSyncIntervalUs;
    return oss.str();
}

} // namespace config
} // namespace swift
