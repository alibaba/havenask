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

#include <stddef.h>
#include <stdint.h>
#include <string>

#include "autil/Log.h"
#include "swift/common/Common.h"
#include "swift/config/AuthorizerInfo.h"

namespace swift {
namespace config {

class PartitionConfig;

class BrokerConfig {
public:
    BrokerConfig();
    ~BrokerConfig();

    bool validate();
    std::string getApplicationId();
    void getDefaultPartitionConfig(PartitionConfig &partitionConfig) const;

    void setThreadNum(int64_t value);
    int getThreadNum() const;
    void setReportMetricThreadNum(int64_t value);
    int getReportMetricThreadNum() const;
    void setPartitionMinBufferSize(double value);
    int64_t getPartitionMinBufferSize() const;

    void setPartitionMaxBufferSize(double value);
    int64_t getPartitionMaxBufferSize() const;

    void setTotalBufferSize(double value);
    int64_t getTotalBufferSize();
    int64_t getTotalFileBufferSize();
    int64_t getBrokerFileBufferSize() const;
    int64_t getBrokerFileMetaBufferSize() const;
    int64_t getBrokerMessageBufferSize() const;

    void setBufferBlockSize(double value);
    int64_t getBufferBlockSize() const;

    void setBufferMinReserveSize(double size);
    int64_t getBufferMinReserveSize();

    void setDfsCommitBlockSize(double value);
    int64_t getDfsCommitBlockSize() const;

    void setDfsFileSize(double value);
    int64_t getDfsFileSize() const;

    void setDfsFileMinSize(double value);
    int64_t getDfsFileMinSize() const;

    void setConfigUnlimited(std::string value);
    bool getConfigUnlimited() const;

    void setObsoleteFileTimeIntervalSec(int64_t timeIntervalSec);
    void setObsoleteFileTimeIntervalHour(int64_t timeIntervalHour);
    int64_t getObsoleteFileTimeInterval() const;

    void setDelObsoleteFileIntervalSec(int64_t timeIntervalSec);
    void addDelObsoleteFileIntervalSec(int64_t timeIntervalSec);
    int64_t getDelObsoleteFileInterval() const;

    void setCandidateObsoleteFileIntervalSec(int64_t timeIntervalSec);
    int64_t getCandidateObsoleteFileInterval() const;

    void setRequestTimeoutSec(int64_t timeout);
    int64_t getRequestTimeout() const;

    void setOneFileFdNum(uint32_t fdNum);
    uint32_t getOneFileFdNum() const;

    void setCacheFileReserveTime(int64_t timeSec);
    int64_t getCacheFileReserveTime() const;

    void setCacheBlockReserveTime(int64_t timeSec);
    int64_t getCacheBlockReserveTime() const;

    void setObsoleteReaderIntervalSec(int64_t timeIntervalSec);
    int64_t getObsoleteReaderInterval() const;

    void setObsoleteReaderMetricIntervalSec(int64_t timeIntervalSec);
    int64_t getObsoleteReaderMetricInterval() const;

    void setCommitThreadLoopIntervalMs(int32_t intervalMs);
    int32_t getCommitThreadLoopIntervalMs() const;

    void setLogSampleCount(uint32_t count);
    uint32_t getLogSampleCount() const;

    void setCloseForceLog(bool flag);
    bool getCloseForceLog() const;

    void setMaxGetMessageSizeKb(size_t sizeKb);
    size_t getMaxGetMessageSizeKb() const;

    void setHoldNoDataRequestTimeMs(size_t timeMs);
    size_t getHoldNoDataRequestTimeMs() const;

    void setNoDataRequestNotfiyIntervalMs(size_t timeMs);
    size_t getNoDataRequestNotfiyIntervalMs() const;

    void setMaxReadSizeSec(int32_t sizeMB);
    int32_t getMaxReadSizeSec() const;

    void setTimestampOffsetInUs(int64_t offset);
    int64_t getTimestampOffsetInUs() const;

    std::string getConfigStr();

private:
    BrokerConfig(const BrokerConfig &);
    BrokerConfig &operator=(const BrokerConfig &);
    size_t getPhyMemSize();

public:
    std::string zkRoot;
    std::string userName;
    std::string serviceName;
    uint16_t amonitorPort;
    std::string dfsRoot;
    std::string ipMapFile;
    int queueSize;
    int maxReadThreadNum;
    int maxWriteThreadNum;
    uint32_t cacheMetaCount;
    uint32_t concurrentReadFileLimit;
    int64_t maxPermissionWaitTime;
    int64_t maxMessageCountInOneFile;
    int32_t reservedFileCount;
    double fileBufferUseRatio;
    double fileMetaBufferUseRatio;
    int32_t commitThreadLoopIntervalMs;
    double dfsCommitInterval;
    double dfsCommitIntervalForMemoryPreferTopic;
    double dfsCommitIntervalWhenDelay;
    int dfsCommitThreadNum;
    int dfsCommitQueueSize;
    int64_t dfsFileSplitTime;
    int64_t closeNoWriteFileTime;
    double minChangeFileForDfsErrorTime;
    double maxCommitTimeAsError;
    int32_t leaderLeaseTime;
    double leaderLoopInterval;
    int loadThreadNum;
    int loadQueueSize;
    int unloadThreadNum;
    int unloadQueueSize;
    double recyclePercent;
    bool supportMergeMsg; // for test;
    bool supportFb;       // for test;
    bool checkFieldFilterMsg;
    int httpThreadNum;
    int httpQueueSize;
    int readQueueSize;
    uint32_t statusReportIntervalSec;
    int clearPollingThreadNum;
    int clearPollingQueueSize;
    bool enableFastRecover;
    bool randomDelObsoleteFileInterval;
    bool useRecommendPort;
    bool localMode;
    std::string mirrorZkRoot;
    AuthorizerInfo authConf;
    bool readNotCommittedMsg;
    int64_t maxTopicAclSyncIntervalUs;

private:
    int _threadNum;
    int _reportMetricThreadNum;
    int64_t _partitionMinBufferSize;
    int64_t _partitionMaxBufferSize;
    int64_t _totalBufferSize;
    int64_t _bufferBlockSize;
    int64_t _bufferMinReserveSize;
    int64_t _dfsCommitBlockSize;
    int64_t _dfsFileSize;
    int64_t _dfsFileMinSize;
    int64_t _obsoleteFileTimeInterval;
    int64_t _delObsoleteFileInterval;
    int64_t _candidateObsoleteFileInterval;
    int64_t _requestTimeout;
    uint32_t _oneFileFdNum;
    int64_t _cacheFileReserveTime;
    int64_t _cacheBlockReserveTime;
    int64_t _obsoleteReaderInterval;
    int64_t _obsoleteReaderMetricInterval;
    bool _configUnlimited;
    uint32_t _logSampleCount;
    bool _closeForceLog;
    size_t _maxGetMessageSizeKb;
    size_t _holdNoDataRequestTimeMs;
    size_t _noDataRequestNotfiyIntervalMs;
    int32_t _maxReadSizeSec;
    int64_t _timestampOffsetInUs;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(BrokerConfig);

} // namespace config
} // namespace swift
