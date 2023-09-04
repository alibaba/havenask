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

#include <cstdint>
#include <memory>
#include <stddef.h>
#include <string>
#include <unordered_map>
#include <vector>

#include "autil/Lock.h"
#include "autil/Log.h"
#include "autil/WorkItem.h"
#include "kmonitor/client/MetricsReporter.h"
#include "kmonitor/client/core/MetricsTags.h"
#include "swift/common/Common.h"
#include "swift/protocol/Common.pb.h"

namespace kmonitor {
class MutableMetric;
} // namespace kmonitor

namespace autil {
class LockFreeThreadPool;
} // namespace autil

namespace swift {
namespace monitor {

#define FUNC_GET_LATENCY(ret, func, latency)                                                                           \
    {                                                                                                                  \
        int64_t beginTime = TimeUtility::currentTime();                                                                \
        ret = func;                                                                                                    \
        int64_t endTime = TimeUtility::currentTime();                                                                  \
        latency = (endTime - beginTime) / 1000.0;                                                                      \
    }

#define FUNC_REPORT_LATENCY(ret, func, reportFun, tags)                                                                \
    {                                                                                                                  \
        int64_t beginTime = TimeUtility::currentTime();                                                                \
        ret = func;                                                                                                    \
        if (_metricsReporter) {                                                                                        \
            int64_t endTime = TimeUtility::currentTime();                                                              \
            double latency = (endTime - beginTime) / 1000.0;                                                           \
            _metricsReporter->reportFun(latency, &tags);                                                               \
        }                                                                                                              \
    }

struct PartitionMetricsCollector {
    std::string topicName;
    int32_t partId = -1;

    int32_t writeRequestQueueSize = -1;
    int64_t writeRequestQueueDataSize = -1;
    int64_t actualDataSize = -1;
    int64_t actualMetaSize = -1;
    int64_t leftToBeCommittedDataSize = -1;
    int64_t lastAcceptedMsgTimeStamp = -1;
    int64_t oldestMsgTimeStampInBuffer = -1;
    int64_t commitDelay = -1;
    int64_t usedBufferSize = -1;
    int64_t unusedMaxBufferSize = -1;
    int64_t reserveFreeBufferSize = -1;
    int64_t maxMsgId = -1;
    int64_t msgCountInBuffer = -1;
    protocol::TopicMode topicMode;
    int32_t partitionStatus = -1;
    int32_t writeDataFileCount = -1;
    int32_t writeMetaFileCount = -1;
    int32_t writeFailDataFileCount = -1;
    int32_t writeFailMetaFileCount = -1;
    int64_t writeBufferSize = -1;
    int64_t cacheFileCount = -1;
    int64_t cacheMetaBlockCount = -1;
    int64_t cacheDataBlockCount = -1;
    int64_t lastReadMsgTimeStamp = -1;
};

struct LongPollingMetricsCollector {
    int64_t longPollingHoldCount = -1;
    int64_t longPollingTimeoutCount = -1;
    int64_t longPollingEnqueueTimes = -1;
    int64_t longPollingLatencyOnce = -1;
    int64_t longPollingLatencyAcc = -1;
    int64_t longPollingLatencyActive = -1;
    int64_t readRequestToCurTimeInterval = -1;
    int64_t readMsgToCurTimeInterval = -1;
    int64_t lastAcceptedMsgToCurTimeInterval = -1;
};

class LongPollingMetrics : public kmonitor::MetricsGroup {
public:
    LongPollingMetrics();
    bool init(kmonitor::MetricsGroupManager *manager) override;
    void report(const kmonitor::MetricsTags *tags, LongPollingMetricsCollector *collector);

private:
    kmonitor::MutableMetric *longPollingHoldCount = nullptr;
    kmonitor::MutableMetric *longPollingTimeoutCount = nullptr;
    kmonitor::MutableMetric *longPollingEnqueueTimes = nullptr;
    kmonitor::MutableMetric *longPollingLatencyOnce = nullptr;
    kmonitor::MutableMetric *longPollingLatencyAcc = nullptr;
    kmonitor::MutableMetric *longPollingLatencyActive = nullptr;
    kmonitor::MutableMetric *readRequestToCurTimeInterval = nullptr;
    kmonitor::MutableMetric *readMsgToCurTimeInterval = nullptr;
    kmonitor::MutableMetric *lastAcceptedMsgToCurTimeInterval = nullptr;
};

class PartitionMetrics : public kmonitor::MetricsGroup {
public:
    PartitionMetrics();
    bool init(kmonitor::MetricsGroupManager *manager) override;
    void report(const kmonitor::MetricsTags *tags, PartitionMetricsCollector *collector);

private:
    kmonitor::MutableMetric *lastReadMsgTimeInterval = nullptr;
    kmonitor::MutableMetric *numOfWriteRequestToProcess = nullptr;
    kmonitor::MutableMetric *dataSizeOfWriteRequestToProcess = nullptr;
    kmonitor::MutableMetric *partitionDataSizeInBuffer = nullptr;
    kmonitor::MutableMetric *partitionMetaSizeInBuffer = nullptr;
    kmonitor::MutableMetric *leftToBeCommittedDataSize = nullptr;
    kmonitor::MutableMetric *leftToBeCommittedDataUseBufferRatio = nullptr;
    kmonitor::MutableMetric *lastAcceptedMsgTimeInterval = nullptr;
    kmonitor::MutableMetric *partitionOldestMsgTimeStampInBuffer = nullptr;
    kmonitor::MutableMetric *partitionCommitDelay = nullptr;
    kmonitor::MutableMetric *partitionUsedBufferSize = nullptr;
    kmonitor::MutableMetric *partitionUnusedMaxBufferSize = nullptr;
    kmonitor::MutableMetric *partitionReserveFreeBufferSize = nullptr;
    kmonitor::MutableMetric *maxMsgId = nullptr;
    kmonitor::MutableMetric *msgCountInBuffer = nullptr;
    kmonitor::MutableMetric *partitionStatus = nullptr;
    kmonitor::MutableMetric *writeDataFileCount = nullptr;
    kmonitor::MutableMetric *writeMetaFileCount = nullptr;
    kmonitor::MutableMetric *writeFailMetaFileCount = nullptr;
    kmonitor::MutableMetric *writeFailDataFileCount = nullptr;
    kmonitor::MutableMetric *writeBufferSize = nullptr;
    kmonitor::MutableMetric *cacheFileCount = nullptr;
    kmonitor::MutableMetric *cacheMetaBlockCount = nullptr;
    kmonitor::MutableMetric *cacheDataBlockCount = nullptr;
};

struct WorkerMetricsCollector {
    int64_t fileCacheUsedSize = 0;
    double fileCacheUsedRatio = 0;
    int64_t loadedPartitionCount = 0;
    int64_t messageTotalUsedBufferSize = 0;
    int64_t messageTotalUnusedBufferSize = 0;
    size_t serviceStatus = 0;
    double rejectTime = 0;
    int32_t commitMessageQueueSize = -1;
    int32_t commitMessageActiveThreadNum = -1;
    int32_t readFileUsedThreadCount = -1;
    int32_t actualReadingFileThreadCount = -1;
    int32_t actualReadingFileCount = -1;
    int32_t writeUsedThreadCount = -1;
    int32_t maxMsgIdUsedThreadCount = -1;
    int32_t minMsgIdUsedThreadCount = -1;
    std::vector<PartitionMetricsCollector> partMetricsCollectors;
};

class WorkerMetrics : public kmonitor::MetricsGroup {
public:
    bool init(kmonitor::MetricsGroupManager *manager) override;
    void report(const kmonitor::MetricsTags *tags, WorkerMetricsCollector *collector);

private:
    kmonitor::MutableMetric *fileCacheUsedSize = nullptr;
    kmonitor::MutableMetric *fileCacheUsedRatio = nullptr;
    kmonitor::MutableMetric *loadedPartitionCount = nullptr;
    kmonitor::MutableMetric *messageTotalUsedBufferSize = nullptr;
    kmonitor::MutableMetric *messageTotalUnusedBufferSize = nullptr;
    kmonitor::MutableMetric *serviceStatus = nullptr;
    kmonitor::MutableMetric *rejectTime = nullptr;
    kmonitor::MutableMetric *commitMessageQueueSize = nullptr;
    kmonitor::MutableMetric *commitMessageQueueActiveThreadNum = nullptr;
    kmonitor::MutableMetric *readFileUsedThreadCount = nullptr;
    kmonitor::MutableMetric *actualReadingFileThreadCount = nullptr;
    kmonitor::MutableMetric *actualReadingFileCount = nullptr;
    kmonitor::MutableMetric *writeUsedThreadCount = nullptr;
    kmonitor::MutableMetric *maxMsgIdUsedThreadCount = nullptr;
    kmonitor::MutableMetric *minMsgIdUsedThreadCount = nullptr;
};

struct BrokerInStatusCollector {
    double cpu = 0;
    double mem = 0;
    uint64_t writeRate1min = 0;
    uint64_t writeRate5min = 0;
    uint64_t readRate1min = 0;
    uint64_t readRate5min = 0;
    uint64_t writeRequest1min = 0;
    uint64_t writeRequest5min = 0;
    uint64_t readRequest1min = 0;
    uint64_t readRequest5min = 0;
    int64_t zfsTimeout = -1;
    int64_t commitDelay = -1;
};

class BrokerInStatusMetrics : public kmonitor::MetricsGroup {
public:
    bool init(kmonitor::MetricsGroupManager *manager) override;
    void report(const kmonitor::MetricsTags *tags, BrokerInStatusCollector *collector);

private:
    kmonitor::MutableMetric *cpu = nullptr;
    kmonitor::MutableMetric *mem = nullptr;
    kmonitor::MutableMetric *writeRate1min = nullptr;
    kmonitor::MutableMetric *writeRate5min = nullptr;
    kmonitor::MutableMetric *readRate1min = nullptr;
    kmonitor::MutableMetric *readRate5min = nullptr;
    kmonitor::MutableMetric *writeRequest1min = nullptr;
    kmonitor::MutableMetric *writeRequest5min = nullptr;
    kmonitor::MutableMetric *readRequest1min = nullptr;
    kmonitor::MutableMetric *readRequest5min = nullptr;
    kmonitor::MutableMetric *zfsTimeout = nullptr;
    kmonitor::MutableMetric *commitDelay = nullptr;
};

struct BrokerSysMetricsCollector {
    size_t getMsgQueueSize = 0;
    size_t readThreadNum = 0;
    size_t brokerQueueSize = 0;
    size_t holdQueueSize = 0;
    size_t holdTimeoutCount = 0;
    size_t clearPollingQueueSize = 0;
};

class BrokerSysMetrics : public kmonitor::MetricsGroup {
public:
    bool init(kmonitor::MetricsGroupManager *manager) override;
    void report(const kmonitor::MetricsTags *tags, BrokerSysMetricsCollector *collector);

private:
    kmonitor::MutableMetric *getMsgQueueSize = nullptr;
    kmonitor::MutableMetric *readUsedThreadCount = nullptr;
    kmonitor::MutableMetric *brokerQueueSize = nullptr;
    kmonitor::MutableMetric *holdQueueSize = nullptr;
    kmonitor::MutableMetric *holdTimeoutCount = nullptr;
    kmonitor::MutableMetric *clearPollingQueueSize = nullptr;
};

struct WriteMetricsCollector {
    WriteMetricsCollector(const std::string &topic, uint32_t part) : topicName(topic), partId(part) {}
    WriteMetricsCollector(const std::string &topic, uint32_t part, const std::string &id)
        : topicName(topic), partId(part), accessId(id) {}
    std::string topicName;
    uint32_t partId;
    std::string accessId;

    // broker partition
    float requestDecompressedRatio = -1;
    double requestDecompressedLatency = -1;
    bool writeBusyError = false;
    bool otherWriteError = false;
    bool writeVersionError = false;

    // brain
    int64_t msgsSizePerWriteRequest = 0;
    uint32_t msgsCountPerWriteRequest = 0;
    uint32_t msgsCountWithMergedPerWriteRequest = 0;
    uint32_t deniedNumOfWriteRequestWithError = 0;
    float msgCompressedRatio = -1;
    double msgCompressedLatency = -1;
    double writeRequestLatency = 0;
    float writeMsgDecompressedRatio = -1;
    double writeMsgDecompressedLatency = -1;

    // security
    uint32_t timeoutNumOfWriteRequest = 0;

    // group
    int64_t acceptedMsgCount = 0;
    int64_t mergedAcceptedMsgCount = -1;
    int64_t totalAcceptedMsgCount = 0;
    int64_t acceptedMsgSize = 0;
    int64_t acceptedMsgsCountPerWriteRequest = 0;
    int64_t acceptedMsgsSizePerWriteRequest = 0;
    int64_t totalAcceptedMsgsSize = 0;
    int64_t acceptedMsgCount_FB = -1;
    int64_t acceptedMsgCount_PB = -1;
    int64_t acceptedCompressMsgCount = -1;
    int64_t acceptedUncompressMsgCount = 0;
};

typedef std::shared_ptr<WriteMetricsCollector> WriteMetricsCollectorPtr;

class WriteMetrics : public kmonitor::MetricsGroup {
public:
    bool init(kmonitor::MetricsGroupManager *manager) override;
    void report(const kmonitor::MetricsTags *tags, WriteMetricsCollector *collector);

private:
    // broker partition
    kmonitor::MutableMetric *writeRequestQps = nullptr;
    kmonitor::MutableMetric *requestDecompressedRatio = nullptr;
    kmonitor::MutableMetric *requestDecompressedLatency = nullptr;
    kmonitor::MutableMetric *writeBusyErrorQps = nullptr;
    kmonitor::MutableMetric *otherWriteErrorQps = nullptr;
    kmonitor::MutableMetric *writeVersionErrorQps = nullptr;

    // brain
    kmonitor::MutableMetric *msgsSizePerWriteRequest = nullptr;
    kmonitor::MutableMetric *msgsCountPerWriteRequest = nullptr;
    kmonitor::MutableMetric *msgsCountWithMergedPerWriteRequest = nullptr;
    kmonitor::MutableMetric *deniedNumOfWriteRequestWithError = nullptr;
    kmonitor::MutableMetric *msgCompressedRatio = nullptr;
    kmonitor::MutableMetric *msgCompressedLatency = nullptr;
    kmonitor::MutableMetric *writeRequestLatency = nullptr;
    kmonitor::MutableMetric *writeMsgDecompressedRatio = nullptr;
    kmonitor::MutableMetric *writeMsgDecompressedLatency = nullptr;

    // security
    kmonitor::MutableMetric *timeoutNumOfWriteRequest = nullptr;

    // group
    kmonitor::MutableMetric *acceptedMsgQps = nullptr;
    kmonitor::MutableMetric *mergedAcceptedMsgQps = nullptr;
    kmonitor::MutableMetric *totalAcceptedMsgQps = nullptr;
    kmonitor::MutableMetric *acceptedMsgSize = nullptr;
    kmonitor::MutableMetric *acceptedMsgsCountPerWriteRequest = nullptr;
    kmonitor::MutableMetric *acceptedMsgsSizePerWriteRequest = nullptr;
    kmonitor::MutableMetric *totalAcceptedMsgsSize = nullptr;
    kmonitor::MutableMetric *acceptedMsgQps_FB = nullptr;
    kmonitor::MutableMetric *acceptedMsgQps_PB = nullptr;
    kmonitor::MutableMetric *acceptedCompressMsgQps = nullptr;
    kmonitor::MutableMetric *acceptedUncompressMsgQps = nullptr;
};

struct ReadMetricsCollector {
    ReadMetricsCollector(const std::string &topic, uint32_t part) : topicName(topic), partId(part) {}
    ReadMetricsCollector(const std::string &topic, uint32_t part, const std::string &id)
        : topicName(topic), partId(part), accessId(id) {}
    std::string topicName;
    uint32_t partId;
    std::string accessId;

    // broker partition
    uint32_t requiredMsgsCountPerReadRequest = 0;
    bool readBusyError = false;
    bool messageLostError = false;
    bool brokerNodataError = false;
    bool otherReadError = false;
    double readRequestLatency = 0;
    double readMsgDecompressedRatio = -1;
    double readMsgDecompressedLatency = -1;
    double requestCompressedRatio = -1;
    double msgCompressedLatency = -1;
    int64_t unpackMsgQPS = -1;

    // brain
    int32_t readLimitQps = -1;
    uint64_t readMsgCount = 0;
    int64_t readMsgCount_PB = -1;
    int64_t readMsgCount_FB = -1;
    uint64_t returnedMsgSize = 0;
    uint64_t returnedMsgsSizePerReadRequest = 0;
    uint32_t returnedMsgsCountPerReadRequest = 0;
    uint32_t returnedMsgsCountWithMergePerReadRequest = 0;
    uint64_t mergedReadMsgCount = 0;
    uint64_t totalReadMsgCount = 0;

    // field filter
    int64_t fieldFilterReadMsgCountPerReadRequest = -1;
    double fieldFilterUpdatedMsgRatio = -1;
    int64_t fieldFilteredMsgSize = -1;
    int64_t rawFieldFilteredMsgSize = -1;
    double fieldFilteredMsgRatio = -1;
    int64_t fieldFilterMsgsSizePerReadRequest = -1;

    // group
    int32_t msgReadQpsFromMemory = -1;
    int32_t msgReadQpsWithMergedFromMemory = -1;
    int64_t msgReadRateFromMemory = -1;

    // fs message reader
    bool partitionReadFileMetaFailed = false;
    bool partitionReadFileDataFailed = false;
    int64_t partitionReadMetaRateFromDFS = -1;
    int64_t partitionReadDataRateFromDFS = -1;
    int64_t partitionActualReadMetaRateFromDFS = -1;
    int64_t partitionActualReadDataRateFromDFS = -1;
    int64_t msgReadRateFromDFS = -1;
    int32_t msgReadQpsFromDFS = -1;
    int32_t msgReadQpsWithMergedFromDFS = -1;

    // file cache block
    uint32_t partitionReadBlockQps = 0;
    uint32_t partitionReadBlockErrorQps = 0;
    uint32_t partitionReadDataBlockQps = 0;
    uint32_t partitionReadDataBlockErrorQps = 0;
    uint32_t partitionReadMetaBlockQps = 0;
    uint32_t partitionReadMetaBlockErrorQps = 0;
    uint32_t partitionBlockCacheHitTimes = 0;
    uint32_t partitionBlockCacheNotHitTimes = 0;
    uint32_t partitionDataBlockCacheHitTimes = 0;
    uint32_t partitionDataBlockCacheNotHitTimes = 0;
    uint32_t partitionMetaBlockCacheHitTimes = 0;
    uint32_t partitionMetaBlockCacheNotHitTimes = 0;
    uint64_t partitionDfsActualReadRate = 0;
    uint64_t partitionDfsActualReadDataRate = 0;
    uint64_t partitionDfsActualReadMetaRate = 0;
    uint32_t actualReadBlockCount = 0;
};
typedef std::shared_ptr<ReadMetricsCollector> ReadMetricsCollectorPtr;
class ReadMetrics : public kmonitor::MetricsGroup {
public:
    bool init(kmonitor::MetricsGroupManager *manager) override;
    void report(const kmonitor::MetricsTags *tags, ReadMetricsCollector *collector);

private:
    kmonitor::MutableMetric *readRequestQps = nullptr;
    kmonitor::MutableMetric *requiredMsgsCountPerReadRequest = nullptr;
    kmonitor::MutableMetric *readBusyErrorQps = nullptr;
    kmonitor::MutableMetric *messageLostErrorQps = nullptr;
    kmonitor::MutableMetric *brokerNodataErrorQps = nullptr;
    kmonitor::MutableMetric *otherReadErrorQps = nullptr;
    kmonitor::MutableMetric *readRequestLatency = nullptr;
    kmonitor::MutableMetric *readMsgDecompressedRatio = nullptr;
    kmonitor::MutableMetric *readMsgDecompressedLatency = nullptr;
    kmonitor::MutableMetric *requestCompressedRatio = nullptr;
    kmonitor::MutableMetric *msgCompressedLatency = nullptr;
    kmonitor::MutableMetric *unpackMsgQPS = nullptr;

    // brain
    kmonitor::MutableMetric *readMsgQPS = nullptr;
    kmonitor::MutableMetric *readMsgQPS_PB = nullptr;
    kmonitor::MutableMetric *readMsgQPS_FB = nullptr;
    kmonitor::MutableMetric *returnedMsgSize = nullptr;
    kmonitor::MutableMetric *returnedMsgsSizePerReadRequest = nullptr;
    kmonitor::MutableMetric *returnedMsgsCountPerReadRequest = nullptr;
    kmonitor::MutableMetric *returnedMsgsCountWithMergePerReadRequest = nullptr;
    kmonitor::MutableMetric *mergedReadMsgQPS = nullptr;
    kmonitor::MutableMetric *totalReadMsgQPS = nullptr;
    kmonitor::MutableMetric *readLimitQps = nullptr;

    // field filter
    kmonitor::MutableMetric *fieldFilterReadMsgCountPerReadRequest = nullptr;
    kmonitor::MutableMetric *fieldFilterUpdatedMsgRatio = nullptr;
    kmonitor::MutableMetric *fieldFilteredMsgSize = nullptr;
    kmonitor::MutableMetric *rawFieldFilteredMsgSize = nullptr;
    kmonitor::MutableMetric *fieldFilteredMsgRatio = nullptr;
    kmonitor::MutableMetric *fieldFilterMsgsSizePerReadRequest = nullptr;

    // group
    kmonitor::MutableMetric *msgReadQpsFromMemory = nullptr;
    kmonitor::MutableMetric *msgReadQpsWithMergedFromMemory = nullptr;
    kmonitor::MutableMetric *msgReadRateFromMemory = nullptr;

    // fs message reader
    kmonitor::MutableMetric *partitionReadFileMetaFailedQps = nullptr;
    kmonitor::MutableMetric *partitionReadFileDataFailedQps = nullptr;
    kmonitor::MutableMetric *partitionReadMetaRateFromDFS = nullptr;
    kmonitor::MutableMetric *partitionReadDataRateFromDFS = nullptr;
    kmonitor::MutableMetric *partitionActualReadMetaRateFromDFS = nullptr;
    kmonitor::MutableMetric *partitionActualReadDataRateFromDFS = nullptr;
    kmonitor::MutableMetric *msgReadRateFromDFS = nullptr;
    kmonitor::MutableMetric *msgReadQpsFromDFS = nullptr;
    kmonitor::MutableMetric *msgReadQpsWithMergedFromDFS = nullptr;

    // file cache read block
    kmonitor::MutableMetric *partitionReadBlockQps = nullptr;
    kmonitor::MutableMetric *partitionReadBlockErrorQps = nullptr;
    kmonitor::MutableMetric *partitionBlockCacheHitRatio = nullptr;
    kmonitor::MutableMetric *partitionDfsActualReadRate = nullptr;
    kmonitor::MutableMetric *partitionReadDataBlockQps = nullptr;
    kmonitor::MutableMetric *partitionReadDataBlockErrorQps = nullptr;
    kmonitor::MutableMetric *partitionDataBlockCacheHitRatio = nullptr;
    kmonitor::MutableMetric *partitionDfsActualReadDataRate = nullptr;
    kmonitor::MutableMetric *partitionReadMetaBlockQps = nullptr;
    kmonitor::MutableMetric *partitionReadMetaBlockErrorQps = nullptr;
    kmonitor::MutableMetric *partitionMetaBlockCacheHitRatio = nullptr;
    kmonitor::MutableMetric *partitionDfsActualReadMetaRate = nullptr;
    kmonitor::MutableMetric *actualReadBlockCount = nullptr;
};

struct CommitFileMetricsCollector {
    CommitFileMetricsCollector(const std::string &topic, uint32_t part) : topicName(topic), partId(part) {}
    std::string topicName;
    uint32_t partId;

    double dfsFlushDataLatency = 0;
    double dfsFlushMetaLatency = 0;
    double dfsCommitLatency = 0;
    uint64_t dfsCommitSize = 0;
    uint32_t dfsCommitQps = 0;
    double dfsCommitInterval = 0;
};

class CommitFileMetrics : public kmonitor::MetricsGroup {
public:
    bool init(kmonitor::MetricsGroupManager *manager) override;
    void report(const kmonitor::MetricsTags *tags, CommitFileMetricsCollector *collector);

private:
    kmonitor::MutableMetric *dfsFlushDataLatency = nullptr;
    kmonitor::MutableMetric *dfsFlushMetaLatency = nullptr;
    kmonitor::MutableMetric *dfsCommitLatency = nullptr;
    kmonitor::MutableMetric *dfsCommitSize = nullptr;
    kmonitor::MutableMetric *dfsCommitQps = nullptr;
    kmonitor::MutableMetric *dfsCommitInterval = nullptr;
};

struct RecycleBlockMetricsCollector {
    RecycleBlockMetricsCollector(const std::string &topic, uint32_t part) : topicName(topic), partId(part) {}
    std::string topicName;
    uint32_t partId;

    uint32_t recycleObsoleteMetaBlockCount = 0;
    uint32_t recycleObsoleteDataBlockCount = 0;
    uint64_t recycleObsoleteBlockLatency = 0;
    uint32_t recycleObsoleteBlockCount = 0;
    uint32_t recycleByDisMetaBlockCount = 0;
    uint32_t recycleByDisDataBlockCount = 0;
    uint64_t recycleByDisLatency = 0;
    uint32_t recycleByDisBlockCount = 0;
    int64_t recycleByDisMetaThreshold = 0;
    int64_t recycleByDisDataThreshold = 0;
    uint32_t recycleBlockCount = 0;
    uint32_t recycleBlockCacheQps = 0;
    uint64_t recycleBlockCacheLatency = 0;
};

class RecycleBlockMetrics : public kmonitor::MetricsGroup {
public:
    bool init(kmonitor::MetricsGroupManager *manager) override;
    void report(const kmonitor::MetricsTags *tags, RecycleBlockMetricsCollector *collector);

private:
    kmonitor::MutableMetric *recycleObsoleteMetaBlockCount = nullptr;
    kmonitor::MutableMetric *recycleObsoleteDataBlockCount = nullptr;
    kmonitor::MutableMetric *recycleObsoleteBlockLatency = nullptr;
    kmonitor::MutableMetric *recycleObsoleteBlockCount = nullptr;
    kmonitor::MutableMetric *recycleByDisMetaBlockCount = nullptr;
    kmonitor::MutableMetric *recycleByDisDataBlockCount = nullptr;
    kmonitor::MutableMetric *recycleByDisLatency = nullptr;
    kmonitor::MutableMetric *recycleByDisBlockCount = nullptr;
    kmonitor::MutableMetric *recycleByDisMetaThreshold = nullptr;
    kmonitor::MutableMetric *recycleByDisDataThreshold = nullptr;
    kmonitor::MutableMetric *recycleBlockCount = nullptr;
    kmonitor::MutableMetric *recycleBlockCacheQps = nullptr;
    kmonitor::MutableMetric *recycleBlockCacheLatency = nullptr;
};

struct AccessInfoCollector {
    AccessInfoCollector(const std::string &topic,
                        uint32_t part,
                        const std::string &version,
                        const std::string &cType,
                        const std::string &src,
                        const std::string &aType)
        : topicName(topic), partId(part), clientVersion(version), clientType(cType), srcDrc(src), accessType(aType) {

        if (clientVersion.empty()) {
            clientVersion = "unknown";
        }
        if (clientType.empty()) {
            clientType = "unknown";
        }
        if (srcDrc.empty()) {
            srcDrc = "unknown";
        }
    }

public:
    std::string topicName;
    uint32_t partId = 0;
    std::string clientVersion;
    std::string clientType;
    std::string srcDrc;
    std::string accessType;

    uint32_t requestLength = 0;
    uint32_t responseLength = 0;
};

typedef std::shared_ptr<AccessInfoCollector> AccessInfoCollectorPtr;

class AccessInfoMetrics : public kmonitor::MetricsGroup {
public:
    bool init(kmonitor::MetricsGroupManager *manager) override;
    void report(const kmonitor::MetricsTags *tags, AccessInfoCollector *collector);

private:
    kmonitor::MutableMetric *requestLength = nullptr;
    kmonitor::MutableMetric *responseLength = nullptr;
    kmonitor::MutableMetric *accessQps = nullptr;
};

class BrokerMetricsReporter {
public:
    BrokerMetricsReporter(int reportThreadNum = 0);
    ~BrokerMetricsReporter();

private:
    BrokerMetricsReporter(const BrokerMetricsReporter &);
    BrokerMetricsReporter &operator=(const BrokerMetricsReporter &);

public:
    //////partition//////
    void reportWriteMetricsBackupThread(const WriteMetricsCollectorPtr &collector,
                                        const kmonitor::MetricsTagsPtr &tags = nullptr);
    void reportWriteMetrics(WriteMetricsCollector &collector, const kmonitor::MetricsTagsPtr &tags = nullptr);
    void reportReadMetricsBackupThread(const ReadMetricsCollectorPtr &collector,
                                       const kmonitor::MetricsTagsPtr &tags = nullptr);
    void reportReadMetrics(ReadMetricsCollector &collector, const kmonitor::MetricsTagsPtr &tags = nullptr);
    void reportAccessInfoMetricsBackupThread(const AccessInfoCollectorPtr &collector);
    void reportAccessInfoMetrics(AccessInfoCollector &collector);
    void reportLongPollingMetrics(const kmonitor::MetricsTagsPtr &tags, LongPollingMetricsCollector &collector);

    void incGetMinMessageIdByTimeQps(kmonitor::MetricsTags *tags);
    void reportGetMinMessageIdByTimeLatency(double latency, kmonitor::MetricsTags *tags);
    void reportClientCommitQps(kmonitor::MetricsTags *tags);
    void reportClientCommitDelay(double delay, kmonitor::MetricsTags *tags);
    void reportRecycleWriteCacheByReaderSize(int64_t recycleSize, kmonitor::MetricsTags *tags);
    void reportRecycleWriteCacheByForceSize(int64_t recycleSize, kmonitor::MetricsTags *tags);
    void incRecycleQps(kmonitor::MetricsTags *tags);
    void reportLongPollingQps(const kmonitor::MetricsTagsPtr &tags);

    /////// worker metrics
    void reportBrokerSysMetrics(BrokerSysMetricsCollector &collector);
    void reportWorkerMetrics(WorkerMetricsCollector &collector);
    void reportBrokerInStatus(const std::string &broker, BrokerInStatusCollector &collector);

    void reportWorkerLoadPartitionQps();
    void reportWorkerUnloadPartitionQps();
    void reportWorkerLoadPartitionLatency(double latency);
    void reportWorkerUnloadPartitionLatency(double latency);
    void reportRecycleWriteCacheBlockCount(int64_t count);
    void reportRecycleWriteCacheLatency(int64_t latency);
    void reportRecycleFileCacheBlockCount(int64_t count);
    void reportRecycleFileCacheLatency(int64_t latency);
    void reportRecycleObsoleteReaderLatency(int64_t latency);
    void reportRecycleByDisMetaThreshold(int64_t threshold);
    void reportRecycleByDisDataThreshold(int64_t threshold);

    ////// dfs
    void reportCommitFileMetrics(CommitFileMetricsCollector &collector);
    void reportDfsWriteDataLatency(double latency, kmonitor::MetricsTags *tags);
    void reportDfsWriteMetaLatency(double latency, kmonitor::MetricsTags *tags);
    void reportDFSWriteRate(uint64_t size, kmonitor::MetricsTags *tags);

    ////// file cache
    void reportRecycleBlockMetrics(RecycleBlockMetricsCollector &collector);
    void incRecycleFileCacheQps(kmonitor::MetricsTags *tags);
    void reportDfsReadOneBlockLatency(double latency, kmonitor::MetricsTags *tags);

private:
    kmonitor::MetricsTagsPtr getAccessInfoMetricsTags(AccessInfoCollector &collector);
    kmonitor::MetricsTagsPtr
    getPartitionInfoMetricsTags(const std::string &topicName, const std::string &partId, const std::string &accessId);

private:
    kmonitor::MetricsReporter *_metricsReporter = nullptr;
    autil::ReadWriteLock _accessInfoMapMutex;
    std::unordered_map<uint32_t, kmonitor::MetricsTagsPtr> _accessInfoTagsMap;
    autil::ReadWriteLock _partitionInfoMapMutex;
    std::unordered_map<uint32_t, kmonitor::MetricsTagsPtr> _partitionInfoTagsMap;
    autil::LockFreeThreadPool *_threadPool = nullptr;

private:
    AUTIL_LOG_DECLARE();
};

class ReportAccessInfoWorkItem : public autil::WorkItem {
public:
    ReportAccessInfoWorkItem(const AccessInfoCollectorPtr &collector, BrokerMetricsReporter *reporter)
        : _collector(collector), _reporter(reporter) {}
    ~ReportAccessInfoWorkItem() {
        _collector.reset();
        _reporter = nullptr;
    }

public:
    void process() override {
        if (_collector && _reporter) {
            _reporter->reportAccessInfoMetrics(*_collector);
        }
    }
    void destroy() override { delete this; }
    void drop() override { destroy(); }

private:
    AccessInfoCollectorPtr _collector;
    BrokerMetricsReporter *_reporter = nullptr;
};
class ReportWriteInfoWorkItem : public autil::WorkItem {
public:
    ReportWriteInfoWorkItem(const WriteMetricsCollectorPtr &collector,
                            BrokerMetricsReporter *reporter,
                            const kmonitor::MetricsTagsPtr &tags)
        : _collector(collector), _reporter(reporter), _tags(tags) {}
    ~ReportWriteInfoWorkItem() {
        _collector.reset();
        _reporter = nullptr;
    }

public:
    void process() override {
        if (_collector && _reporter) {
            _reporter->reportWriteMetrics(*_collector, _tags);
        }
    }
    void destroy() override { delete this; }
    void drop() override { destroy(); }

private:
    WriteMetricsCollectorPtr _collector;
    BrokerMetricsReporter *_reporter = nullptr;
    kmonitor::MetricsTagsPtr _tags = nullptr;
};

class ReportReadInfoWorkItem : public autil::WorkItem {
public:
    ReportReadInfoWorkItem(const ReadMetricsCollectorPtr &collector,
                           BrokerMetricsReporter *reporter,
                           const kmonitor::MetricsTagsPtr &tags)
        : _collector(collector), _reporter(reporter), _tags(tags) {}

    ~ReportReadInfoWorkItem() {
        _collector.reset();
        _reporter = nullptr;
    }

public:
    void process() override {
        if (_collector && _reporter) {
            _reporter->reportReadMetrics(*_collector, _tags);
        }
    }
    void destroy() override { delete this; }
    void drop() override { destroy(); }

private:
    ReadMetricsCollectorPtr _collector;
    BrokerMetricsReporter *_reporter = nullptr;
    kmonitor::MetricsTagsPtr _tags = nullptr;
};

SWIFT_TYPEDEF_PTR(BrokerMetricsReporter);

} // namespace monitor
} // namespace swift
