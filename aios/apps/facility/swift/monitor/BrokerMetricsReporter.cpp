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
#include "swift/monitor/BrokerMetricsReporter.h"

#include <iosfwd>
#include <utility>

#include "autil/CommonMacros.h"
#include "autil/HashAlgorithm.h"
#include "autil/LockFreeThreadPool.h"
#include "autil/StringUtil.h"
#include "autil/ThreadPool.h"
#include "autil/TimeUtility.h"
#include "autil/WorkItemQueue.h"
#include "kmonitor/client/MetricLevel.h"
#include "kmonitor/client/MetricMacro.h"
#include "kmonitor/client/core/MetricsTags.h"
#include "kmonitor/client/core/MutableMetric.h"
#include "swift/monitor/MetricsCommon.h"
#include "swift/protocol/Common.pb.h"

using namespace kmonitor;
using namespace std;
using namespace autil;

namespace swift {
namespace monitor {
AUTIL_LOG_SETUP(swift, BrokerMetricsReporter);

static size_t CLEAR_TAGS_MAP_THRESHOLD = 100000;

BrokerMetricsReporter::BrokerMetricsReporter(int reportThreadNum) {
    _metricsReporter = new MetricsReporter("swift", "", {});
    if (reportThreadNum > 0) {
        _threadPool = new LockFreeThreadPool(reportThreadNum, 10000, nullptr, "metrics");
        if (!_threadPool->start()) {
            AUTIL_LOG(ERROR, "start backup report metric thread failed.");
            DELETE_AND_SET_NULL(_threadPool);
        }
    }
}

BrokerMetricsReporter::~BrokerMetricsReporter() {
    if (_threadPool) {
        _threadPool->stop();
        DELETE_AND_SET_NULL(_threadPool);
    }
    DELETE_AND_SET_NULL(_metricsReporter);
}

LongPollingMetrics::LongPollingMetrics() {}

bool LongPollingMetrics::init(MetricsGroupManager *manager) {
    SWIFT_REGISTER_GAUGE_METRIC(longPollingHoldCount, BROKER_WORKER_GROUP_STATUS_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(longPollingTimeoutCount, BROKER_WORKER_GROUP_STATUS_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(longPollingEnqueueTimes, BROKER_WORKER_GROUP_STATUS_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(longPollingLatencyOnce, BROKER_WORKER_GROUP_STATUS_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(longPollingLatencyAcc, BROKER_WORKER_GROUP_STATUS_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(longPollingLatencyActive, BROKER_WORKER_GROUP_STATUS_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(readRequestToCurTimeInterval, BROKER_WORKER_GROUP_STATUS_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(readMsgToCurTimeInterval, BROKER_WORKER_GROUP_STATUS_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(lastAcceptedMsgToCurTimeInterval, BROKER_WORKER_GROUP_STATUS_METRIC);
    return true;
}

void LongPollingMetrics::report(const MetricsTags *tags, LongPollingMetricsCollector *collector) {
    SWIFT_REPORT_MUTABLE_METRIC(longPollingHoldCount, collector->longPollingHoldCount);
    SWIFT_REPORT_MUTABLE_METRIC(longPollingTimeoutCount, collector->longPollingTimeoutCount);
    SWIFT_REPORT_MUTABLE_METRIC(longPollingEnqueueTimes, collector->longPollingEnqueueTimes);
    SWIFT_REPORT_MUTABLE_METRIC(longPollingLatencyOnce, collector->longPollingLatencyOnce);
    SWIFT_REPORT_MUTABLE_METRIC(longPollingLatencyAcc, collector->longPollingLatencyAcc);
    SWIFT_REPORT_MUTABLE_METRIC(longPollingLatencyActive, collector->longPollingLatencyActive);
    SWIFT_REPORT_MUTABLE_METRIC(readRequestToCurTimeInterval, collector->readRequestToCurTimeInterval);
    SWIFT_REPORT_MUTABLE_METRIC(readMsgToCurTimeInterval, collector->readMsgToCurTimeInterval);
    SWIFT_REPORT_MUTABLE_METRIC(lastAcceptedMsgToCurTimeInterval, collector->lastAcceptedMsgToCurTimeInterval);
}

PartitionMetrics::PartitionMetrics() {}

bool PartitionMetrics::init(MetricsGroupManager *manager) {
    SWIFT_REGISTER_GAUGE_METRIC(lastReadMsgTimeInterval, PARTITION_GROUP_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(numOfWriteRequestToProcess, WRITE_REQUEST_SECURITY_GROUP_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(dataSizeOfWriteRequestToProcess, WRITE_REQUEST_SECURITY_GROUP_METRIC);
    SWIFT_REGISTER_STATUS_METRIC(partitionDataSizeInBuffer, PARTITION_MEMORY_GROUP_METRIC);
    SWIFT_REGISTER_STATUS_METRIC(partitionMetaSizeInBuffer, PARTITION_MEMORY_GROUP_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(leftToBeCommittedDataSize, PARTITION_MEMORY_GROUP_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(leftToBeCommittedDataUseBufferRatio, PARTITION_MEMORY_GROUP_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(lastAcceptedMsgTimeInterval, PARTITION_GROUP_METRIC);
    SWIFT_REGISTER_STATUS_METRIC(partitionOldestMsgTimeStampInBuffer, PARTITION_GROUP_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(partitionCommitDelay, PARTITION_GROUP_METRIC);
    SWIFT_REGISTER_STATUS_METRIC(partitionUsedBufferSize, PARTITION_MEMORY_GROUP_METRIC);
    SWIFT_REGISTER_STATUS_METRIC(partitionUnusedMaxBufferSize, PARTITION_MEMORY_GROUP_METRIC);
    SWIFT_REGISTER_STATUS_METRIC(partitionReserveFreeBufferSize, PARTITION_MEMORY_GROUP_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(maxMsgId, PARTITION_GROUP_METRIC);
    SWIFT_REGISTER_STATUS_METRIC(msgCountInBuffer, PARTITION_GROUP_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(partitionStatus, PARTITION_GROUP_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(writeDataFileCount, PARTITION_GROUP_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(writeMetaFileCount, PARTITION_GROUP_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(writeFailMetaFileCount, PARTITION_GROUP_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(writeFailDataFileCount, PARTITION_GROUP_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(writeBufferSize, PARTITION_GROUP_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(cacheFileCount, PARTITION_GROUP_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(cacheMetaBlockCount, PARTITION_GROUP_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(cacheDataBlockCount, PARTITION_GROUP_METRIC);
    return true;
}

void PartitionMetrics::report(const MetricsTags *tags, PartitionMetricsCollector *collector) {
    int64_t currentTime = autil::TimeUtility::currentTime();
    SWIFT_REPORT_MUTABLE_METRIC(lastAcceptedMsgTimeInterval,
                                autil::TimeUtility::us2ms(currentTime - collector->lastAcceptedMsgTimeStamp));
    int64_t lastReadMsgTimeInter =
        autil::TimeUtility::us2ms(collector->lastAcceptedMsgTimeStamp - collector->lastReadMsgTimeStamp);
    SWIFT_REPORT_MUTABLE_METRIC(lastReadMsgTimeInterval, lastReadMsgTimeInter);

    if (collector->oldestMsgTimeStampInBuffer >= 0) {
        REPORT_MUTABLE_METRIC(partitionOldestMsgTimeStampInBuffer,
                              collector->oldestMsgTimeStampInBuffer / 1000000.0); // second
    }
    SWIFT_REPORT_MUTABLE_METRIC(partitionCommitDelay, collector->commitDelay / 1000.0); // ms
    SWIFT_REPORT_MUTABLE_METRIC(leftToBeCommittedDataSize, collector->leftToBeCommittedDataSize);
    SWIFT_REPORT_MUTABLE_METRIC(leftToBeCommittedDataUseBufferRatio,
                                100.0 * collector->leftToBeCommittedDataSize /
                                    (double)(collector->usedBufferSize + collector->unusedMaxBufferSize));
    if (protocol::TOPIC_MODE_SECURITY == collector->topicMode) {
        SWIFT_REPORT_MUTABLE_METRIC(numOfWriteRequestToProcess, collector->writeRequestQueueSize);
        SWIFT_REPORT_MUTABLE_METRIC(dataSizeOfWriteRequestToProcess, collector->writeRequestQueueDataSize);
    }

    SWIFT_REPORT_MUTABLE_METRIC(partitionUsedBufferSize, collector->usedBufferSize);
    SWIFT_REPORT_MUTABLE_METRIC(partitionDataSizeInBuffer, collector->actualDataSize);
    SWIFT_REPORT_MUTABLE_METRIC(partitionMetaSizeInBuffer, collector->actualMetaSize);
    SWIFT_REPORT_MUTABLE_METRIC(partitionUnusedMaxBufferSize, collector->unusedMaxBufferSize);
    SWIFT_REPORT_MUTABLE_METRIC(partitionReserveFreeBufferSize, collector->reserveFreeBufferSize);
    SWIFT_REPORT_MUTABLE_METRIC(maxMsgId, collector->maxMsgId);
    SWIFT_REPORT_MUTABLE_METRIC(msgCountInBuffer, collector->msgCountInBuffer);
    SWIFT_REPORT_MUTABLE_METRIC(partitionStatus, collector->partitionStatus);
    SWIFT_REPORT_MUTABLE_METRIC(writeMetaFileCount, collector->writeMetaFileCount);
    SWIFT_REPORT_MUTABLE_METRIC(writeFailMetaFileCount, collector->writeFailMetaFileCount);
    SWIFT_REPORT_MUTABLE_METRIC(writeDataFileCount, collector->writeDataFileCount);
    SWIFT_REPORT_MUTABLE_METRIC(writeFailDataFileCount, collector->writeFailDataFileCount);
    SWIFT_REPORT_MUTABLE_METRIC(writeBufferSize, collector->writeBufferSize);
    SWIFT_REPORT_MUTABLE_METRIC(cacheFileCount, collector->cacheFileCount);
    SWIFT_REPORT_MUTABLE_METRIC(cacheMetaBlockCount, collector->cacheMetaBlockCount);
    SWIFT_REPORT_MUTABLE_METRIC(cacheDataBlockCount, collector->cacheDataBlockCount);
}

bool WorkerMetrics::init(MetricsGroupManager *manager) {
    SWIFT_REGISTER_GAUGE_METRIC(fileCacheUsedSize, BROKER_WORKER_GROUP_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(fileCacheUsedRatio, BROKER_WORKER_GROUP_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(loadedPartitionCount, BROKER_WORKER_GROUP_METRIC);
    SWIFT_REGISTER_STATUS_METRIC(messageTotalUsedBufferSize, BROKER_WORKER_GROUP_METRIC);   // byte
    SWIFT_REGISTER_STATUS_METRIC(messageTotalUnusedBufferSize, BROKER_WORKER_GROUP_METRIC); // byte
    SWIFT_REGISTER_GAUGE_METRIC(serviceStatus, BROKER_WORKER_GROUP_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(rejectTime, BROKER_WORKER_GROUP_METRIC); // ms
    SWIFT_REGISTER_GAUGE_METRIC(commitMessageQueueSize, BROKER_WORKER_GROUP_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(commitMessageQueueActiveThreadNum, BROKER_WORKER_GROUP_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(readFileUsedThreadCount, BROKER_WORKER_DFS_GROUP_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(actualReadingFileThreadCount, BROKER_WORKER_DFS_GROUP_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(actualReadingFileCount, BROKER_WORKER_DFS_GROUP_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(writeUsedThreadCount, BROKER_WORKER_GROUP_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(maxMsgIdUsedThreadCount, BROKER_WORKER_GROUP_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(minMsgIdUsedThreadCount, BROKER_WORKER_GROUP_METRIC);
    return true;
}

void WorkerMetrics::report(const MetricsTags *tags, WorkerMetricsCollector *collector) {
    SWIFT_REPORT_MUTABLE_METRIC(fileCacheUsedSize, collector->fileCacheUsedSize);
    SWIFT_REPORT_MUTABLE_METRIC(fileCacheUsedRatio, collector->fileCacheUsedRatio);
    SWIFT_REPORT_MUTABLE_METRIC(loadedPartitionCount, collector->loadedPartitionCount);
    SWIFT_REPORT_MUTABLE_METRIC(messageTotalUsedBufferSize, collector->messageTotalUsedBufferSize);
    SWIFT_REPORT_MUTABLE_METRIC(messageTotalUnusedBufferSize, collector->messageTotalUnusedBufferSize);
    REPORT_MUTABLE_METRIC(serviceStatus, collector->serviceStatus);
    SWIFT_REPORT_MUTABLE_METRIC(rejectTime, collector->rejectTime);
    SWIFT_REPORT_MUTABLE_METRIC(commitMessageQueueSize, collector->commitMessageQueueSize);
    SWIFT_REPORT_MUTABLE_METRIC(commitMessageQueueActiveThreadNum, collector->commitMessageActiveThreadNum);
    SWIFT_REPORT_MUTABLE_METRIC(readFileUsedThreadCount, collector->readFileUsedThreadCount);
    SWIFT_REPORT_MUTABLE_METRIC(actualReadingFileThreadCount, collector->actualReadingFileThreadCount);
    SWIFT_REPORT_MUTABLE_METRIC(actualReadingFileCount, collector->actualReadingFileCount);
    SWIFT_REPORT_MUTABLE_METRIC(writeUsedThreadCount, collector->writeUsedThreadCount);
    SWIFT_REPORT_MUTABLE_METRIC(maxMsgIdUsedThreadCount, collector->maxMsgIdUsedThreadCount);
    SWIFT_REPORT_MUTABLE_METRIC(minMsgIdUsedThreadCount, collector->minMsgIdUsedThreadCount);
}

bool BrokerInStatusMetrics::init(kmonitor::MetricsGroupManager *manager) {
    SWIFT_REGISTER_GAUGE_METRIC(cpu, BROKER_WORKER_GROUP_STATUS_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(mem, BROKER_WORKER_GROUP_STATUS_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(writeRate1min, BROKER_WORKER_GROUP_STATUS_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(writeRate5min, BROKER_WORKER_GROUP_STATUS_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(writeRequest1min, BROKER_WORKER_GROUP_STATUS_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(writeRequest5min, BROKER_WORKER_GROUP_STATUS_METRIC);

    SWIFT_REGISTER_GAUGE_METRIC(readRate1min, BROKER_WORKER_GROUP_STATUS_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(readRate5min, BROKER_WORKER_GROUP_STATUS_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(readRequest1min, BROKER_WORKER_GROUP_STATUS_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(readRequest5min, BROKER_WORKER_GROUP_STATUS_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(commitDelay, BROKER_WORKER_GROUP_STATUS_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(zfsTimeout, BROKER_WORKER_GROUP_STATUS_METRIC);
    return true;
}

void BrokerInStatusMetrics::report(const MetricsTags *tags, BrokerInStatusCollector *collector) {
    REPORT_MUTABLE_METRIC(cpu, collector->cpu);
    REPORT_MUTABLE_METRIC(mem, collector->mem);
    REPORT_MUTABLE_METRIC(writeRate1min, collector->writeRate1min);
    REPORT_MUTABLE_METRIC(writeRate5min, collector->writeRate5min);
    REPORT_MUTABLE_METRIC(writeRequest1min, collector->writeRequest1min);
    REPORT_MUTABLE_METRIC(writeRequest5min, collector->writeRequest5min);

    REPORT_MUTABLE_METRIC(readRate1min, collector->readRate1min);
    REPORT_MUTABLE_METRIC(readRate5min, collector->readRate5min);
    REPORT_MUTABLE_METRIC(readRequest1min, collector->readRequest1min);
    REPORT_MUTABLE_METRIC(readRequest5min, collector->readRequest5min);
    SWIFT_REPORT_MUTABLE_METRIC(commitDelay, collector->commitDelay);
    SWIFT_REPORT_MUTABLE_METRIC(zfsTimeout, collector->zfsTimeout);
}

bool BrokerSysMetrics::init(MetricsGroupManager *manager) {
    SWIFT_REGISTER_GAUGE_METRIC(getMsgQueueSize, BROKER_WORKER_GROUP_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(readUsedThreadCount, BROKER_WORKER_GROUP_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(brokerQueueSize, BROKER_WORKER_GROUP_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(holdQueueSize, BROKER_WORKER_GROUP_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(holdTimeoutCount, BROKER_WORKER_GROUP_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(clearPollingQueueSize, BROKER_WORKER_GROUP_METRIC);
    return true;
}

void BrokerSysMetrics::report(const MetricsTags *tags, BrokerSysMetricsCollector *collector) {
    SWIFT_REPORT_MUTABLE_METRIC(getMsgQueueSize, collector->getMsgQueueSize);
    SWIFT_REPORT_MUTABLE_METRIC(readUsedThreadCount, collector->readThreadNum);
    SWIFT_REPORT_MUTABLE_METRIC(brokerQueueSize, collector->brokerQueueSize);
    SWIFT_REPORT_MUTABLE_METRIC(holdQueueSize, collector->holdQueueSize);
    SWIFT_REPORT_MUTABLE_METRIC(holdTimeoutCount, collector->holdTimeoutCount);
    SWIFT_REPORT_MUTABLE_METRIC(clearPollingQueueSize, collector->clearPollingQueueSize);
}

bool WriteMetrics::init(MetricsGroupManager *manager) {
    // broker partition
    SWIFT_REGISTER_QPS_METRIC(writeRequestQps, WRITE_REQUEST_GROUP_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(requestDecompressedRatio, WRITE_REQUEST_GROUP_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(requestDecompressedLatency, WRITE_REQUEST_GROUP_METRIC);
    SWIFT_REGISTER_QPS_METRIC(writeBusyErrorQps, PARTITION_ERROR_GROUP_METRIC);
    SWIFT_REGISTER_QPS_METRIC(otherWriteErrorQps, PARTITION_ERROR_GROUP_METRIC);
    SWIFT_REGISTER_QPS_METRIC(writeVersionErrorQps, PARTITION_ERROR_GROUP_METRIC);

    // brain
    SWIFT_REGISTER_GAUGE_METRIC(msgsSizePerWriteRequest, WRITE_REQUEST_GROUP_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(msgsCountPerWriteRequest, WRITE_REQUEST_GROUP_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(msgsCountWithMergedPerWriteRequest, WRITE_REQUEST_GROUP_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(deniedNumOfWriteRequestWithError, WRITE_REQUEST_GROUP_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(msgCompressedRatio, WRITE_MSG_GROUP_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(msgCompressedLatency, WRITE_MSG_GROUP_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(writeRequestLatency, WRITE_REQUEST_GROUP_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(writeMsgDecompressedRatio, WRITE_MSG_GROUP_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(writeMsgDecompressedLatency, WRITE_MSG_GROUP_METRIC);

    // security
    SWIFT_REGISTER_QPS_METRIC(timeoutNumOfWriteRequest, WRITE_REQUEST_SECURITY_GROUP_METRIC);

    // group
    SWIFT_REGISTER_QPS_METRIC(acceptedMsgQps, WRITE_MSG_GROUP_METRIC);
    SWIFT_REGISTER_QPS_METRIC(mergedAcceptedMsgQps, WRITE_MSG_GROUP_METRIC);
    SWIFT_REGISTER_QPS_METRIC(totalAcceptedMsgQps, WRITE_MSG_GROUP_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(acceptedMsgSize, WRITE_MSG_GROUP_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(acceptedMsgsCountPerWriteRequest, WRITE_REQUEST_GROUP_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(acceptedMsgsSizePerWriteRequest, WRITE_REQUEST_GROUP_METRIC);
    SWIFT_REGISTER_QPS_METRIC(totalAcceptedMsgsSize, WRITE_MSG_GROUP_METRIC);
    SWIFT_REGISTER_QPS_METRIC(acceptedMsgQps_FB, WRITE_MSG_GROUP_METRIC);
    SWIFT_REGISTER_QPS_METRIC(acceptedMsgQps_PB, WRITE_MSG_GROUP_METRIC);
    SWIFT_REGISTER_QPS_METRIC(acceptedCompressMsgQps, WRITE_MSG_GROUP_METRIC);
    SWIFT_REGISTER_QPS_METRIC(acceptedUncompressMsgQps, WRITE_MSG_GROUP_METRIC);

    return true;
}

void WriteMetrics::report(const MetricsTags *tags, WriteMetricsCollector *collector) {
    // broker partition
    SWIFT_REPORT_QPS_METRIC(writeRequestQps);
    SWIFT_REPORT_MUTABLE_METRIC(requestDecompressedRatio, collector->requestDecompressedRatio);
    SWIFT_REPORT_MUTABLE_METRIC(requestDecompressedLatency, collector->requestDecompressedLatency);
    if (collector->writeBusyError) {
        SWIFT_REPORT_QPS_METRIC(writeBusyErrorQps);
    }
    if (collector->otherWriteError) {
        SWIFT_REPORT_QPS_METRIC(otherWriteErrorQps);
    }
    if (collector->writeVersionError) {
        SWIFT_REPORT_QPS_METRIC(writeVersionErrorQps);
    }

    // brain
    SWIFT_REPORT_MUTABLE_METRIC(msgsSizePerWriteRequest, collector->msgsSizePerWriteRequest);
    SWIFT_REPORT_MUTABLE_METRIC(msgsCountPerWriteRequest, collector->msgsCountPerWriteRequest);
    SWIFT_REPORT_MUTABLE_METRIC(msgsCountWithMergedPerWriteRequest, collector->msgsCountWithMergedPerWriteRequest);
    SWIFT_REPORT_MUTABLE_METRIC(deniedNumOfWriteRequestWithError, collector->deniedNumOfWriteRequestWithError);
    SWIFT_REPORT_MUTABLE_METRIC(msgCompressedRatio, collector->msgCompressedRatio);
    SWIFT_REPORT_MUTABLE_METRIC(msgCompressedLatency, collector->msgCompressedLatency);
    SWIFT_REPORT_MUTABLE_METRIC(writeRequestLatency, collector->writeRequestLatency);
    SWIFT_REPORT_MUTABLE_METRIC(writeMsgDecompressedRatio, collector->writeMsgDecompressedRatio);
    SWIFT_REPORT_MUTABLE_METRIC(writeMsgDecompressedLatency, collector->writeMsgDecompressedLatency);

    // security
    SWIFT_REPORT_MUTABLE_METRIC(timeoutNumOfWriteRequest, collector->timeoutNumOfWriteRequest);

    // group
    SWIFT_REPORT_MUTABLE_METRIC(acceptedMsgQps, collector->acceptedMsgCount);
    if (collector->mergedAcceptedMsgCount > 0) {
        REPORT_MUTABLE_METRIC(mergedAcceptedMsgQps, collector->mergedAcceptedMsgCount);
    }
    SWIFT_REPORT_MUTABLE_METRIC(totalAcceptedMsgQps, collector->totalAcceptedMsgCount);
    SWIFT_REPORT_MUTABLE_METRIC(acceptedMsgSize, collector->acceptedMsgSize);
    SWIFT_REPORT_MUTABLE_METRIC(acceptedMsgsCountPerWriteRequest, collector->acceptedMsgsCountPerWriteRequest);
    SWIFT_REPORT_MUTABLE_METRIC(acceptedMsgsSizePerWriteRequest, collector->acceptedMsgsSizePerWriteRequest);
    SWIFT_REPORT_MUTABLE_METRIC(totalAcceptedMsgsSize, collector->totalAcceptedMsgsSize);
    SWIFT_REPORT_MUTABLE_METRIC(acceptedMsgQps_FB, collector->acceptedMsgCount_FB);
    SWIFT_REPORT_MUTABLE_METRIC(acceptedMsgQps_PB, collector->acceptedMsgCount_PB);
    SWIFT_REPORT_MUTABLE_METRIC(acceptedCompressMsgQps, collector->acceptedCompressMsgCount);
    SWIFT_REPORT_MUTABLE_METRIC(acceptedUncompressMsgQps, collector->acceptedUncompressMsgCount);
}

bool ReadMetrics::init(MetricsGroupManager *manager) {
    // broker partition
    SWIFT_REGISTER_QPS_METRIC(readRequestQps, READ_REQUEST_GROUP_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(requiredMsgsCountPerReadRequest, READ_REQUEST_GROUP_METRIC);
    SWIFT_REGISTER_QPS_METRIC(readBusyErrorQps, PARTITION_ERROR_GROUP_METRIC);
    SWIFT_REGISTER_QPS_METRIC(messageLostErrorQps, PARTITION_ERROR_GROUP_METRIC);
    SWIFT_REGISTER_QPS_METRIC(brokerNodataErrorQps, PARTITION_ERROR_GROUP_METRIC);
    SWIFT_REGISTER_QPS_METRIC(otherReadErrorQps, PARTITION_ERROR_GROUP_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(readRequestLatency, READ_REQUEST_GROUP_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(readMsgDecompressedRatio, READ_MSG_GROUP_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(readMsgDecompressedLatency, READ_MSG_GROUP_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(requestCompressedRatio, READ_MSG_GROUP_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(msgCompressedLatency, READ_MSG_GROUP_METRIC);
    SWIFT_REGISTER_QPS_METRIC(unpackMsgQPS, READ_MSG_GROUP_METRIC);

    // brain
    SWIFT_REGISTER_QPS_METRIC(readMsgQPS, READ_MSG_GROUP_METRIC);
    SWIFT_REGISTER_QPS_METRIC(readMsgQPS_PB, READ_MSG_GROUP_METRIC);
    SWIFT_REGISTER_QPS_METRIC(readMsgQPS_FB, READ_MSG_GROUP_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(returnedMsgSize, READ_MSG_GROUP_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(returnedMsgsSizePerReadRequest, READ_REQUEST_GROUP_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(returnedMsgsCountPerReadRequest, READ_REQUEST_GROUP_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(returnedMsgsCountWithMergePerReadRequest, READ_REQUEST_GROUP_METRIC);
    SWIFT_REGISTER_QPS_METRIC(mergedReadMsgQPS, READ_MSG_GROUP_METRIC);
    SWIFT_REGISTER_QPS_METRIC(totalReadMsgQPS, READ_MSG_GROUP_METRIC);
    SWIFT_REGISTER_QPS_METRIC(readLimitQps, READ_REQUEST_GROUP_METRIC);

    // field filter
    SWIFT_REGISTER_GAUGE_METRIC(fieldFilterReadMsgCountPerReadRequest, READ_REQUEST_GROUP_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(fieldFilterUpdatedMsgRatio, READ_MSG_GROUP_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(fieldFilteredMsgSize, READ_MSG_GROUP_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(rawFieldFilteredMsgSize, READ_MSG_GROUP_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(fieldFilteredMsgRatio, READ_MSG_GROUP_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(fieldFilterMsgsSizePerReadRequest, READ_REQUEST_GROUP_METRIC);

    // group
    SWIFT_REGISTER_QPS_METRIC(msgReadQpsFromMemory, READ_MSG_GROUP_METRIC);
    SWIFT_REGISTER_QPS_METRIC(msgReadQpsWithMergedFromMemory, READ_MSG_GROUP_METRIC);
    SWIFT_REGISTER_QPS_METRIC(msgReadRateFromMemory, READ_MSG_GROUP_METRIC);

    // fs message reader
    SWIFT_REGISTER_QPS_METRIC(partitionReadFileMetaFailedQps, PARTITION_DFS_GROUP_METRIC);
    SWIFT_REGISTER_QPS_METRIC(partitionReadFileDataFailedQps, PARTITION_DFS_GROUP_METRIC);
    SWIFT_REGISTER_QPS_METRIC(partitionReadMetaRateFromDFS, PARTITION_DFS_GROUP_METRIC);
    SWIFT_REGISTER_QPS_METRIC(partitionReadDataRateFromDFS, PARTITION_DFS_GROUP_METRIC);
    SWIFT_REGISTER_QPS_METRIC(partitionActualReadMetaRateFromDFS, PARTITION_DFS_GROUP_METRIC);
    SWIFT_REGISTER_QPS_METRIC(partitionActualReadDataRateFromDFS, PARTITION_DFS_GROUP_METRIC);
    SWIFT_REGISTER_QPS_METRIC(msgReadRateFromDFS, READ_MSG_GROUP_METRIC);
    SWIFT_REGISTER_QPS_METRIC(msgReadQpsFromDFS, READ_MSG_GROUP_METRIC);
    SWIFT_REGISTER_QPS_METRIC(msgReadQpsWithMergedFromDFS, READ_MSG_GROUP_METRIC);

    // file cache read block
    SWIFT_REGISTER_QPS_METRIC(partitionReadBlockQps, PARTITION_DFS_GROUP_METRIC);
    SWIFT_REGISTER_QPS_METRIC(partitionReadBlockErrorQps, PARTITION_DFS_GROUP_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(partitionBlockCacheHitRatio, PARTITION_DFS_GROUP_METRIC);
    SWIFT_REGISTER_QPS_METRIC(partitionDfsActualReadRate, PARTITION_DFS_GROUP_METRIC);
    SWIFT_REGISTER_QPS_METRIC(partitionReadDataBlockQps, PARTITION_DFS_GROUP_METRIC);
    SWIFT_REGISTER_QPS_METRIC(partitionReadDataBlockErrorQps, PARTITION_DFS_GROUP_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(partitionDataBlockCacheHitRatio, PARTITION_DFS_GROUP_METRIC);
    SWIFT_REGISTER_QPS_METRIC(partitionDfsActualReadDataRate, PARTITION_DFS_GROUP_METRIC);
    SWIFT_REGISTER_QPS_METRIC(partitionReadMetaBlockQps, PARTITION_DFS_GROUP_METRIC);
    SWIFT_REGISTER_QPS_METRIC(partitionReadMetaBlockErrorQps, PARTITION_DFS_GROUP_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(partitionMetaBlockCacheHitRatio, PARTITION_DFS_GROUP_METRIC);
    SWIFT_REGISTER_QPS_METRIC(partitionDfsActualReadMetaRate, PARTITION_DFS_GROUP_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(actualReadBlockCount, PARTITION_DFS_GROUP_METRIC);
    return true;
}

void ReadMetrics::report(const MetricsTags *tags, ReadMetricsCollector *collector) {
    // broker partition
    SWIFT_REPORT_QPS_METRIC(readRequestQps);
    SWIFT_REPORT_MUTABLE_METRIC(requiredMsgsCountPerReadRequest, collector->requiredMsgsCountPerReadRequest);
    if (collector->readBusyError) {
        SWIFT_REPORT_QPS_METRIC(readBusyErrorQps);
    }
    if (collector->messageLostError) {
        SWIFT_REPORT_QPS_METRIC(messageLostErrorQps);
    }
    if (collector->brokerNodataError) {
        SWIFT_REPORT_QPS_METRIC(brokerNodataErrorQps);
    }
    if (collector->otherReadError) {
        SWIFT_REPORT_QPS_METRIC(otherReadErrorQps);
    }
    SWIFT_REPORT_MUTABLE_METRIC(readRequestLatency, collector->readRequestLatency);
    SWIFT_REPORT_MUTABLE_METRIC(readMsgDecompressedRatio, collector->readMsgDecompressedRatio);
    SWIFT_REPORT_MUTABLE_METRIC(readMsgDecompressedLatency, collector->readMsgDecompressedLatency);
    SWIFT_REPORT_MUTABLE_METRIC(requestCompressedRatio, collector->requestCompressedRatio);
    SWIFT_REPORT_MUTABLE_METRIC(msgCompressedLatency, collector->msgCompressedLatency);
    SWIFT_REPORT_MUTABLE_METRIC(unpackMsgQPS, collector->unpackMsgQPS);

    // brain
    SWIFT_REPORT_MUTABLE_METRIC(readMsgQPS, collector->readMsgCount);
    SWIFT_REPORT_MUTABLE_METRIC(readMsgQPS_PB, collector->readMsgCount_PB);
    SWIFT_REPORT_MUTABLE_METRIC(readMsgQPS_FB, collector->readMsgCount_FB);
    SWIFT_REPORT_MUTABLE_METRIC(returnedMsgSize, collector->returnedMsgSize);
    SWIFT_REPORT_MUTABLE_METRIC(returnedMsgsSizePerReadRequest, collector->returnedMsgsSizePerReadRequest);
    SWIFT_REPORT_MUTABLE_METRIC(returnedMsgsCountPerReadRequest, collector->returnedMsgsCountPerReadRequest);
    SWIFT_REPORT_MUTABLE_METRIC(returnedMsgsCountWithMergePerReadRequest,
                                collector->returnedMsgsCountWithMergePerReadRequest);
    SWIFT_REPORT_MUTABLE_METRIC(mergedReadMsgQPS, collector->mergedReadMsgCount);
    SWIFT_REPORT_MUTABLE_METRIC(totalReadMsgQPS, collector->totalReadMsgCount);
    SWIFT_REPORT_MUTABLE_METRIC(readLimitQps, collector->readLimitQps);

    // field filter
    SWIFT_REPORT_MUTABLE_METRIC(fieldFilterReadMsgCountPerReadRequest,
                                collector->fieldFilterReadMsgCountPerReadRequest);
    SWIFT_REPORT_MUTABLE_METRIC(fieldFilterUpdatedMsgRatio, collector->fieldFilterUpdatedMsgRatio);
    SWIFT_REPORT_MUTABLE_METRIC(fieldFilteredMsgSize, collector->fieldFilteredMsgSize);
    SWIFT_REPORT_MUTABLE_METRIC(rawFieldFilteredMsgSize, collector->rawFieldFilteredMsgSize);
    SWIFT_REPORT_MUTABLE_METRIC(fieldFilteredMsgRatio, collector->fieldFilteredMsgRatio);
    SWIFT_REPORT_MUTABLE_METRIC(fieldFilterMsgsSizePerReadRequest, collector->fieldFilterMsgsSizePerReadRequest);

    // group
    SWIFT_REPORT_MUTABLE_METRIC(msgReadQpsFromMemory, collector->msgReadQpsFromMemory);
    SWIFT_REPORT_MUTABLE_METRIC(msgReadQpsWithMergedFromMemory, collector->msgReadQpsWithMergedFromMemory);
    SWIFT_REPORT_MUTABLE_METRIC(msgReadRateFromMemory, collector->msgReadRateFromMemory);

    // fs message reader
    if (collector->partitionReadFileMetaFailed) {
        SWIFT_REPORT_QPS_METRIC(partitionReadFileMetaFailedQps);
    }
    if (collector->partitionReadFileDataFailed) {
        SWIFT_REPORT_QPS_METRIC(partitionReadFileDataFailedQps);
    }
    SWIFT_REPORT_MUTABLE_METRIC(partitionReadMetaRateFromDFS, collector->partitionReadMetaRateFromDFS);
    SWIFT_REPORT_MUTABLE_METRIC(partitionReadDataRateFromDFS, collector->partitionReadDataRateFromDFS);
    SWIFT_REPORT_MUTABLE_METRIC(partitionActualReadMetaRateFromDFS, collector->partitionActualReadMetaRateFromDFS);
    SWIFT_REPORT_MUTABLE_METRIC(partitionActualReadDataRateFromDFS, collector->partitionActualReadDataRateFromDFS);
    SWIFT_REPORT_MUTABLE_METRIC(msgReadRateFromDFS, collector->msgReadRateFromDFS);
    SWIFT_REPORT_MUTABLE_METRIC(msgReadQpsFromDFS, collector->msgReadQpsFromDFS);
    SWIFT_REPORT_MUTABLE_METRIC(msgReadQpsWithMergedFromDFS, collector->msgReadQpsWithMergedFromDFS);

    // file cache read block
    if (collector->partitionReadBlockQps > 0) {
        REPORT_MUTABLE_METRIC(partitionReadBlockQps, collector->partitionReadBlockQps);
    }
    if (collector->partitionReadBlockErrorQps > 0) {
        REPORT_MUTABLE_METRIC(partitionReadBlockErrorQps, collector->partitionReadBlockErrorQps);
    }
    if (collector->partitionReadDataBlockQps > 0) {
        REPORT_MUTABLE_METRIC(partitionReadDataBlockQps, collector->partitionReadDataBlockQps);
    }
    if (collector->partitionReadDataBlockErrorQps > 0) {
        REPORT_MUTABLE_METRIC(partitionReadDataBlockErrorQps, collector->partitionReadDataBlockErrorQps);
    }
    if (collector->partitionReadMetaBlockQps > 0) {
        REPORT_MUTABLE_METRIC(partitionReadMetaBlockQps, collector->partitionReadMetaBlockQps);
    }
    if (collector->partitionReadMetaBlockErrorQps > 0) {
        REPORT_MUTABLE_METRIC(partitionReadMetaBlockErrorQps, collector->partitionReadMetaBlockErrorQps);
    }
    if (collector->partitionDfsActualReadRate > 0) {
        REPORT_MUTABLE_METRIC(partitionDfsActualReadRate, collector->partitionDfsActualReadRate);
    }
    if (collector->partitionDfsActualReadDataRate > 0) {
        REPORT_MUTABLE_METRIC(partitionDfsActualReadDataRate, collector->partitionDfsActualReadDataRate);
    }
    if (collector->partitionDfsActualReadMetaRate > 0) {
        REPORT_MUTABLE_METRIC(partitionDfsActualReadMetaRate, collector->partitionDfsActualReadMetaRate);
    }
    if (collector->actualReadBlockCount > 0) {
        REPORT_MUTABLE_METRIC(actualReadBlockCount, collector->actualReadBlockCount);
    }
    for (uint32_t i = 0; i < collector->partitionBlockCacheHitTimes; ++i) {
        REPORT_MUTABLE_METRIC(partitionBlockCacheHitRatio, 100.0);
    }
    for (uint32_t i = 0; i < collector->partitionBlockCacheNotHitTimes; ++i) {
        REPORT_MUTABLE_METRIC(partitionBlockCacheHitRatio, 0);
    }
    for (uint32_t i = 0; i < collector->partitionDataBlockCacheHitTimes; ++i) {
        REPORT_MUTABLE_METRIC(partitionDataBlockCacheHitRatio, 100.0);
    }
    for (uint32_t i = 0; i < collector->partitionDataBlockCacheNotHitTimes; ++i) {
        REPORT_MUTABLE_METRIC(partitionDataBlockCacheHitRatio, 0);
    }
    for (uint32_t i = 0; i < collector->partitionMetaBlockCacheHitTimes; ++i) {
        REPORT_MUTABLE_METRIC(partitionMetaBlockCacheHitRatio, 100.0);
    }
    for (uint32_t i = 0; i < collector->partitionMetaBlockCacheNotHitTimes; ++i) {
        REPORT_MUTABLE_METRIC(partitionMetaBlockCacheHitRatio, 0);
    }
}

bool CommitFileMetrics::init(MetricsGroupManager *manager) {
    SWIFT_REGISTER_GAUGE_METRIC(dfsFlushDataLatency, PARTITION_DFS_GROUP_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(dfsFlushMetaLatency, PARTITION_DFS_GROUP_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(dfsCommitLatency, PARTITION_DFS_GROUP_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(dfsCommitSize, PARTITION_DFS_GROUP_METRIC);
    SWIFT_REGISTER_QPS_METRIC(dfsCommitQps, PARTITION_DFS_GROUP_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(dfsCommitInterval, PARTITION_DFS_GROUP_METRIC);
    return true;
}

void CommitFileMetrics::report(const MetricsTags *tags, CommitFileMetricsCollector *collector) {
    SWIFT_REPORT_MUTABLE_METRIC(dfsFlushDataLatency, collector->dfsFlushDataLatency);
    SWIFT_REPORT_MUTABLE_METRIC(dfsFlushMetaLatency, collector->dfsFlushMetaLatency);
    SWIFT_REPORT_MUTABLE_METRIC(dfsCommitLatency, collector->dfsCommitLatency);
    SWIFT_REPORT_MUTABLE_METRIC(dfsCommitSize, collector->dfsCommitSize);
    SWIFT_REPORT_MUTABLE_METRIC(dfsCommitQps, collector->dfsCommitQps);
    SWIFT_REPORT_MUTABLE_METRIC(dfsCommitInterval, collector->dfsCommitInterval);
}

bool RecycleBlockMetrics::init(MetricsGroupManager *manager) {
    SWIFT_REGISTER_QPS_METRIC(recycleObsoleteMetaBlockCount, PARTITION_GROUP_METRIC);
    SWIFT_REGISTER_QPS_METRIC(recycleObsoleteDataBlockCount, PARTITION_GROUP_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(recycleObsoleteBlockLatency, PARTITION_GROUP_METRIC);
    SWIFT_REGISTER_QPS_METRIC(recycleObsoleteBlockCount, PARTITION_GROUP_METRIC);
    SWIFT_REGISTER_QPS_METRIC(recycleByDisMetaBlockCount, PARTITION_GROUP_METRIC);
    SWIFT_REGISTER_QPS_METRIC(recycleByDisDataBlockCount, PARTITION_GROUP_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(recycleByDisLatency, PARTITION_GROUP_METRIC);
    SWIFT_REGISTER_QPS_METRIC(recycleByDisBlockCount, PARTITION_GROUP_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(recycleByDisMetaThreshold, PARTITION_GROUP_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(recycleByDisDataThreshold, PARTITION_GROUP_METRIC);
    SWIFT_REGISTER_QPS_METRIC(recycleBlockCount, PARTITION_GROUP_METRIC);
    SWIFT_REGISTER_QPS_METRIC(recycleBlockCacheQps, PARTITION_GROUP_METRIC);
    SWIFT_REGISTER_GAUGE_METRIC(recycleBlockCacheLatency, PARTITION_GROUP_METRIC);
    return true;
}

void RecycleBlockMetrics::report(const MetricsTags *tags, RecycleBlockMetricsCollector *collector) {
    SWIFT_REPORT_MUTABLE_METRIC(recycleObsoleteMetaBlockCount, collector->recycleObsoleteMetaBlockCount);
    SWIFT_REPORT_MUTABLE_METRIC(recycleObsoleteDataBlockCount, collector->recycleObsoleteDataBlockCount);
    SWIFT_REPORT_MUTABLE_METRIC(recycleObsoleteBlockLatency, collector->recycleObsoleteBlockLatency);
    SWIFT_REPORT_MUTABLE_METRIC(recycleObsoleteBlockCount, collector->recycleObsoleteBlockCount);
    SWIFT_REPORT_MUTABLE_METRIC(recycleByDisMetaBlockCount, collector->recycleByDisMetaBlockCount);
    SWIFT_REPORT_MUTABLE_METRIC(recycleByDisDataBlockCount, collector->recycleByDisDataBlockCount);
    SWIFT_REPORT_MUTABLE_METRIC(recycleByDisLatency, collector->recycleByDisLatency);
    SWIFT_REPORT_MUTABLE_METRIC(recycleByDisBlockCount, collector->recycleByDisBlockCount);
    SWIFT_REPORT_MUTABLE_METRIC(recycleByDisMetaThreshold, collector->recycleByDisMetaThreshold);
    SWIFT_REPORT_MUTABLE_METRIC(recycleByDisDataThreshold, collector->recycleByDisDataThreshold);
    SWIFT_REPORT_MUTABLE_METRIC(recycleBlockCount, collector->recycleBlockCount);
    SWIFT_REPORT_MUTABLE_METRIC(recycleBlockCacheQps, collector->recycleBlockCacheQps);
    SWIFT_REPORT_MUTABLE_METRIC(recycleBlockCacheLatency, collector->recycleBlockCacheLatency);
}

bool AccessInfoMetrics::init(MetricsGroupManager *manager) {
    SWIFT_REGISTER_QPS_METRIC(requestLength, BROKER_WORKER_GROUP_METRIC);
    SWIFT_REGISTER_QPS_METRIC(responseLength, BROKER_WORKER_GROUP_METRIC);
    SWIFT_REGISTER_QPS_METRIC(accessQps, PARTITION_GROUP_METRIC);
    return true;
}

void AccessInfoMetrics::report(const MetricsTags *tags, AccessInfoCollector *collector) {
    SWIFT_REPORT_MUTABLE_METRIC(requestLength, collector->requestLength);
    SWIFT_REPORT_MUTABLE_METRIC(responseLength, collector->responseLength);
    SWIFT_REPORT_MUTABLE_METRIC(accessQps, 1);
}

//////partition//////
void BrokerMetricsReporter::reportWriteMetricsBackupThread(const WriteMetricsCollectorPtr &collector,
                                                           const MetricsTagsPtr &tags) {
    if (collector == nullptr) {
        return;
    }
    if (_threadPool) {
        auto item = new ReportWriteInfoWorkItem(collector, this, tags);
        if (autil::ThreadPool::ERROR_NONE != _threadPool->pushWorkItem(item, false)) {
            item->process();
            item->destroy();
        }
    } else {
        reportWriteMetrics(*collector, tags);
    }
}

void BrokerMetricsReporter::reportWriteMetrics(WriteMetricsCollector &collector, const MetricsTagsPtr &tags) {
    auto tag =
        tags ? tags
             : getPartitionInfoMetricsTags(collector.topicName, intToString(collector.partId), collector.accessId);
    _metricsReporter->report<WriteMetrics, WriteMetricsCollector>(tag.get(), &collector);
}

void BrokerMetricsReporter::reportReadMetricsBackupThread(const ReadMetricsCollectorPtr &collector,
                                                          const MetricsTagsPtr &tags) {
    if (collector == nullptr) {
        return;
    }
    if (_threadPool) {
        auto item = new ReportReadInfoWorkItem(collector, this, tags);
        if (autil::ThreadPool::ERROR_NONE != _threadPool->pushWorkItem(item, false)) {
            item->process();
            item->destroy();
        }
    } else {
        reportReadMetrics(*collector, tags);
    }
}

void BrokerMetricsReporter::reportReadMetrics(ReadMetricsCollector &collector, const MetricsTagsPtr &tags) {
    auto tag =
        tags ? tags
             : getPartitionInfoMetricsTags(collector.topicName, intToString(collector.partId), collector.accessId);
    _metricsReporter->report<ReadMetrics, ReadMetricsCollector>(tag.get(), &collector);
}

void BrokerMetricsReporter::reportAccessInfoMetricsBackupThread(const AccessInfoCollectorPtr &collector) {
    if (collector == nullptr) {
        return;
    }
    if (_threadPool) {
        auto item = new ReportAccessInfoWorkItem(collector, this);
        if (autil::ThreadPool::ERROR_NONE != _threadPool->pushWorkItem(item, false)) {
            item->process();
            item->destroy();
        }
    } else {
        reportAccessInfoMetrics(*collector);
    }
}

void BrokerMetricsReporter::reportAccessInfoMetrics(AccessInfoCollector &collector) {
    auto tags = getAccessInfoMetricsTags(collector);
    _metricsReporter->report<AccessInfoMetrics, AccessInfoCollector>(tags.get(), &collector);
}

void BrokerMetricsReporter::reportLongPollingMetrics(const MetricsTagsPtr &tags,
                                                     LongPollingMetricsCollector &collector) {
    _metricsReporter->report<LongPollingMetrics, LongPollingMetricsCollector>(tags.get(), &collector);
}

void BrokerMetricsReporter::incGetMinMessageIdByTimeQps(MetricsTags *tags) {
    static const string name = PARTITION_GROUP_METRIC + "getMinMessageIdByTimeQps";
    REPORT_USER_MUTABLE_QPS_TAGS(_metricsReporter, name, tags);
}

void BrokerMetricsReporter::reportGetMinMessageIdByTimeLatency(double latency, MetricsTags *tags) {
    static const string name = PARTITION_GROUP_METRIC + "getMinMessageIdByTimeLatency";
    REPORT_USER_MUTABLE_METRIC_TAGS(_metricsReporter, name, latency, tags);
}

void BrokerMetricsReporter::reportClientCommitQps(MetricsTags *tags) {
    static const string name = PARTITION_DFS_GROUP_METRIC + "clientCommitQps";
    REPORT_USER_MUTABLE_QPS_TAGS(_metricsReporter, name, tags);
}
void BrokerMetricsReporter::reportClientCommitDelay(double delay, MetricsTags *tags) {
    static const string name = PARTITION_DFS_GROUP_METRIC + "clientCommitDelay";
    REPORT_USER_MUTABLE_METRIC_TAGS(_metricsReporter, name, delay, tags);
}
void BrokerMetricsReporter::reportRecycleWriteCacheByReaderSize(int64_t recycleSize, MetricsTags *tags) {
    static const string name = PARTITION_MEMORY_GROUP_METRIC + "partitionRecycleWriteCacheByReaderSize";
    REPORT_USER_MUTABLE_METRIC_TAGS(_metricsReporter, name, recycleSize, tags);
}
void BrokerMetricsReporter::reportRecycleWriteCacheByForceSize(int64_t recycleSize, MetricsTags *tags) {
    static const string name = PARTITION_MEMORY_GROUP_METRIC + "partitionRecycleWriteCacheByForceSize";
    REPORT_USER_MUTABLE_METRIC_TAGS(_metricsReporter, name, recycleSize, tags);
}
void BrokerMetricsReporter::incRecycleQps(MetricsTags *tags) {
    static const string name = PARTITION_MEMORY_GROUP_METRIC + "partitionRecycleQps";
    REPORT_USER_MUTABLE_QPS_TAGS(_metricsReporter, name, tags);
}
void BrokerMetricsReporter::reportLongPollingQps(const kmonitor::MetricsTagsPtr &tags) {
    static const string name = BROKER_WORKER_GROUP_STATUS_METRIC + "longPollingQps";
    REPORT_USER_MUTABLE_QPS_TAGS(_metricsReporter, name, tags.get());
}

/////// worker metrics

void BrokerMetricsReporter::reportBrokerSysMetrics(BrokerSysMetricsCollector &collector) {
    _metricsReporter->report<BrokerSysMetrics, BrokerSysMetricsCollector>(nullptr, &collector);
}

void BrokerMetricsReporter::reportWorkerMetrics(WorkerMetricsCollector &collector) {
    _metricsReporter->report<WorkerMetrics, WorkerMetricsCollector>(nullptr, &collector);

    // partition metrics
    for (auto &partCollector : collector.partMetricsCollectors) {
        MetricsTags tags;
        tags.AddTag("topic", partCollector.topicName);
        tags.AddTag("partition", intToString(partCollector.partId));
        _metricsReporter->report<PartitionMetrics, PartitionMetricsCollector>(&tags, &partCollector);
    }
}

void BrokerMetricsReporter::reportBrokerInStatus(const string &broker, BrokerInStatusCollector &collector) {
    string newName(broker);
    autil::StringUtil::replace(newName, '#', '_');
    MetricsTags tags("broker", newName);
    _metricsReporter->report<BrokerInStatusMetrics, BrokerInStatusCollector>(&tags, &collector);
}

void BrokerMetricsReporter::reportWorkerLoadPartitionQps() {
    static const string name = BROKER_WORKER_GROUP_METRIC + "loadPartitionQps";
    REPORT_USER_MUTABLE_QPS(_metricsReporter, name);
}
void BrokerMetricsReporter::reportWorkerUnloadPartitionQps() {
    static const string name = BROKER_WORKER_GROUP_METRIC + "unloadPartitionQps";
    REPORT_USER_MUTABLE_QPS(_metricsReporter, name);
}
void BrokerMetricsReporter::reportWorkerLoadPartitionLatency(double latency) {
    static const string name = BROKER_WORKER_GROUP_METRIC + "loadPartitionLatency";
    REPORT_USER_MUTABLE_METRIC(_metricsReporter, name, latency);
}
void BrokerMetricsReporter::reportWorkerUnloadPartitionLatency(double latency) {
    static const string name = BROKER_WORKER_GROUP_METRIC + "unloadPartitionLatency";
    REPORT_USER_MUTABLE_METRIC(_metricsReporter, name, latency);
}
void BrokerMetricsReporter::reportRecycleWriteCacheBlockCount(int64_t count) {
    static const string name = BROKER_WORKER_GROUP_METRIC + "recycleWriteCacheBlockCount";
    REPORT_USER_MUTABLE_METRIC(_metricsReporter, name, count);
}
void BrokerMetricsReporter::reportRecycleWriteCacheLatency(int64_t latency) {
    static const string name = BROKER_WORKER_GROUP_METRIC + "recycleWriteCacheLatency";
    REPORT_USER_MUTABLE_METRIC(_metricsReporter, name, latency);
}
void BrokerMetricsReporter::reportRecycleFileCacheBlockCount(int64_t count) {
    static const string name = BROKER_WORKER_GROUP_METRIC + "recycleFileCacheBlockCount";
    REPORT_USER_MUTABLE_METRIC(_metricsReporter, name, count);
}
void BrokerMetricsReporter::reportRecycleFileCacheLatency(int64_t latency) {
    static const string name = BROKER_WORKER_GROUP_METRIC + "recycleFileCacheLatency";
    REPORT_USER_MUTABLE_METRIC(_metricsReporter, name, latency);
}
void BrokerMetricsReporter::reportRecycleObsoleteReaderLatency(int64_t latency) {
    static const string name = BROKER_WORKER_GROUP_METRIC + "recycleObsoleteReaderLatency";
    REPORT_USER_MUTABLE_METRIC(_metricsReporter, name, latency);
}
void BrokerMetricsReporter::reportRecycleByDisMetaThreshold(int64_t threshold) {
    static const string name = BROKER_WORKER_GROUP_METRIC + "recycleByDisMetaThreshold";
    REPORT_USER_MUTABLE_METRIC(_metricsReporter, name, threshold);
}
void BrokerMetricsReporter::reportRecycleByDisDataThreshold(int64_t threshold) {
    static const string name = BROKER_WORKER_GROUP_METRIC + "recycleByDisDataThreshold";
    REPORT_USER_MUTABLE_METRIC(_metricsReporter, name, threshold);
}

/////dfs
void BrokerMetricsReporter::reportCommitFileMetrics(CommitFileMetricsCollector &collector) {
    MetricsTags tags;
    tags.AddTag("topic", collector.topicName);
    tags.AddTag("partition", intToString(collector.partId));
    _metricsReporter->report<CommitFileMetrics, CommitFileMetricsCollector>(&tags, &collector);
}
void BrokerMetricsReporter::reportDfsWriteDataLatency(double latency, MetricsTags *tags) {
    static const string name = PARTITION_DFS_GROUP_METRIC + "dfsWriteDataLatency";
    REPORT_USER_MUTABLE_METRIC_TAGS(_metricsReporter, name, latency, tags);
}
void BrokerMetricsReporter::reportDfsWriteMetaLatency(double latency, MetricsTags *tags) {
    static const string name = PARTITION_DFS_GROUP_METRIC + "dfsWriteMetaLatency";
    REPORT_USER_MUTABLE_METRIC_TAGS(_metricsReporter, name, latency, tags);
}
void BrokerMetricsReporter::reportDFSWriteRate(uint64_t size, MetricsTags *tags) {
    static const string name = PARTITION_DFS_GROUP_METRIC + "dfsWriteRate";
    REPORT_USER_MUTABLE_QPSN_TAGS(_metricsReporter, name, size, tags);
}

//////cache
void BrokerMetricsReporter::reportRecycleBlockMetrics(RecycleBlockMetricsCollector &collector) {
    MetricsTags tags;
    tags.AddTag("topic", collector.topicName);
    tags.AddTag("partition", intToString(collector.partId));
    _metricsReporter->report<RecycleBlockMetrics, RecycleBlockMetricsCollector>(&tags, &collector);
}

void BrokerMetricsReporter::incRecycleFileCacheQps(MetricsTags *tags) {
    static const string name = PARTITION_GROUP_METRIC + "recycleFileCacheQps";
    REPORT_USER_MUTABLE_QPS_TAGS(_metricsReporter, name, tags);
}

void BrokerMetricsReporter::reportDfsReadOneBlockLatency(double latency, MetricsTags *tags) {
    static const string name = PARTITION_GROUP_METRIC + "dfsReadOneBlockLatency";
    REPORT_USER_MUTABLE_METRIC_TAGS(_metricsReporter, name, latency, tags);
}
MetricsTagsPtr BrokerMetricsReporter::getAccessInfoMetricsTags(AccessInfoCollector &collector) {
    string key;
    key.reserve(256);
    key.append(collector.topicName);
    key.append(intToString(collector.partId));
    key.append(collector.clientVersion);
    key.append(collector.clientType);
    key.append(collector.srcDrc);
    key.append(collector.accessType);
    uint32_t hashKey = autil::HashAlgorithm::hashString(key.c_str(), key.size(), 0);
    {
        ScopedReadLock lock(_accessInfoMapMutex);
        auto iter = _accessInfoTagsMap.find(hashKey);
        if (iter != _accessInfoTagsMap.end()) {
            return iter->second;
        }
    }
    MetricsTagsPtr tags(new MetricsTags({{"topic", collector.topicName},
                                         {"partition", intToString(collector.partId)},
                                         {"clientVersion", collector.clientVersion},
                                         {"clientType", collector.clientType},
                                         {"srcDrc", collector.srcDrc},
                                         {"type", collector.accessType}}));
    {
        ScopedWriteLock lock(_accessInfoMapMutex);
        if (_accessInfoTagsMap.size() > CLEAR_TAGS_MAP_THRESHOLD) {
            _accessInfoTagsMap.clear();
        }
        _accessInfoTagsMap[hashKey] = tags;
    }
    return tags;
}

kmonitor::MetricsTagsPtr BrokerMetricsReporter::getPartitionInfoMetricsTags(const std::string &topicName,
                                                                            const std::string &partId,
                                                                            const std::string &accessId) {
    string key;
    key.reserve(256);
    key.append(topicName);
    key.append(partId);
    key.append(accessId);
    uint32_t hashKey = autil::HashAlgorithm::hashString(key.c_str(), key.size(), 0);
    {
        ScopedReadLock lock(_partitionInfoMapMutex);
        auto iter = _partitionInfoTagsMap.find(hashKey);
        if (iter != _partitionInfoTagsMap.end()) {
            return iter->second;
        }
    }
    MetricsTagsPtr tags(new MetricsTags({{"topic", topicName}, {"partition", partId}, {"access_id", accessId}}));
    {
        ScopedWriteLock lock(_partitionInfoMapMutex);
        _partitionInfoTagsMap[hashKey] = tags;
    }
    return tags;
}

} // namespace monitor
} // namespace swift
