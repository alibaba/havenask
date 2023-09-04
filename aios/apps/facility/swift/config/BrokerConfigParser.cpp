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
#include "swift/config/BrokerConfigParser.h"

#include <cstddef>
#include <memory>
#include <stdint.h>

#include "swift/config/AuthorizerParser.h"
#include "swift/config/BrokerConfig.h"
#include "swift/config/ConfigDefine.h"
#include "swift/config/ConfigReader.h"

using namespace std;

namespace swift {
namespace config {
AUTIL_LOG_SETUP(swift, BrokerConfigParser);

BrokerConfigParser::BrokerConfigParser() {}

BrokerConfigParser::~BrokerConfigParser() {}

BrokerConfig *BrokerConfigParser::parse(const std::string &confFile) {
    ConfigReader reader;
    if (!reader.read(confFile)) {
        AUTIL_LOG(ERROR, "read confFile %s fail", confFile.c_str());
        return NULL;
    }

    if (!reader.hasSection(SECTION_BROKER)) {
        AUTIL_LOG(ERROR, "Failed to find section[%s].", SECTION_BROKER);
        return NULL;
    }
    if (!reader.hasSection(SECTION_COMMON)) {
        AUTIL_LOG(ERROR, "Failed to find section[%s].", SECTION_COMMON);
        return NULL;
    }

    return parse(reader);
}

BrokerConfig *BrokerConfigParser::parse(ConfigReader &reader) {
    unique_ptr<BrokerConfig> brokerConfig(new BrokerConfig);
#define GET_BROKER_CONFIG(type, name, value, need, sectionName)                                                        \
    {                                                                                                                  \
        type v;                                                                                                        \
        if (reader.getOption(sectionName, name, v)) {                                                                  \
            brokerConfig->set##value(v);                                                                               \
        } else if (need) {                                                                                             \
            AUTIL_LOG(ERROR, "Invalid config: [%s] is needed", name);                                                  \
            return NULL;                                                                                               \
        }                                                                                                              \
    }

#define GET_BROKER_CONFIG_VALUE(type, name, value, need, sectionName)                                                  \
    {                                                                                                                  \
        type v;                                                                                                        \
        if (reader.getOption(sectionName, name, v)) {                                                                  \
            brokerConfig->value = v;                                                                                   \
        } else if (need) {                                                                                             \
            AUTIL_LOG(ERROR, "Invalid config: [%s] is needed", name);                                                  \
            return NULL;                                                                                               \
        }                                                                                                              \
    }

    GET_BROKER_CONFIG_VALUE(string, USER_NAME, userName, true, SECTION_COMMON);
    GET_BROKER_CONFIG_VALUE(string, SERVICE_NAME, serviceName, true, SECTION_COMMON);
    GET_BROKER_CONFIG_VALUE(string, ZOOKEEPER_ROOT_PATH, zkRoot, true, SECTION_COMMON);
    GET_BROKER_CONFIG_VALUE(uint16_t, AMONITOR_PORT, amonitorPort, false, SECTION_COMMON);
    GET_BROKER_CONFIG_VALUE(int32_t, LEADER_LEASE_TIME, leaderLeaseTime, false, SECTION_COMMON);
    GET_BROKER_CONFIG_VALUE(double, LEADER_LOOP_INTERVAL, leaderLoopInterval, false, SECTION_COMMON);
    GET_BROKER_CONFIG_VALUE(bool, USE_RECOMMEND_PORT, useRecommendPort, false, SECTION_COMMON);
    GET_BROKER_CONFIG_VALUE(bool, IS_LOCAL_MODE, localMode, false, SECTION_COMMON);
    GET_BROKER_CONFIG_VALUE(string, MIRROR_ZKROOT, mirrorZkRoot, false, SECTION_COMMON);
    GET_BROKER_CONFIG_VALUE(int64_t, MAX_TOPIC_ACL_SYNC_INTERVAL_US, maxTopicAclSyncIntervalUs, false, SECTION_COMMON);

    GET_BROKER_CONFIG_VALUE(string, DFS_ROOT_PATH, dfsRoot, false, SECTION_BROKER);
    GET_BROKER_CONFIG_VALUE(string, IP_MAP_FILE, ipMapFile, false, SECTION_BROKER);
    GET_BROKER_CONFIG_VALUE(bool, READ_NOT_COMMITED_MSG, readNotCommittedMsg, false, SECTION_BROKER);

    GET_BROKER_CONFIG(
        int, THREAD_NUM, ThreadNum, false, SECTION_BROKER); // read-write thread num may depent total thread num
    GET_BROKER_CONFIG(int, REPORT_METRIC_THREAD_NUM, ReportMetricThreadNum, false, SECTION_BROKER);
    GET_BROKER_CONFIG(int, LOG_SAMPLE_COUNT, LogSampleCount, false, SECTION_BROKER);
    GET_BROKER_CONFIG(bool, CLOSE_FORCE_LOG, CloseForceLog, false, SECTION_BROKER);
    GET_BROKER_CONFIG(int, MAX_GET_MESSAGE_SIZE_KB, MaxGetMessageSizeKb, false, SECTION_BROKER);
    GET_BROKER_CONFIG(int, HOLD_NO_DATA_REQUEST_TIME_MS, HoldNoDataRequestTimeMs, false, SECTION_BROKER);
    GET_BROKER_CONFIG(int, NO_DATA_REQUEST_NOTFIY_INTERVAL_MS, NoDataRequestNotfiyIntervalMs, false, SECTION_BROKER);

    GET_BROKER_CONFIG_VALUE(int, MAX_READ_THREAD_NUM, maxReadThreadNum, false, SECTION_BROKER);
    GET_BROKER_CONFIG_VALUE(int, MAX_WRITE_THREAD_NUM, maxWriteThreadNum, false, SECTION_BROKER);
    GET_BROKER_CONFIG_VALUE(uint32_t, CACHE_META_FILE_COUNT, cacheMetaCount, false, SECTION_BROKER);
    GET_BROKER_CONFIG_VALUE(uint32_t, CONCURRENT_READ_FILE_LIMIT, concurrentReadFileLimit, false, SECTION_BROKER);
    GET_BROKER_CONFIG_VALUE(int64_t, MAX_WAIT_FILE_LOCK_TIME, maxPermissionWaitTime, false, SECTION_BROKER);
    GET_BROKER_CONFIG_VALUE(int64_t, MAX_MESSAGE_COUNT_IN_ONE_FILE, maxMessageCountInOneFile, false, SECTION_BROKER);
    GET_BROKER_CONFIG_VALUE(int32_t, RESERVED_FILE_COUNT, reservedFileCount, false, SECTION_BROKER);
    GET_BROKER_CONFIG_VALUE(double, FILE_BUFFER_USE_RATIO, fileBufferUseRatio, false, SECTION_BROKER);
    GET_BROKER_CONFIG_VALUE(double, FILE_META_BUFFER_USE_RATIO, fileMetaBufferUseRatio, false, SECTION_BROKER);
    GET_BROKER_CONFIG_VALUE(double, COMMIT_INTERVAL_SEC, dfsCommitInterval, false, SECTION_BROKER);
    GET_BROKER_CONFIG_VALUE(double,
                            COMMIT_INTERVAL_FOR_MEMORY_PREFER_TOPIC_SEC,
                            dfsCommitIntervalForMemoryPreferTopic,
                            false,
                            SECTION_BROKER);
    GET_BROKER_CONFIG_VALUE(double, COMMIT_INTERVAL_WHEN_DELAY_SEC, dfsCommitIntervalWhenDelay, false, SECTION_BROKER);

    GET_BROKER_CONFIG_VALUE(int, HTTP_THREAD_NUM, httpThreadNum, false, SECTION_BROKER);
    GET_BROKER_CONFIG_VALUE(int, HTTP_THREAD_QUEUE_SIZE, httpQueueSize, false, SECTION_BROKER);
    GET_BROKER_CONFIG_VALUE(int, COMMIT_THREAD_NUM, dfsCommitThreadNum, false, SECTION_BROKER);
    GET_BROKER_CONFIG_VALUE(int, COMMIT_QUEUE_SIZE, dfsCommitQueueSize, false, SECTION_BROKER);
    GET_BROKER_CONFIG_VALUE(int, FILE_SPLIT_INTERVAL_SEC, dfsFileSplitTime, false, SECTION_BROKER);
    GET_BROKER_CONFIG_VALUE(int, CLOSE_NO_WRITE_FILE_SEC, closeNoWriteFileTime, false, SECTION_BROKER);
    GET_BROKER_CONFIG_VALUE(int, LOAD_THREAD_NUM, loadThreadNum, false, SECTION_BROKER);
    GET_BROKER_CONFIG_VALUE(int, LOAD_QUEUE_SIZE, loadQueueSize, false, SECTION_BROKER);
    GET_BROKER_CONFIG_VALUE(int, UNLOAD_THREAD_NUM, unloadThreadNum, false, SECTION_BROKER);
    GET_BROKER_CONFIG_VALUE(int, UNLOAD_QUEUE_SIZE, unloadQueueSize, false, SECTION_BROKER);
    GET_BROKER_CONFIG_VALUE(double, COMMIT_INTERVAL_AS_ERROR_MS, maxCommitTimeAsError, false, SECTION_BROKER);
    GET_BROKER_CONFIG_VALUE(double, FILE_CHANGE_FOR_DFS_ERROR_SEC, minChangeFileForDfsErrorTime, false, SECTION_BROKER);
    GET_BROKER_CONFIG_VALUE(double, RECYCLE_PERCENT, recyclePercent, false, SECTION_BROKER);
    GET_BROKER_CONFIG_VALUE(bool, SUPPORT_MERGE_MSG, supportMergeMsg, false, SECTION_BROKER);
    GET_BROKER_CONFIG_VALUE(bool, SUPPORT_FB, supportFb, false, SECTION_BROKER);
    GET_BROKER_CONFIG_VALUE(bool, CHECK_FIELD_FILTER_MSG, checkFieldFilterMsg, false, SECTION_BROKER);

    GET_BROKER_CONFIG(double, PARTITION_MIN_BUFFER_SIZE_MB, PartitionMinBufferSize, false, SECTION_BROKER);
    GET_BROKER_CONFIG(double, PARTITION_MAX_BUFFER_SIZE_MB, PartitionMaxBufferSize, false, SECTION_BROKER);
    GET_BROKER_CONFIG(double, TOTAL_BUFFER_SIZE_MB, TotalBufferSize, false, SECTION_BROKER);
    GET_BROKER_CONFIG(double, BUFFER_MIN_RESERVE_SIZE_MB, BufferMinReserveSize, false, SECTION_BROKER);
    GET_BROKER_CONFIG(double, BUFFER_BLOCK_SIZE_MB, BufferBlockSize, false, SECTION_BROKER);

    GET_BROKER_CONFIG(double, COMMIT_BLOCK_SIZE_MB, DfsCommitBlockSize, false, SECTION_BROKER);
    GET_BROKER_CONFIG(double, FILE_SPLIT_SIZE_MB, DfsFileSize, false, SECTION_BROKER);
    GET_BROKER_CONFIG(double, FILE_SPLIT_MIN_SIZE_MB, DfsFileMinSize, false, SECTION_BROKER);
    GET_BROKER_CONFIG(string, CONFIG_UNLIMITED, ConfigUnlimited, false, SECTION_BROKER);
    GET_BROKER_CONFIG(int64_t, OBSOLETE_FILE_TIME_INTERVAL_HOUR, ObsoleteFileTimeIntervalHour, false, SECTION_BROKER);
    GET_BROKER_CONFIG(int64_t, OBSOLETE_FILE_TIME_INTERVAL_SEC, ObsoleteFileTimeIntervalSec, false, SECTION_BROKER);
    GET_BROKER_CONFIG(int64_t, DEL_OBSOLETE_FILE_INTERVAL_SEC, DelObsoleteFileIntervalSec, false, SECTION_BROKER);
    GET_BROKER_CONFIG(
        int64_t, CANDIDATE_OBSOLETE_FILE_INTERVAL_SEC, CandidateObsoleteFileIntervalSec, false, SECTION_BROKER);
    GET_BROKER_CONFIG(int64_t, REQUEST_TIMEOUT, RequestTimeoutSec, false, SECTION_BROKER);
    GET_BROKER_CONFIG(uint32_t, ONE_FILE_FD_NUM, OneFileFdNum, false, SECTION_BROKER);
    GET_BROKER_CONFIG(int64_t, CACHE_FILE_RESERVE_TIME, CacheFileReserveTime, false, SECTION_BROKER);
    GET_BROKER_CONFIG(int64_t, CACHE_BLOCK_RESERVE_TIME, CacheBlockReserveTime, false, SECTION_BROKER);
    GET_BROKER_CONFIG(int64_t, OBSOLETE_READER_INTERVAL_SEC, ObsoleteReaderIntervalSec, false, SECTION_BROKER);
    GET_BROKER_CONFIG(
        int64_t, OBSOLETE_READER_METRIC_INTERVAL_SEC, ObsoleteReaderMetricIntervalSec, false, SECTION_BROKER);
    GET_BROKER_CONFIG(int64_t, COMMIT_THREAD_LOOP_INTERVAL_MS, CommitThreadLoopIntervalMs, false, SECTION_BROKER);
    GET_BROKER_CONFIG_VALUE(int, READ_QUEUE_SIZE, readQueueSize, false, SECTION_BROKER);
    GET_BROKER_CONFIG_VALUE(uint32_t, STATUS_REPORT_INTERVAL_SEC, statusReportIntervalSec, false, SECTION_BROKER);
    GET_BROKER_CONFIG(int32_t, MAX_READ_SIZE_MB_SEC, MaxReadSizeSec, false, SECTION_BROKER);
    GET_BROKER_CONFIG(int64_t, TIMESTAMP_OFFSET_IN_US, TimestampOffsetInUs, false, SECTION_BROKER);
    GET_BROKER_CONFIG_VALUE(bool, ENABLE_FAST_RECOVER, enableFastRecover, false, SECTION_BROKER);
    GET_BROKER_CONFIG_VALUE(
        bool, RANDOM_DEL_OBSOLETE_FILE_INTERVAL, randomDelObsoleteFileInterval, false, SECTION_BROKER);
    AuthorizerParser authorizerParser;
    if (!authorizerParser.parseAuthorizer(reader, brokerConfig->authConf)) {
        AUTIL_LOG(ERROR, "lack of necessary internal authentication");
        return NULL;
    }
    return brokerConfig.release();
}

} // namespace config
} // namespace swift
