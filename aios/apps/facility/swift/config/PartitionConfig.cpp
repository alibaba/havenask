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
#include "swift/config/PartitionConfig.h"

#include "swift/config/ConfigDefine.h"
#include "swift/protocol/Common.pb.h"

namespace swift {
namespace config {
AUTIL_LOG_SETUP(swift, PartitionConfig);

PartitionConfig::PartitionConfig() {
    _partitionMinBufferSize = MIN_PARTITION_BUFFER_SIZE;
    _partitionMaxBufferSize = MIN_PARTITION_BUFFER_SIZE * 4;
    _blockSize = MIN_BLOCK_SIZE;
    _maxCommitSize = MIN_MAX_COMMIT_SIZE;
    _maxCommitInterval = MIN_MAX_COMMIT_INTERVAL;
    _maxCommitIntervalForMemoryPreferTopic = MIN_MAX_COMMIT_INTERVAL;
    _maxCommitIntervalWhenDelay = MIN_MAX_COMMIT_INTERVAL;
    _maxFileSize = MIN_MAX_FILE_SIZE;
    _minFileSize = MIN_MAX_FILE_SIZE;
    _maxFileSplitTime = MIN_MAX_FILE_SPLIT_INTERVAL;
    _closeNoWriteFileInterval = MIN_CLOSE_NO_WRITE_FILE_INTERVAL;
    _minChangeFileForDfsErrorTime = MIN_MIN_CHANGE_FILE_FOR_DFS_ERROR_TIME;
    _maxCommitTimeAsError = MIN_MAX_COMMIT_TIME_AS_ERROR;
    _configUnlimited = false;
    _topicMode = protocol::TOPIC_MODE_NORMAL;
    _needFieldFilter = false;
    _obsoleteFileTimeInterval = MIN_OBSOLETE_FILE_TIME_INTERVAL;
    _reservedFileCount = MIN_RESERVED_FILE_COUNT;
    _delObsoleteFileInterval = DEFAULT_DEL_OBSOLETE_FILE_INTERVAL;
    _candidateObsoleteFileInterval = DEFAULT_CANDIDATE_OBSOLETE_FILE_INTERVAL;
    _maxMessageCountInOneFile = DEFAULT_MAX_MESSAGE_COUNT_IN_ONE_FILE;
    _cacheMetaCount = DEFAULT_CACHE_META_COUNT;
    _maxWaitTimeForSecurityCommit = MAX_WAIT_TIME_FOR_SECURITY_COMMIT;
    _maxDataSizeForSecurityCommit = MAX_DATA_SIZE_FOR_SECURITY_COMMIT;
    _compressMsg = false;
    _compressThres = 0;
    _recyclePercent = DEFAULT_RECYCLE_PERCENT;
    _checkFieldFilterMsg = DEFAULT_CHECK_FIELD_FILTER_MSG;
    _obsoleteReaderInterval = DEFAULT_OBSOLETE_READER_INTERVAL;
    _obsoleteReaderMetricInterval = DEFAULT_OBSOLETE_METRIC_INTERVAL;
    _maxReadSizeSec = DEFAULT_MAX_READ_SIZE_MB_SEC << 20;
    _enableLongPolling = false;
    _readNotCommittedMsg = true;
    _timestampOffsetInUs = 0;
}

PartitionConfig::~PartitionConfig() {}

} // namespace config
} // namespace swift
