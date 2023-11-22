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
#include "swift/broker/storage/MessageBrain.h"

#include <assert.h>
#include <cmath>
#include <flatbuffers/flatbuffers.h>
#include <functional>
#include <iosfwd>
#include <map>
#include <memory>
#include <unistd.h>
#include <utility>

#include "autil/CommonMacros.h"
#include "autil/TimeUtility.h"
#include "kmonitor/client/core/MetricsTags.h"
#include "swift/broker/storage/BrokerResponseError.h"
#include "swift/broker/storage/CommitManager.h" // IWYU pragma: keep
#include "swift/broker/storage/FlowControl.h"
#include "swift/broker/storage_dfs/FileManager.h"
#include "swift/broker/storage_dfs/FsMessageReader.h"
#include "swift/broker/storage_dfs/MessageCommitter.h" // IWYU pragma: keep
#include "swift/common/FieldGroupReader.h"
#include "swift/common/MemoryMessage.h"
#include "swift/config/ConfigDefine.h"
#include "swift/config/PartitionConfig.h"
#include "swift/filter/DescFieldFilter.h"
#include "swift/filter/FieldFilter.h"
#include "swift/log/BrokerLogClosure.h"
#include "swift/monitor/BrokerMetricsReporter.h"
#include "swift/monitor/MetricsCommon.h"
#include "swift/monitor/TimeTrigger.h"
#include "swift/protocol/BrokerRequestResponse.pb.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/protocol/FBMessageReader.h"
#include "swift/protocol/MessageCompressor.h"
#include "swift/protocol/SwiftMessage.pb.h"
#include "swift/protocol/SwiftMessage_generated.h"
#include "swift/util/Block.h"
#include "swift/util/BlockPool.h"
#include "swift/util/PanguInlineFileUtil.h"
#include "swift/util/Permission.h"
#include "swift/util/PermissionCenter.h"

using namespace std;
using namespace autil;
using namespace swift::config;
using namespace swift::protocol;
using namespace swift::filter;
using namespace swift::common;
using namespace swift::heartbeat;
using namespace swift::service;
using namespace swift::monitor;
using namespace swift::util;

namespace swift {
namespace storage {
AUTIL_LOG_SETUP(swift, MessageBrain);

MessageBrain::MessageBrain(int64_t sessionId) {
    _partitionBlockPool = NULL;
    _fileManager = NULL;
    _fsMessageReader = NULL;
    _messageGroup = NULL;
    _messageCommitter = NULL;
    _metricsReporter = NULL;
    _permissionCenter = NULL;
    _maxMessageSize = common::MAX_MESSAGE_SIZE;
    _minBufferSize = 128 << 20;                // 128M
    _maxBufferSize = 1 << 30;                  // 1G
    _blockSize = 1 << 20;                      // 1M
    _maxDataSizeForSecurityCommit = 1 << 20;   // 1M
    _maxWaitTimeForSecurityCommit = 50 * 1000; // 50ms
    _maxCommitIntervalForMemoryPrefer = config::DEFAULT_DFS_COMMIT_INTERVAL_FOR_MEMORY_PREFER_TOPIC;
    _isStopped = false;
    _topicMode = TOPIC_MODE_NORMAL;
    _compressMsg = false;
    _compressThres = 0;
    _sessionId = sessionId;
    _commitManager = NULL;
    _recyclePercent = 0.1;
    _checkFieldFilterMsg = true;
    _forceUnload = false;
    _obsoleteReaderInterval = config::DEFAULT_OBSOLETE_READER_INTERVAL;
    _obsoleteReaderMetricInterval = config::DEFAULT_OBSOLETE_METRIC_INTERVAL;
    _lastReadMsgTimeStamp = TimeUtility::currentTime();
    _lastReadTime = 0;
    _innerPartMetric = NULL;
    _maxReadSizeSec = DEFAULT_MAX_READ_SIZE_MB_SEC << 20;
    _flowCtrol = new FlowControl(_maxReadSizeSec, _maxReadSizeSec);
    _enableFastRecover = false;
}

MessageBrain::~MessageBrain() {
    stopSecurityModeThread();
    if (!_forceUnload) {
        if (TOPIC_MODE_NORMAL == _topicMode && _messageCommitter != NULL && _messageGroup != NULL) {
            commitAllMessage();
        }
    } else {
        AUTIL_LOG(INFO, "[%s] force unload", _partitionId.ShortDebugString().c_str());
    }
    DELETE_AND_SET_NULL(_commitManager);
    DELETE_AND_SET_NULL(_messageCommitter);
    DELETE_AND_SET_NULL(_messageGroup);
    DELETE_AND_SET_NULL(_fsMessageReader);
    DELETE_AND_SET_NULL(_fileManager);
    DELETE_AND_SET_NULL(_partitionBlockPool);
    DELETE_AND_SET_NULL(_innerPartMetric);
    DELETE_AND_SET_NULL(_flowCtrol);
}

ErrorCode MessageBrain::init(const config::PartitionConfig &config,
                             BlockPool *centerPool,
                             BlockPool *fileCachePool,
                             PermissionCenter *permissionCenter,
                             BrokerMetricsReporter *metricsReporter,
                             const ThreadSafeTaskStatusPtr &taskStatus,
                             int32_t oneFileFdNum,
                             int64_t fileReserveTime,
                             bool enableFastRecover) {
    _taskStatus = taskStatus;
    _metricsReporter = metricsReporter;
    _permissionCenter = permissionCenter;
    _minBufferSize = config.getPartitionMinBufferSize();
    _maxBufferSize = config.getPartitionMaxBufferSize();
    _blockSize = config.getBlockSize();
    _needFieldFilter = config.needFieldFilter();
    _topicMode = config.getTopicMode();
    _compressMsg = config.compressMsg();
    _compressThres = config.getCompressThres();
    _maxCommitIntervalForMemoryPrefer = config.getMaxCommitIntervalForMemoryPreferTopic();
    _maxCommitIntervalWhenDelay = config.getMaxCommitIntervalWhenDelay();
    _recyclePercent = config.getRecyclePercent();
    _obsoleteReaderInterval = config.getObsoleteReaderInterval();
    _obsoleteReaderMetricInterval = config.getObsoleteReaderMetricInterval();
    _maxReadSizeSec = config.getMaxReadSizeSec();
    _enableFastRecover = enableFastRecover;
    util::InlineVersion inlineVersion;
    if (_taskStatus) {
        _partitionId = _taskStatus->getPartitionId();
        inlineVersion.fromProto(_partitionId.inlineversion());
        _metricsTags.reset(new kmonitor::MetricsTags());
        _metricsTags->AddTag("topic", _partitionId.topicname());
        _metricsTags->AddTag("partition", intToString(_partitionId.id()));
        _metricsTags->AddTag("access_id", DEFAULT_METRIC_ACCESSID);
    }
    _checkFieldFilterMsg = config.checkFieldFilterMsg();
    int64_t obsoleteFileTimeInterval = config.getObsoleteFileTimeInterval();
    int32_t reservedFileCount = config.getReservedFileCount();
    int64_t delObsoleteFileInterval = config.getDelObsoleteFileInterval();
    int64_t candidateObsoleteFileInterval = config.getCandidateObsoleteFileInterval();
    int64_t maxMessageCountInOneFile = config.getMaxMessageCountInOneFile();
    uint32_t cacheMetaCount = config.getCacheMetaCount();
    std::string dataDir = config.getDataRoot();
    AUTIL_LOG(INFO,
              "partition[%s], minBufferSize[%ld], maxBufferSize[%ld], block size[%ld], "
              "max message length[%ld], needFieldFilter[%d], topicMode[%d], "
              "obsoleteFileTimeInterval[%ld], reservedFileCount[%d], "
              "delObsoleteFileTimeInterval[%ld], candidateObsoleteFileInterval[%ld] "
              "maxMessageCountInOneFile[%ld], cacheMetaCount[%d], compressMsg[%d], "
              "compressThres[%d], dir root[%s], checkFieldFilterMsg[%d], maxReadSizeSec[%ld], "
              "enableFastRecover[%d], timestampOffset[%ld], inlineVersion %s",
              _partitionId.ShortDebugString().c_str(),
              _minBufferSize,
              _maxBufferSize,
              _blockSize,
              _maxMessageSize,
              _needFieldFilter,
              _topicMode,
              obsoleteFileTimeInterval,
              reservedFileCount,
              delObsoleteFileInterval,
              candidateObsoleteFileInterval,
              maxMessageCountInOneFile,
              cacheMetaCount,
              (int)_compressMsg,
              _compressThres,
              dataDir.c_str(),
              _checkFieldFilterMsg,
              _maxReadSizeSec,
              _enableFastRecover,
              config.getTimestampOffsetInUs(),
              inlineVersion.toDebugString().c_str());
    if (centerPool) {
        _partitionBlockPool = new BlockPool(centerPool, _maxBufferSize, _minBufferSize);
    } else {
        _partitionBlockPool = new BlockPool(_maxBufferSize, _blockSize);
    }
    _flowCtrol->setMaxReadSize(_maxReadSizeSec);
    ErrorCode errorCode;
    // if dataDir is not configured, then we will use memory only.
    if (dataDir.empty()) {
        if (TOPIC_MODE_MEMORY_ONLY != _topicMode) {
            AUTIL_LOG(ERROR,
                      "dataDir should not be empty, "
                      "when topic mode is not  TOPIC_MODE_MEMORY_ONLY");
            return ERROR_BROKER_DATA_DIR_EMPTY;
        }
        AUTIL_LOG(WARN, "dataDir is empty, use memory only and not write any files");
        _messageGroup = new MessageGroup();
        errorCode = _messageGroup->init(
            _partitionId, _partitionBlockPool, NULL, _topicMode, true, config.getTimestampOffsetInUs());
        if (errorCode != ERROR_NONE) {
            AUTIL_LOG(ERROR,
                      "partition [%s] init message group failed,errorcode[%d]",
                      _partitionId.ShortDebugString().c_str(),
                      errorCode);
            return errorCode;
        }
    } else {
        StageTime stageTime;
        _fileManager = new FileManager();
        ObsoleteFileCriterion obsoleteFileCriterion;
        obsoleteFileCriterion.obsoleteFileTimeInterval = obsoleteFileTimeInterval;
        obsoleteFileCriterion.reservedFileCount = reservedFileCount;
        obsoleteFileCriterion.delObsoleteFileInterval = delObsoleteFileInterval;
        obsoleteFileCriterion.candidateObsoleteFileInterval = candidateObsoleteFileInterval;
        errorCode = _fileManager->init(
            dataDir, config.getExtendDataRoots(), obsoleteFileCriterion, inlineVersion, _enableFastRecover);
        if (errorCode != ERROR_NONE) {
            AUTIL_LOG(ERROR,
                      "partition [%s] init filemanager failed, errorcode[%d], "
                      "datadir[%s]",
                      _partitionId.ShortDebugString().c_str(),
                      errorCode,
                      dataDir.c_str());
            return errorCode;
        }
        stageTime.end_stage();
        if (_metricsReporter) {
            _metricsReporter->reportInitFileManagerLatency(stageTime.last_us(), _metricsTags.get());
        }

        _fsMessageReader = new FsMessageReader(_partitionId,
                                               _fileManager,
                                               fileCachePool,
                                               oneFileFdNum,
                                               fileReserveTime,
                                               _permissionCenter,
                                               _metricsReporter);
        _messageCommitter =
            new MessageCommitter(_partitionId, config, _fileManager, _metricsReporter, _enableFastRecover);
        _messageGroup = new MessageGroup();
        if (_topicMode == TOPIC_MODE_MEMORY_PREFER || _topicMode == TOPIC_MODE_PERSIST_DATA) {
            _commitManager = new CommitManager(_messageGroup, _partitionId.from(), _partitionId.to());
        }
        // recover lastest message
        MessageResponse lastMsgs;
        errorCode = _fsMessageReader->getLastMessage(&lastMsgs);
        if (errorCode != ERROR_NONE) {
            AUTIL_LOG(ERROR,
                      "partition [%s] recover msgs for fsMessageReader failed,"
                      "errorcode[%d]",
                      _partitionId.ShortDebugString().c_str(),
                      errorCode);
            return errorCode;
        }
        errorCode = _messageGroup->init(_partitionId,
                                        _partitionBlockPool,
                                        &lastMsgs,
                                        _topicMode,
                                        config.readNotCommittedMsg(),
                                        config.getTimestampOffsetInUs());
        if (errorCode != ERROR_NONE) {
            AUTIL_LOG(ERROR,
                      "init message group failed,"
                      "errorcode[%d]",
                      errorCode);
            return errorCode;
        }
    }
    if (TOPIC_MODE_SECURITY == _topicMode) {
        _maxWaitTimeForSecurityCommit = config.getMaxWaitTimeForSecurityCommit();
        _maxDataSizeForSecurityCommit = config.getMaxDataSizeForSecurityCommit();
        _backGroundThreadForSecurityMode =
            Thread::createThread(bind(&MessageBrain::backgroundCommitForSecurityMode, this), "sec_back_comm");
        if (!_backGroundThreadForSecurityMode) {
            AUTIL_LOG(ERROR,
                      "partition [%s] create backgroud thread for security mode failed",
                      _partitionId.ShortDebugString().c_str());
            return ERROR_BROKER_CREATE_THREAD;
        }
        AUTIL_LOG(INFO,
                  "security partition [%s] commit data param: %ld us, %ld byte",
                  _partitionId.ShortDebugString().c_str(),
                  _maxWaitTimeForSecurityCommit,
                  _maxDataSizeForSecurityCommit);
    }
    AUTIL_LOG(INFO, "partition [%s] init message brain success.", _partitionId.ShortDebugString().c_str());
    return ERROR_NONE;
}

void MessageBrain::stopSecurityModeThread() {
    if (!_isStopped) {
        _isStopped = true;
        {
            autil::ScopedLock lock(_writeRequestCond);
            _writeRequestCond.signal();
        }
        _backGroundThreadForSecurityMode.reset();
        auto leftItemCount = _writeRequestItemQueue.getQueueSize();
        if (leftItemCount > 0) {
            AUTIL_LOG(INFO,
                      "partition[%s] still has [%d] item not processed.",
                      _partitionId.ShortDebugString().c_str(),
                      leftItemCount);
            commitForSecurityMode();
            AUTIL_LOG(INFO,
                      "partition[%s] after commit left item is [%d]",
                      _partitionId.ShortDebugString().c_str(),
                      _writeRequestItemQueue.getQueueSize());
        }
    }
}

void MessageBrain::commitForSecurityMode() {
    while (_writeRequestItemQueue.getQueueSize() > 0) {
        vector<WriteRequestItem *> requestItemVec;
        vector<WriteMetricsCollectorPtr> collectors;
        _writeRequestItemQueue.popRequestItem(requestItemVec, WriteRequestItemQueue::DEFAULT_REQUESTITEM_LIMIT);
        checkTimeOutWriteRequest(requestItemVec);
        addWriteRequestToMemory(requestItemVec);
        commitAllMessage();
        WriteRequestItem *requestItem = NULL;
        for (size_t i = 0; i < requestItemVec.size(); ++i) {
            requestItem = requestItemVec[i];
            if (requestItem) {
                doneRunForProductRequest(requestItem->closure, requestItem->collector);
                DELETE_AND_SET_NULL(requestItem);
            }
        }
        requestItemVec.clear();
    }
}

void MessageBrain::backgroundCommitForSecurityMode() {
    do {
        commitForSecurityMode();
        if (!checkSecurityModeCommitCondition()) {
            autil::ScopedLock lock(_writeRequestCond);
            _writeRequestCond.wait(_maxWaitTimeForSecurityCommit);
        }
    } while (!_isStopped);
}

bool MessageBrain::checkSecurityModeCommitCondition() {
    if (_writeRequestItemQueue.getMaxWaitTime() > _maxWaitTimeForSecurityCommit ||
        _writeRequestItemQueue.getQueueDataSize() > (uint64_t)_maxDataSizeForSecurityCommit) {
        return true;
    } else {
        return false;
    }
}

void MessageBrain::checkTimeOutWriteRequest(vector<WriteRequestItem *> &requestItemVec) {
    int64_t currTime = autil::TimeUtility::currentTime();
    WriteRequestItem *requestItem = NULL;
    for (size_t i = 0; i < requestItemVec.size(); ++i) {
        requestItem = requestItemVec[i];
        if (currTime - requestItem->receivedTime >= DEFAULT_TIMETOUT_LIMIT) {
            ++requestItem->collector->timeoutNumOfWriteRequest;
            doneRunForProductRequest(requestItem->closure, requestItem->collector, ERROR_BROKER_BUSY);
            DELETE_AND_SET_NULL(requestItem);
            requestItemVec[i] = NULL;
        } else {
            break;
        }
    }
}

void MessageBrain::addWriteRequestToMemory(vector<WriteRequestItem *> &requestItemVec) {
    WriteRequestItem *requestItem = NULL;
    ErrorCode ec = ERROR_NONE;
    for (size_t i = 0; i < requestItemVec.size(); ++i) {
        requestItem = requestItemVec[i];
        if (NULL == requestItem) {
            continue;
        }
        auto *response = requestItem->closure->_response;
        ec = doAddMessage(requestItem->closure->_request,
                          response,
                          requestItem->msgCount,
                          requestItem->dataSize,
                          requestItem->collector);
        setBrokerResponseError(response->mutable_errorinfo(), ec);
        if (0 == response->acceptedmsgcount()) {
            ++requestItem->collector->deniedNumOfWriteRequestWithError;
            doneRunForProductRequest(requestItem->closure, requestItem->collector, ec);
            DELETE_AND_SET_NULL(requestItem);
            requestItemVec[i] = NULL;
        }
    }
}

void MessageBrain::commitAllMessage() {
    if (_topicMode == TOPIC_MODE_MEMORY_PREFER || _topicMode == TOPIC_MODE_MEMORY_ONLY ||
        _messageCommitter->hasSealError()) {
        return;
    }
    int64_t lastMsgId = _messageGroup->getLastReceivedMsgId();
    int64_t lastCommittedId = _messageCommitter->getCommittedId();
    ErrorCode ec = ERROR_NONE;
    while (lastMsgId > lastCommittedId) {
        ec = commitMessage();
        if (ec != ERROR_NONE) {
            AUTIL_LOG(WARN,
                      "partition[%s] some error happen when commit message, "
                      "errorCode[%d], errorMsg[%s]",
                      _partitionId.ShortDebugString().c_str(),
                      ec,
                      ErrorCode_Name(ec).c_str());
            if (_taskStatus) {
                _taskStatus->setError(ec, ErrorCode_Name(ec).c_str());
            }
            if (_messageCommitter->hasSealError()) {
                AUTIL_LOG(WARN, "[%s] has seal error, break commit", _partitionId.ShortDebugString().c_str());
                break;
            }
        }

        ec = _messageCommitter->commitFile();
        if (ec != ERROR_NONE) {
            AUTIL_LOG(WARN,
                      "partition[%s] some error happen when commit file, "
                      "errorCode[%d], errorMsg[%s]",
                      _partitionId.ShortDebugString().c_str(),
                      ec,
                      ErrorCode_Name(ec).c_str());
            if (_taskStatus) {
                _taskStatus->setError(ec, ErrorCode_Name(ec).c_str());
            }
            _messageCommitter->closeFile();
            if (_messageCommitter->hasSealError()) {
                AUTIL_LOG(WARN, "[%s] has seal error, break commit", _partitionId.ShortDebugString().c_str());
                break;
            }
        }

        lastMsgId = _messageGroup->getLastReceivedMsgId();
        lastCommittedId = _messageCommitter->getCommittedId();
        _messageGroup->setCommittedId(lastCommittedId);
        if (lastMsgId > lastCommittedId) {
            usleep(DEFAULT_RETRY_COMMIT_INTERVAL);
        }
    }
}

bool MessageBrain::needCommitMessage() {
    if (!_messageGroup) {
        return false;
    }
    int64_t lastMsgId = _messageGroup->getLastReceivedMsgId();
    if (_messageCommitter) {
        int64_t lastCommittedId = _messageCommitter->getCommittedId();
        return lastMsgId != lastCommittedId;
    } else {
        return true;
    }
}

ErrorCode MessageBrain::commitMessage() {
    if (!_messageGroup) {
        return ERROR_NONE;
    }
    int64_t lastMsgId = _messageGroup->getLastReceivedMsgId();
    if (_topicMode == TOPIC_MODE_MEMORY_ONLY) {
        _messageGroup->setCommittedId(lastMsgId);
        if (_messageCommitter) {
            _messageCommitter->updateCommittedId(lastMsgId);
        }
        return ERROR_NONE;
    }
    if (_topicMode == TOPIC_MODE_MEMORY_PREFER) {
        int64_t committedId;
        int64_t lastAccessTime;
        int64_t committedTime;
        _commitManager->getCommitIdAndAccessTime(committedId, lastAccessTime, committedTime);
        int64_t lastCommittedId = _messageCommitter->getCommittedId();
        int64_t curTime = TimeUtility::currentTime();
        if (lastCommittedId < committedId && committedId != -1) {
            _messageCommitter->updateCommittedId(committedId);
            lastCommittedId = _messageCommitter->getCommittedId();
            _messageGroup->setCommittedId(lastCommittedId);
            int64_t delay = (curTime - committedTime) / 1000.0;
            AUTIL_LOG(INFO,
                      "[%s] update committed id [%ld], committed timestamp[%ld], delay [%ld]",
                      _partitionId.ShortDebugString().c_str(),
                      lastCommittedId,
                      _messageGroup->getCommittedTimestamp(),
                      delay);
            if (_metricsReporter) {
                _metricsReporter->reportClientCommitQps(_metricsTags.get());
                _metricsReporter->reportClientCommitDelay(delay, _metricsTags.get());
            }
        }
        int64_t commitDelay = _messageGroup->getCommitDelay();
        if (curTime - lastAccessTime < _maxCommitIntervalForMemoryPrefer && commitDelay < _maxCommitIntervalWhenDelay) {
            return ERROR_NONE;
        }
    }
    int64_t lastWritedId = _messageCommitter->getWritedId();
    MemoryMessageVector vec;
    ErrorCode ec = ERROR_NONE;
    int64_t lastCommittedId = 0;
    while (lastMsgId > lastWritedId) {
        _messageGroup->getMemoryMessage(lastWritedId + 1, 256, vec);
        ec = _messageCommitter->write(vec);
        if (ec != ERROR_NONE) {
            break;
        }
        lastWritedId = _messageCommitter->getWritedId();
        lastCommittedId = _messageCommitter->getCommittedId();
        _messageGroup->setCommittedId(lastCommittedId);
    }
    // write a null vector to trigger commit
    if (ec == ERROR_NONE) {
        ec = _messageCommitter->write(MemoryMessageVector());
    }
    lastCommittedId = _messageCommitter->getCommittedId();
    _messageGroup->setCommittedId(lastCommittedId);
    return ec;
}

int64_t MessageBrain::getCommittedMsgId() const {
    if (_messageCommitter) {
        return _messageCommitter->getCommittedId();
    } else {
        return _messageGroup->getCommittedId();
    }
}

size_t MessageBrain::calNeedBlockCount(size_t msgCount, int64_t totalDataSize) {
    size_t needBlockCount = 0;
    size_t remainMetaCount = _messageGroup->getRemainMetaCount();
    if (msgCount > remainMetaCount) {
        size_t metaCountInBlock = _messageGroup->getMetaCountInOneBlock();
        needBlockCount += (msgCount - remainMetaCount + metaCountInBlock - 1) / metaCountInBlock;
    }
    int64_t remainDataSize = _messageGroup->getRemainDataSize();
    if (totalDataSize > remainDataSize) {
        int64_t blockSize = _partitionBlockPool->getBlockSize();
        needBlockCount += (totalDataSize - remainDataSize + blockSize - 1) / blockSize;
    }
    if (needBlockCount > 0) {
        needBlockCount++;
    }
    return needBlockCount;
}

RecycleInfo MessageBrain::getRecycleInfo() { return _messageGroup->getRecycleInfo(); }

void MessageBrain::delExpiredFile() {
    if (_fileManager) {
        int64_t committedTs = FileManager::COMMITTED_TIMESTAMP_INVALID;
        if (TOPIC_MODE_PERSIST_DATA == _topicMode) {
            _commitManager->getCommitTimestamp(committedTs);
        }
        uint32_t deletedFileCount = 0;
        _fileManager->delExpiredFile(committedTs, deletedFileCount);
        if (_metricsReporter && deletedFileCount > 0) {
            _metricsReporter->reportDeleteObsoleteFileQps(deletedFileCount, _metricsTags.get());
        }
    }
}

void MessageBrain::syncDfsUsedSize() {
    if (_fileManager && !_fileManager->isUsedDfsSizeSynced()) {
        FileManagerMetricsCollector collector;
        _fileManager->syncDfsUsedSize(collector);
        if (_metricsReporter) {
            _metricsReporter->reportFileManagerMetrics(_metricsTags, collector);
        }
    }
}

// partition recycle condition: 1. current write may exceed max partition buffer
// 2.no free block in center pool and partition reserved blocks also can not hold those messags
void MessageBrain::recycleBuffer(int64_t blockCount) {
    if (!_messageGroup) {
        return;
    }
    if (_recycleMutex.trylock() != 0) {
        return;
    }
    if (!_messageGroup->canRecycle()) {
        AUTIL_INTERVAL_LOG(100,
                           INFO,
                           "partition [%s ] no msg can be recycled,"
                           " msg count  [%ld], used block [%ld], unused block [%ld],"
                           " max use block [%ld], parent unused block [%ld]",
                           _partitionId.ShortDebugString().c_str(),
                           _messageGroup->getMsgCountInBuffer(),
                           _partitionBlockPool->getUsedBlockCount(),
                           _partitionBlockPool->getUnusedBlockCount(),
                           _partitionBlockPool->getMaxBlockCount(),
                           _partitionBlockPool->getParentUnusedBlockCount());
        _recycleMutex.unlock();
        return;
    }
    int64_t begTime = TimeUtility::currentTime();
    kmonitor::MetricsTags tags;
    tags.AddTag("topic", _partitionId.topicname());
    tags.AddTag("partition", intToString(_partitionId.id()));
    if (blockCount != 0) { //自己写调用进来的，如果资源够不用回收
        int64_t unusedBlock = _partitionBlockPool->getUnusedBlockCount();
        int64_t partReserveBlock = min(unusedBlock, _partitionBlockPool->getReserveBlockCount());
        if (partReserveBlock >= blockCount) {
            _recycleMutex.unlock();
            return;
        }
        static int64_t RECYCLE_MIN_BLOCK_THRESHOLD = 32;
        int64_t maxCanUseBlock = _partitionBlockPool->getMaxBlockCount();
        int64_t usedBlock = _partitionBlockPool->getUsedBlockCount();
        int64_t totalUnusedBlock = unusedBlock + _partitionBlockPool->getParentUnusedBlockCount();
        int64_t needBlock = max(blockCount, RECYCLE_MIN_BLOCK_THRESHOLD);
        if (usedBlock + blockCount <= maxCanUseBlock && totalUnusedBlock > needBlock) {
            _recycleMutex.unlock();
            return;
        }
    }
    int64_t actualRecycleSize = 0;
    int64_t actualRecycleCount = 0;
    { // recycle by reader info first
        int64_t minTimestamp = 0, minMsgId = 0;
        {
            ScopedReadWriteLock lock(_readerRW, 'r');
            getSlowestMemReaderInfo(&_readerInfoMap, minTimestamp, minMsgId);
        }
        _messageGroup->tryRecycleByReaderInfo(minTimestamp, actualRecycleSize, actualRecycleCount);
        if (_metricsReporter) {
            _metricsReporter->reportRecycleWriteCacheByReaderSize(actualRecycleSize, &tags);
        }
    }
    if (actualRecycleSize <= 0) {
        _messageGroup->tryRecycleFast(_recyclePercent, actualRecycleSize, actualRecycleCount);
        if (actualRecycleSize <= 0) {
            int64_t maxBlockCount = _partitionBlockPool->getMaxBlockCount();
            int64_t usedBlockCount = _partitionBlockPool->getUsedBlockCount();
            int64_t blockSize = _partitionBlockPool->getBlockSize();
            int64_t recycleBlock = 0;
            if (usedBlockCount + blockCount > maxBlockCount) { // each recycle 10% used memory at least
                recycleBlock = max(blockCount, int64_t(std::ceil(maxBlockCount * _recyclePercent)));
            } else {
                recycleBlock = max(blockCount, int64_t(std::ceil(usedBlockCount * _recyclePercent)));
            }
            int64_t recycleSize = recycleBlock * blockSize;
            _messageGroup->tryRecycle(recycleSize, actualRecycleSize, actualRecycleCount);
        }
        if (_metricsReporter) {
            _metricsReporter->reportRecycleWriteCacheByForceSize(actualRecycleSize, &tags);
        }
    }
    int64_t endTime = TimeUtility::currentTime();
    AUTIL_LOG(INFO,
              "partition [%s ] recycle msg count [%ld], recycle size [%ld],"
              " left msg count  [%ld], used block [%ld], unused block [%ld],"
              " max use block [%ld], parent unused block [%ld], use time [%ld].",
              _partitionId.ShortDebugString().c_str(),
              actualRecycleCount,
              actualRecycleSize,
              _messageGroup->getMsgCountInBuffer(),
              _partitionBlockPool->getUsedBlockCount(),
              _partitionBlockPool->getUnusedBlockCount(),
              _partitionBlockPool->getMaxBlockCount(),
              _partitionBlockPool->getParentUnusedBlockCount(),
              endTime - begTime);
    if (_metricsReporter) {
        _metricsReporter->incRecycleQps(&tags);
    }
    _recycleMutex.unlock();
}

void MessageBrain::recycleFileCache(int64_t metaThreshold, int64_t dataThreshold) {
    if (_fsMessageReader) {
        ReaderInfoMap readerInfoMap;
        {
            ScopedReadWriteLock lock(_readerRW, 'r');
            readerInfoMap = _readerInfoMap;
        }
        _fsMessageReader->recycle(&readerInfoMap, metaThreshold, dataThreshold);
    }
}

ErrorCode MessageBrain::getMessage(const ConsumptionRequest *request,
                                   MessageResponse *response,
                                   TimeoutChecker *timeoutChecker,
                                   const string *srcIpPort,
                                   ReadMetricsCollector &collector) {
    ++_metricStat.readRequestNum;
    if (_enableFastRecover && _messageCommitter && _messageCommitter->hasSealError()) {
        AUTIL_LOG(
            ERROR, "%s-%d write dfs error, return part not found", _partitionId.topicname().c_str(), _partitionId.id());
        return ERROR_BROKER_TOPIC_PARTITION_NOT_FOUND;
    }
    _lastReadTime = TimeUtility::currentTime();
    if (!_flowCtrol->canRead(_lastReadTime)) {
        collector.readLimitQps = 1;
        AUTIL_INTERVAL_LOG(20,
                           WARN,
                           "%s-%d recent read size[%ld] exceed limit[%ld]",
                           _partitionId.topicname().c_str(),
                           _partitionId.id(),
                           _flowCtrol->getReadSize(),
                           _maxReadSizeSec);
        return ERROR_BROKER_BUSY;
    }
    if (!(request->has_starttimestamp() || request->has_startid())) {
        AUTIL_LOG(WARN, "no start id and msg time stamp");
        return ERROR_BROKER_MESSAGE_FORMAT_INVALID;
    }
    response->set_sessionid(_sessionId);
    if (request->has_sessionidextend()) {
        int64_t sessionId = request->sessionidextend();
        if (sessionId != -1 && sessionId != _sessionId) {
            // drop this request
            return ERROR_BROKER_SESSION_CHANGED;
        }
    }
    if (_commitManager) {
        bool needUpdateMsgId = (TOPIC_MODE_MEMORY_PREFER == _topicMode);
        _commitManager->updateCommit(request, needUpdateMsgId);
    }

    ReaderInfoPtr readerInfo(new ReaderInfo(autil::TimeUtility::currentTime()));
    ErrorCode ec = ERROR_NONE;
    if (request->requiredfieldnames_size() != 0 || request->has_fieldfilterdesc()) {
        if (_needFieldFilter) {
            ec = getFieldFilterMessage(request, response, timeoutChecker, readerInfo, collector);
        } else {
            AUTIL_LOG(ERROR,
                      "partition[%s] receive request required field names or "
                      "has field filter desc, but partition not support field filter",
                      _partitionId.ShortDebugString().c_str());
            ec = ERROR_BROKER_TOPIC_NOT_SUPPORT_FIELD_FILTER;
        }
    } else {
        ec = doGetMessage(request, response, timeoutChecker, readerInfo, collector);
    }
    int64_t totalSize = 0;
    int64_t msgCount = 0;
    if (response->messageformat() == MF_PB) {
        msgCount = response->msgs_size();
        for (int i = 0; i < msgCount; ++i) {
            size_t len = response->msgs(i).data().length();
            totalSize += len;
        }
        collector.readMsgCount_PB = msgCount;
        if (msgCount > 0) {
            const Message &lastMsg = response->msgs(msgCount - 1);
            if (lastMsg.has_timestamp()) {
                _lastReadMsgTimeStamp = lastMsg.timestamp();
            }
        }
    } else if (response->messageformat() == MF_FB) {
        FBMessageReader reader;
        totalSize = response->fbmsgs().size();
        reader.init(response->fbmsgs(), false);
        msgCount = reader.size();
        collector.readMsgCount_FB = msgCount;
        if (msgCount > 0) {
            const protocol::flat::Message *fbMsg = reader.read(msgCount - 1);
            int64_t timestamp = fbMsg->timestamp();
            if (timestamp != 0) {
                _lastReadMsgTimeStamp = fbMsg->timestamp();
            }
        }
    }
    if (msgCount > 0) {
        collector.returnedMsgSize = totalSize / msgCount;
    }
    _metricStat.readSize += totalSize;
    int64_t readDfsSize = collector.partitionActualReadDataRateFromDFS + collector.partitionActualReadMetaRateFromDFS;
    if (readDfsSize > 0) {
        _flowCtrol->updateReadSize(readDfsSize);
    }
    collector.returnedMsgsSizePerReadRequest = totalSize;
    collector.returnedMsgsCountPerReadRequest = msgCount;
    collector.returnedMsgsCountWithMergePerReadRequest = response->totalmsgcount();
    collector.readMsgCount = msgCount;
    collector.mergedReadMsgCount = response->totalmsgcount() - msgCount;
    collector.totalReadMsgCount = response->totalmsgcount();
    updateReaderInfo(ec, request, srcIpPort, response, readerInfo);
    return ec;
}

ErrorCode MessageBrain::getFieldFilterMessage(const ConsumptionRequest *request,
                                              MessageResponse *response,
                                              TimeoutChecker *timeoutChecker,
                                              ReaderInfoPtr &readerInfo,
                                              ReadMetricsCollector &collector) {
    ErrorCode ec = ERROR_NONE;
    FieldFilter fieldFilter;
    if (ERROR_NONE != (ec = fieldFilter.init(request))) {
        return ec;
    }
    fieldFilter.setTopicName(_partitionId.topicname());
    fieldFilter.setPartId(_partitionId.id());
    int64_t totalSize = 0;
    int64_t rawTotalSize = 0;
    int64_t readMsgCount = 0;
    int64_t updateMsgCount = 0;
    ec = doGetMessage(request, response, timeoutChecker, readerInfo, collector);
    ErrorCode ec2 = MessageCompressor::decompressResponseMessage(response, false);
    if (ec2 != ERROR_NONE) {
        AUTIL_LOG(WARN, "decompress message error.");
    }
    fieldFilter.filter(response, totalSize, rawTotalSize, readMsgCount, updateMsgCount);
    if (readMsgCount != 0) {
        collector.fieldFilterReadMsgCountPerReadRequest = readMsgCount;
        collector.fieldFilterUpdatedMsgRatio = 100 * updateMsgCount / (double)readMsgCount;
        collector.fieldFilteredMsgSize = totalSize / readMsgCount;
        collector.rawFieldFilteredMsgSize = rawTotalSize / readMsgCount;
    }
    if (rawTotalSize != 0) {
        double ratio = totalSize / (double)rawTotalSize;
        collector.fieldFilteredMsgRatio = ratio * 100;
        int64_t filterSize = rawTotalSize - totalSize;
        collector.fieldFilterMsgsSizePerReadRequest = filterSize;
    }
    return ec;
}

ErrorCode MessageBrain::doGetMessage(const ConsumptionRequest *request,
                                     MessageResponse *response,
                                     TimeoutChecker *timeoutChecker,
                                     ReaderInfoPtr &readerInfo,
                                     ReadMetricsCollector &collector) {
    if (messageIdValidInMemory(request)) {
        ErrorCode ec = _messageGroup->getMessage(request, !_fsMessageReader, response, readerInfo, collector);
        if (ec == ERROR_NONE) {
            return ERROR_NONE;
        }
    }
    if (!_permissionCenter->incReadFileCount()) {
        AUTIL_LOG(WARN,
                  "read message from partition [%s] exceed read file limit [%d],",
                  _partitionId.ShortDebugString().c_str(),
                  _permissionCenter->getReadFileLimit());
        return ERROR_BROKER_BUSY;
    }
    ScopedReadFilePermission readFilePermission(_permissionCenter);
    unique_ptr<ConsumptionRequest> newRequest;
    ErrorCode ec = ERROR_NONE;
    if (!messageIdValid(request, timeoutChecker, &collector)) {
        AUTIL_MASSIVE_LOG(
            WARN, "validate msgId for request[%s] failed, need seek", request->ShortDebugString().c_str());
        assert(request->has_starttimestamp());
        MessageIdResponse msgIdResponse;
        ec = getMinMessageIdByTime(request->starttimestamp(), &msgIdResponse, timeoutChecker, false);
        int64_t msgId = msgIdResponse.msgid();
        if (ec != ERROR_NONE) {
            if (ec == ERROR_BROKER_NO_DATA) { // memory topic or all date remove
                response->set_nextmsgid(0);
                response->set_nexttimestamp(_messageGroup->getLastAvailableMsgTimestamp());
                AUTIL_LOG(WARN,
                          "partition[%s] no data, but request start[%ld], timestamp[%ld]",
                          _partitionId.ShortDebugString().c_str(),
                          request->startid(),
                          request->starttimestamp());
                return ec;
            } else if (ec == ERROR_BROKER_TIMESTAMP_TOO_LATEST) {
                msgId++;
            } else {
                AUTIL_MASSIVE_LOG(ERROR,
                                  "[%s] getMinMessageIdByTime[%ld] failed, ec [%d]",
                                  _partitionId.ShortDebugString().c_str(),
                                  request->starttimestamp(),
                                  ec);
                return ec;
            }
        }
        newRequest.reset(new ConsumptionRequest(*request));
        newRequest->set_startid(msgId);
        request = newRequest.get(); // request has changed
    }
    ec = _messageGroup->getMessage(request, !_fsMessageReader, response, readerInfo, collector);
    if (ERROR_BROKER_NO_DATA_IN_MEM == ec) {
        bool isAppending = false;
        ReaderInfoMap readerInfoMap;
        {
            ScopedReadWriteLock lock(_readerRW, 'r');
            readerInfoMap = _readerInfoMap;
        }
        ec = _fsMessageReader->getMessage(
            request, response, timeoutChecker, isAppending, readerInfo.get(), &readerInfoMap, &collector);
        if (isAppending) {
            AUTIL_MASSIVE_LOG(WARN, "[%s] set commit file is readed flag", _partitionId.ShortDebugString().c_str());
            _messageCommitter->setCommitFileIsReaded();
        }
    }
    return ec;
}

ErrorCode MessageBrain::checkProductionRequest(const ProductionRequest *request,
                                               int64_t &totalSize,
                                               int64_t &msgCount,
                                               int64_t &msgCountWithMerged,
                                               WriteMetricsCollector &collector) {
    totalSize = 0;
    ErrorCode ec = ERROR_NONE;
    FieldGroupReader fieldGroupReader;
    if (_needFieldFilter && _checkFieldFilterMsg) {
        TimeTrigger decompressTimeTrigger;
        decompressTimeTrigger.beginTrigger();
        float ratio;
        ec = MessageCompressor::decompressProductionMessage(const_cast<ProductionRequest *>(request), ratio);
        decompressTimeTrigger.endTrigger();
        collector.writeMsgDecompressedRatio = ratio * 100;
        collector.writeMsgDecompressedLatency = decompressTimeTrigger.getLatency();
        if (ec != ERROR_NONE) {
            return ec;
        }
    }
    if (request->messageformat() == MF_PB) {
        msgCount = request->msgs_size();
        msgCountWithMerged = msgCount;
        for (int i = 0; i < request->msgs_size(); ++i) {
            const protocol::Message &msg = request->msgs(i);
            const string &data = msg.data();
            int64_t msgSize = data.size();
            if (msgSize > _maxMessageSize) {
                ec = ERROR_BROKER_MSG_LENGTH_EXCEEDED;
                break;
            }
            if (_checkFieldFilterMsg && _needFieldFilter && !msg.merged()) {
                if (!fieldGroupReader.fromProductionString(data)) {
                    ec = ERROR_BROKER_MESSAGE_FORMAT_INVALID;
                    break;
                }
            }
            if (msg.merged()) {
                msgCountWithMerged += *(const uint16_t *)msg.data().c_str();
                msgCountWithMerged -= 1;
            }
            totalSize += msgSize;
        }
    } else if (request->messageformat() == MF_FB) {
        FBMessageReader reader;
        if (!reader.init(request->fbmsgs(), true)) {
            return ERROR_BROKER_MESSAGE_FORMAT_INVALID;
        }
        msgCount = reader.size();
        msgCountWithMerged = msgCount;
        for (int64_t i = 0; i < msgCount; i++) {
            const protocol::flat::Message *fbMsg = reader.read(i);
            if (fbMsg == NULL || fbMsg->data() == NULL) {
                return ERROR_BROKER_MESSAGE_FORMAT_INVALID;
            }
            int64_t msgSize = fbMsg->data()->size();
            if (msgSize > _maxMessageSize) {
                ec = ERROR_BROKER_MSG_LENGTH_EXCEEDED;
                break;
            }
            if (_checkFieldFilterMsg && _needFieldFilter && !fbMsg->merged()) {
                if (!fieldGroupReader.fromProductionString(fbMsg->data()->c_str(), fbMsg->data()->size())) {
                    ec = ERROR_BROKER_MESSAGE_FORMAT_INVALID;
                    break;
                }
            }
            totalSize += msgSize;
            if (fbMsg->merged()) {
                msgCountWithMerged += *(const uint16_t *)fbMsg->data()->c_str();
                msgCountWithMerged -= 1;
            }
        }
    }
    return ec;
}

ErrorCode MessageBrain::addMessage(ProductionLogClosure *closure, WriteMetricsCollectorPtr &collector) {
    ++_metricStat.writeRequestNum;
    const ProductionRequest *request = closure->_request;
    MessageResponse *response = closure->_response;
    if (_enableFastRecover && _messageCommitter && _messageCommitter->hasSealError()) {
        AUTIL_LOG(
            ERROR, "%s-%d write dfs error, return part not found", _partitionId.topicname().c_str(), _partitionId.id());
        doneRunForProductRequest(closure, collector, ERROR_BROKER_TOPIC_PARTITION_NOT_FOUND);
        return ERROR_BROKER_TOPIC_PARTITION_NOT_FOUND;
    }
    if (!_permissionCenter->incWriteCount()) {
        AUTIL_LOG(WARN,
                  "write msg to partition[%s] exceed write limit[%d]",
                  _partitionId.ShortDebugString().c_str(),
                  _permissionCenter->getWriteLimit());
        doneRunForProductRequest(closure, collector, ERROR_BROKER_BUSY);
        return ERROR_BROKER_BUSY;
    }
    ScopedWritePermission writePermission(_permissionCenter);
    int64_t totalMsgSize = 0;
    int64_t msgCount = 0;
    int64_t msgCountWithMerged = 0;
    ErrorCode errorCode = checkProductionRequest(request, totalMsgSize, msgCount, msgCountWithMerged, *collector);
    collector->msgsSizePerWriteRequest = totalMsgSize;
    collector->msgsCountPerWriteRequest = msgCount;
    collector->msgsCountWithMergedPerWriteRequest = msgCountWithMerged;
    if (errorCode != ERROR_NONE) {
        collector->acceptedMsgsCountPerWriteRequest = 0;
        collector->acceptedMsgsSizePerWriteRequest = 0;
        ++collector->deniedNumOfWriteRequestWithError;
        response->set_maxallowmsglen(_maxMessageSize);
        doneRunForProductRequest(closure, collector, errorCode);
        return errorCode;
    }
    if (_compressMsg || request->compressmsginbroker()) {
        TimeTrigger compressTimeTrigger;
        compressTimeTrigger.beginTrigger();
        float ratio = 1.0;
        MessageCompressor::compressProductionMessage(const_cast<ProductionRequest *>(request), _compressThres, ratio);
        compressTimeTrigger.endTrigger();
        collector->msgCompressedRatio = ratio * 100;
        collector->msgCompressedLatency = compressTimeTrigger.getLatency();
    }

    bool replicationMode = request->has_replicationmode() && request->replicationmode();
    if (!replicationMode && TOPIC_MODE_SECURITY == _topicMode) {
        WriteRequestItem *requestItem = new WriteRequestItem;
        requestItem->closure = closure;
        requestItem->msgCount = msgCount;
        requestItem->dataSize = totalMsgSize;
        requestItem->collector = collector;
        if (!_writeRequestItemQueue.pushRequestItem(requestItem)) {
            errorCode = ERROR_BROKER_BUSY;
            doneRunForProductRequest(closure, collector, errorCode);
            delete requestItem;
        } else if (checkSecurityModeCommitCondition()) {
            autil::ScopedLock lock(_writeRequestCond);
            _writeRequestCond.signal();
        }
        return errorCode;
    }

    // TOPIC_MODE_NORMAL == _topicMode
    errorCode = doAddMessage(request, response, msgCount, totalMsgSize, collector);
    if (replicationMode) {
        commitAllMessage();
    }
    doneRunForProductRequest(closure, collector, errorCode);
    _metricStat.writeSize += totalMsgSize;
    return errorCode;
}

void MessageBrain::doneRunForProductRequest(ProductionLogClosure *closure,
                                            WriteMetricsCollectorPtr &collector,
                                            ErrorCode ec) {
    closure->_response->set_sessionid(_sessionId);
    closure->_response->set_committedid(getCommittedMsgId());
    if (!closure->_response->has_acceptedmsgcount()) {
        closure->_response->set_acceptedmsgcount(0);
    }
    closure->_response->set_maxmsgid(_messageGroup->getLastAvailableMsgId());
    int64_t currTime = autil::TimeUtility::currentTime();
    if (collector && _metricsReporter) {
        collector->writeRequestLatency = (currTime - closure->_beginTime) / 1000.0;
        if (ERROR_BROKER_BUSY == ec) {
            collector->writeBusyError = true;
        } else if (ERROR_NONE != ec) {
            collector->otherWriteError = true;
        }
        _metricsReporter->reportWriteMetricsBackupThread(collector,
                                                         collector->accessId.empty() ? _metricsTags : nullptr);
    }
    ErrorInfo *errorInfo = closure->_response->mutable_errorinfo();
    setBrokerResponseError(errorInfo, ec);
    closure->_response->set_donetimestamp(currTime);
    closure->Run();
}

ErrorCode MessageBrain::doAddMessage(const ProductionRequest *request,
                                     MessageResponse *response,
                                     int64_t msgCount,
                                     int64_t totalMsgSize,
                                     WriteMetricsCollectorPtr &collector) {
    ScopedLock lock(_addMsgLock);
    size_t needBlockCount = calNeedBlockCount(msgCount, totalMsgSize);
    if (needBlockCount > 0) {
        recycleBuffer(needBlockCount);
    }
    return _messageGroup->addMessage(request, response, *collector);
}

ErrorCode MessageBrain::getMaxMessageId(const MessageIdRequest *request, MessageIdResponse *response) {
    response->set_sessionid(_sessionId);
    if (request->has_sessionid()) {
        int64_t sessionId = request->sessionid();
        if (sessionId != -1 && sessionId != _sessionId) {
            // drop this request
            return ERROR_BROKER_SESSION_CHANGED;
        }
    }
    if (_enableFastRecover && _messageCommitter && _messageCommitter->hasSealError()) {
        AUTIL_LOG(
            ERROR, "%s-%d write dfs error, return part not found", _partitionId.topicname().c_str(), _partitionId.id());
        return ERROR_BROKER_TOPIC_PARTITION_NOT_FOUND;
    }
    if (!_permissionCenter->incGetMaxIdCount()) {
        AUTIL_LOG(WARN,
                  "get max msgid from partition [%s] exceed read limit [%d],"
                  " current get maxid threadcount [%d].",
                  _partitionId.ShortDebugString().c_str(),
                  _permissionCenter->getReadLimit(),
                  _permissionCenter->getCurMaxIdCount());
        return ERROR_BROKER_BUSY;
    }
    ScopedMaxIdPermission maxIdPermission(_permissionCenter);
    return _messageGroup->getMaxMessageId(response);
}

ErrorCode MessageBrain::getMinMessageIdByTime(const MessageIdRequest *request,
                                              MessageIdResponse *response,
                                              TimeoutChecker *timeoutChecker) {
    response->set_sessionid(_sessionId);
    if (request->has_sessionid()) {
        int64_t sessionId = request->sessionid();
        if (sessionId != -1 && sessionId != _sessionId) {
            // drop this request
            return ERROR_BROKER_SESSION_CHANGED;
        }
    }
    if (_enableFastRecover && _messageCommitter && _messageCommitter->hasSealError()) {
        AUTIL_LOG(
            ERROR, "%s-%d write dfs error, return part not found", _partitionId.topicname().c_str(), _partitionId.id());
        return ERROR_BROKER_TOPIC_PARTITION_NOT_FOUND;
    }
    int64_t timestamp = request->timestamp();
    return getMinMessageIdByTime(timestamp, response, timeoutChecker, true);
}

ErrorCode MessageBrain::getMinMessageIdByTime(int64_t timestamp,
                                              MessageIdResponse *response,
                                              TimeoutChecker *timeoutChecker,
                                              bool applyFileRead) {
    if (!_permissionCenter->incGetMinIdCount()) {
        AUTIL_LOG(WARN,
                  "get min msgid from partition [%s] exceed read limit [%d],"
                  " current get minId thread count [%d].",
                  _partitionId.ShortDebugString().c_str(),
                  _permissionCenter->getReadLimit(),
                  _permissionCenter->getCurMinIdCount());
        return ERROR_BROKER_BUSY;
    }
    ScopedMinIdPermission minIdPermission(_permissionCenter);
    bool isMinInMemory = false;
    ErrorCode errorCode = _messageGroup->getMinMessageIdByTime(timestamp, response, isMinInMemory);
    if (errorCode != ERROR_NONE) {
        return errorCode;
    }

    int64_t memoryTime = response->timestamp();
    if (isMinInMemory && memoryTime > timestamp && _fsMessageReader) {
        if (applyFileRead) {
            if (!_permissionCenter->incReadFileCount()) {
                AUTIL_MASSIVE_LOG(WARN,
                                  "get min msgid from partition [%s] exceed read file limit [%d],",
                                  _partitionId.ShortDebugString().c_str(),
                                  _permissionCenter->getReadFileLimit());
                return ERROR_BROKER_BUSY;
            }
            ScopedReadFilePermission readFilePermission(_permissionCenter);
            errorCode = _fsMessageReader->getMinMessageIdByTime(timestamp, response, timeoutChecker);
        } else {
            errorCode = _fsMessageReader->getMinMessageIdByTime(timestamp, response, timeoutChecker);
        }
    }
    return errorCode;
}

void MessageBrain::getPartitionMetrics(monitor::PartitionMetricsCollector &collector) const {
    collector.topicName = _partitionId.topicname();
    collector.partId = _partitionId.id();
    collector.partitionStatus = 1;
    collector.actualDataSize = _messageGroup->getDataSize();
    collector.actualMetaSize = _messageGroup->getMetaSize();
    int64_t blockSize = _partitionBlockPool->getBlockSize();
    collector.usedBufferSize = _partitionBlockPool->getUsedBlockCount() * blockSize;
    collector.unusedMaxBufferSize = _partitionBlockPool->getMaxUnusedBlockCount() * blockSize;
    collector.reserveFreeBufferSize = _partitionBlockPool->getUnusedBlockCount() * blockSize;
    collector.leftToBeCommittedDataSize = _messageGroup->getLeftToBeCommittedDataSize();
    collector.lastAcceptedMsgTimeStamp = _messageGroup->getLastAcceptedMsgTimeStamp();
    collector.lastReadMsgTimeStamp = _lastReadMsgTimeStamp;
    int64_t firstTimestamp = _messageGroup->getRecycleInfo().firstMsgTimestamp;
    int64_t curTime = autil::TimeUtility::currentTime();
    collector.oldestMsgTimeStampInBuffer = curTime - firstTimestamp;
    collector.commitDelay = _messageGroup->getCommitDelay();
    collector.maxMsgId = _messageGroup->getLastReceivedMsgId();
    collector.msgCountInBuffer = _messageGroup->getMsgCountInBuffer();
    collector.topicMode = _topicMode;
    collector.writeRequestQueueSize = _writeRequestItemQueue.getQueueSize();
    collector.writeRequestQueueDataSize = _writeRequestItemQueue.getQueueDataSize();
    if (_messageCommitter) {
        collector.writeDataFileCount = _messageCommitter->getDataFile() == nullptr ? 0 : 1;
        collector.writeMetaFileCount = _messageCommitter->getMetaFile() == nullptr ? 0 : 1;
        collector.writeFailMetaFileCount = _messageCommitter->getWriteFailMetaCount();
        collector.writeFailDataFileCount = _messageCommitter->getWriteFailDataCount();
        collector.writeBufferSize = _messageCommitter->getWriteBufferSize();
    }
    if (_fsMessageReader) {
        collector.cacheFileCount = _fsMessageReader->getCacheFileCount();
        collector.cacheMetaBlockCount = _fsMessageReader->getCacheMetaBlockCount();
        collector.cacheDataBlockCount = _fsMessageReader->getCacheDataBlockCount();
    }
    if (_fileManager && _fileManager->isUsedDfsSizeSynced()) {
        collector.usedDfsSize = _fileManager->getUsedDfsSize();
    }
}

void MessageBrain::getPartitionInMetric(uint32_t interval, protocol::PartitionInMetric *metric) {
    metric->set_topicname(_partitionId.topicname());
    metric->set_partid(_partitionId.id());
    metric->set_lastwritetime(_messageGroup->getLastAcceptedMsgTimeStamp() / 1000000);
    metric->set_lastreadtime(_lastReadTime / 1000000);
    if (NULL == _innerPartMetric) {
        _innerPartMetric = new InnerPartMetric(interval);
    }
    _innerPartMetric->update(_metricStat);
    InnerPartMetricStat stat1min, stat5min;
    _innerPartMetric->getMetric1min(stat1min);
    _innerPartMetric->getMetric5min(stat5min);
    metric->set_writerate1min(stat1min.writeSize);
    metric->set_writerate5min(stat5min.writeSize / 5);
    metric->set_readrate1min(stat1min.readSize);
    metric->set_readrate5min(stat5min.readSize / 5);
    metric->set_writerequest1min(stat1min.writeRequestNum);
    metric->set_writerequest5min(stat5min.writeRequestNum / 5);
    metric->set_readrequest1min(stat1min.readRequestNum);
    metric->set_readrequest5min(stat5min.readRequestNum / 5);
    int64_t dfsCommitDelay = -1;
    if (_fileManager) {
        dfsCommitDelay = _messageGroup->getCommitDelay();
    }
    if (-1 != dfsCommitDelay) {
        metric->set_commitdelay(dfsCommitDelay);
    }
}

protocol::TopicMode MessageBrain::getTopicMode() { return _topicMode; }

bool MessageBrain::messageIdValidInMemory(const ConsumptionRequest *request) {
    if (!request->has_startid()) {
        return false;
    }
    if (!request->has_starttimestamp()) {
        return true;
    }
    bool valid = false;
    int64_t msgId = request->startid();
    int64_t msgTime = request->starttimestamp();

    if (msgId == 0) {
        return true;
    }
    if (_messageGroup->messageIdValid(msgId, msgTime, valid)) {
        return valid;
    }
    return false;
}

bool MessageBrain::messageIdValid(const ConsumptionRequest *request,
                                  TimeoutChecker *timeoutChecker,
                                  ReadMetricsCollector *collector) {
    if (!request->has_startid()) {
        return false;
    }
    if (!request->has_starttimestamp()) {
        return true;
    }
    bool valid = false;
    int64_t msgId = request->startid();
    int64_t msgTime = request->starttimestamp();

    if (msgId == 0) {
        return true;
    }
    if (_messageGroup->messageIdValid(msgId, msgTime, valid)) {
        return valid;
    }
    if (!_fsMessageReader) {
        return false;
    }
    return _fsMessageReader->messageIdValid(msgId, msgTime, timeoutChecker, collector);
}

void MessageBrain::setForceUnload(bool flag) { _forceUnload = flag; }

int64_t MessageBrain::getFileCacheBlockCount() {
    if (_fsMessageReader) {
        return _fsMessageReader->getCacheBlockCount();
    } else {
        return 0;
    }
}

void MessageBrain::updateReaderInfo(ErrorCode ec,
                                    const ConsumptionRequest *request,
                                    const string *srcIpPort,
                                    const MessageResponse *response,
                                    ReaderInfoPtr &info) {
    if (ec != ERROR_NONE && ec != ERROR_BROKER_BUSY) {
        return;
    }
    if (NULL == srcIpPort || srcIpPort->empty()) {
        return;
    }
    const ReaderDes &reader = getReaderDescription(request, *srcIpPort);
    info->nextTimestamp = response->nexttimestamp();
    info->nextMsgId = response->nextmsgid();

    ScopedReadWriteLock lock(_readerRW, 'w');
    _readerInfoMap[reader] = info;
}

ReaderDes MessageBrain::getReaderDescription(const ConsumptionRequest *request, const string &srcIpPort) {
    ReaderDes reader;
    reader.setIpPort(srcIpPort);
    reader.from = request->filter().from();
    reader.to = request->filter().to();
    return reader;
}

void MessageBrain::recycleFile() {
    if (_fsMessageReader) {
        _fsMessageReader->recycleFile();
    }
}

void MessageBrain::recycleObsoleteReader() {
    // clear obsolete reader
    int64_t currentTime = autil::TimeUtility::currentTime();
    int64_t expireTime = currentTime - _obsoleteReaderInterval;
    string removeInfo;
    {
        ScopedReadWriteLock lock(_readerRW, 'w');
        auto iter = _readerInfoMap.begin();
        while (iter != _readerInfoMap.end()) {
            if (iter->second->requestTime < expireTime) {
                const string &readerDesStr = (iter->first).toString();
                removeInfo += readerDesStr;
                iter = _readerInfoMap.erase(iter);
            } else {
                iter++;
            }
        }
    }
    { // clear metrics history data
        ScopedReadWriteLock lock(_readerRW, 'w');
        expireTime = currentTime - _obsoleteReaderMetricInterval;
        auto iter = _readerInfoMap.begin();
        while (iter != _readerInfoMap.end()) {
            iter->second->metaInfo->clearHistoryData(expireTime);
            iter->second->dataInfo->clearHistoryData(expireTime);
            iter++;
        }
    }
    if (!removeInfo.empty()) {
        AUTIL_LOG(
            INFO, "erase expire readers[%s], now:%ld, expire time:%ld", removeInfo.c_str(), currentTime, expireTime);
    }
}

bool MessageBrain::getFileBlockDis(vector<int64_t> &metaDisBlocks, vector<int64_t> &dataDisBlocks) {
    if (_fsMessageReader) {
        ReaderInfoMap readerInfoMap;
        {
            ScopedReadWriteLock lock(_readerRW, 'r');
            readerInfoMap = _readerInfoMap;
        }
        return _fsMessageReader->getFileBlockDis(&readerInfoMap, metaDisBlocks, dataDisBlocks);
    } else {
        return false;
    }
}

bool MessageBrain::hasSealError() {
    if (_messageCommitter) {
        return _messageCommitter->hasSealError();
    } else {
        return false;
    }
}

} // namespace storage
} // namespace swift
