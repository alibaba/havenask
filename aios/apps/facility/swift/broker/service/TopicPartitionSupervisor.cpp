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
#include "swift/broker/service/TopicPartitionSupervisor.h"

#include <algorithm>
#include <assert.h>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <unistd.h>

#include "autil/CommonMacros.h"
#include "autil/TimeUtility.h"
#include "ext/alloc_traits.h"
#include "fslib/fs/FileSystem.h"
#include "swift/broker/service/CommitWorkItem.h"
#include "swift/broker/service/LoadWorkItem.h"
#include "swift/broker/service/UnloadWorkItem.h"
#include "swift/broker/storage/MessageGroup.h"
#include "swift/config/BrokerConfig.h"
#include "swift/heartbeat/ZkConnectionStatus.h"
#include "swift/monitor/BrokerMetricsReporter.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"
#include "swift/protocol/Heartbeat.pb.h"
#include "swift/protocol/MessageComparator.h" // IWYU pragma: keep
#include "swift/util/Block.h"
#include "swift/util/BlockPool.h"
#include "swift/util/PermissionCenter.h"
#include "swift/util/ProtoUtil.h"

using namespace fslib::cache;
using namespace std;
using namespace autil;
using namespace swift::protocol;
using namespace swift::storage;
using namespace swift::util;
using namespace swift::monitor;
using namespace swift::heartbeat;

namespace swift {
namespace service {
AUTIL_LOG_SETUP(swift, TopicPartitionSupervisor);
static const int32_t DEL_FILE_PARTITION_BATCH_SIZE = 10;              // del 10 partition for each
static const int32_t SYNC_DFS_SIZE_PARTITION_BATCH_SIZE = 5;          // sync 5 partition for each
static const int64_t SYNC_DFS_SIZE_THREAD_INTEVAL = 60 * 1000 * 1000; // 1min
const float FILE_CACHE_BLOCK_RESERVED_PERCENT = 0.01;

TopicPartitionSupervisor::TopicPartitionSupervisor(config::BrokerConfig *brokerConfig,
                                                   ZkConnectionStatus *status,
                                                   BrokerMetricsReporter *metricsReporter)
    : _brokerConfig(brokerConfig)
    , _status(status)
    , _metricsReporter(metricsReporter)
    , _centerBlockPool(nullptr)
    , _fileCachePool(nullptr)
    , _permissionCenter(nullptr)
    , _isStop(true)
    , _delCount(0)
    , _syncDfsCount(0)
    , _firstLoad(true)
    , _zkDataAccessor(nullptr) {}

TopicPartitionSupervisor::~TopicPartitionSupervisor() {
    stop();
    _status = NULL;
    DELETE_AND_SET_NULL(_fileCachePool);
    DELETE_AND_SET_NULL(_centerBlockPool);
    DELETE_AND_SET_NULL(_permissionCenter);
}

void TopicPartitionSupervisor::setChannelManager(
    const std::shared_ptr<network::SwiftRpcChannelManager> &channelManager) {
    _channelManager = channelManager;
}

void TopicPartitionSupervisor::start(util::ZkDataAccessorPtr zkDataAccessor) {
    std::random_device rd;
    _random.seed(rd());
    _uniform = std::make_unique<std::uniform_int_distribution<int32_t>>(1, SYNC_DFS_SIZE_THREAD_INTEVAL);

    _zkDataAccessor = zkDataAccessor;
    int64_t bufferBlockSize = _brokerConfig->getBufferBlockSize();
    int64_t totalBufferSize = _brokerConfig->getTotalBufferSize();
    _centerBlockPool = new BlockPool(totalBufferSize, bufferBlockSize);
    _permissionCenter = new PermissionCenter(_brokerConfig->concurrentReadFileLimit,
                                             _brokerConfig->maxPermissionWaitTime,
                                             _brokerConfig->maxReadThreadNum,
                                             _brokerConfig->maxWriteThreadNum);
    int64_t fileCacheSize = _brokerConfig->getTotalFileBufferSize();
    if (fileCacheSize < bufferBlockSize) {
        fileCacheSize = bufferBlockSize;
    }
    AUTIL_LOG(INFO,
              "init file cache, size [%ld], one file fd num [%d], "
              "cache file reserve time [%ld], cache block reserve time [%ld]",
              fileCacheSize,
              _brokerConfig->getOneFileFdNum(),
              _brokerConfig->getCacheFileReserveTime(),
              _brokerConfig->getCacheBlockReserveTime());

    _fileCachePool = new BlockPool(_centerBlockPool, fileCacheSize, fileCacheSize / 32);
    int64_t reserveCount = _fileCachePool->getMaxBlockCount() * FILE_CACHE_BLOCK_RESERVED_PERCENT;
    _fileCachePool->setReserveBlockCount(reserveCount);
    _recycleBufferThread = autil::LoopThread::createLoopThread(
        std::bind(&TopicPartitionSupervisor::recycleBuffer, this), 200 * 1000ll, "recycle_buffer");
    _commitMessageThread =
        autil::LoopThread::createLoopThread(std::bind(&TopicPartitionSupervisor::commitMessage, this),
                                            _brokerConfig->commitThreadLoopIntervalMs * 1000ll,
                                            "commit_message");
    _delExpiredFileThread = autil::LoopThread::createLoopThread(
        std::bind(&TopicPartitionSupervisor::delExpiredFile, this), 1000 * 1000ll, "del_file");
    _commitThreadPool.reset(
        new autil::ThreadPool(_brokerConfig->dfsCommitThreadNum, _brokerConfig->dfsCommitQueueSize));
    _commitThreadPool->start("commit_msg");
    _loadThreadPool.reset(new autil::ThreadPool(_brokerConfig->loadThreadNum, _brokerConfig->loadQueueSize));
    _unloadThreadPool.reset(new autil::ThreadPool(_brokerConfig->unloadThreadNum, _brokerConfig->unloadQueueSize));

    AUTIL_LOG(INFO,
              "topic partition superviser started. center pool size[%ld], "
              "data file pool size[%ld], block size[%ld]",
              totalBufferSize,
              fileCacheSize,
              bufferBlockSize);
    _isStop = false;
    // create after _isStop changed to false
    _syncDfsUsedSizeThread =
        autil::Thread::createThread(std::bind(&TopicPartitionSupervisor::syncDfsUsedSize, this), "sync_dfs_size");
}

void TopicPartitionSupervisor::loadBrokerPartition(const vector<TaskInfo> &toLoad) {
    if (toLoad.empty()) {
        return;
    }
    if (_loadThreadPool == NULL || _loadThreadPool->getThreadNum() <= 1) {
        for (vector<TaskInfo>::const_iterator it = toLoad.begin(); it != toLoad.end(); ++it) {
            protocol::ErrorCode ec = loadBrokerPartition(*it);
            if (ec != protocol::ERROR_NONE) {
                AUTIL_LOG(ERROR, "failed to load partition[%s].", it->ShortDebugString().c_str());
            }
        }
        return;
    }
    string threadName = _firstLoad ? "io_pangu" : "load_parts";
    _firstLoad = false;
    if (!_loadThreadPool->start(threadName)) {
        AUTIL_LOG(WARN, "start load thread pool failed.");
        return;
    }
    for (vector<TaskInfo>::const_iterator it = toLoad.begin(); it != toLoad.end(); ++it) {
        LoadWorkItem *item = new LoadWorkItem(this, *it);
        if (ThreadPool::ERROR_NONE != _loadThreadPool->pushWorkItem(item)) {
            AUTIL_LOG(WARN, "add load work item failed [%s].", it->ShortDebugString().c_str());
            delete item;
        }
    }
    _loadThreadPool->stop(); // wait all partition loaded
}

ErrorCode TopicPartitionSupervisor::loadBrokerPartition(const TaskInfo &taskInfo) {
    int64_t begTime = TimeUtility::currentTime();

    BrokerPartitionPtr brokerPartPtr = getBrokerPartition(taskInfo.partitionid());
    if (brokerPartPtr) {
        AUTIL_LOG(WARN, "Load partition failed, partition[%s] exist.", taskInfo.ShortDebugString().c_str());
        return ERROR_BROKER_REPEATED_ADD_TOPIC_PARTITION;
    }
    AUTIL_LOG(INFO, "begin load partition[%s]", taskInfo.ShortDebugString().c_str());

    ThreadSafeTaskStatusPtr threadSafeTaskInfo(new ThreadSafeTaskStatus(taskInfo));
    brokerPartPtr.reset(new BrokerPartition(threadSafeTaskInfo, _metricsReporter, _zkDataAccessor));
    // addMetrics should be called before setBrokerPartition,
    // otherwise may lead to thread safe problem
    // setBrokerPartition to make heartBeat reported to admin
    setBrokerPartition(taskInfo.partitionid(), brokerPartPtr);
    if (!brokerPartPtr->init(*_brokerConfig, _centerBlockPool, _fileCachePool, _permissionCenter, _channelManager)) {
        AUTIL_LOG(ERROR, "init broker partition[%s] failed", taskInfo.ShortDebugString().c_str());
        brokerPartPtr.reset();
        unLoadBrokerPartition(taskInfo.partitionid());
        return ERROR_BROKER_FAIL;
    }
    int64_t useTime = TimeUtility::currentTime() - begTime;
    if (_metricsReporter) {
        _metricsReporter->reportWorkerLoadPartitionLatency(useTime);
        _metricsReporter->reportWorkerLoadPartitionQps();
    }
    AUTIL_LOG(INFO, "finish load partition[%s], use time [%ld]", ProtoUtil::getTaskInfoStr(taskInfo).c_str(), useTime);
    return ERROR_NONE;
}

void TopicPartitionSupervisor::unLoadBrokerPartition(const vector<PartitionId> &toUnLoad) {
    ScopedLock lock(_unloadMutex);
    if (_unloadThreadPool == NULL || _unloadThreadPool->getThreadNum() <= 1) {
        for (vector<PartitionId>::const_iterator it = toUnLoad.begin(); it != toUnLoad.end(); ++it) {
            protocol::ErrorCode ec = unLoadBrokerPartition(*it);
            if (ec != protocol::ERROR_NONE) {
                AUTIL_LOG(ERROR, "failed to unload partition[%s].", it->ShortDebugString().c_str());
            }
        }
        return;
    }
    if (!_unloadThreadPool->start("unload_parts")) {
        AUTIL_LOG(WARN, "start unload thread pool failed.");
        return;
    }
    for (vector<PartitionId>::const_iterator it = toUnLoad.begin(); it != toUnLoad.end(); ++it) {
        UnloadWorkItem *item = new UnloadWorkItem(this, *it);
        if (ThreadPool::ERROR_NONE != _unloadThreadPool->pushWorkItem(item)) {
            AUTIL_LOG(WARN, "add unload work item failed [%s].", it->ShortDebugString().c_str());
            delete item;
        }
    }
    _unloadThreadPool->stop(); // wait all partition loaded
}

ErrorCode TopicPartitionSupervisor::unLoadBrokerPartition(const PartitionId &partId) {
    AUTIL_LOG(INFO, "unload partition[%s]", partId.ShortDebugString().c_str());
    int64_t begTime = TimeUtility::currentTime();
    BrokerPartitionPtr brokerPartition;
    {
        autil::ScopedWriteLock lock(_brokerPartitionMapMutex);
        BrokerPartitionMap::iterator it = _brokerPartitionMap.find(partId);
        if (it == _brokerPartitionMap.end()) {
            AUTIL_LOG(WARN, "unload partition failed: partition[%s] not exist.", partId.ShortDebugString().c_str());
            return ERROR_BROKER_TOPIC_PARTITION_NOT_FOUND;
        }
        std::pair<ThreadSafeTaskStatusPtr, BrokerPartitionPtr> &item = it->second;
        ThreadSafeTaskStatusPtr taskInfo = item.first;
        brokerPartition = item.second;
        taskInfo->setPartitionStatus(PARTITION_STATUS_STOPPING);
        if (partId.has_forceunload() && partId.forceunload()) {
            item.second->setForceUnload(true);
        }
        item.second.reset(); // head beat thread still using taskStatus
    }
    // wait util all reference of broker partition had been released
    while (brokerPartition.use_count() > 1) {
        if (TimeUtility::currentTime() - begTime > 2 * 1000 * 1000) {
            AUTIL_LOG(WARN,
                      "wait partition[%s] unload too long, have used [%d] us,"
                      " current use count [%d]",
                      partId.ShortDebugString().c_str(),
                      int(TimeUtility::currentTime() - begTime),
                      (int)brokerPartition.use_count());
        } else {
            AUTIL_LOG(INFO,
                      "wait partition[%s] use count to 1, current use count [%d]",
                      partId.ShortDebugString().c_str(),
                      (int)brokerPartition.use_count());
        }
        usleep(100 * 1000);
    }
    AUTIL_LOG(INFO,
              "reset broker partition [%s], current use count [%u]",
              partId.ShortDebugString().c_str(),
              (uint32_t)brokerPartition.use_count());
    brokerPartition.reset();
    {
        AUTIL_LOG(INFO, "clear broker partition info [%s]", partId.ShortDebugString().c_str());
        autil::ScopedWriteLock lock(_brokerPartitionMapMutex);
        BrokerPartitionMap::iterator it = _brokerPartitionMap.find(partId);
        if (it == _brokerPartitionMap.end()) {
            AUTIL_LOG(WARN, "stop borker worker when unloading partition[%s].", partId.ShortDebugString().c_str());
            return ERROR_NONE;
        }
        std::pair<ThreadSafeTaskStatusPtr, BrokerPartitionPtr> &item = it->second;
        item.first.reset(); // release task info,
        _brokerPartitionMap.erase(it);
    }
    int64_t useTime = TimeUtility::currentTime() - begTime;
    if (_metricsReporter) {
        _metricsReporter->reportWorkerUnloadPartitionLatency(useTime);
        _metricsReporter->reportWorkerUnloadPartitionQps();
    }
    AUTIL_LOG(INFO, "unload partition[%s] succeed", partId.ShortDebugString().c_str());
    return ERROR_NONE;
}

void TopicPartitionSupervisor::getAllPartitions(std::vector<PartitionId> &partitionIds) {
    partitionIds.clear();
    autil::ScopedReadLock lock(_brokerPartitionMapMutex);
    partitionIds.reserve(_brokerPartitionMap.size());
    for (BrokerPartitionMap::iterator it = _brokerPartitionMap.begin(); it != _brokerPartitionMap.end(); ++it) {
        partitionIds.push_back(it->first);
    }
}

bool TopicPartitionSupervisor::needRejectServing() const {
    return _status->needRejectServing(autil::TimeUtility::currentTime());
}

void TopicPartitionSupervisor::getPartitionStatus(HeartbeatInfo &info) {
    info.set_role(ROLE_TYPE_BROKER);
    autil::ScopedReadLock lock(_brokerPartitionMapMutex);
    for (BrokerPartitionMap::iterator it = _brokerPartitionMap.begin(); it != _brokerPartitionMap.end(); ++it) {
        TaskStatus *task = info.add_tasks();
        const pair<ThreadSafeTaskStatusPtr, BrokerPartitionPtr> &item = it->second;
        *task = (item.first)->getTaskStatus();
    }
}

void TopicPartitionSupervisor::stop() {
    AUTIL_LOG(INFO, "stop TopicPartitionSupervisor");
    _isStop = true;
    _recycleBufferThread.reset();
    _delExpiredFileThread.reset();
    _syncNotifier.notify();
    if (_syncDfsUsedSizeThread) {
        _syncDfsUsedSizeThread->join();
        _syncDfsUsedSizeThread.reset();
    }
    _commitMessageThread.reset(); // reset before thread pool
    {
        autil::ScopedReadWriteLock lock(_commitThreadPoolMutex, 'w');
        _commitThreadPool.reset();
    }
    _loadThreadPool.reset(); // stop loading
    vector<PartitionId> allPartition;
    {
        autil::ScopedReadLock lock(_brokerPartitionMapMutex);
        for (BrokerPartitionMap::iterator it = _brokerPartitionMap.begin(); it != _brokerPartitionMap.end(); ++it) {
            allPartition.push_back(it->first);
        }
    }
    unLoadBrokerPartition(allPartition);
    _unloadThreadPool.reset();
    assert(_brokerPartitionMap.empty());
}

void TopicPartitionSupervisor::stopSecurityModeThread() {
    vector<PartitionId> partitionIds;
    getAllPartitions(partitionIds);
    for (const auto &partitionId : partitionIds) {
        BrokerPartitionPtr brokerPartition = getBrokerPartition(partitionId);
        if (brokerPartition) {
            brokerPartition->stopSecurityModeThread();
        }
    }
}

void TopicPartitionSupervisor::releaseLongPolling() {
    vector<PartitionId> partitionIds;
    getAllPartitions(partitionIds);
    for (const auto &partitionId : partitionIds) {
        BrokerPartitionPtr brokerPartition = getBrokerPartition(partitionId);
        if (!brokerPartition) {
            continue;
        }
        BrokerPartition::ReadRequestQueue pollingQueue;
        brokerPartition->stealAllPolling(pollingQueue);
    }
}

BrokerPartitionPtr TopicPartitionSupervisor::getBrokerPartition(const PartitionId &partitionId) {
    autil::ScopedReadLock lock(_brokerPartitionMapMutex);
    BrokerPartitionMap::iterator it = _brokerPartitionMap.find(partitionId);
    if (it == _brokerPartitionMap.end()) {
        return BrokerPartitionPtr();
    }
    const pair<ThreadSafeTaskStatusPtr, BrokerPartitionPtr> &item = it->second;
    return item.second;
}

void TopicPartitionSupervisor::setBrokerPartition(const PartitionId &partitionId,
                                                  const BrokerPartitionPtr &borkerPartition) {
    autil::ScopedWriteLock lock(_brokerPartitionMapMutex);
    _brokerPartitionMap[partitionId] = make_pair(borkerPartition->getTaskStatus(), borkerPartition);
}

void TopicPartitionSupervisor::amonReport() {
    if (!_metricsReporter) {
        return;
    }
    WorkerMetricsCollector collector;
    collector.fileCacheUsedSize = _fileCachePool->getUsedBlockCount() * _fileCachePool->getBlockSize();
    collector.fileCacheUsedRatio = 100 * _fileCachePool->getUsedBlockCount() * 1.0 / _fileCachePool->getMaxBlockCount();
    vector<BrokerPartitionPtr> allPartition;
    int64_t curTime = TimeUtility::currentTime();
    {
        autil::ScopedReadLock lock(_brokerPartitionMapMutex);
        BrokerPartitionMap::iterator it = _brokerPartitionMap.begin();
        for (; it != _brokerPartitionMap.end(); ++it) {
            BrokerPartitionPtr brokerPartitionPtr = it->second.second;
            if (brokerPartitionPtr && protocol::PARTITION_STATUS_RUNNING == brokerPartitionPtr->getPartitionStatus()) {
                allPartition.push_back(brokerPartitionPtr);
            }
        }
    }
    collector.loadedPartitionCount = allPartition.size();
    for (size_t i = 0; i < allPartition.size(); i++) {
        PartitionMetricsCollector partMetricCollector;
        allPartition[i]->collectMetrics(partMetricCollector);
        collector.partMetricsCollectors.push_back(partMetricCollector);
    }
    collector.messageTotalUsedBufferSize = _centerBlockPool->getUsedBlockCount() * _centerBlockPool->getBlockSize();
    collector.messageTotalUnusedBufferSize =
        _centerBlockPool->getMaxUnusedBlockCount() * _centerBlockPool->getBlockSize();
    int64_t rejectTime = 0;
    if (!_status->rejectStatus(curTime, rejectTime)) {
        collector.serviceStatus = 1;
    } else {
        collector.serviceStatus = 0;
    }
    collector.rejectTime = rejectTime;
    {
        autil::ScopedReadWriteLock lock(_commitThreadPoolMutex, 'r');
        if (_commitThreadPool != NULL) {
            collector.commitMessageQueueSize = _commitThreadPool->getItemCount();
            collector.commitMessageActiveThreadNum = _commitThreadPool->getActiveThreadNum();
        }
    }
    if (_permissionCenter != NULL) {
        collector.readFileUsedThreadCount = _permissionCenter->getCurReadFileCount();
        collector.actualReadingFileThreadCount = _permissionCenter->getActualReadFileThreadCount();
        collector.actualReadingFileCount = _permissionCenter->getActualReadFileCount();
        collector.writeUsedThreadCount = _permissionCenter->getCurWriteCount();
        collector.maxMsgIdUsedThreadCount = _permissionCenter->getCurMaxIdCount();
        collector.minMsgIdUsedThreadCount = _permissionCenter->getCurMinIdCount();
    }
    _metricsReporter->reportWorkerMetrics(collector);
}

// every 10 seconds call once
void TopicPartitionSupervisor::getPartitionInMetrics(uint32_t interval,
                                                     google::protobuf::RepeatedPtrField<PartitionInMetric> *partMetrics,
                                                     int64_t &rejectTime) {
    vector<BrokerPartitionPtr> allPartition;
    {
        autil::ScopedReadLock lock(_brokerPartitionMapMutex);
        BrokerPartitionMap::iterator it = _brokerPartitionMap.begin();
        for (; it != _brokerPartitionMap.end(); ++it) {
            BrokerPartitionPtr brokerPartitionPtr = it->second.second;
            if (brokerPartitionPtr && PARTITION_STATUS_RUNNING == brokerPartitionPtr->getPartitionStatus()) {
                allPartition.push_back(brokerPartitionPtr);
            }
        }
    }
    for (size_t i = 0; i < allPartition.size(); i++) {
        PartitionInMetric *metric = partMetrics->Add();
        allPartition[i]->collectInMetric(interval, metric);
    }
    if (!_status->rejectStatus(TimeUtility::currentTime(), rejectTime)) {
        rejectTime = -1;
    }
}

void TopicPartitionSupervisor::recycleBuffer() {
    bool needRecycleFile = false;
    int64_t minReserveBlock = _brokerConfig->getBufferMinReserveSize() / _brokerConfig->getBufferBlockSize();
    int64_t maxUnusedBlock = _centerBlockPool->getMaxUnusedBlockCount();
    if (maxUnusedBlock < minReserveBlock) {
        needRecycleFile = true;
        recycleWriteCache(maxUnusedBlock, minReserveBlock);
    } else {
        AUTIL_LOG(DEBUG,
                  "max unused block [%ld] >= min reserve block [%ld], no need recycle write cache",
                  maxUnusedBlock,
                  minReserveBlock);
    }

    maxUnusedBlock = _fileCachePool->getMaxUnusedBlockCount();
    int64_t reserveCount = _fileCachePool->getMaxBlockCount() * FILE_CACHE_BLOCK_RESERVED_PERCENT;
    if (needRecycleFile || (maxUnusedBlock < reserveCount)) {
        recycleFileCache();
    } else {
        AUTIL_LOG(DEBUG,
                  "max unused block [%ld] >= reserve block [%ld], no need recycle file cache",
                  maxUnusedBlock,
                  reserveCount);
    }
    recycleFile();
    recycleObsoleteReader();
}

void TopicPartitionSupervisor::recycleWriteCache(int64_t maxUnusedBlock, int64_t minReserveBlock) {
    int64_t begTime = TimeUtility::currentTime();
    vector<pair<int64_t, BrokerPartitionPtr>> allPartition;
    {
        autil::ScopedReadLock lock(_brokerPartitionMapMutex);
        BrokerPartitionMap::iterator iter = _brokerPartitionMap.begin();
        allPartition.reserve(_brokerPartitionMap.size());
        int64_t timestamp;
        while (iter != _brokerPartitionMap.end()) {
            BrokerPartitionPtr brokerPartition = iter->second.second;
            if (brokerPartition && brokerPartition->getPartitionStatus() == PARTITION_STATUS_RUNNING) {
                timestamp = brokerPartition->getRecycleInfo().firstMsgTimestamp;
                allPartition.push_back(make_pair(timestamp, brokerPartition));
            }
            iter++;
        }
    }
    sort(allPartition.begin(), allPartition.end());
    int64_t totalRecycleCount = 0;
    for (size_t count = 0; count < allPartition.size(); count++) {
        allPartition[count].second->recycleBuffer();
        int64_t nowMaxUnusedBlock = _centerBlockPool->getMaxUnusedBlockCount();
        PartitionId partId = allPartition[count].second->getPartitionId();
        int64_t recycleCount = nowMaxUnusedBlock - maxUnusedBlock;
        if (recycleCount != 0) {
            totalRecycleCount += recycleCount;
            AUTIL_LOG(INFO,
                      "recycle write cache [%ld] block from partition [%s],"
                      " first msg timestamp [%ld],"
                      " unused block[%ld], max unused block[%ld]",
                      recycleCount,
                      partId.ShortDebugString().c_str(),
                      allPartition[count].first,
                      _centerBlockPool->getUnusedBlockCount(),
                      nowMaxUnusedBlock);
        }
        maxUnusedBlock = nowMaxUnusedBlock;
        if (nowMaxUnusedBlock > minReserveBlock) {
            break;
        }
    }
    if (_metricsReporter) {
        int64_t useTime = TimeUtility::currentTime() - begTime;
        _metricsReporter->reportRecycleWriteCacheBlockCount(totalRecycleCount);
        _metricsReporter->reportRecycleWriteCacheLatency(useTime / 1000);
    }
}

void TopicPartitionSupervisor::recycleFileCache() {
    int64_t begTime = TimeUtility::currentTime();
    vector<BrokerPartitionPtr> allPartition;
    vector<int64_t> metaDisBlocks, dataDisBlocks;
    {
        autil::ScopedReadLock lock(_brokerPartitionMapMutex);
        allPartition.reserve(_brokerPartitionMap.size());
        BrokerPartitionMap::iterator iter = _brokerPartitionMap.begin();
        while (iter != _brokerPartitionMap.end()) {
            BrokerPartitionPtr brokerPartition = iter->second.second;
            if (brokerPartition && brokerPartition->getPartitionStatus() == PARTITION_STATUS_RUNNING) {
                if (brokerPartition->getFileBlockDis(metaDisBlocks, dataDisBlocks)) {
                    allPartition.push_back(brokerPartition);
                }
            }
            iter++;
        }
    }
    sort(metaDisBlocks.begin(), metaDisBlocks.end(), std::greater<int64_t>());
    sort(dataDisBlocks.begin(), dataDisBlocks.end(), std::greater<int64_t>());
    int64_t metaThreshold = -1, dataThreshold = -1;
    getRecycleFileThreshold(metaThreshold, metaDisBlocks);
    getRecycleFileThreshold(dataThreshold, dataDisBlocks);
    AUTIL_LOG(DEBUG, "metaThreshold[%ld], dataThreshold[%ld]", metaThreshold, dataThreshold);
    if (metaThreshold < 0 && dataThreshold < 0) {
        return;
    }
    int64_t totalRecycleCount = 0;
    int64_t maxUnusedBlock = _fileCachePool->getMaxUnusedBlockCount();
    for (size_t index = 0; index < allPartition.size(); ++index) {
        allPartition[index]->recycleFileCache(metaThreshold, dataThreshold);
        int64_t nowMaxUnusedBlock = _fileCachePool->getMaxUnusedBlockCount();
        PartitionId partId = allPartition[index]->getPartitionId();
        int64_t recycleCount = nowMaxUnusedBlock - maxUnusedBlock;
        if (recycleCount != 0) {
            totalRecycleCount += recycleCount;
            AUTIL_LOG(INFO,
                      "file cache recycle [%ld] block from partition[%s],"
                      " unused block[%ld], max unused block[%ld]",
                      recycleCount,
                      partId.ShortDebugString().c_str(),
                      _fileCachePool->getUnusedBlockCount(),
                      nowMaxUnusedBlock);
        }
        maxUnusedBlock = nowMaxUnusedBlock;
    }
    if (_metricsReporter) {
        int64_t useTime = TimeUtility::currentTime() - begTime;
        if (metaThreshold > 0) {
            _metricsReporter->reportRecycleByDisMetaThreshold(metaThreshold);
        }
        if (dataThreshold > 0) {
            _metricsReporter->reportRecycleByDisDataThreshold(dataThreshold);
        }
        _metricsReporter->reportRecycleFileCacheBlockCount(totalRecycleCount);
        _metricsReporter->reportRecycleFileCacheLatency(useTime / 1000);
    }
}

void TopicPartitionSupervisor::recycleFile() {
    vector<BrokerPartitionPtr> allPartition;
    {
        autil::ScopedReadLock lock(_brokerPartitionMapMutex);
        allPartition.reserve(_brokerPartitionMap.size());
        BrokerPartitionMap::iterator iter = _brokerPartitionMap.begin();
        while (iter != _brokerPartitionMap.end()) {
            BrokerPartitionPtr brokerPartition = iter->second.second;
            if (brokerPartition && brokerPartition->getPartitionStatus() == PARTITION_STATUS_RUNNING) {
                allPartition.push_back(brokerPartition);
            }
            iter++;
        }
    }
    for (size_t index = 0; index < allPartition.size(); ++index) {
        allPartition[index]->recycleFile();
    }
}

void TopicPartitionSupervisor::recycleObsoleteReader() {
    int64_t begTime = TimeUtility::currentTime();
    vector<BrokerPartitionPtr> allPartition;
    {
        autil::ScopedReadLock lock(_brokerPartitionMapMutex);
        allPartition.reserve(_brokerPartitionMap.size());
        BrokerPartitionMap::iterator iter = _brokerPartitionMap.begin();
        while (iter != _brokerPartitionMap.end()) {
            BrokerPartitionPtr brokerPartition = iter->second.second;
            if (brokerPartition && brokerPartition->getPartitionStatus() == PARTITION_STATUS_RUNNING) {
                allPartition.push_back(brokerPartition);
            }
            iter++;
        }
    }
    for (size_t index = 0; index < allPartition.size(); ++index) {
        allPartition[index]->recycleObsoleteReader();
    }
    if (_metricsReporter) {
        int64_t useTime = TimeUtility::currentTime() - begTime;
        _metricsReporter->reportRecycleObsoleteReaderLatency(useTime / 1000);
    }
}

void TopicPartitionSupervisor::getRecycleFileThreshold(int64_t &threshold,
                                                       const vector<int64_t> &fileBlocks) { //每次回收10%，取10%的阈值
    if (fileBlocks.size() > 0) {
        threshold = fileBlocks[fileBlocks.size() / 10];
    } else {
        threshold = -1;
    }
}

void TopicPartitionSupervisor::commitMessage() {
    vector<BrokerPartitionPtr> allPartition;
    {
        autil::ScopedReadLock lock(_brokerPartitionMapMutex);
        BrokerPartitionMap::iterator iter = _brokerPartitionMap.begin();
        allPartition.reserve(_brokerPartitionMap.size());
        while (iter != _brokerPartitionMap.end()) {
            BrokerPartitionPtr brokerPartition = iter->second.second;
            if (brokerPartition && brokerPartition->getPartitionStatus() == PARTITION_STATUS_RUNNING &&
                brokerPartition->getTopicMode() != TOPIC_MODE_SECURITY && !brokerPartition->isCommitting()) {
                allPartition.push_back(brokerPartition);
            }
            iter++;
        }
    }
    for (size_t i = 0; i < allPartition.size(); i++) {
        if (allPartition[i]->needCommitMessage()) {
            CommitWorkItem *workItem = new CommitWorkItem(allPartition[i]);
            if (_commitThreadPool->pushWorkItem(workItem) != ThreadPool::ERROR_NONE) {
                AUTIL_LOG(WARN, "push work item into commit thread pool failed");
                DELETE_AND_SET_NULL(workItem);
            }
        }
    }
}

vector<BrokerPartitionPtr> TopicPartitionSupervisor::getBatchPartition(int32_t batchSize, int64_t &count) {
    vector<BrokerPartitionPtr> partitions;
    {
        autil::ScopedReadLock lock(_brokerPartitionMapMutex);
        BrokerPartitionMap::iterator iter = _brokerPartitionMap.begin();
        int32_t partCnt = (_brokerPartitionMap.size() + batchSize) / batchSize;
        int rangeBeg = (count % partCnt) * batchSize;
        int rangeEnd = (count % partCnt + 1) * batchSize;
        partitions.reserve(batchSize);
        int32_t offset = 0;
        while (iter != _brokerPartitionMap.end()) {
            BrokerPartitionPtr brokerPartition = iter->second.second;
            if (offset >= rangeBeg && offset < rangeEnd && brokerPartition &&
                brokerPartition->getPartitionStatus() == PARTITION_STATUS_RUNNING) {
                partitions.push_back(brokerPartition);
            }
            offset++;
            iter++;
        }
    }
    count++;
    return partitions;
}

void TopicPartitionSupervisor::delExpiredFile() {
    vector<BrokerPartitionPtr> todelPartition = getBatchPartition(DEL_FILE_PARTITION_BATCH_SIZE, _delCount);
    for (size_t i = 0; i < todelPartition.size(); i++) {
        if (_isStop) {
            AUTIL_LOG(WARN, "system is stopping, break del expired file.");
            break;
        }
        todelPartition[i]->delExpiredFile();
        todelPartition[i].reset();
    }
}

void TopicPartitionSupervisor::syncDfsUsedSize() {
    AUTIL_LOG(INFO, "thread syncDfsUsedSize start");
    while (true) {
        int32_t randomTimeout = (*_uniform)(_random);
        _syncNotifier.waitNotification(randomTimeout);
        if (_isStop) {
            AUTIL_LOG(INFO, "thread syncDfsUsedSize stop");
            return;
        }
        vector<BrokerPartitionPtr> syncPartition = getBatchPartition(SYNC_DFS_SIZE_PARTITION_BATCH_SIZE, _syncDfsCount);
        for (size_t i = 0; i < syncPartition.size(); i++) {
            if (_isStop) {
                AUTIL_LOG(WARN, "system is stopping, break sync dfs size.");
                break;
            }
            syncPartition[i]->syncDfsUsedSize();
            syncPartition[i].reset();
        }
    }
}

} // namespace service
} // namespace swift
