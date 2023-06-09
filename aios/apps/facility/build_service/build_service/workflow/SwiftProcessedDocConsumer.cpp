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
#include "build_service/workflow/SwiftProcessedDocConsumer.h"

#include <unistd.h>

#include "autil/DataBuffer.h"
#include "autil/EnvUtil.h"
#include "build_service/common/BeeperCollectorDefine.h"
#include "build_service/common/CounterSynchronizer.h"
#include "build_service/common/Locator.h"
#include "build_service/common/PkTracer.h"
#include "build_service/util/Monitor.h"
#include "indexlib/util/DocTracer.h"
#include "indexlib/util/counter/AccumulativeCounter.h"
#include "indexlib/util/counter/CounterMap.h"
#include "indexlib/util/counter/StateCounter.h"
#include "indexlib/util/counter/StringCounter.h"

using namespace std;
using namespace autil;

using namespace indexlib::document;
using namespace indexlib::util;
using namespace swift::protocol;
using namespace swift::client;

using namespace build_service::document;
using namespace build_service::common;
using namespace build_service::util;

namespace build_service { namespace workflow {

SwiftWriterWithMetric::SwiftWriterWithMetric()
    : unsendMsgSizeMetric(nullptr)
    , uncommittedMsgSizeMetric(nullptr)
    , sendBufferFullQpsMetric(nullptr)
{
}

bool SwiftWriterWithMetric::isSendingDoc() const { return !swiftWriter->isFinished(); }

void SwiftWriterWithMetric::updateMetrics()
{
    swift::client::WriterMetrics writerMetric;
    swiftWriter->getWriterMetrics(zkRootPath, topicName, writerMetric);
    REPORT_METRIC(unsendMsgSizeMetric, writerMetric.unsendMsgSize);

    REPORT_METRIC(uncommittedMsgSizeMetric, writerMetric.uncommittedMsgSize);
}

BS_LOG_SETUP(workflow, SwiftProcessedDocConsumer);

SwiftProcessedDocConsumer::SwiftProcessedDocConsumer()
    : _src(0)
    , _latestInvalidDocCheckpointId(-1)
    , _hasCounter(false)
    , _batchMask(-1)
{
    if (EnvUtil::getEnvWithoutDefault(BS_ENV_DOC_TRACE_FIELD.c_str(), _traceField)) {
        _docCounter = PeriodDocCounterHelper::create(FieldType::ft_string);
    } else {
        _docCounter = NULL;
    }
}

SwiftProcessedDocConsumer::~SwiftProcessedDocConsumer()
{
    if (_syncCounterThread) {
        _syncCounterThread->runOnce();
        _syncCounterThread->stop();
    }
    if (_updateMetricThread) {
        _updateMetricThread->runOnce();
        _updateMetricThread->stop();
    }
}

indexlib::util::CounterBasePtr
SwiftProcessedDocConsumer::getOrCreateCounter(const CounterMapPtr& counterMap, const std::string& counterName,
                                              indexlib::util::CounterBase::CounterType type,
                                              int64_t defaultStateCounterValue)
{
    indexlib::util::CounterBasePtr counter;
    std::vector<CounterBasePtr> targetCounter = counterMap->FindCounters(counterName);
    if (targetCounter.size() > 1) {
        BS_LOG(ERROR, "init counter [%s] failed", counterName.c_str());
        return nullptr;
    }
    if (targetCounter.size() == 0) {
        counter = counterMap->GetCounter(counterName, type);
        if (!counter) {
            BS_LOG(ERROR, "init counter [%s] failed", counterName.c_str());
            return nullptr;
        }
        if (type == indexlib::util::CounterBase::CT_STATE) {
            auto stateCounter = DYNAMIC_POINTER_CAST(StateCounter, counter);
            assert(stateCounter);
            stateCounter->Set(defaultStateCounterValue);
        }
        return counter;
    }
    return targetCounter[0];
}

bool SwiftProcessedDocConsumer::initCounters(const CounterMapPtr& counterMap, int64_t startTimestamp)
{
    if (startTimestamp == 0) {
        startTimestamp = -1;
    }
    if (!counterMap) {
        BS_LOG(ERROR, "counter map should not be empty");
        return false;
    }

    _srcCounter = DYNAMIC_POINTER_CAST(StateCounter, getOrCreateCounter(counterMap, BS_COUNTER(locatorSource),
                                                                        indexlib::util::CounterBase::CT_STATE,
                                                                        /*defaultStateCounterValue=*/0));
    _checkpointCounter = DYNAMIC_POINTER_CAST(
        StateCounter, getOrCreateCounter(counterMap, BS_COUNTER(locatorCheckpoint),
                                         indexlib::util::CounterBase::CT_STATE, /*defaultStateCounterValue=*/-1));
    _userDataCounter = DYNAMIC_POINTER_CAST(StringCounter, getOrCreateCounter(counterMap, BS_COUNTER(locatorUserData),
                                                                              indexlib::util::CounterBase::CT_STRING,
                                                                              /*unused=*/-1));

    _consumedDocCountCounter = GET_ACC_COUNTER(counterMap, consumedDocCount);
    if (!_consumedDocCountCounter) {
        return false;
    }
    int64_t remoteSrc = _srcCounter->Get();
    std::string userData = _userDataCounter->Get();
    if (remoteSrc >= 0 && (uint64_t)remoteSrc >= _src) {
        _cachedLocator.SetSrc(remoteSrc);
        _cachedLocator.SetOffset(max(startTimestamp, _checkpointCounter->Get()));
        _cachedLocator.SetUserData(userData);
    }
    _hasCounter = true;
    return true;
}

bool SwiftProcessedDocConsumer::init(const map<string, SwiftWriterWithMetric>& writers,
                                     const CounterSynchronizerPtr& counterSynchronizer, int64_t checkpointInterval,
                                     const common::Locator& locator, int32_t batchMask, bool disableCounter)
{
    _src = locator.GetSrc();
    _writers = writers;
    if (disableCounter) {
        _cachedLocator = locator;
        BS_LOG(INFO, "SwiftProcessedDocConsumer disable counter");
    } else if (counterSynchronizer) {
        _counterSynchronizer = counterSynchronizer;
        if (!initCounters(_counterSynchronizer->getCounterMap(), locator.GetOffset())) {
            BS_LOG(ERROR, "initCounters failed");
        }
    } else {
        BS_LOG(ERROR, "counterSynchronizer is NULL");
    }

    _syncCounterThread =
        autil::LoopThread::createLoopThread(std::bind(&SwiftProcessedDocConsumer::syncCountersWrapper, this),
                                            checkpointInterval * 1000 * 1000, "BsSwiftCounter");
    if (!_syncCounterThread) {
        string errorMsg = "create [sync counter] thread failed!";
        BS_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }

    _updateMetricThread = autil::LoopThread::createLoopThread(std::bind(&SwiftProcessedDocConsumer::updateMetric, this),
                                                              1 * 1000 * 1000, "BsSwiftMetric");
    _batchMask = batchMask;
    if (_batchMask != -1) {
        BS_LOG(INFO, "write swift message with batchMask [%d]", _batchMask);
    }
    return true;
}

// processor write processed doc to swift topic
FlowError SwiftProcessedDocConsumer::consume(const ProcessedDocumentVecPtr& item)
{
    autil::ScopedLock lock(_consumeLock);
    int64_t latestInvalidDocCheckpointId = _latestInvalidDocCheckpointId;
    for (size_t i = 0; i < (*item).size(); i++) {
        const ProcessedDocumentPtr& processedDoc = (*item)[i];
        const ProcessedDocument::DocClusterMetaVec& metaVec = processedDoc->getDocClusterMetaVec();
        const auto& document = processedDoc->getDocument();
        const common::Locator& locator = processedDoc->getLocator();

        if (!document || document->GetDocOperateType() == SKIP_DOC || document->GetDocOperateType() == CHECKPOINT_DOC ||
            document->GetDocOperateType() == UNKNOWN_OP) {
            int64_t docCheckpointId = locator.GetOffset();
            latestInvalidDocCheckpointId = max(latestInvalidDocCheckpointId, docCheckpointId);
            _checkpointIdToUserData.push_back(std::make_pair(latestInvalidDocCheckpointId, locator.GetUserData()));
            IE_DOC_TRACE(document, "processed doc to skip");
            continue;
        }
        if (processedDoc->needSkip()) {
            continue;
        }

        string docString = processedDoc->transToDocString();
        for (size_t j = 0; j < metaVec.size(); j++) {
            const string& clusterName = metaVec[j].clusterName;
            auto it = _writers.find(clusterName);
            if (it == _writers.end()) {
                string errorMsg = "cluster[" + clusterName + "] not exist for build";
                BS_LOG(WARN, "%s", errorMsg.c_str());
                continue;
            }
            MessageInfo message;
            message.data = docString;
            message.uint16Payload = metaVec[j].hashId;
            message.uint8Payload = metaVec[j].buildType;
            if (_batchMask != -1) {
                message.uint8Payload = _batchMask;
            }
            message.checkpointId = locator.GetOffset();
            ErrorCode ec = it->second.swiftWriter->write(message);
            if (ec != ERROR_NONE) {
                if (ec == ERROR_CLIENT_SEND_BUFFER_FULL) {
                    INCREASE_QPS(it->second.sendBufferFullQpsMetric);
                    BS_INTERVAL_LOG(300, WARN, "swift send buffer is full");
                    BEEPER_INTERVAL_REPORT(120, WORKER_ERROR_COLLECTOR_NAME, "swift send buffer is full");
                } else {
                    string errorMsg = "write error for cluster[" + clusterName + "]";
                    BS_LOG(ERROR, "%s, errorCode[%u]", errorMsg.c_str(), ec);
                    BEEPER_INTERVAL_REPORT(10, WORKER_ERROR_COLLECTOR_NAME, errorMsg);
                }
                return FE_RETRY;
            }
        }
        PkTracer::toSwiftTrace(document->GetTraceId().to_string(), document->GetLocatorV2().DebugString());
        PeriodDocCounterHelper::count(PeriodDocCounterType::Processor, processedDoc->getTraceField(), _docCounter);
        IE_DOC_TRACE(document, "write processed doc to swift");
        _src = locator.GetSrc();
        _checkpointIdToUserData.push_back(std::make_pair(locator.GetOffset(), locator.GetUserData()));
        processedDoc->setNeedSkip(true);
    }
    if (_consumedDocCountCounter) {
        if (item->size() != 0 && (*item)[0] && (*item)[0]->isUserDoc()) {
            _consumedDocCountCounter->Increase(1);
        }
    }
    _latestInvalidDocCheckpointId = latestInvalidDocCheckpointId;
    return FE_OK;
}

void SwiftProcessedDocConsumer::updateMetric()
{
    for (auto it = _writers.begin(); it != _writers.end(); ++it) {
        it->second.updateMetrics();
    }
}

int64_t SwiftProcessedDocConsumer::updateCheckpointIdToUserData(int64_t checkpointId)
{
    autil::ScopedLock lock(_consumeLock);
    int64_t lastCommittedCheckpointId = -1;
    // remove entries in _checkpointIdToUserData that are synced to all swift writers.
    while (!_checkpointIdToUserData.empty()) {
        const std::pair<int64_t, std::string>& pair = _checkpointIdToUserData.front();
        if (pair.first < checkpointId) {
            lastCommittedCheckpointId = pair.first;
            _checkpointIdToUserData.pop_front();
        } else if (pair.first == checkpointId) {
            lastCommittedCheckpointId = pair.first;
            break;
        } else {
            break;
        }
    }
    return lastCommittedCheckpointId;
}

void SwiftProcessedDocConsumer::syncCountersWrapper()
{
    int64_t checkpointId;
    uint64_t src;
    std::string userData;
    getMinCheckpoint(&checkpointId, &src, &userData);
    int64_t lastCommittedCheckpointId = updateCheckpointIdToUserData(checkpointId);
    syncCounters(lastCommittedCheckpointId, src, userData);
}

bool SwiftProcessedDocConsumer::syncCounters(int64_t checkpointId, uint64_t src, std::string userData)
{
    if (checkpointId < 0) {
        return true;
    }
    if (_docCounter) {
        _docCounter->flush();
    }
    if (!_hasCounter) {
        autil::ScopedLock lock(_lock);
        if (!_cachedLocator.IsSameSrc(common::Locator(_src, 0), false) || _cachedLocator.GetOffset() < checkpointId) {
            _cachedLocator.SetSrc(src);
            _cachedLocator.SetOffset(checkpointId);
            _cachedLocator.SetUserData(userData);
        }
        return true;
    }

    uint64_t remoteSrc = _srcCounter->Get();
    int64_t remoteCheckpointId = _checkpointCounter->Get();
    std::string remoteUserData = _userDataCounter->Get();

    if (remoteSrc != src || checkpointId > remoteCheckpointId) {
        BS_INTERVAL_LOG(10, INFO, "serialize checkpoint[%lu:%ld:%s]", src, checkpointId, userData.c_str());
        {
            autil::ScopedLock lock(_lock);
            if (!_cachedLocator.IsSameSrc(common::Locator(src, 0), false) ||
                _cachedLocator.GetOffset() < checkpointId) {
                _cachedLocator.SetSrc(src);
                _cachedLocator.SetOffset(checkpointId);
                _cachedLocator.SetUserData(userData);
            }
        }
        _srcCounter->Set(src);
        _checkpointCounter->Set(checkpointId);
        _userDataCounter->Set(userData);
        if (!_counterSynchronizer->sync()) {
            _srcCounter->Set(remoteSrc);
            _checkpointCounter->Set(remoteCheckpointId);
            _userDataCounter->Set(remoteUserData);
            return false;
        }
    }
    return true;
}

// It's possible that checkpointIdToUserData does not contain every checkpointId that we are looking for, e.g.
// skip_doc's checkpoint_id_a will not be added to checkpointIdToUserData. In such case, we should find checkpoint that
// is the nearest smaller one than checkpoint_id_a.
std::string SwiftProcessedDocConsumer::getCheckpointUserData(
    const std::deque<std::pair<int64_t, std::string>>& checkpointIdToUserData, int64_t checkpointId)
{
    if (checkpointIdToUserData.empty()) {
        BS_LOG(WARN, "Empty checkpointIdToUserData.");
        return "";
    }
    int64_t closestCheckpointId = -1;
    std::string closestUserData = "";
    for (int i = 0; i < checkpointIdToUserData.size(); ++i) {
        const std::pair<int64_t, std::string>& pair = checkpointIdToUserData.at(i);
        if (pair.first == checkpointId) {
            BS_LOG(INFO, "Found checkpointId[%ld], userData:[%s]", checkpointId, pair.second.c_str());
            return pair.second;
        }
        if (pair.first < checkpointId) {
            closestCheckpointId = pair.first;
            closestUserData = pair.second;
        }
    }
    // This log should not be seen when processor read from Hologres type data source.
    BS_LOG(INFO,
           "Unable to find userData corresponding to checkpointId[%ld], returning nearest smaller checkpoint[%ld]:[%s]",
           checkpointId, closestCheckpointId, closestUserData.c_str());
    return closestUserData;
}

void SwiftProcessedDocConsumer::getMinCheckpoint(int64_t* checkpointId, uint64_t* src, std::string* userData) const
{
    autil::ScopedLock lock(_consumeLock);
    int64_t sendingCheckpointId = numeric_limits<int64_t>::max();
    bool allSend = true;
    int64_t allSendCheckpointId = numeric_limits<int64_t>::min();
    for (auto it = _writers.begin(); it != _writers.end(); ++it) {
        bool isSending = it->second.isSendingDoc();
        int64_t commitedCheckpointId = it->second.swiftWriter->getCommittedCheckpointId();
        if (isSending) {
            allSend = false;
            sendingCheckpointId = min(sendingCheckpointId, commitedCheckpointId);
        }

        if (allSend) {
            allSendCheckpointId = max(allSendCheckpointId, commitedCheckpointId);
        }
    }

    *src = _src;

    if (allSend) {
        *checkpointId = max(allSendCheckpointId, _latestInvalidDocCheckpointId);
        *userData = getCheckpointUserData(_checkpointIdToUserData, *checkpointId);
        return;
    }
    *checkpointId = sendingCheckpointId;
    *userData = getCheckpointUserData(_checkpointIdToUserData, *checkpointId);
    return;
}

bool SwiftProcessedDocConsumer::getLocator(common::Locator& locator) const
{
    autil::ScopedLock lock(_lock);
    if (_cachedLocator.GetOffset() != -1) {
        locator = _cachedLocator;
    }
    return true;
}

bool SwiftProcessedDocConsumer::stop(FlowError lastError, StopOption stopOption)
{
    while (!isFinished()) {
        BS_LOG(INFO, "waiting swift message flush");
        usleep(1 * 1000 * 1000);
    }

    if (_syncCounterThread) {
        _syncCounterThread->stop();
        _syncCounterThread.reset();
    }
    int64_t checkpointId;
    uint64_t src;
    std::string userData;
    getMinCheckpoint(&checkpointId, &src, &userData);
    int64_t lastCommittedCheckpointId = updateCheckpointIdToUserData(checkpointId);
    checkpointId = max(_latestInvalidDocCheckpointId, lastCommittedCheckpointId);
    int64_t retry = 0;
    while (!syncCounters(checkpointId, src, userData)) {
        if (++retry > MAX_SYNC_RETRY) {
            break;
        }
        usleep(1 * 1000 * 1000);
    }

    return true;
}

bool SwiftProcessedDocConsumer::isFinished() const
{
    static constexpr int64_t WAIT_FINISH_TIME_US = 1800 * 1000 * 1000; // 30min
    for (auto it = _writers.begin(); it != _writers.end(); ++it) {
        ErrorCode ec = it->second.swiftWriter->waitFinished(WAIT_FINISH_TIME_US);
        if (ec != ERROR_NONE) {
            return false;
        }
    }
    return true;
}

}} // namespace build_service::workflow
