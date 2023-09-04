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
#include "build_service/workflow/MultiSwiftProcessedDocProducer.h"

#include "autil/EnvUtil.h"
#include "build_service/util/LocatorUtil.h"

using namespace std;

namespace build_service { namespace workflow {
BS_LOG_SETUP(workflow, MultiSwiftProcessedDocProducer);

#define LOG_PREFIX _buildIdStr.c_str()

MultiSwiftProcessedDocProducer::MultiSwiftProcessedDocProducer(std::vector<common::SwiftParam> params,
                                                               const indexlib::config::IndexPartitionSchemaPtr& schema,
                                                               const proto::PartitionId& partitionId,
                                                               const indexlib::util::TaskSchedulerPtr& taskScheduler)
    : _parallelNum(params.size())
    , _stopedSingleProducerCount(0)
    , _buildIdStr(partitionId.buildid().ShortDebugString())
    , _isRunning(false)
    , _hasFatalError(false)
{
    _singleProducers.reserve(_parallelNum);
    _progress.reserve(_parallelNum);
    for (auto& param : params) {
        assert(param.reader);
        _singleProducers.push_back(new SingleSwiftProcessedDocProducer(param, schema, partitionId, taskScheduler));
        indexlibv2::base::Progress progress =
            indexlibv2::base::Progress(param.from, param.to, indexlibv2::base::Progress::INVALID_OFFSET);
        _progress.push_back({progress});
    }
}

MultiSwiftProcessedDocProducer::~MultiSwiftProcessedDocProducer()
{
    assert(!_isRunning);
    _isRunning = false;
    if (_threadPool) {
        _threadPool->stop(autil::ThreadPool::STOP_AND_CLEAR_QUEUE_IGNORE_EXCEPTION);
    }
    _threadPool.reset();
    for (SwiftProcessedDocProducer* singleProducer : _singleProducers) {
        DELETE_AND_SET_NULL(singleProducer);
    }
    _singleProducers.clear();
}

bool MultiSwiftProcessedDocProducer::init(indexlib::util::MetricProviderPtr metricProvider,
                                          const std::string& pluginPath,
                                          const indexlib::util::CounterMapPtr& counterMap, int64_t startTimestamp,
                                          int64_t stopTimestamp, int64_t noMoreMsgPeriod, int64_t maxCommitInterval,
                                          uint64_t sourceSignature, bool allowSeekCrossSrc)
{
    uint32_t documentProducerQueueSize =
        autil::EnvUtil::getEnv("multi_thread_read_processed_document_queue_size", (uint32_t)10000);
    _documentQueue.reset(new autil::SynchronizedQueue<std::pair<size_t, document::ProcessedDocumentPtr>>());
    _documentQueue->setCapacity(_parallelNum * documentProducerQueueSize);

    _threadPool.reset(new autil::ThreadPool(_parallelNum, _parallelNum));
    for (SwiftProcessedDocProducer* singleProducer : _singleProducers) {
        if (!singleProducer->init(metricProvider, pluginPath, counterMap, startTimestamp, stopTimestamp,
                                  noMoreMsgPeriod, maxCommitInterval, sourceSignature, allowSeekCrossSrc)) {
            return false;
        }
    }
    for (size_t i = 0; i < _parallelNum; i++) {
        _progress[i][0].offset = {startTimestamp, 0};
    }
    return true;
}

void MultiSwiftProcessedDocProducer::StartWork()
{
    for (size_t parallelIdx = 0; parallelIdx < _parallelNum; ++parallelIdx) {
        _threadPool->pushTask([this, parallelIdx] {
            document::ProcessedDocumentVecPtr processedDocVec;
            while (true) {
                if (unlikely(!_isRunning)) {
                    break;
                }
                FlowError flowError = _singleProducers[parallelIdx]->produce(processedDocVec);
                switch (flowError) {
                case FE_EOF:
                    BS_PREFIX_LOG(INFO, "parallel id [%lu] read from swift meet eof, exit", parallelIdx);
                    return;
                case FE_OK:
                    assert(processedDocVec->size() == 1);
                    while (true) {
                        if (unlikely(!_isRunning)) {
                            break;
                        } else if (_documentQueue->tryPush(std::make_pair(parallelIdx, (*processedDocVec)[0]))) {
                            break;
                        } else {
                            usleep(5000);
                        }
                    }
                    processedDocVec->clear();
                    break;
                case FE_FATAL:
                    _hasFatalError = true;
                    break;
                default:
                    usleep(5000);
                }
            }
        });
    }
    _threadPool->setThreadStopHook([this]() { ++_stopedSingleProducerCount; });
    _threadPool->start();
}

FlowError MultiSwiftProcessedDocProducer::produce(document::ProcessedDocumentVecPtr& processedDocVec)
{
    if (unlikely(!_isRunning)) {
        _isRunning = true;
        StartWork();
    }

    processedDocVec.reset(new document::ProcessedDocumentVec());
    processedDocVec->resize(1);
    while (true) {
        if (unlikely(_hasFatalError)) {
            return FE_FATAL;
        }
        std::pair<size_t, document::ProcessedDocumentPtr> ret;
        if (_documentQueue->tryGetAndPopFront(ret)) {
            (*processedDocVec)[0] = ret.second;
            _progress[ret.first] = (*processedDocVec)[0]->getLocator().GetProgress();
            std::vector<indexlibv2::base::Progress> progress;
            for (const auto& singleProgress : _progress) {
                progress.insert(progress.end(), singleProgress.begin(), singleProgress.end());
            }
            auto locator = (*processedDocVec)[0]->getLocator();
            locator.SetProgress(progress);
            (*processedDocVec)[0]->setLocator(locator);
            return FE_OK;
        } else if (_stopedSingleProducerCount.load() == _parallelNum) {
            BS_PREFIX_LOG(INFO, "all single producer stoped, read document from queue meet eof");
            return FE_EOF;
        } else {
            usleep(5000);
        }
    }
    return FE_OK;
}

bool MultiSwiftProcessedDocProducer::seek(const common::Locator& locator)
{
    assert(_singleProducers.size() == _parallelNum);

    for (size_t parallelId = 0; parallelId < _parallelNum; ++parallelId) {
        auto& singleProgressVec = _progress[parallelId];
        uint32_t from = singleProgressVec[0].from;
        uint32_t to = singleProgressVec[singleProgressVec.size() - 1].to;
        auto seekLocator = locator;
        if (!seekLocator.ShrinkToRange(from, to)) {
            BS_LOG(ERROR, "seek producer [%lu/%u][%u ~ %u] locator [%s] failed", parallelId, _parallelNum, from, to,
                   seekLocator.DebugString().c_str());
            return false;
        }
        if (seekLocator.GetProgress().empty()) {
            BS_LOG(ERROR, "skip seek producer [%lu/%u][%u ~ %u], empty progress", parallelId, _parallelNum, from, to);
            continue;
        }
        auto [success, actualLocator] = _singleProducers[parallelId]->seekAndGetLocator(seekLocator);
        if (success) {
            _progress[parallelId] = actualLocator.GetProgress();
        } else {
            BS_LOG(ERROR, "seek producer [%lu/%u][%u ~ %u] failed", parallelId, _parallelNum, from, to);
            return false;
        }
    }
    return true;
}

bool MultiSwiftProcessedDocProducer::stop(StopOption stopOption)
{
    assert(_singleProducers.size() == _parallelNum);

    _isRunning = false;
    if (_threadPool) {
        _threadPool->stop(autil::ThreadPool::STOP_AND_CLEAR_QUEUE_IGNORE_EXCEPTION);
    }
    bool isSuccess = true;
    for (size_t parallelId = 0; parallelId < _parallelNum; ++parallelId) {
        isSuccess &= _singleProducers[parallelId]->stop(stopOption);
    }
    return isSuccess;
}

int64_t MultiSwiftProcessedDocProducer::suspendReadAtTimestamp(int64_t timestamp, common::ExceedTsAction action)
{
    assert(_singleProducers.size() == _parallelNum);

    int64_t actualTimestamp = std::numeric_limits<int64_t>::max();
    for (size_t parallelId = 0; parallelId < _parallelNum; ++parallelId) {
        actualTimestamp =
            std::min(actualTimestamp, _singleProducers[parallelId]->suspendReadAtTimestamp(timestamp, action));
    }
    return actualTimestamp;
}

bool MultiSwiftProcessedDocProducer::needUpdateCommittedCheckpoint() const
{
    assert(_singleProducers.size() == _parallelNum);

    for (size_t parallelId = 0; parallelId < _parallelNum; ++parallelId) {
        if (!_singleProducers[parallelId]->needUpdateCommittedCheckpoint()) {
            return false;
        }
    }
    return true;
}

bool MultiSwiftProcessedDocProducer::updateCommittedCheckpoint(const indexlibv2::base::Progress::Offset& checkpoint)
{
    assert(_singleProducers.size() == _parallelNum);

    for (size_t parallelId = 0; parallelId < _parallelNum; ++parallelId) {
        if (!_singleProducers[parallelId]->updateCommittedCheckpoint(checkpoint)) {
            return false;
        }
    }
    return true;
}

int64_t MultiSwiftProcessedDocProducer::getStartTimestamp() const { return _singleProducers[0]->getStartTimestamp(); }

bool MultiSwiftProcessedDocProducer::getMaxTimestamp(int64_t& timestamp)
{
    assert(_singleProducers.size() == _parallelNum);

    timestamp = -1;
    int64_t tmpTimestamp = -1;
    for (size_t parallelId = 0; parallelId < _parallelNum; ++parallelId) {
        if (!_singleProducers[parallelId]->getMaxTimestamp(tmpTimestamp)) {
            return false;
        }
        timestamp = std::max(tmpTimestamp, timestamp);
    }
    return true;
}

bool MultiSwiftProcessedDocProducer::getLastReadTimestamp(int64_t& timestamp)
{
    assert(_singleProducers.size() == _parallelNum);

    timestamp = std::numeric_limits<int64_t>::max();
    int64_t tmpTimestamp = std::numeric_limits<int64_t>::max();
    for (size_t parallelId = 0; parallelId < _parallelNum; ++parallelId) {
        if (!_singleProducers[parallelId]->getLastReadTimestamp(tmpTimestamp)) {
            return false;
        }
        timestamp = std::min(tmpTimestamp, timestamp);
    }
    return true;
}

void MultiSwiftProcessedDocProducer::setLinkReporter(const common::SwiftLinkFreshnessReporterPtr& reporter)
{
    for (SwiftProcessedDocProducer* singleProducer : _singleProducers) {
        singleProducer->setLinkReporter(reporter);
    }
}

void MultiSwiftProcessedDocProducer::reportFreshnessMetrics(int64_t locatorTimestamp, bool noMoreMsg,
                                                            const std::string& docSource,
                                                            bool isReportFastQueueSwiftReadDelay)
{
    for (SwiftProcessedDocProducer* singleProducer : _singleProducers) {
        singleProducer->reportFreshnessMetrics(locatorTimestamp, noMoreMsg, docSource, isReportFastQueueSwiftReadDelay);
    }
}

void MultiSwiftProcessedDocProducer::setRecovered(bool isServiceRecovered)
{
    for (SwiftProcessedDocProducer* singleProducer : _singleProducers) {
        singleProducer->setRecovered(isServiceRecovered);
    }
}

void MultiSwiftProcessedDocProducer::resumeRead()
{
    for (SwiftProcessedDocProducer* singleProducer : _singleProducers) {
        singleProducer->resumeRead();
    }
}

}} // namespace build_service::workflow
