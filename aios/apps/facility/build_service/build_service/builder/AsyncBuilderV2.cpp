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
#include "build_service/builder/AsyncBuilderV2.h"

#include "build_service/builder/BuilderV2Impl.h"
#include "build_service/proto/ProtoUtil.h"
#include "build_service/util/Monitor.h"
#include "indexlib/base/Status.h"
#include "indexlib/document/IDocumentBatch.h"
#include "indexlib/framework/ITablet.h"
#include "indexlib/indexlib.h"

namespace build_service::builder {

#define TABLET_LOG(level, format, args...) AUTIL_LOG(level, "[%s] " format, _buildId.ShortDebugString().c_str(), ##args)

BS_LOG_SETUP(builder, AsyncBuilderV2);

AsyncBuilderV2::AsyncBuilderV2(std::shared_ptr<indexlibv2::framework::ITablet> tablet, const proto::BuildId& buildId)
    : AsyncBuilderV2(std::make_unique<BuilderV2Impl>(tablet, buildId))
{
}

AsyncBuilderV2::AsyncBuilderV2(std::unique_ptr<BuilderV2> impl)
    : BuilderV2(impl->getBuildId())
    , _impl(std::move(impl))
    , _running(false)
    , _needStop(false)
{
}

AsyncBuilderV2::~AsyncBuilderV2() { stop(INVALID_TIMESTAMP, /*needSeal*/ false, true); }

bool AsyncBuilderV2::init(const config::BuilderConfig& builderConfig, indexlib::util::MetricProviderPtr metricProvider)
{
    if (!_impl->init(builderConfig, metricProvider)) {
        return false;
    }
    assert(builderConfig.asyncBuild);
    _docQueue.reset(new DocQueue(builderConfig.asyncQueueSize, builderConfig.asyncQueueMem * 1024 * 1024));
    _batchBuildSize = builderConfig.batchBuildSize;

    // (+ _batchBuildSize + 2) to avoid block build thread
    _releaseDocQueue.reset(new Queue(builderConfig.asyncQueueSize + _batchBuildSize + 2));

    _asyncQueueSizeMetric = DECLARE_METRIC(metricProvider, "perf/asyncQueueSize", kmonitor::STATUS, "count");
    _asyncQueueMemMetric = DECLARE_METRIC(metricProvider, "perf/asyncQueueMemUse", kmonitor::STATUS, "MB");
    _releaseQueueSizeMetric = DECLARE_METRIC(metricProvider, "perf/releaseQueueSize", kmonitor::STATUS, "count");

    _running = true;
    _asyncBuildThreadPtr = autil::Thread::createThread(std::bind(&AsyncBuilderV2::buildThread, this), "BsAsyncBuilder");

    if (!_asyncBuildThreadPtr) {
        _running = false;
        std::string errorMsg = "start async build thread failed";
        TABLET_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    return true;
}

void AsyncBuilderV2::clearQueue()
{
    while (!_docQueue->empty()) {
        std::shared_ptr<indexlibv2::document::IDocumentBatch> batch;
        _docQueue->pop(batch);
        batch.reset();
    }
}

void AsyncBuilderV2::fillDocBatches(std::vector<std::shared_ptr<indexlibv2::document::IDocumentBatch>>& docBatches)
{
    std::shared_ptr<indexlibv2::document::IDocumentBatch> batch;
    for (size_t i = 0; i < _batchBuildSize; i++) {
        if (i > 0 && _docQueue->empty()) {
            return;
        }
        // fix bug: 8214135
        if (!_docQueue->top(batch)) {
            TABLET_LOG(TRACE1, "doc queue get doc return false");
            return;
        }
        docBatches.push_back(batch);
        _ongoingDocSize++;
        _docQueue->pop(batch);
    }
}

void AsyncBuilderV2::buildThread()
{
    std::vector<std::shared_ptr<indexlibv2::document::IDocumentBatch>> docBatches;
    // docBatches.reserve(_batchBuildSize);
    while (_running) {
        fillDocBatches(docBatches);
        if (docBatches.size() <= 0) {
            continue;
        }
        auto batch = docBatches[0];
        for (size_t idx = 1; idx < docBatches.size(); idx++) {
            for (size_t j = 0; j < docBatches[idx]->GetBatchSize(); j++) {
                batch->AddDocument((*docBatches[idx])[j]);
            }
        }
        bool consumed = false;
        while (_running) {
            consumed = _impl->build(batch);
            if (consumed) {
                break;
            }
            if (hasFatalError() || needReconstruct() || isSealed() || _needStop.load()) {
                clearQueue();
                docBatches.clear();
                _running = false;
                break;
            }
        }
        _ongoingDocSize = 0;
        //_docQueue->pop(batch);
        for (auto docBatch : docBatches) {
            _releaseDocQueue->push(docBatch);
        }
        docBatches.clear();
    }
    TABLET_LOG(INFO, "async builder thread exit");
}

void AsyncBuilderV2::releaseDocs()
{
    while (!_releaseDocQueue->empty()) {
        std::shared_ptr<indexlibv2::document::IDocumentBatch> batch;
        _releaseDocQueue->pop(batch);
        batch.reset();
    }
}

bool AsyncBuilderV2::build(const std::shared_ptr<indexlibv2::document::IDocumentBatch>& batch)
{
    if (needReconstruct()) {
        TABLET_LOG(INFO, "%s", "need Reconstruct after reopen");
        return false;
    }
    if (!_running) {
        std::string errorMsg = "receive document after builder stop.";
        TABLET_LOG(WARN, "%s", errorMsg.c_str());
        return false;
    }
    if (hasFatalError()) {
        return false;
    }
    releaseDocs();
    _docQueue->push(batch, batch->EstimateMemory());
    REPORT_METRIC(_asyncQueueSizeMetric, _docQueue->size());
    REPORT_METRIC(_asyncQueueMemMetric, _docQueue->memoryUse() / 1024.0 / 1024.0);
    REPORT_METRIC(_releaseQueueSizeMetric, _releaseDocQueue->size());
    return true;
}

void AsyncBuilderV2::stop(std::optional<int64_t> stopTimestamp, bool needSeal, bool immediately)
{
    TABLET_LOG(INFO, "async builder stop begin");
    _docQueue->setFinish();

    TABLET_LOG(INFO, "wait all docs in queue built");
    if (!_running && !_docQueue->empty()) {
        std::string errorMsg = "some docs in queue, but build thread is not running";
        TABLET_LOG(ERROR, "%s", errorMsg.c_str());
    }
    if (!immediately) {
        const uint32_t interval = 50000; // 50ms
        while (!isFinished()) {
            usleep(interval);
        }
    }

    TABLET_LOG(INFO, "all docs in queue build finish");
    _running = false;
    _asyncBuildThreadPtr.reset();
    releaseDocs();
    _impl->stop(stopTimestamp, needSeal, immediately);
    TABLET_LOG(INFO, "async builder stop end");
}

void AsyncBuilderV2::notifyStop() { _needStop = true; }

int64_t AsyncBuilderV2::getIncVersionTimestamp() const { return _impl->getIncVersionTimestamp(); }
indexlibv2::framework::Locator AsyncBuilderV2::getLastLocator() const { return _impl->getLastLocator(); }
indexlibv2::framework::Locator AsyncBuilderV2::getLatestVersionLocator() const
{
    return _impl->getLatestVersionLocator();
}
indexlibv2::framework::Locator AsyncBuilderV2::getLastFlushedLocator() const { return _impl->getLastFlushedLocator(); }
bool AsyncBuilderV2::hasFatalError() const { return _impl->hasFatalError(); }
bool AsyncBuilderV2::needReconstruct() const { return _impl->needReconstruct(); }
bool AsyncBuilderV2::isSealed() const { return _impl->isSealed(); }
void AsyncBuilderV2::setFatalError() { _impl->setFatalError(); }
std::shared_ptr<indexlib::util::CounterMap> AsyncBuilderV2::GetCounterMap() const { return _impl->GetCounterMap(); }
void AsyncBuilderV2::switchToConsistentMode() { return _impl->switchToConsistentMode(); }

#undef TABLET_NAME

} // namespace build_service::builder
