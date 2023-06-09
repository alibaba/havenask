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

#include <memory>

#include "autil/Thread.h"
#include "build_service/builder/BuilderV2.h"
#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include "build_service/util/MemControlStreamQueue.h"
#include "build_service/util/StreamQueue.h"

namespace indexlib::util {
class MetricsProvider;
class Metric;
} // namespace indexlib::util

namespace indexlibv2::framework {
class ITablet;
class Locator;
} // namespace indexlibv2::framework

namespace indexlibv2::document {
class IDocumentBatch;
}

namespace build_service::builder {

class AsyncBuilderV2 : public BuilderV2
{
public:
    explicit AsyncBuilderV2(std::unique_ptr<BuilderV2> impl);
    AsyncBuilderV2(std::shared_ptr<indexlibv2::framework::ITablet> tablet,
                   const proto::BuildId& buildId = proto::BuildId());
    /*override*/ ~AsyncBuilderV2();
    AsyncBuilderV2(const AsyncBuilderV2&) = delete;
    AsyncBuilderV2& operator=(const AsyncBuilderV2&) = delete;

public:
    bool init(const config::BuilderConfig& builderConfig,
              std::shared_ptr<indexlib::util::MetricProvider> metricProvider = nullptr) override;
    // only return false when exception
    bool build(const std::shared_ptr<indexlibv2::document::IDocumentBatch>& batch) override;
    bool merge() override { return true; };
    void stop(std::optional<int64_t> stopTimestamp, bool needSeal, bool immediately) override;
    void notifyStop() override;
    int64_t getIncVersionTimestamp() const override; // for realtime build
    indexlibv2::framework::Locator getLastLocator() const override;
    indexlibv2::framework::Locator getLatestVersionLocator() const override;
    indexlibv2::framework::Locator getLastFlushedLocator() const override;
    bool hasFatalError() const override;
    bool needReconstruct() const override;
    bool isSealed() const override;
    void setFatalError() override;
    std::shared_ptr<indexlib::util::CounterMap> GetCounterMap() const override;
    void switchToConsistentMode() override;

private:
    bool isFinished() const { return _ongoingDocSize == 0 && (!_running || _docQueue->empty()); }
    void buildThread();
    void releaseDocs();
    void clearQueue();
    // virtual for test
    virtual void fillDocBatches(std::vector<std::shared_ptr<indexlibv2::document::IDocumentBatch>>& docBatches);

private:
    using DocQueue = util::MemControlStreamQueue<std::shared_ptr<indexlibv2::document::IDocumentBatch>>;
    using Queue = util::StreamQueue<std::shared_ptr<indexlibv2::document::IDocumentBatch>>;
    using DocQueuePtr = std::unique_ptr<DocQueue>;
    using QueuePtr = std::unique_ptr<Queue>;

    std::unique_ptr<BuilderV2> _impl;
    volatile size_t _ongoingDocSize = 0;
    volatile bool _running;
    std::atomic<bool> _needStop;

    autil::ThreadPtr _asyncBuildThreadPtr;
    DocQueuePtr _docQueue;
    QueuePtr _releaseDocQueue;
    size_t _batchBuildSize = 1;
    std::shared_ptr<indexlib::util::Metric> _asyncQueueSizeMetric;
    std::shared_ptr<indexlib::util::Metric> _asyncQueueMemMetric;
    std::shared_ptr<indexlib::util::Metric> _releaseQueueSizeMetric;

private:
    BS_LOG_DECLARE();
};

} // namespace build_service::builder
