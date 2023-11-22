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
#include <optional>
#include <stdint.h>
#include <string>
#include <utility>

#include "build_service/builder/BuilderV2.h"
#include "build_service/config/BuilderConfig.h"
#include "build_service/config/ResourceReader.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/util/Log.h"
#include "future_lite/Executor.h"
#include "indexlib/base/Status.h"
#include "indexlib/config/ITabletSchema.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/framework/ITabletMergeController.h"
#include "indexlib/framework/Locator.h"
#include "indexlib/framework/VersionCoord.h"

namespace future_lite {
class TaskScheduler;
} // namespace future_lite

namespace build_service::builder {
class BuilderV2Impl;

class OfflineBuilderV2 : public BuilderV2
{
public:
    OfflineBuilderV2(const std::string& clusterName, const proto::PartitionId& partitionId,
                     const config::ResourceReaderPtr& resourceReader, const std::string& indexRoot);
    ~OfflineBuilderV2();

public:
    bool init(const config::BuilderConfig& builderConfig,
              std::shared_ptr<indexlib::util::MetricProvider> metricProvider = nullptr) override;
    bool build(const std::shared_ptr<indexlibv2::document::IDocumentBatch>& batch) override;
    bool merge() override;
    // specify stopTimestamp if eof encountered
    void stop(std::optional<int64_t> stopTimestamp, bool needSeal, bool immediately) override;
    int64_t getIncVersionTimestamp() const override; // for realtime build
    indexlibv2::framework::Locator getLastLocator() const override;
    indexlibv2::framework::Locator getLatestVersionLocator() const override;
    indexlibv2::framework::Locator getLastFlushedLocator() const override;
    bool hasFatalError() const override;
    bool needReconstruct() const override;
    bool isSealed() const override;
    void notifyStop() override {}
    // virtual for test
    void setFatalError() override;
    std::shared_ptr<indexlib::util::CounterMap> GetCounterMap() const override;
    void switchToConsistentMode() override;

private:
    std::unique_ptr<future_lite::Executor> createExecutor(const std::string& executorName, uint32_t threadCount) const;
    std::shared_ptr<indexlibv2::framework::ITabletMergeController>
    createMergeController(const std::shared_ptr<indexlibv2::config::ITabletSchema>& schema,
                          const std::shared_ptr<indexlibv2::config::TabletOptions>& options);
    std::pair<indexlib::Status, indexlibv2::framework::VersionCoord> getLatestVersion() const;

private:
    std::string _clusterName;
    config::ResourceReaderPtr _resourceReader;
    std::string _indexRoot;

    // resource for Tablet
    proto::PartitionId _partitionId;
    std::unique_ptr<future_lite::Executor> _executor;
    std::unique_ptr<future_lite::Executor> _localMergeExecutor;
    std::unique_ptr<future_lite::TaskScheduler> _taskScheduler;
    std::shared_ptr<indexlib::util::MetricProvider> _metricProvider;
    std::unique_ptr<BuilderV2Impl> _impl;

private:
    BS_LOG_DECLARE();
};

} // namespace build_service::builder
