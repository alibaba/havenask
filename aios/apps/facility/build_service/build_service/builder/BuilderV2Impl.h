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

#include <atomic>
#include <memory>
#include <mutex>
#include <optional>
#include <stdint.h>
#include <string>
#include <utility>

#include "build_service/builder/BuilderMetrics.h"
#include "build_service/builder/BuilderV2.h"
#include "build_service/config/BuilderConfig.h"
#include "build_service/proto/BasicDefs.pb.h"
#include "build_service/util/Log.h"
#include "indexlib/base/Status.h"
#include "indexlib/framework/CommitOptions.h"
#include "indexlib/framework/Locator.h"
#include "indexlib/framework/Version.h"
#include "indexlib/framework/VersionMeta.h"

namespace indexlib::util {
class AccumulativeCounter;
class StateCounter;
} // namespace indexlib::util

namespace indexlibv2::framework {
class ITablet;
} // namespace indexlibv2::framework

namespace build_service::builder {

class BuilderV2Impl : public BuilderV2
{
public:
    BuilderV2Impl(std::shared_ptr<indexlibv2::framework::ITablet> tablet,
                  const proto::BuildId& buildId = proto::BuildId());
    virtual ~BuilderV2Impl();
    BuilderV2Impl(const BuilderV2Impl&) = delete;
    BuilderV2Impl& operator=(const BuilderV2Impl&) = delete;

public:
    bool init(const config::BuilderConfig& builderConfig,
              std::shared_ptr<indexlib::util::MetricProvider> metricProvider = nullptr) override;
    bool build(const std::shared_ptr<indexlibv2::document::IDocumentBatch>& batch) override;
    bool merge() override;
    // specify stopTimestamp if eof encountered
    void stop(std::optional<int64_t> stopTimestamp, bool needSeal, bool immediately) override;
    void notifyStop() override {}
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

public:
    std::pair<indexlib::Status, indexlibv2::framework::VersionMeta>
    commit(const indexlibv2::framework::CommitOptions& options);
    void resetNeedReconstruct() { _needReconstruct.store(false); }

protected:
    // virtual for test
    virtual indexlib::Status doBuild(const std::shared_ptr<indexlibv2::document::IDocumentBatch>& batch);
    void setNeedReconstruct();
    void setSealed();

private:
    void ReportCounters(const std::shared_ptr<indexlibv2::document::IDocumentBatch>& batch);
    std::pair<indexlib::Status, indexlibv2::framework::Version> getLatestVersion() const;

protected:
    std::mutex _buildMutex;
    std::shared_ptr<indexlibv2::framework::ITablet> _tablet;
    std::shared_ptr<indexlib::util::AccumulativeCounter> _totalDocCountCounter;
    std::shared_ptr<indexlib::util::StateCounter> _builderDocCountCounter;

private:
    std::atomic<bool> _fatalError;
    std::atomic<bool> _needReconstruct;
    std::atomic<bool> _isSealed;
    std::string _lastPk;
    config::BuilderConfig _builderConfig;

private:
    BS_LOG_DECLARE();
};

} // namespace build_service::builder
