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

#include "build_service/config/BuilderConfig.h"
#include "build_service/proto/ErrorCollector.h"
#include "build_service/util/Log.h"
#include "indexlib/framework/Locator.h"

namespace indexlib::util {
class MetricProvider;
class CounterMap;
} // namespace indexlib::util

namespace indexlibv2::document {
class IDocumentBatch;
}

namespace build_service::builder {

class BuilderV2 : public proto::ErrorCollector
{
public:
    explicit BuilderV2(const proto::BuildId& buildId = proto::BuildId());
    virtual ~BuilderV2();
    BuilderV2(const BuilderV2&) = delete;
    BuilderV2& operator=(const BuilderV2&) = delete;

public:
    virtual bool init(const config::BuilderConfig& builderConfig,
                      std::shared_ptr<indexlib::util::MetricProvider> metricProvider = nullptr) = 0;
    virtual bool build(const std::shared_ptr<indexlibv2::document::IDocumentBatch>& batch) = 0;
    virtual bool merge() = 0;
    // specify stopTimestamp if eof encountered
    virtual void stop(std::optional<int64_t> stopTimestamp, bool needSeal, bool immediately) = 0;
    virtual void notifyStop() = 0;
    virtual int64_t getIncVersionTimestamp() const = 0; // for realtime build
    virtual indexlibv2::framework::Locator getLastLocator() const = 0;
    virtual indexlibv2::framework::Locator getLatestVersionLocator() const = 0;
    virtual indexlibv2::framework::Locator getLastFlushedLocator() const = 0;
    virtual bool hasFatalError() const = 0;
    virtual bool needReconstruct() const = 0;
    virtual bool isSealed() const = 0;
    // virtual for test
    virtual void setFatalError() = 0;
    virtual std::shared_ptr<indexlib::util::CounterMap> GetCounterMap() const = 0;
    virtual void switchToConsistentMode() = 0;

public:
    const proto::BuildId& getBuildId() const;

protected:
    proto::BuildId _buildId;
};

} // namespace build_service::builder
