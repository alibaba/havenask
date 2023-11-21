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

#include "navi/engine/Resource.h"
#include "sql/common/WatermarkType.h"
#include "sql/resource/SqlConfigResource.h"
#include "sql/resource/TabletManagerR.h"
#include "sql/resource/TimeoutTerminatorR.h"

namespace indexlibv2::framework {
class ITablet;
} // namespace indexlibv2::framework

namespace sql {

class WatermarkR : public navi::Resource {
public:
    WatermarkR();
    ~WatermarkR();
    WatermarkR(const WatermarkR &) = delete;
    WatermarkR &operator=(const WatermarkR &) = delete;

public:
    struct MetricsCollector {
        int64_t waitWatermarkTime = 0;
        int64_t buildWatermark = 0;
        bool waitWatermarkFailed = false;
    };

public:
    void def(navi::ResourceDefBuilder &builder) const override;
    bool config(navi::ResourceConfigContext &ctx) override;
    navi::ErrorCode init(navi::ResourceInitContext &ctx) override;

public:
    std::shared_ptr<indexlibv2::framework::ITablet> getTablet() const {
        return _tablet;
    }
    std::shared_ptr<navi::AsyncPipe> getAsyncPipe() const {
        return _asyncPipe;
    }
    int64_t getWaitWatermarkTime() const;
    int64_t getBuildWatermark() const;
    bool waitFailed() const;
    void tryReportMetrics(kmonitor::MetricsReporter &opMetricsReporter) const;

private:
    navi::ErrorCode initAsync(navi::ResourceInitContext &ctx);
    void
    onWaitTabletCallback(autil::result::Result<std::shared_ptr<indexlibv2::framework::ITablet>> res,
                         int64_t waitWatermarkTime);

public:
    static const std::string RESOURCE_ID;

private:
    RESOURCE_DEPEND_DECLARE();

private:
    RESOURCE_DEPEND_ON(TabletManagerR, _tabletManagerR);
    RESOURCE_DEPEND_ON(TimeoutTerminatorR, _timeoutTerminatorR);
    RESOURCE_DEPEND_ON(SqlConfigResource, _sqlConfigResource);
    int64_t _targetWatermark;
    WatermarkType _targetWatermarkType;
    std::string _tableName;
    std::shared_ptr<indexlibv2::framework::ITablet> _tablet;
    MetricsCollector _metricsCollector;
    std::shared_ptr<navi::AsyncPipe> _asyncPipe;

private:
    DECLARE_LOGGER();
};

} // namespace sql
