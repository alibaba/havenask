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
#include "sql/resource/WatermarkR.h"

#include "build_service/util/LocatorUtil.h"
#include "indexlib/framework/ITablet.h"
#include "indexlib/framework/TabletInfos.h"
#include "kmonitor/client/MetricsReporter.h"
#include "navi/log/NaviLogger.h"
#include "sql/common/Log.h"
#include "sql/common/common.h"

using namespace kmonitor;

namespace sql {

class WatermarkWaitMetrics : public kmonitor::MetricsGroup {
public:
    bool init(kmonitor::MetricsGroupManager *manager) override {
        REGISTER_QPS_MUTABLE_METRIC(_qps, "watermark.qps");
        REGISTER_LATENCY_MUTABLE_METRIC(_latency, "watermark.latency");
        REGISTER_QPS_MUTABLE_METRIC(_failedQps, "watermark.failedQps");
        return true;
    }

    void report(const kmonitor::MetricsTags *tags, const WatermarkR::MetricsCollector *metrics) {
        REPORT_MUTABLE_QPS(_qps);
        REPORT_MUTABLE_METRIC(_latency, metrics->waitWatermarkTime / 1000.0f);
        if (metrics->waitWatermarkFailed) {
            REPORT_MUTABLE_QPS(_failedQps);
        }
    }

private:
    MutableMetric *_qps = nullptr;
    MutableMetric *_latency = nullptr;
    MutableMetric *_failedQps = nullptr;
};

const std::string WatermarkR::RESOURCE_ID = "watermark_r";

WatermarkR::WatermarkR()
    : _targetWatermark(0)
    , _targetWatermarkType(WatermarkType::WM_TYPE_DISABLED)
    , _logger(navi::NAVI_TLS_LOGGER ? *navi::NAVI_TLS_LOGGER : navi::NaviObjectLogger()) {}

WatermarkR::~WatermarkR() {}

void WatermarkR::def(navi::ResourceDefBuilder &builder) const {
    builder.name(RESOURCE_ID, navi::RS_KERNEL);
}

bool WatermarkR::config(navi::ResourceConfigContext &ctx) {
    NAVI_JSONIZE(ctx, "table_name", _tableName);
    NAVI_JSONIZE(ctx, SCAN_TARGET_WATERMARK, _targetWatermark, _targetWatermark);
    NAVI_JSONIZE(ctx, SCAN_TARGET_WATERMARK_TYPE, _targetWatermarkType, _targetWatermarkType);
    if (_targetWatermarkType == WatermarkType::WM_TYPE_SYSTEM_TS && _targetWatermark == 0) {
        // use searcher timestamp
        _targetWatermark = autil::TimeUtility::currentTime();
    }
    return true;
}

navi::ErrorCode WatermarkR::init(navi::ResourceInitContext &ctx) {
    _tablet = _tabletManagerR->getTablet(_tableName); // maybe nullptr for v1 search
    SQL_LOG(DEBUG,
            "init watermarkR with tableName[%s], targetWatermark[%ld], targetWatermarkType[%d], "
            "tablet[%p], leftTime[%ld]",
            _tableName.c_str(),
            _targetWatermark,
            _targetWatermarkType,
            _tablet.get(),
            _timeoutTerminatorR->getTimeoutTerminator()->getLeftTime());
    if (WatermarkType::WM_TYPE_DISABLED == _targetWatermarkType) {
        SQL_LOG(TRACE3, "get tablet use sync mode without watermark value");
    } else {
        if (_tablet) {
            return initAsync(ctx);
        } else {
            SQL_LOG(ERROR,
                    "tablet [%s] not exist, can't use watermark type [%d]",
                    _tableName.c_str(),
                    _targetWatermarkType);
            return navi::EC_ABORT;
        }
    }
    return navi::EC_NONE;
}

navi::ErrorCode WatermarkR::initAsync(navi::ResourceInitContext &ctx) {
    _asyncPipe = ctx.createRequireKernelAsyncPipe();
    if (!_asyncPipe) {
        SQL_LOG(ERROR, "create async pipe failed");
        return navi::EC_ABORT;
    }
    auto targetWatermarkTimeoutRatio
        = _sqlConfigResource->getSqlConfig().targetWatermarkTimeoutRatio;
    auto leftTime = _timeoutTerminatorR->getTimeoutTerminator()->getLeftTime();
    autil::ScopedTime2 timer;
    auto done
        = [thisPtr = shared_from_this(), timer_ = std::move(timer)](
              autil::result::Result<std::shared_ptr<indexlibv2::framework::ITablet>> res) mutable {
              auto *watermarkR = dynamic_cast<WatermarkR *>(thisPtr.get());
              assert(watermarkR);
              watermarkR->onWaitTabletCallback(std::move(res), timer_.done_us());
              (void)watermarkR->_asyncPipe->setData(nullptr);
          };
    if (_targetWatermarkType == WatermarkType::WM_TYPE_SYSTEM_TS) {
        SQL_LOG(TRACE3, "wait tablet by target ts");
        _tabletManagerR->waitTabletByTargetTs(
            _tableName, _targetWatermark, std::move(done), leftTime * targetWatermarkTimeoutRatio);
    } else if (_targetWatermarkType == WatermarkType::WM_TYPE_MANUAL) {
        SQL_LOG(TRACE3, "wait tablet by manual watermark");
        _tabletManagerR->waitTabletByWatermark(
            _tableName, _targetWatermark, std::move(done), leftTime * targetWatermarkTimeoutRatio);
    } else {
        assert(false && "invalid watermark type");
    }
    return navi::EC_NONE;
}

void WatermarkR::onWaitTabletCallback(
    autil::result::Result<std::shared_ptr<indexlibv2::framework::ITablet>> res,
    int64_t waitWatermarkTime) {
    SQL_LOG(TRACE3, "wait tablet returned, latency[%ld]", waitWatermarkTime);
    _metricsCollector.waitWatermarkTime = waitWatermarkTime;
    if (res.is_ok()) {
        _tablet = std::move(res).steal_value();
    } else {
        NAVI_INTERVAL_LOG(32,
                          WARN,
                          "wait tablet failed for tableName[%s] error[%s] waitWatermarkTime[%ld]",
                          _tableName.c_str(),
                          res.get_error().message().c_str(),
                          waitWatermarkTime);
        SQL_LOG(DEBUG,
                "wait tablet by target ts failed, tableName[%s] error[%s] "
                "waitWatermarkTime[%ld], do degraded search",
                _tableName.c_str(),
                res.get_error().message().c_str(),
                waitWatermarkTime);
        _metricsCollector.waitWatermarkFailed = true;
    }
    assert(_tablet != nullptr && "invalid tablet");
}

void WatermarkR::tryReportMetrics(kmonitor::MetricsReporter &opMetricsReporter) const {
    if (_asyncPipe) {
        opMetricsReporter.report<WatermarkWaitMetrics>(nullptr, &_metricsCollector);
    }
}

int64_t WatermarkR::getWaitWatermarkTime() const {
    return _metricsCollector.waitWatermarkTime;
}

int64_t WatermarkR::getBuildWatermark() const {
    return _tablet ? build_service::util::LocatorUtil::getSwiftWatermark(
               _tablet->GetTabletInfos()->GetLatestLocator())
                   : 0;
}

bool WatermarkR::waitFailed() const {
    return _metricsCollector.waitWatermarkFailed;
}

REGISTER_RESOURCE(WatermarkR);

} // namespace sql
