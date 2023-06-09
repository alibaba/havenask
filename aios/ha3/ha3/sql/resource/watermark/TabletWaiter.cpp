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
#include "ha3/sql/resource/watermark/TabletWaiter.h"

#include "ha3/sql/common/Log.h"
#include "ha3/sql/resource/watermark/TabletWaiterInitOption.h"
#include "ha3/sql/resource/watermark/TimestampTransformer.h"
#include "ha3/sql/resource/watermark/WatermarkWaiter.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/framework/ITablet.h"
#include "indexlib/framework/TabletInfos.h"

using namespace std;

namespace isearch {
namespace sql {

AUTIL_LOG_SETUP(sql, TabletWaiter);

TabletWaiter::TabletWaiter(std::shared_ptr<indexlibv2::framework::ITablet> tablet)
    : TabletWaiterBase(tablet) {}

TabletWaiter::~TabletWaiter() {
    _timestampTransformer.reset();
    _watermarkWaiter.reset();
}

bool TabletWaiter::init(const TabletWaiterInitOption &option) {
    if (!initTimestampTransformer(option)) {
        return false;
    }
    if (!initWatermarkWaiter(option)) {
        return false;
    }
    SQL_LOG(INFO,
            "init succeed for tabletName[%s]",
            _tablet->GetTabletInfos()->GetTabletName().c_str());
    return true;
}

void TabletWaiter::waitTabletByWatermark(int64_t watermark, CallbackFunc cb, int64_t timeoutUs) {
    _watermarkWaiter->wait(watermark, std::move(cb), timeoutUs);
}

void TabletWaiter::waitTabletByTargetTs(int64_t targetTs, CallbackFunc cb, int64_t timeoutUs) {
    autil::ScopedTime2 timer;
    auto done = [this, done_ = std::move(cb), timeoutUs, timer](
                    autil::Result<TimestampTransformerPack::CallbackParamT> res) {
        if (!res.is_ok()) {
            done_(std::move(res).steal_error());
            return;
        }
        auto value = std::move(res).steal_value();
        if (value == -1) {
            // no any swift message
            done_({_tablet});
        } else {
            _watermarkWaiter->wait(value, std::move(done_), timeoutUs - timer.done_us());
        }
    };
    _timestampTransformer->wait(targetTs, std::move(done), timeoutUs);
}

bool TabletWaiter::initTimestampTransformer(const TabletWaiterInitOption &option) {
    _timestampTransformer.reset(new TimestampTransformer(option.swiftClientCreator, option));
    if (!_timestampTransformer->init()) {
        SQL_LOG(ERROR, "TimestampTransformer init failed.");
        return false;
    }
    return true;
}

bool TabletWaiter::initWatermarkWaiter(const TabletWaiterInitOption &option) {
    auto srcSignature = option.realtimeInfo.getSrcSignature();
    uint64_t src = 0;
    if (!srcSignature.Get(&src)) {
        SQL_LOG(ERROR, "get src signature failed.");
        return false;
    }
    _watermarkWaiter.reset(new WatermarkWaiter(option.tablet, src));
    if (!_watermarkWaiter->init()) {
        SQL_LOG(ERROR, "WatermarkWaiter init failed.");
        return false;
    }
    return true;
}

} // namespace sql
} // namespace isearch
