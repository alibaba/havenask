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
#include "ha3/sql/resource/watermark/TabletWaiterBase.h"

#include "autil/result/Errors.h"
#include "ha3/sql/common/Log.h"
#include "indexlib/framework/ITablet.h"

namespace isearch {
namespace sql {

TabletWaiterBase::TabletWaiterBase(std::shared_ptr<indexlibv2::framework::ITablet> tablet)
    : _tablet(tablet) {}

TabletWaiterBase::~TabletWaiterBase() {}

AUTIL_LOG_SETUP(sql, TabletWaiterNoWatermark);

TabletWaiterNoWatermark::TabletWaiterNoWatermark(std::shared_ptr<indexlibv2::framework::ITablet> tablet)
    : TabletWaiterBase(tablet) {}

TabletWaiterNoWatermark::~TabletWaiterNoWatermark() {}

void TabletWaiterNoWatermark::waitTabletByWatermark(int64_t watermark, CallbackFunc cb, int64_t timeoutUs) {
    SQL_LOG(ERROR, "waiter is disabled, wait tablet by watermark failed");
    cb(autil::result::RuntimeError::make("waiter is disabled, wait tablet by watermark failed"));
}

void TabletWaiterNoWatermark::waitTabletByTargetTs(int64_t targetTs, CallbackFunc cb, int64_t timeoutUs) {
    SQL_LOG(ERROR, "waiter is disabled, wait tablet by watermark failed");
    cb(autil::result::RuntimeError::make("waiter is disabled, wait tablet by targetTs failed"));
}


} // namespace sql
} // namespace isearch
