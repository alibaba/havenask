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

#include "ha3/sql/resource/watermark/TabletWaiterBase.h"

namespace indexlibv2::framework {
class ITablet;
} // namespace indexlibv2::framework

namespace isearch {
namespace sql {

class WatermarkWaiter;
class TimestampTransformer;
struct TabletWaiterInitOption;

class TabletWaiter : public TabletWaiterBase {
public:
    TabletWaiter(std::shared_ptr<indexlibv2::framework::ITablet> tablet);
    ~TabletWaiter();
    TabletWaiter(const TabletWaiter &) = delete;
    TabletWaiter& operator=(const TabletWaiter &) = delete;
public:
    bool init(const TabletWaiterInitOption &option) override;
    void waitTabletByWatermark(int64_t watermark, CallbackFunc cb, int64_t timeoutUs) override;
    void waitTabletByTargetTs(int64_t targetTs, CallbackFunc cb, int64_t timeoutUs) override;

private:
    bool initTimestampTransformer(const TabletWaiterInitOption &option);
    bool initWatermarkWaiter(const TabletWaiterInitOption &option);

private:
    std::unique_ptr<WatermarkWaiter> _watermarkWaiter;
    std::unique_ptr<TimestampTransformer> _timestampTransformer;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<TabletWaiter> TabletWaiterPtr;

} // namespace sql
} // namespace isearch
