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

#include "autil/Log.h" // IWYU pragma: keep
#include "autil/result/Result.h"

namespace indexlibv2::framework {
class ITablet;
} // namespace indexlibv2::framework

namespace isearch {
namespace sql {

struct TabletWaiterInitOption;

class TabletWaiterBase {
protected:
    typedef std::function<void(
        autil::result::Result<std::shared_ptr<indexlibv2::framework::ITablet>>)>
        CallbackFunc;

public:
    TabletWaiterBase(std::shared_ptr<indexlibv2::framework::ITablet> tablet);
    virtual ~TabletWaiterBase();
    TabletWaiterBase(const TabletWaiterBase &) = delete;
    TabletWaiterBase &operator=(const TabletWaiterBase &) = delete;

public:
    virtual bool init(const TabletWaiterInitOption &option) = 0;
    virtual void waitTabletByWatermark(int64_t watermark, CallbackFunc cb, int64_t timeoutUs) = 0;
    virtual void waitTabletByTargetTs(int64_t targetTs, CallbackFunc cb, int64_t timeoutUs) = 0;
    std::shared_ptr<indexlibv2::framework::ITablet> getTablet() {
        return _tablet;
    }

protected:
    std::shared_ptr<indexlibv2::framework::ITablet> _tablet;
};

class TabletWaiterNoWatermark : public TabletWaiterBase {
public:
    TabletWaiterNoWatermark(std::shared_ptr<indexlibv2::framework::ITablet> tablet);
    ~TabletWaiterNoWatermark();

public:
    bool init(const TabletWaiterInitOption &option) override {
        return true;
    }
    void waitTabletByWatermark(int64_t watermark, CallbackFunc cb, int64_t timeoutUs) override;
    void waitTabletByTargetTs(int64_t targetTs, CallbackFunc cb, int64_t timeoutUs) override;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace sql
} // namespace isearch
