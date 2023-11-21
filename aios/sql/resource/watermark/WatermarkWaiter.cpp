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
#include "sql/resource/watermark/WatermarkWaiter.h"

#include <iosfwd>
#include <utility>

#include "build_service/util/LocatorUtil.h"
#include "indexlib/framework/ITablet.h"
#include "indexlib/framework/Locator.h"
#include "indexlib/framework/TabletInfos.h"

using namespace std;

namespace sql {

AUTIL_LOG_SETUP(sql, WatermarkWaiter);

WatermarkWaiter::WatermarkWaiter(std::shared_ptr<indexlibv2::framework::ITablet> tablet,
                                 uint64_t srcSignature)
    : _tablet(std::move(tablet))
    , _srcSignature(srcSignature) {}

WatermarkWaiter::~WatermarkWaiter() {
    stop();
}

std::string WatermarkWaiter::getDesc() const {
    return _tablet->GetTabletInfos()->GetTabletName();
}

std::shared_ptr<indexlibv2::framework::ITablet> WatermarkWaiter::getCurrentStatusImpl() const {
    return _tablet;
}

int64_t WatermarkWaiter::transToKeyImpl(
    const std::shared_ptr<indexlibv2::framework::ITablet> &status) const {
    auto indexLocator = status->GetTabletInfos()->GetLatestLocator();
    bool hasRt = indexLocator.IsValid() && !indexLocator.GetUserData().empty()
                 && _srcSignature == indexLocator.GetSrc();
    if (hasRt) {
        return build_service::util::LocatorUtil::getSwiftWatermark(indexLocator);
    }
    return 0;
}

std::shared_ptr<indexlibv2::framework::ITablet> WatermarkWaiter::transToCallbackParamImpl(
    const std::shared_ptr<indexlibv2::framework::ITablet> &status) const {
    return status;
}

} // namespace sql
