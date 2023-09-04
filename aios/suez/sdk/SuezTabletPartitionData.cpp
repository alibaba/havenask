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
#include "suez/sdk/SuezTabletPartitionData.h"

#include "build_service/util/LocatorUtil.h"
#include "indexlib/framework/ITablet.h"
#include "indexlib/framework/TabletInfos.h"

namespace suez {

SuezTabletPartitionData::SuezTabletPartitionData(const PartitionId &pid,
                                                 int32_t totalPartitionCount,
                                                 const std::shared_ptr<indexlibv2::framework::ITablet> &tablet,
                                                 bool hasRt)
    : SuezPartitionData(pid, SPT_TABLET), _tablet(tablet), _totalPartitionCount(totalPartitionCount), _hasRt(hasRt) {}

SuezTabletPartitionData::~SuezTabletPartitionData() {}

const std::shared_ptr<indexlibv2::framework::ITablet> &SuezTabletPartitionData::getTablet() const { return _tablet; }

int32_t SuezTabletPartitionData::getTotalPartitionCount() const { return _totalPartitionCount; }

bool SuezTabletPartitionData::getPartitionIndex(uint32_t &partId) const {
    return calcPartitionIndex(_pid, _totalPartitionCount, partId);
}

bool SuezTabletPartitionData::hasRt() const { return _hasRt; }

bool SuezTabletPartitionData::getDataTimestamp(int64_t &dataTimestamp) const {
    if (!_tablet || !_tablet->GetTabletInfos()) {
        return false;
    }
    auto latestLocator = _tablet->GetTabletInfos()->GetLatestLocator();
    dataTimestamp = build_service::util::LocatorUtil::GetSwiftWatermark(latestLocator);
    return true;
}

bool SuezTabletPartitionData::detailEqual(const SuezPartitionData &other) const {
    return _tablet.get() == static_cast<const SuezTabletPartitionData &>(other)._tablet.get();
}

} // namespace suez
