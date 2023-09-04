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

#include "suez/sdk/SuezPartitionData.h"

namespace indexlibv2 {
namespace framework {
class ITablet;
}
} // namespace indexlibv2

namespace suez {

class SuezTabletPartitionData final : public SuezPartitionData {
public:
    SuezTabletPartitionData(const PartitionId &pid,
                            int32_t totalPartitionCount,
                            const std::shared_ptr<indexlibv2::framework::ITablet> &tablet,
                            bool hasRt);
    ~SuezTabletPartitionData();

public:
    const std::shared_ptr<indexlibv2::framework::ITablet> &getTablet() const;
    int32_t getTotalPartitionCount() const;
    bool getPartitionIndex(uint32_t &partId) const;
    bool hasRt() const;
    bool getDataTimestamp(int64_t &dataTimestamp) const;

private:
    bool detailEqual(const SuezPartitionData &other) const override;

private:
    std::shared_ptr<indexlibv2::framework::ITablet> _tablet;
    int32_t _totalPartitionCount;
    bool _hasRt;
};

} // namespace suez
