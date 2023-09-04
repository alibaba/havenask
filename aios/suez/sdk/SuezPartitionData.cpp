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
#include "suez/sdk/SuezPartitionData.h"

#include "alog/Logger.h"
#include "autil/Log.h"
#include "autil/RangeUtil.h"
#include "autil/legacy/legacy_jsonizable.h"

using namespace std;
using namespace autil::legacy;

namespace suez {

AUTIL_DECLARE_AND_SETUP_LOGGER(suez, SuezPartitionData);

SuezPartitionData::SuezPartitionData(const PartitionId &pid, SuezPartitionType type) : _pid(pid), _type(type) {}

SuezPartitionData::~SuezPartitionData() {
    if (_releaseFn) {
        _releaseFn();
    }
}

const PartitionId &SuezPartitionData::getPartitionId() const { return _pid; }

bool SuezPartitionData::calcPartitionIndex(const PartitionId &pid, uint32_t maxPartCount, uint32_t &partId) {
    partId = 0;
    const auto &rangeVec =
        autil::RangeUtil::splitRange(0, autil::RangeUtil::MAX_PARTITION_RANGE, (uint32_t)maxPartCount);
    for (; partId < rangeVec.size(); partId++) {
        if (pid.from == rangeVec[partId].first && pid.to == rangeVec[partId].second) {
            return true;
        }
    }
    return false;
}

SuezPartitionType SuezPartitionData::type() const { return _type; }

void SuezPartitionData::setOnRelease(std::function<void()> callback) { _releaseFn = std::move(callback); }

bool SuezPartitionData::operator==(const SuezPartitionData &other) const {
    if (this == &other) {
        return true;
    }
    return _pid == other._pid && _type == other._type && detailEqual(other);
}

bool SuezPartitionData::operator!=(const SuezPartitionData &other) const { return !(*this == other); }

bool SuezPartitionData::detailEqual(const SuezPartitionData &other) const { return true; }

} // namespace suez
