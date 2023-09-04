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
#include "suez/sdk/SuezIndexPartitionData.h"

#include "indexlib/partition/index_partition_reader.h"

namespace suez {

SuezIndexPartitionData::SuezIndexPartitionData(const PartitionId &pid,
                                               int32_t totalPartitionCount,
                                               const indexlib::partition::IndexPartitionPtr &indexPartition,
                                               bool hasRt)
    : SuezPartitionData(pid, SPT_INDEXLIB)
    , _indexPartition(indexPartition)
    , _totalPartitionCount(totalPartitionCount)
    , _hasRt(hasRt) {}

SuezIndexPartitionData::~SuezIndexPartitionData() {}

const indexlib::partition::IndexPartitionPtr &SuezIndexPartitionData::getIndexPartition() const {
    return _indexPartition;
}

int32_t SuezIndexPartitionData::getTotalPartitionCount() const { return _totalPartitionCount; }

bool SuezIndexPartitionData::getPartitionIndex(uint32_t &partId) const {
    return calcPartitionIndex(_pid, _totalPartitionCount, partId);
}

bool SuezIndexPartitionData::hasRt() const { return _hasRt; }

bool SuezIndexPartitionData::getDataTimestamp(int64_t &dataTimestamp) const {
    if (!_indexPartition) {
        return false;
    }
    auto partitionReader = _indexPartition->GetReader();
    if (!partitionReader) {
        return false;
    }
    dataTimestamp = partitionReader->GetLatestDataTimestamp();
    return true;
}

bool SuezIndexPartitionData::detailEqual(const SuezPartitionData &other) const {
    return _indexPartition.get() == static_cast<const SuezIndexPartitionData &>(other)._indexPartition.get();
}

} // namespace suez
