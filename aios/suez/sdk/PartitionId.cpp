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
#include "suez/sdk/PartitionId.h"

#include "autil/RangeUtil.h"

namespace suez {

void PartitionId::setPartitionIndex(int32_t partCount_) {
    if (partCount_ > 0) {
        index = autil::RangeUtil::getRangeIdx(0, autil::RangeUtil::MAX_PARTITION_RANGE, partCount_, {from, to});
        partCount = partCount_;
    }
}

#define LESS_COMPARATOR_HELPER(member)                                                                                 \
    if (left.member != right.member) {                                                                                 \
        return left.member < right.member;                                                                             \
    }

bool operator<(const TableId &left, const TableId &right) {
    LESS_COMPARATOR_HELPER(tableName);
    LESS_COMPARATOR_HELPER(fullVersion);
    return false;
}

bool operator==(const TableId &left, const TableId &right) {
    return left.tableName == right.tableName && left.fullVersion == right.fullVersion;
}

bool operator!=(const TableId &left, const TableId &right) { return !(left == right); }

bool operator<(const PartitionId &left, const PartitionId &right) {
    LESS_COMPARATOR_HELPER(tableId);
    LESS_COMPARATOR_HELPER(from);
    LESS_COMPARATOR_HELPER(to);
    return false;
}

bool operator==(const PartitionId &left, const PartitionId &right) {
    return left.tableId == right.tableId && left.from == right.from && left.to == right.to;
}

bool operator!=(const PartitionId &left, const PartitionId &right) { return !(left == right); }

#undef LESS_COMPARATOR_HELPER

} // namespace suez
