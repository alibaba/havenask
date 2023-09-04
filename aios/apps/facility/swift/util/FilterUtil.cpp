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
#include "swift/util/FilterUtil.h"

#include <algorithm>
#include <string>

#include "swift/common/Common.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/SwiftMessage.pb.h"

namespace swift {
namespace util {

AUTIL_LOG_SETUP(swift, FilterUtil);

using namespace swift::common;

uint64_t FilterUtil::compressPayload(const swift::protocol::PartitionId &partitionId, const uint32_t payload) {
    if (!__builtin_expect(partitionId.from() <= payload && payload <= partitionId.to(), true)) {
        return 0lu;
    }
    uint32_t range = partitionId.to() - partitionId.from() + 1;
    uint32_t precision = (range + 64 - 1) / 64;
    uint32_t compressBit = (payload - partitionId.from()) / precision;
    AUTIL_LOG(DEBUG,
              "compress payload, partitionId [%s] payload [%d], compress payload [%lu]",
              partitionId.DebugString().c_str(),
              payload,
              (uint64_t)1 << compressBit);
    return (uint64_t)1 << compressBit;
}

uint64_t FilterUtil::compressFilterRegion(const swift::protocol::PartitionId &partitionId,
                                          const swift::protocol::Filter &filter) {
    if (!__builtin_expect(partitionId.to() >= filter.from(), true)) {
        return 0lu;
    }
    uint32_t range = partitionId.to() - partitionId.from() + 1;
    uint32_t precision = (range + 64 - 1) / 64;
    uint32_t from = std::max(partitionId.from(), filter.from());
    uint32_t to = std::min(partitionId.to(), filter.to());
    uint32_t lsh1 = (to - partitionId.from()) / precision;
    uint32_t lsh2 = (from - partitionId.from()) / precision;
    AUTIL_LOG(DEBUG,
              "compress region, partitionId [%s] filter [%s], compress region [%lu]",
              partitionId.DebugString().c_str(),
              filter.DebugString().c_str(),
              (~(UINT64_FULL_LSH_1 << (lsh1 - lsh2))) << lsh2);
    if (__builtin_expect(lsh1 >= lsh2, true)) {
        return (~(UINT64_FULL_LSH_1 << (lsh1 - lsh2))) << lsh2;
    }
    return 0lu;
}

} // namespace util
} // namespace swift
