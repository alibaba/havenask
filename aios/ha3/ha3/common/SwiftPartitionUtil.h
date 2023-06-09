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

#include <stdint.h>
#include <memory>

#include "autil/Log.h" // IWYU pragma: keep

namespace isearch {
namespace proto {
class Range;
}  // namespace proto
}  // namespace isearch

namespace isearch {
namespace common {

class SwiftPartitionUtil
{
public:
    SwiftPartitionUtil();
    ~SwiftPartitionUtil();
private:
    SwiftPartitionUtil(const SwiftPartitionUtil &);
    SwiftPartitionUtil& operator=(const SwiftPartitionUtil &);
public:
    static bool rangeToSwiftPartition(const proto::Range &range, uint32_t &swiftPartitionId);
    static bool swiftPartitionToRange(uint32_t swiftPartitionId, proto::Range &range);
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<SwiftPartitionUtil> SwiftPartitionUtilPtr;

} // namespace common
} // namespace isearch

