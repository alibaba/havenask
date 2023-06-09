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
#include "ha3/common/SwiftPartitionUtil.h"

#include <limits>

#include "ha3/proto/BasicDefs.pb.h"
#include "autil/Log.h"

using namespace isearch::proto;

namespace isearch {
namespace common {
AUTIL_LOG_SETUP(ha3, SwiftPartitionUtil);

SwiftPartitionUtil::SwiftPartitionUtil() { 
}

SwiftPartitionUtil::~SwiftPartitionUtil() { 
}

bool SwiftPartitionUtil::rangeToSwiftPartition(
        const Range &range, uint32_t &swiftPartitionId) 
{
    if (range.from() != range.to()) {
        return false;
    }
    swiftPartitionId = range.from();
    return true;
}

bool SwiftPartitionUtil::swiftPartitionToRange(
        uint32_t swiftPartitionId, Range &range)
{
    if (swiftPartitionId > std::numeric_limits<uint16_t>::max()) {
        return false;
    }
    range.set_from(swiftPartitionId);
    range.set_to(swiftPartitionId);
    return true;
}

} // namespace common
} // namespace isearch
