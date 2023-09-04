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

#include "ha3/proto/BasicDefs.pb.h"

namespace isearch {
namespace proto {

bool operator<(const Range &lhs, const Range &rhs);
bool operator<(const PartitionID &lhs, const PartitionID &rhs);
inline bool operator>(const PartitionID &lhs, const PartitionID &rhs) {
    return rhs < lhs;
}

bool operator==(const PartitionID &lhs, const PartitionID &rhs);
bool operator!=(const PartitionID &lhs, const PartitionID &rhs);
bool operator==(const Range &lhs, const Range &rhs);
bool operator!=(const Range &lhs, const Range &rhs);

} // namespace proto
} // namespace isearch
