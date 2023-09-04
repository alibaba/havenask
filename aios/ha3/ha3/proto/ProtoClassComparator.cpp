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
#include "ha3/proto/ProtoClassComparator.h"

#include <string>

#include "ha3/proto/BasicDefs.pb.h"

namespace isearch {
namespace proto {

template <typename T>
struct ProtoMemberEqual {
    bool operator()(const T &lhs, const T &rhs) const {
        return lhs == rhs;
    }
};

#define PROTO_MEMBER_EQUAL_HELPER(member)                                                          \
    (lhs.has_##member() == rhs.has_##member()                                                      \
     && (!lhs.has_##member() || lhs.member() == rhs.member()))

#define PROTO_MEMBER_LESS_HELPER(member)                                                           \
    if (!PROTO_MEMBER_EQUAL_HELPER(member)) {                                                      \
        if (lhs.has_##member()) {                                                                  \
            return rhs.has_##member() && lhs.member() < rhs.member();                              \
        } else {                                                                                   \
            return true;                                                                           \
        }                                                                                          \
    }

bool operator<(const Range &lhs, const Range &rhs) {
    PROTO_MEMBER_LESS_HELPER(from);
    PROTO_MEMBER_LESS_HELPER(to);
    return false;
}

bool operator<(const PartitionID &lhs, const PartitionID &rhs) {
    PROTO_MEMBER_LESS_HELPER(clustername);
    PROTO_MEMBER_LESS_HELPER(majorconfigversion);
    PROTO_MEMBER_LESS_HELPER(role);
    PROTO_MEMBER_LESS_HELPER(fullversion);
    PROTO_MEMBER_LESS_HELPER(range);
    return false;
}

#define PROTO_REPEATED_MEMBER_EQUAL_HELPER(member)                                                 \
    (lhs.member##_size() == rhs.member##_size()                                                    \
     && std::equal(lhs.member().begin(),                                                           \
                   lhs.member().end(),                                                             \
                   rhs.member().begin(),                                                           \
                   ProtoMemberEqual<decltype(*(lhs.member().begin()))>()))

bool operator==(const PartitionID &lhs, const PartitionID &rhs) {
    return PROTO_MEMBER_EQUAL_HELPER(clustername) && PROTO_MEMBER_EQUAL_HELPER(majorconfigversion)
           && PROTO_MEMBER_EQUAL_HELPER(role) && PROTO_MEMBER_EQUAL_HELPER(fullversion)
           && PROTO_MEMBER_EQUAL_HELPER(range);
}

bool operator!=(const PartitionID &lhs, const PartitionID &rhs) {
    return !(lhs == rhs);
}

bool operator==(const Range &lhs, const Range &rhs) {
    return PROTO_MEMBER_EQUAL_HELPER(from) && PROTO_MEMBER_EQUAL_HELPER(to);
}

bool operator!=(const Range &lhs, const Range &rhs) {
    return !(lhs == rhs);
}

} // namespace proto
} // namespace isearch
