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
#include "hippo/proto/ProtoClassComparator.h"

using namespace std;

namespace hippo {
namespace proto {

#define PROTO_MEMBER_EQUAL_HELPER(member)                               \
    (lhs.has_##member() == rhs.has_##member()                           \
     && (!lhs.has_##member() || lhs.member() == rhs.member()))

#define PROTO_MEMBER_LESS_HELPER(member)                                \
    if (!PROTO_MEMBER_EQUAL_HELPER(member)) {                           \
        if (lhs.has_##member()) {                                       \
            return rhs.has_##member() && lhs.member() < rhs.member();   \
        } else {                                                        \
            return true;                                                \
        }                                                               \
    }

#define PROTO_REPEATED_MEMBER_EQUAL_HELPER(member)                      \
    (lhs.member##_size() == rhs.member##_size()                         \
     && std::equal(lhs.member().begin(),                                \
                   lhs.member().end(),                                  \
                   rhs.member().begin(),                                \
                   ProtoMemberEqual<decltype(*(lhs.member().begin()))>()))

bool operator<(const SlotId &lhs, const SlotId &rhs) {
    PROTO_MEMBER_LESS_HELPER(slaveaddress);
    PROTO_MEMBER_LESS_HELPER(id);
    return false;
}

bool operator==(const Resource &lhs, const Resource &rhs) {
    return PROTO_MEMBER_EQUAL_HELPER(type)
        && PROTO_MEMBER_EQUAL_HELPER(name)
        && PROTO_MEMBER_EQUAL_HELPER(amount);
}

bool operator!=(const Resource &lhs, const Resource &rhs) {
    return !(lhs == rhs);
}

bool operator==(const SlotResource &lhs, const SlotResource &rhs) {
    return PROTO_REPEATED_MEMBER_EQUAL_HELPER(resources);
}

bool operator==(const ProcessStatus &lhs, const ProcessStatus &rhs) {
    return PROTO_MEMBER_EQUAL_HELPER(isdaemon)
        && PROTO_MEMBER_EQUAL_HELPER(status)
        && PROTO_MEMBER_EQUAL_HELPER(processname)
        && PROTO_MEMBER_EQUAL_HELPER(restartcount)
        && PROTO_MEMBER_EQUAL_HELPER(starttime)
        && PROTO_MEMBER_EQUAL_HELPER(exitcode);
}

}
}
