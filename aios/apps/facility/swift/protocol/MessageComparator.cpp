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
#include "swift/protocol/MessageComparator.h"

#include <algorithm>
#include <assert.h>
#include <string>

#include "swift/protocol/Common.pb.h"
#include "swift/protocol/Heartbeat.pb.h"
#include "swift/protocol/SwiftMessage.pb.h"

namespace swift {
namespace protocol {

template <typename T>
struct ProtoMemberEqual {
    bool operator()(const T &lhs, const T &rhs) const { return lhs == rhs; }
};

#define PROTO_MEMBER_EQUAL_HELPER(member)                                                                              \
    (lhs.has_##member() == rhs.has_##member() && (!lhs.has_##member() || lhs.member() == rhs.member()))

#define PROTO_MEMBER_LESS_HELPER(member)                                                                               \
    if (!PROTO_MEMBER_EQUAL_HELPER(member)) {                                                                          \
        if (lhs.has_##member()) {                                                                                      \
            return rhs.has_##member() && lhs.member() < rhs.member();                                                  \
        } else {                                                                                                       \
            return true;                                                                                               \
        }                                                                                                              \
    }

#define PROTO_REPEATED_MEMBER_EQUAL_HELPER(member)                                                                     \
    (lhs.member##_size() == rhs.member##_size() && std::equal(lhs.member().begin(),                                    \
                                                              lhs.member().end(),                                      \
                                                              rhs.member().begin(),                                    \
                                                              ProtoMemberEqual<decltype(*(lhs.member().begin()))>()))

bool operator==(const HeartbeatInfo &lhs, const HeartbeatInfo &rhs) {
    return PROTO_MEMBER_EQUAL_HELPER(alive) && PROTO_MEMBER_EQUAL_HELPER(role) && PROTO_MEMBER_EQUAL_HELPER(address) &&
           PROTO_REPEATED_MEMBER_EQUAL_HELPER(tasks) && PROTO_MEMBER_EQUAL_HELPER(errcode) &&
           PROTO_MEMBER_EQUAL_HELPER(errtime) && PROTO_MEMBER_EQUAL_HELPER(errmsg) &&
           PROTO_MEMBER_EQUAL_HELPER(sessionid);
    // heartbeatId change ignore
}
bool operator!=(const HeartbeatInfo &lhs, const HeartbeatInfo &rhs) { return !(lhs == rhs); }

bool operator==(const TaskStatus &lhs, const TaskStatus &rhs) {
    return PROTO_MEMBER_EQUAL_HELPER(taskinfo) && PROTO_MEMBER_EQUAL_HELPER(status) &&
           PROTO_MEMBER_EQUAL_HELPER(errcode) && PROTO_MEMBER_EQUAL_HELPER(errtime) &&
           PROTO_MEMBER_EQUAL_HELPER(errmsg) && PROTO_MEMBER_EQUAL_HELPER(sessionid);
}
bool operator!=(const TaskStatus &lhs, const TaskStatus &rhs) { return !(lhs == rhs); }

bool operator==(const TaskInfo &lhs, const TaskInfo &rhs) {
    return PROTO_MEMBER_EQUAL_HELPER(partitionid) && PROTO_MEMBER_EQUAL_HELPER(minbuffersize) &&
           PROTO_MEMBER_EQUAL_HELPER(maxbuffersize) && PROTO_MEMBER_EQUAL_HELPER(topicmode);
}
bool operator!=(const TaskInfo &lhs, const TaskInfo &rhs) { return !(lhs == rhs); }

bool operator<(const TaskInfo &lhs, const TaskInfo &rhs) { return lhs.partitionid() < rhs.partitionid(); }

bool operator>(const TaskInfo &lhs, const TaskInfo &rhs) { return lhs.partitionid() > rhs.partitionid(); }

bool operator==(const InlineVersion &lhs, const InlineVersion &rhs) {
    return PROTO_MEMBER_EQUAL_HELPER(masterversion) && PROTO_MEMBER_EQUAL_HELPER(partversion);
}
bool operator!=(const InlineVersion &lhs, const InlineVersion &rhs) { return !(lhs == rhs); }
bool operator<(const InlineVersion &lhs, const InlineVersion &rhs) {
    if (lhs.masterversion() == rhs.masterversion()) {
        return lhs.partversion() < rhs.partversion();
    } else {
        return lhs.masterversion() < rhs.masterversion();
    }
}

bool operator==(const PartitionId &lhs, const PartitionId &rhs) {
    bool eq = PROTO_MEMBER_EQUAL_HELPER(topicname) && PROTO_MEMBER_EQUAL_HELPER(id) &&
              PROTO_MEMBER_EQUAL_HELPER(inlineversion);
    if (eq && lhs.has_version() && rhs.has_version()) {
        eq = PROTO_MEMBER_EQUAL_HELPER(version);
    }
    return eq;
}

bool operator!=(const PartitionId &lhs, const PartitionId &rhs) { return !(lhs == rhs); }

bool operator>(const PartitionId &lhs, const PartitionId &rhs) {
    assert(lhs.has_topicname());
    assert(rhs.has_topicname());
    assert(lhs.has_id());
    assert(rhs.has_id());
    if (lhs.topicname() == rhs.topicname()) {
        return lhs.id() > rhs.id();
    }
    return lhs.topicname() > rhs.topicname();
}

bool operator<(const PartitionId &lhs, const PartitionId &rhs) {
    assert(lhs.has_topicname());
    assert(rhs.has_topicname());
    assert(lhs.has_id());
    assert(rhs.has_id());
    if (lhs.topicname() == rhs.topicname()) {
        return lhs.id() < rhs.id();
    }
    return lhs.topicname() < rhs.topicname();
}

std::ostream &operator<<(std::ostream &os, const HeartbeatInfo &msg) { return os << msg.ShortDebugString(); }

bool operator==(const Message &lhs, const Message &rhs) {
    return PROTO_MEMBER_EQUAL_HELPER(msgid) && PROTO_MEMBER_EQUAL_HELPER(timestamp) &&
           PROTO_MEMBER_EQUAL_HELPER(data) && PROTO_MEMBER_EQUAL_HELPER(uint16payload) &&
           PROTO_MEMBER_EQUAL_HELPER(uint8maskpayload) && PROTO_MEMBER_EQUAL_HELPER(compress);
}

bool operator==(const LeaderInfo &lhs, const LeaderInfo &rhs) {
    return PROTO_MEMBER_EQUAL_HELPER(address) && PROTO_MEMBER_EQUAL_HELPER(sysstop) &&
           PROTO_MEMBER_EQUAL_HELPER(starttimesec) && PROTO_REPEATED_MEMBER_EQUAL_HELPER(admininfos) &&
           PROTO_MEMBER_EQUAL_HELPER(selfmasterversion);
}

bool operator==(const AdminInfo &lhs, const AdminInfo &rhs) {
    return PROTO_MEMBER_EQUAL_HELPER(address) && PROTO_MEMBER_EQUAL_HELPER(isprimary) &&
           PROTO_MEMBER_EQUAL_HELPER(isalive);
}

bool operator==(const RoleVersionInfos &lhs, const RoleVersionInfos &rhs) {
    return PROTO_REPEATED_MEMBER_EQUAL_HELPER(versioninfos);
}

bool operator==(const RoleVersionInfo &lhs, const RoleVersionInfo &rhs) {
    return PROTO_MEMBER_EQUAL_HELPER(rolename) && PROTO_MEMBER_EQUAL_HELPER(versioninfo);
}

bool operator==(const BrokerVersionInfo &lhs, const BrokerVersionInfo &rhs) {
    return PROTO_MEMBER_EQUAL_HELPER(version) && PROTO_MEMBER_EQUAL_HELPER(supportfb);
}

} // namespace protocol
} // namespace swift
