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
#include <string>

#include "swift/common/Common.h"
#include "swift/common/PathDefine.h"
#include "swift/protocol/Common.pb.h"

namespace swift {
namespace admin {

class RoleInfo {
public:
    RoleInfo() : port(0), roleType(protocol::ROLE_TYPE_BROKER) {}
    RoleInfo(const std::string &name,
             const std::string &ip,
             uint16_t port,
             protocol::RoleType roleType = protocol::ROLE_TYPE_BROKER)
        : roleName(name), ip(ip), port(port), roleType(roleType) {}
    std::string getAddress() const { return common::PathDefine::getAddress(ip, port); }
    std::string getRoleAddress() const { return common::PathDefine::getRoleAddress(roleName, ip, port); }
    std::string getGroupName() const {
        std::string groupName, name;
        common::PathDefine::parseRoleGroup(roleName, groupName, name);
        return groupName;
    }

public:
    std::string roleName;
    std::string ip;
    uint16_t port;
    protocol::RoleType roleType;
};
SWIFT_TYPEDEF_PTR(RoleInfo);

} // namespace admin
} // namespace swift
