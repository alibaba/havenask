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

#include <iosfwd>

namespace swift {
namespace protocol {
class AdminInfo;
class BrokerVersionInfo;
class HeartbeatInfo;
class LeaderInfo;
class Message;
class InlineVersion;
class PartitionId;
class RoleVersionInfo;
class RoleVersionInfos;
class TaskInfo;
class TaskStatus;

bool operator==(const HeartbeatInfo &lhs, const HeartbeatInfo &rhs);
bool operator!=(const HeartbeatInfo &lhs, const HeartbeatInfo &rhs);
bool operator==(const TaskStatus &lhs, const TaskStatus &rhs);
bool operator!=(const TaskStatus &lhs, const TaskStatus &rhs);

bool operator==(const TaskInfo &lhs, const TaskInfo &rhs);
bool operator!=(const TaskInfo &lhs, const TaskInfo &rhs);
bool operator<(const TaskInfo &lhs, const TaskInfo &rhs);
bool operator>(const TaskInfo &lhs, const TaskInfo &rhs);

bool operator==(const InlineVersion &lhs, const InlineVersion &rhs);
bool operator!=(const InlineVersion &lhs, const InlineVersion &rhs);
bool operator<(const InlineVersion &lhs, const InlineVersion &rhs);

bool operator==(const PartitionId &lhs, const PartitionId &rhs);
bool operator!=(const PartitionId &lhs, const PartitionId &rhs);
bool operator<(const PartitionId &lhs, const PartitionId &rhs);
bool operator>(const PartitionId &lhs, const PartitionId &rhs);

bool operator==(const Message &lhs, const Message &rhs);
bool operator==(const LeaderInfo &lhs, const LeaderInfo &rhs);
bool operator==(const AdminInfo &lhs, const AdminInfo &rhs);

bool operator==(const RoleVersionInfos &lhs, const RoleVersionInfos &rhs);
bool operator==(const RoleVersionInfo &lhs, const RoleVersionInfo &rhs);
bool operator==(const BrokerVersionInfo &lhs, const BrokerVersionInfo &rhs);

std::ostream &operator<<(std::ostream &os, const HeartbeatInfo &msg);

} // namespace protocol
} // namespace swift
