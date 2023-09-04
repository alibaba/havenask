#pragma once

#include <stdint.h>
#include <string>
#include <vector>

#include "autil/Log.h"
#include "swift/common/Common.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/Heartbeat.pb.h"

namespace swift {
namespace admin {

class MessageConstructor {
public:
    MessageConstructor();
    ~MessageConstructor();

private:
    MessageConstructor(const MessageConstructor &);
    MessageConstructor &operator=(const MessageConstructor &);

public:
    static protocol::HeartbeatInfo
    ConstructHeartbeatInfo(const std::string &address,
                           protocol::RoleType role = protocol::ROLE_TYPE_BROKER,
                           bool alive = true,
                           const std::string &topicName = std::string(""),
                           uint32_t partition = 0,
                           protocol::PartitionStatus status = protocol::PARTITION_STATUS_WAITING,
                           int64_t sessionId = -1);
    static protocol::HeartbeatInfo ConstructHeartbeatInfo(const std::string &address,
                                                          protocol::RoleType role,
                                                          bool alive,
                                                          const std::vector<std::string> &topicNames,
                                                          const std::vector<uint32_t> &partitions,
                                                          const std::vector<protocol::PartitionStatus> &status,
                                                          const std::vector<int64_t> &sessionIds);
    static protocol::HeartbeatInfo ConstructHeartbeatInfo(const std::string &address,
                                                          protocol::RoleType role,
                                                          bool alive,
                                                          const std::vector<std::string> &topicNames,
                                                          const std::vector<uint32_t> &partitions,
                                                          const std::vector<protocol::PartitionStatus> &status,
                                                          const std::vector<int64_t> &sessionIds,
                                                          const std::vector<int64_t> &mergeTimestamps);

private:
    static void addTask(const std::string &topicName,
                        uint32_t partition,
                        const protocol::PartitionStatus status,
                        int64_t sessionId,
                        protocol::HeartbeatInfo &heartbeatInfo,
                        int64_t mergeTimestamp = 0);

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(MessageConstructor);

} // namespace admin
} // namespace swift
