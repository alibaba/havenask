#include "swift/admin/test/MessageConstructor.h"

#include <assert.h>
#include <cstddef>

#include "swift/protocol/Common.pb.h"
#include "swift/protocol/Heartbeat.pb.h"
#include "swift/protocol/test/MessageCreator.h"

namespace swift {
namespace admin {
AUTIL_LOG_SETUP(swift, MessageConstructor);

using namespace std;
using namespace swift::protocol;

MessageConstructor::MessageConstructor() {}

MessageConstructor::~MessageConstructor() {}

HeartbeatInfo MessageConstructor::ConstructHeartbeatInfo(const string &address,
                                                         RoleType role,
                                                         bool alive,
                                                         const string &topicName,
                                                         uint32_t partition,
                                                         PartitionStatus status,
                                                         int64_t sessionId) {
    HeartbeatInfo heartbeatInfo;
    heartbeatInfo.set_address(address);
    heartbeatInfo.set_role(role);
    heartbeatInfo.set_alive(alive);
    if (topicName != string("")) {
        addTask(topicName, partition, status, sessionId, heartbeatInfo);
    }
    return heartbeatInfo;
}

HeartbeatInfo MessageConstructor::ConstructHeartbeatInfo(const string &address,
                                                         RoleType role,
                                                         bool alive,
                                                         const vector<string> &topicNames,
                                                         const vector<uint32_t> &partitions,
                                                         const vector<PartitionStatus> &status,
                                                         const vector<int64_t> &sessionIds) {
    assert(topicNames.size() == partitions.size());
    assert(topicNames.size() == status.size());
    HeartbeatInfo heartbeatInfo;
    heartbeatInfo.set_address(address);
    heartbeatInfo.set_role(role);
    heartbeatInfo.set_alive(alive);
    for (size_t i = 0; i < topicNames.size(); ++i) {
        addTask(topicNames[i], partitions[i], status[i], sessionIds[i], heartbeatInfo);
    }
    return heartbeatInfo;
}

HeartbeatInfo MessageConstructor::ConstructHeartbeatInfo(const string &address,
                                                         RoleType role,
                                                         bool alive,
                                                         const vector<string> &topicNames,
                                                         const vector<uint32_t> &partitions,
                                                         const vector<PartitionStatus> &status,
                                                         const vector<int64_t> &sessionIds,
                                                         const vector<int64_t> &mergeTimestamps) {
    assert(topicNames.size() == partitions.size());
    assert(topicNames.size() == status.size());
    HeartbeatInfo heartbeatInfo;
    heartbeatInfo.set_address(address);
    heartbeatInfo.set_role(role);
    heartbeatInfo.set_alive(alive);
    for (size_t i = 0; i < topicNames.size(); ++i) {
        addTask(topicNames[i], partitions[i], status[i], sessionIds[i], heartbeatInfo, mergeTimestamps[i]);
    }
    return heartbeatInfo;
}

void MessageConstructor::addTask(const string &topicName,
                                 uint32_t partition,
                                 const PartitionStatus status,
                                 int64_t sessionId,
                                 HeartbeatInfo &heartbeatInfo,
                                 int64_t mergeTimestamp) {
    TaskStatus *task = heartbeatInfo.add_tasks();
    *(task->mutable_taskinfo()->mutable_partitionid()) = MessageCreator::createPartitionId(topicName, partition);
    task->set_status(status);
    task->set_sessionid(sessionId);
    task->mutable_taskinfo()->set_mergetimestamp(mergeTimestamp);
}

} // namespace admin
} // namespace swift
