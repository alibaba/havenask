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

#include <cstdint>
#include <memory>
#include <optional>
#include <stddef.h>

#include "autil/Lock.h"

namespace swift {
namespace protocol {
class PartitionId;
} // namespace protocol

namespace replication {

class MessageConsumer;
class MessageProducer;

class PartitionReplicator {
public:
    enum State {
        UNKNOWN = 0,
        LEADER,
        FOLLOWER
    };
    static const char *State_Name(State s);

public:
    PartitionReplicator(const protocol::PartitionId *pid,
                        std::unique_ptr<MessageProducer> producer,
                        std::unique_ptr<MessageConsumer> consumer,
                        int64_t committedId = -1);
    ~PartitionReplicator();

public:
    void work();
    int64_t globalId() const;
    bool canServe() const;
    bool canWrite() const;
    uint64_t selfVersion() const;

private:
    void negotiate();
    void leaderRecover();
    void followerCatchup();

    State computeState() const;

    void runPipeline(uint32_t count, bool applyData);
    void maybeUpdateMirrorId(uint64_t mirrorVersion);
    void maybeUpdateMirrorCommittedId(int64_t committedId);

    void updateState(int64_t mirrorId, int64_t mirrorCommittedId, int64_t latestAppliedId);

private:
    const protocol::PartitionId *_pid = nullptr;
    std::unique_ptr<MessageProducer> _producer;
    std::unique_ptr<MessageConsumer> _consumer;
    int64_t _committedId;
    uint64_t _mirrorVersion = 0;
    State _current;
    std::optional<int64_t> _maxCommittedIdOfMirror;
    mutable autil::ReadWriteLock _lock;

private:
    static constexpr size_t MSG_COUNT_PER_REQUEST = 1000;
};

} // namespace replication
} // namespace swift
