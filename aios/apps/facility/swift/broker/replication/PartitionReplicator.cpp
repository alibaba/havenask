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
#include "swift/broker/replication/PartitionReplicator.h"

#include <algorithm>
#include <assert.h>
#include <string>
#include <utility>

#include "autil/Log.h"
#include "autil/result/Result.h"
#include "swift/broker/replication/MessageConsumer.h"
#include "swift/broker/replication/MessageProducer.h"
#include "swift/protocol/BrokerRequestResponse.pb.h"
#include "swift/protocol/Common.pb.h"
#include "swift/protocol/ErrCode.pb.h"

namespace swift {
namespace replication {

AUTIL_DECLARE_AND_SETUP_LOGGER(swift, PartitionReplicator);

const char *PartitionReplicator::State_Name(State s) {
    switch (s) {
    case FOLLOWER:
        return "FOLLOWER";
    case LEADER:
        return "LEADER";
    default:
        return "UNKNOWN";
    }
}

PartitionReplicator::PartitionReplicator(const protocol::PartitionId *pid,
                                         std::unique_ptr<MessageProducer> producer,
                                         std::unique_ptr<MessageConsumer> consumer,
                                         int64_t committedId)
    : _pid(pid)
    , _producer(std::move(producer))
    , _consumer(std::move(consumer))
    , _committedId(committedId)
    , _mirrorVersion(0)
    , _current(UNKNOWN) {}

PartitionReplicator::~PartitionReplicator() {}

void PartitionReplicator::work() {
    switch (_current) {
    case LEADER:
        leaderRecover();
        break;
    case FOLLOWER:
        followerCatchup();
        break;
    default:
        negotiate();
    }
}

void PartitionReplicator::negotiate() {
    assert(_current == UNKNOWN);
    runPipeline(1, false);
}

void PartitionReplicator::leaderRecover() {
    if (_current == LEADER &&
        (_maxCommittedIdOfMirror.has_value() && _committedId >= _maxCommittedIdOfMirror.value())) {
        // keep heartbeat, in case follower become leader
        runPipeline(1, false);
    } else {
        // recover mode
        runPipeline(MSG_COUNT_PER_REQUEST, true);
    }
}

void PartitionReplicator::followerCatchup() { runPipeline(MSG_COUNT_PER_REQUEST, true); }

PartitionReplicator::State PartitionReplicator::computeState() const {
    if (_mirrorVersion == 0 || selfVersion() == _mirrorVersion) {
        return UNKNOWN;
    }
    return selfVersion() > _mirrorVersion ? LEADER : FOLLOWER;
}

void PartitionReplicator::runPipeline(uint32_t count, bool applyData) {
    protocol::MessageResponse response;
    _producer->produce(_committedId + 1, count, &response);

    uint64_t mirrorVersion = response.has_selfversion() ? response.selfversion() : 0;
    int64_t mirrorCommittedId = response.has_maxmsgid() ? response.maxmsgid() : -1;
    int64_t latestAppliedId = -1;
    if (applyData) {
        auto ec = response.errorinfo().errcode();
        if (ec == protocol::ERROR_NONE) {
            auto res = _consumer->consume(&response);
            if (res.is_ok()) {
                latestAppliedId = res.get();
            } else {
                AUTIL_LOG(WARN,
                          "%s: consume message failed, error: %s",
                          _pid->ShortDebugString().c_str(),
                          res.get_error().message().c_str());
                // maybe sleep a while
            }
        } else if (ec == protocol::ERROR_BROKER_NO_DATA) {
            // do nothing
        } else {
            AUTIL_LOG(WARN,
                      "%s: produce message failed, error: %s",
                      _pid->ShortDebugString().c_str(),
                      protocol::ErrorCode_Name(ec).c_str());
            return;
        }
    }
    updateState(mirrorVersion, mirrorCommittedId, latestAppliedId);
}

void PartitionReplicator::updateState(int64_t mirrorVersion, int64_t mirrorCommittedId, int64_t latestAppliedId) {
    autil::ScopedWriteLock lock(_lock);
    maybeUpdateMirrorId(mirrorVersion);
    maybeUpdateMirrorCommittedId(mirrorCommittedId);
    _committedId = std::max(_committedId, latestAppliedId);
    auto state = computeState();
    if (state != _current) {
        AUTIL_LOG(INFO,
                  "%s: state changed from %s to %s",
                  _pid->ShortDebugString().c_str(),
                  State_Name(_current),
                  State_Name(state));
    }
    _current = state;
}

void PartitionReplicator::maybeUpdateMirrorId(uint64_t mirrorVersion) {
    if (mirrorVersion != _mirrorVersion) {
        AUTIL_LOG(INFO,
                  "%s: mirrorVersion updated from %ld to %ld",
                  _pid->ShortDebugString().c_str(),
                  _mirrorVersion,
                  mirrorVersion);
        _mirrorVersion = mirrorVersion;
    }
}

void PartitionReplicator::maybeUpdateMirrorCommittedId(int64_t id) {
    if (!_maxCommittedIdOfMirror.has_value() || _maxCommittedIdOfMirror.value() <= id) {
        _maxCommittedIdOfMirror = id;
    } else {
        AUTIL_LOG(WARN,
                  "%s: maxCommittedIdOfMirror rollback: %ld -> %ld",
                  _pid->ShortDebugString().c_str(),
                  _maxCommittedIdOfMirror.value(),
                  id);
    }
}

uint64_t PartitionReplicator::selfVersion() const { return _producer->getSelfVersion(); }

bool PartitionReplicator::canServe() const {
    autil::ScopedReadLock lock(_lock);
    if (_current == UNKNOWN) {
        return false;
    }
    if (_current == FOLLOWER) {
        return true;
    }
    return _maxCommittedIdOfMirror.has_value() && _maxCommittedIdOfMirror.value() <= _committedId;
}

bool PartitionReplicator::canWrite() const {
    autil::ScopedReadLock lock(_lock);
    return _current == LEADER && _maxCommittedIdOfMirror.has_value() && _maxCommittedIdOfMirror.value() <= _committedId;
}

} // namespace replication
} // namespace swift
