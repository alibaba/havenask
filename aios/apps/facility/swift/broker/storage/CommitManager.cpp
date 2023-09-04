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
#include "swift/broker/storage/CommitManager.h"

#include <algorithm>
#include <cstddef>
#include <memory>
#include <string>
#include <vector>

#include "autil/TimeUtility.h"
#include "swift/broker/storage/MessageGroup.h"
#include "swift/protocol/BrokerRequestResponse.pb.h"
#include "swift/protocol/SwiftMessage.pb.h"

using namespace autil;
using namespace std;
using namespace swift::protocol;

namespace swift {
namespace storage {
AUTIL_LOG_SETUP(swift, CommitManager);

CommitManager::CommitManager(MessageGroup *messageGroup, uint16_t from, uint16_t to) : _messageGroup(messageGroup) {
    _partitionRange = make_pair(from, to);
    _startTime = TimeUtility::currentTime();
}

CommitManager::~CommitManager() {}

void CommitManager::updateCommit(const ConsumptionRequest *request, bool needUpdateMsgId) {
    Range range = _partitionRange;
    if (request->has_filter()) {
        const Filter &filter = request->filter();
        range = getCoverRange((uint16_t)filter.from(), (uint16_t)filter.to());
    }

    ScopedLock scopedLock(_mutex);
    CommitInfo &info = _rangeInfo[range];
    info.lastAccessTimestamp = TimeUtility::currentTime();
    if (request->has_committedcheckpoint() && request->committedcheckpoint() > info.commitTimestamp) {
        info.commitTimestamp = request->committedcheckpoint();
        if (needUpdateMsgId) {
            info.commitMsgId = _messageGroup->findMessageId(request->committedcheckpoint());
        }
        AUTIL_LOG(INFO,
                  "[%s %u] range[%d %d] update commit timestamp[%ld], "
                  "needUpdateMsgId[%d], commit msgid [%ld]",
                  request->topicname().c_str(),
                  request->partitionid(),
                  range.first,
                  range.second,
                  info.commitTimestamp,
                  needUpdateMsgId,
                  info.commitMsgId);
        if (range != _partitionRange && needUpdateMsgId) {
            alignCommitId();
        }
    }
}

void CommitManager::getCommitIdAndAccessTime(int64_t &committedId, int64_t &accessTime, int64_t &committedTime) {
    committedId = -1;
    accessTime = -1;
    committedTime = -1;
    vector<CommitInfo> infoVec;
    uint32_t from = _partitionRange.first;
    {
        ScopedLock scopedLock(_mutex);
        std::map<Range, CommitInfo>::iterator iter = _rangeInfo.begin();
        for (; iter != _rangeInfo.end(); iter++) {
            if (iter->first.first == from) {
                infoVec.push_back(iter->second);
                from = iter->first.second + 1;
            } else if (iter->first.first < from) {
                AUTIL_LOG(INFO, "error range info [%d] from [%d]", iter->first.first, from);
                _rangeInfo.clear();
                break;
            } else {
                AUTIL_LOG(INFO, "range not complete, range info [%d] from [%d]", iter->first.first, from);
                break;
            }
        }
    }
    if (infoVec.size() > 0 && from == (uint32_t)_partitionRange.second + 1) {
        committedId = infoVec[0].commitMsgId;
        accessTime = infoVec[0].lastAccessTimestamp;
        committedTime = infoVec[0].commitTimestamp;
        for (size_t i = 1; i < infoVec.size(); i++) {
            if (committedId > infoVec[i].commitMsgId) {
                committedId = infoVec[i].commitMsgId;
            }
            if (committedTime > infoVec[i].commitTimestamp) {
                committedTime = infoVec[i].commitTimestamp;
            }
            if (accessTime > infoVec[i].lastAccessTimestamp) {
                accessTime = infoVec[i].lastAccessTimestamp;
            }
        }
    }
    if (accessTime == -1) {
        AUTIL_INTERVAL_LOG(500, INFO, "access time not found use start time [%ld]", _startTime);
        accessTime = _startTime;
    }
}

void CommitManager::getCommitTimestamp(int64_t &committedTimestamp) {
    int64_t committedId = -1;
    int64_t accessTime = -1;
    getCommitIdAndAccessTime(committedId, accessTime, committedTimestamp);
}

CommitManager::Range CommitManager::getCoverRange(uint16_t from, uint16_t to) {
    Range cover;
    cover.first = _partitionRange.first < from ? from : _partitionRange.first;
    cover.second = _partitionRange.second < to ? _partitionRange.second : to;
    return cover;
}

void CommitManager::alignCommitId() {
    std::map<Range, CommitInfo>::iterator iter = _rangeInfo.begin();
    int64_t maxTimestamp = -1;
    int64_t maxMsgId = -1;
    for (; iter != _rangeInfo.end(); iter++) {
        if (maxMsgId < iter->second.commitMsgId) {
            maxMsgId = iter->second.commitMsgId;
            maxTimestamp = iter->second.commitTimestamp;
        }
    }
    for (iter = _rangeInfo.begin(); iter != _rangeInfo.end(); iter++) {
        int64_t msgId = iter->second.commitMsgId;
        bool hasMsg = _messageGroup->hasMsgInRange(iter->first.first, iter->first.second, msgId, maxMsgId);
        if (!hasMsg) {
            AUTIL_LOG(INFO,
                      "align commit msgid for range[%d %d], from[%ld] to[%ld]",
                      iter->first.first,
                      iter->first.second,
                      iter->second.commitMsgId,
                      maxMsgId);
            iter->second.commitTimestamp = maxTimestamp;
            iter->second.commitMsgId = maxMsgId;
        }
    }
}

} // namespace storage
} // namespace swift
