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
#include <map>
#include <utility>

#include "autil/Lock.h"
#include "autil/Log.h"
#include "autil/TimeUtility.h"
#include "swift/common/Common.h"

namespace swift {
namespace protocol {
class ConsumptionRequest;
} // namespace protocol
namespace storage {
class MessageGroup;
} // namespace storage
} // namespace swift

namespace swift {
namespace storage {
struct CommitInfo {
    CommitInfo() : commitTimestamp(-1), commitMsgId(-1) { lastAccessTimestamp = autil::TimeUtility::currentTime(); }
    int64_t commitTimestamp;
    int64_t lastAccessTimestamp;
    int64_t commitMsgId;
};

class CommitManager {
public:
    typedef std::pair<uint16_t, uint16_t> Range;

public:
    CommitManager(MessageGroup *messageGroup, uint16_t from, uint16_t to);
    ~CommitManager();

private:
    CommitManager(const CommitManager &);
    CommitManager &operator=(const CommitManager &);

public:
    void updateCommit(const protocol::ConsumptionRequest *request, bool needUpdateMsgId = true);
    void getCommitIdAndAccessTime(int64_t &commitId, int64_t &accessTime, int64_t &committedTime);
    void getCommitTimestamp(int64_t &committedTimestamp);

private:
    Range getCoverRange(uint16_t from, uint16_t to);
    void alignCommitId();

private:
    autil::ThreadMutex _mutex;
    MessageGroup *_messageGroup;
    Range _partitionRange;
    std::map<Range, CommitInfo> _rangeInfo;
    int64_t _startTime;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(CommitManager);

} // namespace storage
} // namespace swift
