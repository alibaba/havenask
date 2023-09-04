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
#include "swift/client/helper/CheckpointBuffer.h"

#include <algorithm>
#include <cstdint>
#include <string>

#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"

using namespace autil;

namespace swift {
namespace client {

AUTIL_LOG_SETUP(swift, CheckpointBuffer);

CheckpointBuffer::CheckpointBuffer(void *parent) : _parent(parent) {}

void CheckpointBuffer::init(int64_t startCkptId) {
    _buffer.clear();
    _buffer.resize(BUF_SIZE, INVALID_TS);
    _startId = startCkptId;
    _committedId = startCkptId - 1;
}

bool CheckpointBuffer::addTimestamp(const std::vector<std::pair<int64_t, int64_t>> &commitInfo) {
    AUTIL_LOG(DEBUG, "[%p] add timestamp[%s]", _parent, autil::StringUtil::toString(commitInfo).c_str());

    ScopedLock lock(_lock);
    for (const auto &pair : commitInfo) {
        size_t offset = getOffset(pair.first);
        assert(_buffer[offset] == INVALID_TS && "add multi times");
        _buffer[offset] = pair.second;
    }
    return updateCommittedId();
}

bool CheckpointBuffer::updateCommittedId() {
    auto oldCommitedId = _committedId;
    while ((_committedId + 1 < _startId + BUF_SIZE) && _buffer[getOffset(_committedId + 1)] != INVALID_TS) {
        ++_committedId;
    }
    AUTIL_LOG(DEBUG, "[%p] update committedId from[%ld] to[%ld]", _parent, oldCommitedId, _committedId);
    return _committedId != oldCommitedId;
}

int64_t CheckpointBuffer::getCommittedId() const {
    ScopedLock lock(_lock);
    return _committedId;
}

int64_t CheckpointBuffer::getIdLimit() const {
    ScopedLock lock(_lock);
    return _startId + BUF_SIZE;
}

void CheckpointBuffer::stealRange(std::vector<int64_t> &timestamps, int64_t ckptId) {
    timestamps.clear();
    {
        ScopedLock lock(_lock);
        AUTIL_LOG(DEBUG, "[%p] steal range from[%ld] to[%ld]", _parent, _startId, ckptId);
        assert(ckptId <= _committedId && "checkpoint id too large");
        for (int64_t i = _startId; i <= ckptId; ++i) {
            auto &ts = _buffer[getOffset(i)];
            timestamps.emplace_back(ts);
            ts = INVALID_TS;
        }
        _startOffset = getOffset(ckptId + 1);
        _startId = ckptId + 1;
    }
}

} // end namespace client
} // end namespace swift
