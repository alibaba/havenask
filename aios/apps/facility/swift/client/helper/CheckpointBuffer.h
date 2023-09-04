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

#include <assert.h>
#include <limits>
#include <stddef.h>
#include <stdint.h>
#include <utility>
#include <vector>

#include "autil/Lock.h"
#include "autil/Log.h"

namespace swift {
namespace client {

class CheckpointBuffer {
public:
    CheckpointBuffer(void *parent = nullptr);
    ~CheckpointBuffer() = default;

public:
    void init(int64_t startCkptId);
    bool addTimestamp(const std::vector<std::pair<int64_t, int64_t>> &commitInfo);
    int64_t getCommittedId() const;
    int64_t getIdLimit() const;
    void stealRange(std::vector<int64_t> &timestamps, int64_t ckptId);

private:
    size_t getOffset(int64_t ckptId) const {
        assert(ckptId >= _startId && "too small checkpoint id");
        assert(ckptId < _startId + BUF_SIZE && "too large checkpoint id");
        return (ckptId - _startId + _startOffset) % BUF_SIZE;
    }
    bool updateCommittedId();

private:
    static constexpr int64_t INVALID_TS = std::numeric_limits<int64_t>::max();
    static constexpr size_t BUF_SIZE = 1 << 16;

private:
    void *_parent = nullptr;
    std::vector<int64_t> _buffer;
    mutable autil::ThreadMutex _lock;
    int64_t _startOffset = 0;
    int64_t _startId = 0;
    int64_t _committedId = -1;

private:
    AUTIL_LOG_DECLARE();
};

} // end namespace client
} // end namespace swift
