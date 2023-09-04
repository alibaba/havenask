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

#include "swift/common/Common.h"
#include "swift/util/Atomic.h"

namespace swift {
namespace storage {

class FlowControl {
public:
    FlowControl(int64_t writeSizeLimitSec, int64_t readSizeLimitSec);
    ~FlowControl();

private:
    FlowControl(const FlowControl &) = delete;
    FlowControl &operator=(const FlowControl &) = delete;

public:
    void setMaxReadSize(int64_t readSizeLimitSec);
    void updateWriteSize(int64_t writeSize);
    void updateReadSize(int64_t readSize);
    bool canWrite(int64_t writeTime);
    bool canRead(int64_t readTime);
    int64_t getWriteSize() { return _writeSize.get(); }
    int64_t getReadSize() { return _readSize.get(); }

private:
    int64_t _writeSizeLimit = 0;
    int64_t _readSizeLimit = 0;
    util::Atomic64 _lastWriteTime;
    util::Atomic64 _lastReadTime;
    util::Atomic64 _writeSize;
    util::Atomic64 _readSize;
};

SWIFT_TYPEDEF_PTR(FlowControl);

} // namespace storage
} // namespace swift
