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
#include "swift/broker/storage/FlowControl.h"

namespace swift {
namespace storage {

const int64_t STAT_INTERVAL_US = 1000000;

FlowControl::FlowControl(int64_t writeSizeLimitSec, int64_t readSizeLimitSec)
    : _writeSizeLimit(writeSizeLimitSec), _readSizeLimit(readSizeLimitSec) {}

FlowControl::~FlowControl() {}

void FlowControl::setMaxReadSize(int64_t readSizeLimitSec) { _readSizeLimit = readSizeLimitSec; }

void FlowControl::updateReadSize(int64_t readSize) { _readSize.add(readSize); }

bool FlowControl::canRead(int64_t readTime) {
    if (readTime - _lastReadTime.get() > STAT_INTERVAL_US) {
        _lastReadTime.getAndSet(readTime);
        _readSize.getAndSet(0);
        return true;
    } else {
        return _readSize.get() < _readSizeLimit;
    }
}

void FlowControl::updateWriteSize(int64_t writeSize) { _writeSize.add(writeSize); }

bool FlowControl::canWrite(int64_t writeTime) {
    if (writeTime - _lastWriteTime.get() > STAT_INTERVAL_US) {
        _lastWriteTime.getAndSet(writeTime);
        _writeSize.getAndSet(0);
        return true;
    } else {
        return _writeSize.get() < _writeSizeLimit;
    }
}

} // namespace storage
} // namespace swift
