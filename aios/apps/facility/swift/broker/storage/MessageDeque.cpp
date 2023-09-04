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
#include "swift/broker/storage/MessageDeque.h"

#include <iosfwd>
#include <type_traits>
#include <utility>

#include "swift/util/Block.h"

namespace swift {
namespace util {
class BlockPool;
template <class T>
class ObjectBlock;
} // namespace util
} // namespace swift

namespace swift {
namespace storage {
AUTIL_LOG_SETUP(swift, MessageDeque);
using namespace swift::util;
using namespace swift::common;
using namespace std;

MessageDeque::MessageDeque(util::BlockPool *blockPool, TimestampAllocator *timestampAllocator)
    : _timestampAllocator(timestampAllocator), _msgQueue(blockPool) {
    clear();
}

MessageDeque::~MessageDeque() {}

void MessageDeque::stealBlock(int64_t count, int64_t &startOffset, vector<ObjectBlock<MemoryMessage> *> &objectVec) {
    int64_t msgCount = 0;
    _msgQueue.stealBlock(count, startOffset, msgCount, objectVec);
    _committedIndex -= msgCount;
    _metaSize -= MEMORY_MESSAGE_META_SIZE * msgCount;
}

void MessageDeque::updateStealDataLen(int64_t dataLen) { _dataSize -= dataLen; }

static int64_t getMsgSize(const common::MemoryMessage &msg) { return msg.getLen() + MEMORY_MESSAGE_META_SIZE; }

void MessageDeque::recycleByTimestamp(int64_t timestamp, int64_t &actualRecycleSize, int64_t &actualRecycleCount) {
    auto p = [timestamp](const common::MemoryMessage &msg) { return msg.getTimestamp() < timestamp; };
    recycle(actualRecycleSize, actualRecycleCount, std::move(p));
}

void MessageDeque::recycleBySize(const int64_t targetSize, int64_t &actualRecycleSize, int64_t &actualRecycleCount) {
    auto p = [&actualRecycleSize, targetSize](const common::MemoryMessage &msg) {
        return actualRecycleSize + getMsgSize(msg) <= targetSize;
    };
    recycle(actualRecycleSize, actualRecycleCount, std::move(p));
}

void MessageDeque::recycle(int64_t &actualRecycleSize, int64_t &actualRecycleCount, Predicate p) {
    actualRecycleSize = 0;
    actualRecycleCount = 0;
    while (_msgQueue.size() > 1) {
        const auto &msg = front();
        // do not recycle message which is not committed, reserve at least one committed msg
        if (msg.getMsgId() > _lastCommittedId - 1) {
            break;
        }
        if (!p(msg)) {
            break;
        }
        actualRecycleSize += getMsgSize(msg);
        ++actualRecycleCount;
        popFront();
    }
}

} // namespace storage
} // namespace swift
