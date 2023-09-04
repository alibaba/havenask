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

#include <algorithm>
#include <assert.h>
#include <cstdint>
#include <functional>
#include <stddef.h>
#include <vector>

#include "autil/Log.h"
#include "autil/TimeUtility.h"
#include "swift/broker/storage/TimestampAllocator.h"
#include "swift/common/Common.h"
#include "swift/common/MemoryMessage.h"
#include "swift/util/MessageQueue.h"

namespace swift {
namespace util {
class BlockPool;
template <class T>
class ObjectBlock;
} // namespace util
} // namespace swift

namespace swift {
namespace storage {

class MessageDeque {
public:
    typedef util::MessageQueueIterator<common::MemoryMessage> MessageDequeIter;

public:
    class View {
    public:
        View(const util::MessageQueue<common::MemoryMessage> *queue,
             const size_t size,
             TimestampAllocator *timestampAllocator)
            : _queue(queue), _size(size), _timestampAllocator(timestampAllocator) {
            assert(_queue != nullptr);
        }

    public:
        size_t size() const { return _size; }
        bool empty() const { return size() == 0; }

        const common::MemoryMessage &get(size_t idx) const {
            assert(idx < size());
            return (*_queue)[idx];
        }

        const common::MemoryMessage &operator[](size_t idx) const { return get(idx); }

        int64_t getLastMsgId() const {
            if (_size > 0) {
                return get(_size - 1).getMsgId();
            }
            return -1;
        }
        int64_t getLastMsgTimestamp() const {
            if (_size > 0) {
                return get(_size - 1).getTimestamp();
            } else if (_queue->empty()) {
                return _timestampAllocator->getCurrentTimestamp();
            } else {
                return (*_queue)[0].getTimestamp() - 1;
            }
        }
        int64_t getNextMsgIdAfterLast() const { return getLastMsgId() + 1; }
        int64_t getNextMsgTimestampAfterLast() const {
            if (_size >= _queue->size()) {
                return _timestampAllocator->getNextMsgTimestamp(getLastMsgTimestamp());
            } else {
                return getLastMsgTimestamp() + 1;
            }
        }

        MessageDequeIter begin() const { return _queue->begin(); }
        MessageDequeIter end() const { return begin() + _size; }
        MessageDequeIter findByTimestamp(int64_t timestamp) const {
            struct MessageTimestampComp {
                bool operator()(const common::MemoryMessage &first, uint64_t second) const {
                    return first.getTimestamp() < second;
                }
            };
            return std::lower_bound(begin(), end(), timestamp, MessageTimestampComp());
        }

    private:
        const util::MessageQueue<common::MemoryMessage> *_queue;
        const size_t _size; // [0, size)
        TimestampAllocator *_timestampAllocator = nullptr;
    };

public:
    explicit MessageDeque(util::BlockPool *blockPool, TimestampAllocator *timestampAllocator);
    ~MessageDeque();

private:
    MessageDeque(const MessageDeque &);
    MessageDeque &operator=(const MessageDeque &);

public:
    // first use reserve() to reserve mete for adding msg, then pushBack() msg
    inline bool reserve(size_t count = 1);
    inline void pushBack(const common::MemoryMessage &memoryMsg);
    inline const common::MemoryMessage &front() const;
    inline void popFront();
    inline void clear();
    inline size_t size() const;
    inline size_t getCommittedCount() const { return _committedIndex; }
    inline const common::MemoryMessage &operator[](int64_t i) const;

public:
    inline int64_t setCommittedMsgId(int64_t msgId); // return commitSize
    inline int64_t getCommittedMsgId() const;
    inline int64_t getLastReceivedMsgId() const;
    inline int64_t getLastReceivedMsgTimestamp() const;
    inline int64_t getDataSize() const;
    inline int64_t getMetaSize() const;
    inline int64_t getLeftToBeCommittedDataSize() const;
    inline int64_t getLastAcceptedMsgTimeStamp() const;
    inline int64_t getCommitDelay() const;
    inline int64_t getCommittedTimestamp() const;
    inline void getMemoryMessage(int64_t startId, size_t count, common::MemoryMessageVector &vec) const;
    int64_t remainMetaCount() const;
    int64_t msgCountInOneBlock() const;
    int64_t getMetaUsedBlockCount() const;
    void stealBlock(int64_t offset,
                    int64_t &startOffset,
                    std::vector<util::ObjectBlock<common::MemoryMessage> *> &objectVec);
    void updateStealDataLen(int64_t dataLen);

public:
    View createView(bool readNotCommittedMsg) const {
        size_t size = readNotCommittedMsg ? _msgQueue.size() : getCommittedCount();
        return View(&_msgQueue, size, _timestampAllocator);
    }

    // write lock must be held
    void recycleByTimestamp(int64_t timestamp, int64_t &actualRecycleSize, int64_t &actualRecycleCount);
    void recycleBySize(const int64_t targetSize, int64_t &actualRecycleSize, int64_t &actualRecycleCount);

private:
    using Predicate = std::function<bool(const common::MemoryMessage &)>;
    void recycle(int64_t &actualRecycleSize, int64_t &actualRecycleCount, Predicate p);

private:
    TimestampAllocator *_timestampAllocator = nullptr;
    int64_t _committedIndex;
    int64_t _lastCommittedId;
    int64_t _dataSize;
    int64_t _metaSize;
    int64_t _leftToBeCommittedDataSize;
    int64_t _lastAcceptedMsgTimeStamp;
    util::MessageQueue<common::MemoryMessage> _msgQueue;

private:
    friend class MessageDequeTest;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(MessageDeque);
inline bool MessageDeque::reserve(size_t count) { return _msgQueue.reserve(count); }

inline void MessageDeque::pushBack(const common::MemoryMessage &memoryMsg) {
    _dataSize += memoryMsg.getLen();
    _metaSize += common::MEMORY_MESSAGE_META_SIZE;
    _leftToBeCommittedDataSize += memoryMsg.getLen();
    _lastAcceptedMsgTimeStamp = memoryMsg.getTimestamp();
    _msgQueue.push_back(memoryMsg);
}

inline const common::MemoryMessage &MessageDeque::front() const { return _msgQueue.front(); }

inline void MessageDeque::popFront() {
    if (!_msgQueue.empty()) {
        _dataSize -= front().getLen();
        _metaSize -= common::MEMORY_MESSAGE_META_SIZE;
        _msgQueue.pop_front();
        _committedIndex--;
    }
}

inline void MessageDeque::getMemoryMessage(int64_t startId, size_t count, common::MemoryMessageVector &vec) const {
    if (_msgQueue.empty()) {
        return;
    }
    int64_t beginPos = startId - front().getMsgId();
    if (beginPos < 0 || beginPos >= (int64_t)_msgQueue.size()) {
        AUTIL_LOG(ERROR,
                  "getMemoryMessage parameters error, startId[%ld], "
                  "first msgId[%ld], msgQueue size[%d]",
                  startId,
                  front().getMsgId(),
                  (int32_t)_msgQueue.size());
        return;
    }
    MessageDequeIter it = _msgQueue.begin() + beginPos;
    while (vec.size() < count && it != _msgQueue.end()) {
        vec.push_back(*it++);
    }
}

inline int64_t MessageDeque::setCommittedMsgId(int64_t msgId) {
    _lastCommittedId = msgId;
    int64_t commitSize = 0;
    while (_committedIndex < _msgQueue.size()) {
        const common::MemoryMessage &m = _msgQueue[_committedIndex];
        if (m.getMsgId() > msgId) {
            break;
        }
        commitSize += m.getLen();
        _committedIndex++;
    }
    _leftToBeCommittedDataSize -= commitSize;
    return commitSize;
}

inline int64_t MessageDeque::getCommittedMsgId() const { return _lastCommittedId; }

inline void MessageDeque::clear() {
    _committedIndex = 0;
    _lastCommittedId = -1;
    _dataSize = 0;
    _metaSize = 0;
    _leftToBeCommittedDataSize = 0;
    _lastAcceptedMsgTimeStamp = 0;
    _msgQueue.clear();
}

inline size_t MessageDeque::size() const { return _msgQueue.size(); }

inline const common::MemoryMessage &MessageDeque::operator[](int64_t i) const {
    assert(i < size());
    return _msgQueue[i];
}

inline int64_t MessageDeque::getLastReceivedMsgId() const {
    if (_msgQueue.empty()) {
        return -1;
    }
    return _msgQueue.back().getMsgId();
}

inline int64_t MessageDeque::getLastReceivedMsgTimestamp() const {
    if (_msgQueue.empty()) {
        return _timestampAllocator->getCurrentTimestamp();
    } else {
        return _msgQueue[_msgQueue.size() - 1].getTimestamp();
    }
}

inline int64_t MessageDeque::getDataSize() const { return _dataSize; }

inline int64_t MessageDeque::getMetaSize() const { return _metaSize; }

inline int64_t MessageDeque::getLeftToBeCommittedDataSize() const { return _leftToBeCommittedDataSize; }

inline int64_t MessageDeque::getLastAcceptedMsgTimeStamp() const { return _lastAcceptedMsgTimeStamp; }

inline int64_t MessageDeque::getCommitDelay() const {
    if (_committedIndex >= _msgQueue.size()) {
        return 0;
    }
    int64_t curTime = autil::TimeUtility::currentTime();
    return curTime - _msgQueue[_committedIndex].getTimestamp();
}

inline int64_t MessageDeque::getCommittedTimestamp() const {
    size_t count = getCommittedCount();
    if (_msgQueue.size() == 0 || count == 0 || count > _msgQueue.size()) {
        return 0;
    } else {
        return _msgQueue[count - 1].getTimestamp();
    }
}

inline int64_t MessageDeque::remainMetaCount() const { return _msgQueue.remainMetaCount(); }

inline int64_t MessageDeque::msgCountInOneBlock() const { return _msgQueue.msgCountInOneBlock(); }
inline int64_t MessageDeque::getMetaUsedBlockCount() const { return _msgQueue.usedBlockCount(); }

} // namespace storage
} // namespace swift
