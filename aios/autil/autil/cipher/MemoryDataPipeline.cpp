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
#include "autil/cipher/MemoryDataPipeline.h"

#include <string.h>

namespace autil {
namespace cipher {
AUTIL_LOG_SETUP(autil, MemoryDataPipeline);

MemoryDataPipeline::MemoryDataPipeline(size_t blockUnitSize,
                                       size_t maxCapacity,
                                       size_t readTimeoutInMs,
                                       size_t maxFreeListNodeCnt)
    : _blockUnitSize(blockUnitSize)
    , _maxFreeListNodeCnt(maxFreeListNodeCnt)
    , _maxCapacity(maxCapacity)
    , _readTimeoutInMs(readTimeoutInMs) {
    MemBufferNode *node = newBufferNode(_blockUnitSize);
    _datalistHeader = node;
    _datalistTail = node;
    _sealed = false;
}

MemoryDataPipeline::~MemoryDataPipeline() {
    MemBufferNode *node = _datalistHeader;
    while (node != nullptr) {
        auto *next = node->next;
        delete node;
        node = next;
    }
    node = _freelist;
    while (node != nullptr) {
        auto *next = node->next;
        delete node;
        node = next;
    }
}

char *MemoryDataPipeline::getMemoryBuffer(size_t length) {
    if (_sealed) {
        return nullptr;
    }
    MemBufferNode *node = _datalistTail;
    assert(node != nullptr);
    char *writeBuffer = node->buffer.getBuffer() + node->writeCursor;
    size_t writeBufferLen = node->buffer.size() - node->writeCursor;
    if (length <= writeBufferLen) {
        return (char *)writeBuffer;
    }

    waitCapacity();
    MemBufferNode *newNode = newBufferNode(std::max(length, _blockUnitSize));
    node->next = newNode;
    _datalistTail = newNode;
    node = newNode;
    return node->buffer.getBuffer();
}

bool MemoryDataPipeline::increaseDataLength(size_t length) {
    if (_sealed) {
        return false;
    }
    MemBufferNode *node = _datalistTail;
    assert(node != nullptr);
    size_t writeBufferLen = node->buffer.size() - node->writeCursor;
    if (length > writeBufferLen) {
        return false;
    }
    node->writeCursor += length;
    _validDataSize.fetch_add(length, std::memory_order_release);
    makeDataReady();
    return true;
}

bool MemoryDataPipeline::write(const char *data, size_t size) {
    if (_sealed) {
        return false;
    }
    waitCapacity();
    MemBufferNode *node = _datalistTail;
    assert(node != nullptr);
    char *writeBuffer = node->buffer.getBuffer() + node->writeCursor;
    size_t writeBufferLen = node->buffer.size() - node->writeCursor;

    const char *dataCursor = data;
    size_t leftLen = size;
    while (leftLen > 0) {
        size_t tmpWriteSize = std::min(leftLen, writeBufferLen);
        if (tmpWriteSize > 0) {
            memcpy(writeBuffer, dataCursor, tmpWriteSize);
            __asm__ __volatile__("" ::: "memory");
            node->writeCursor += tmpWriteSize;
            dataCursor += tmpWriteSize;
            leftLen -= tmpWriteSize;
        } else {
            MemBufferNode *newNode = newBufferNode(_blockUnitSize);
            node->next = newNode;
            _datalistTail = newNode;
            node = newNode;
        }
        writeBuffer = node->buffer.getBuffer() + node->writeCursor;
        writeBufferLen = node->buffer.size() - node->writeCursor;
    }
    _validDataSize.fetch_add(size, std::memory_order_release);
    makeDataReady();
    return true;
}

size_t MemoryDataPipeline::read(char *buffer, size_t bufLen) {
    waitDataReady();
    MemBufferNode *node = _datalistHeader;
    size_t readLen = 0;
    while (node != nullptr) {
        if (node->readCursor < node->writeCursor) {
            size_t tmpReadLen = std::min((node->writeCursor - node->readCursor), bufLen - readLen);
            memcpy(buffer + readLen, node->buffer.getBuffer() + node->readCursor, tmpReadLen);
            readLen += tmpReadLen;
            node->readCursor += tmpReadLen;
            if (readLen == bufLen) {
                break;
            }
            continue;
        }

        if (node->next == nullptr) {
            break;
        }
        // node->writeCursor may increase by other write thread
        if (node->readCursor == node->writeCursor) {
            _datalistHeader = node->next;
            freeNode(node);
            node = _datalistHeader;
        }
    }
    if (readLen > 0) {
        _validDataSize.fetch_sub(readLen, std::memory_order_release);
    }
    return readLen;
}

void MemoryDataPipeline::freeNode(MemBufferNode *node) {
    assert(node);
    _totalDataNodeCapacity.fetch_sub(node->buffer.size(), std::memory_order_relaxed);
    autil::ScopedLock lock(_capacityLock);
    _capacityLock.signal();
    if (!addNodeToFreelist(node)) {
        delete node;
    }
}

bool MemoryDataPipeline::addNodeToFreelist(MemBufferNode *node) {
    if (node->buffer.size() != _blockUnitSize) {
        return false;
    }
    node->writeCursor = 0;
    node->readCursor = 0;
    autil::ScopedSpinLock lock(_freelistLock);
    if (_totalFreeNodeCnt >= _maxFreeListNodeCnt) {
        return false;
    }
    node->next = _freelist;
    _freelist = node;
    ++_totalFreeNodeCnt;
    return true;
}

MemoryDataPipeline::MemBufferNode *MemoryDataPipeline::newBufferNode(size_t unitSize) {
    _totalDataNodeCapacity.fetch_add(unitSize, std::memory_order_relaxed);
    MemBufferNode *node = getNodeFromFreelist(unitSize);
    if (node != nullptr) {
        return node;
    }
    node = new MemBufferNode(unitSize);
    return node;
}

MemoryDataPipeline::MemBufferNode *MemoryDataPipeline::getNodeFromFreelist(size_t unitSize) {
    if (unitSize != _blockUnitSize) {
        return nullptr;
    }
    autil::ScopedSpinLock lock(_freelistLock);
    if (_freelist == nullptr) {
        return nullptr;
    }
    MemBufferNode *node = _freelist;
    _freelist = node->next;
    node->next = nullptr;
    assert(_totalFreeNodeCnt > 0);
    --_totalFreeNodeCnt;
    return node;
}

size_t MemoryDataPipeline::getCurrentMemoryUse() const {
    size_t sum = 0;
    auto node = _datalistHeader;
    while (node != nullptr) {
        sum += node->buffer.size();
        node = node->next;
    }
    size_t dataNodeMemUse = _totalDataNodeCapacity.load(std::memory_order_relaxed);
    autil::ScopedSpinLock lock(_freelistLock);
    return dataNodeMemUse + _totalFreeNodeCnt * _blockUnitSize;
}

size_t MemoryDataPipeline::getInpipeDataSize() const { return _validDataSize.load(std::memory_order_acquire); }

void MemoryDataPipeline::waitDataReady() const {
    if (_readTimeoutInMs == 0) {
        return;
    }

    int64_t beginTs = autil::TimeUtility::monotonicTimeUs();
    autil::ScopedLock lock(_dataLock);
    while (_validDataSize.load(std::memory_order_acquire) == 0) {
        int64_t gap = autil::TimeUtility::monotonicTimeUs() - beginTs;
        int64_t leftTimeout = _readTimeoutInMs * 1000;
        if (gap >= leftTimeout) {
            return;
        }
        int64_t usToWait(std::min(leftTimeout - gap, (int64_t)1000));
        _dataLock.wait(usToWait);
    }
}

void MemoryDataPipeline::makeDataReady() {
    if (_readTimeoutInMs == 0) {
        return;
    }
    autil::ScopedLock lock(_dataLock);
    _dataLock.signal();
}

void MemoryDataPipeline::waitCapacity() {
    if (_maxCapacity == std::numeric_limits<size_t>::max() ||
        _totalDataNodeCapacity.load(std::memory_order_relaxed) <= _maxCapacity) {
        return;
    }
    autil::ScopedLock lock(_capacityLock);
    while (_totalDataNodeCapacity.load(std::memory_order_relaxed) > _maxCapacity) {
        _capacityLock.wait();
    }
}

void MemoryDataPipeline::seal() {
    _sealed = true;
    if (_readTimeoutInMs != 0) {
        _readTimeoutInMs = 0;
        autil::ScopedLock lock(_dataLock);
        _dataLock.signal();
    }
}

bool MemoryDataPipeline::isEof() const {
    if (!_sealed) {
        return false;
    }
    return getInpipeDataSize() == 0;
}

} // namespace cipher
} // namespace autil
