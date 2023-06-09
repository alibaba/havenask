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

#include "autil/Log.h"
#include "autil/Lock.h"

namespace autil { namespace cipher {

// sync read & write
// maybe support one thread read & one thread write
class MemoryDataPipeline
{
public:
    MemoryDataPipeline(size_t blockUnitSize = 64 * 1024,
                       size_t maxCapacity = std::numeric_limits<size_t>::max(),
                       size_t readTimeoutInMs = 50 /* 50 ms, 0 means no wait */,
                       size_t maxFreeListNodeCnt = 10);

    ~MemoryDataPipeline();

public:
    bool write(const char* data, size_t size);
    size_t read(char* buffer, size_t bufLen);

    // for optimize: reuse memory buffer, avoid copy once by write interface
    // first: getMemoryBuffer, second: increase length <= allocate length
    char* getMemoryBuffer(size_t length);
    bool increaseDataLength(size_t length);

    size_t getCurrentMemoryUse() const;
    size_t getInpipeDataSize() const;
    
    void seal();
    bool isEof() const;
    
private:
    class SafeBuffer
    {
    public:
        SafeBuffer(int64_t size)
            : _size(size)
        {
            _buffer = new char[size];
        }
        ~SafeBuffer() {
            if (_buffer) {
                delete[] _buffer;
                _buffer = NULL;
            }
        }
        char* getBuffer() const { return _buffer; }
        int64_t size() const { return _size; }
    private:
        char* _buffer;
        int64_t _size;
    };
    
    struct MemBufferNode
    {
        MemBufferNode(size_t bufSize)
            : buffer(bufSize)
        {}
        SafeBuffer buffer;
        volatile size_t writeCursor = 0;
        volatile size_t readCursor = 0;
        MemBufferNode* next = nullptr;
    };

    void freeNode(MemBufferNode* node);
    MemBufferNode* newBufferNode(size_t unitSize);

    bool addNodeToFreelist(MemBufferNode* node);
    MemBufferNode* getNodeFromFreelist(size_t unitSize);
    void waitDataReady() const;
    void makeDataReady();
    void waitCapacity();
    
private:
    MemBufferNode* _datalistHeader = nullptr;
    MemBufferNode* _datalistTail = nullptr;    
    MemBufferNode* _freelist = nullptr;
    size_t _totalFreeNodeCnt = 0;
    size_t _blockUnitSize = 0;
    size_t _maxFreeListNodeCnt = 0;
    size_t _maxCapacity = std::numeric_limits<size_t>::max();
    size_t _readTimeoutInMs = 0;
    mutable autil::SpinLock _freelistLock;
    mutable autil::ThreadCond _dataLock;
    mutable autil::ThreadCond _capacityLock;    
    std::atomic_uint_fast64_t _validDataSize {0};    
    std::atomic_uint_fast64_t _totalDataNodeCapacity {0};
    volatile bool _sealed = false;
    
private:
    AUTIL_LOG_DECLARE();
};

}}

