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
#include <flatbuffers/flatbuffers.h>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/Log.h"
#include "swift/common/Common.h"
#include "swift/protocol/SwiftMessage_generated.h"

namespace swift {
namespace protocol {

class SimpleFbAllocator : public flatbuffers::Allocator {
public:
    SimpleFbAllocator() : _allocatedSize(0) {}

public:
    uint8_t *allocate(size_t size) {
        _allocatedSize += size;
        return new uint8_t[size];
    }

    void deallocate(uint8_t *p, size_t size) {
        _allocatedSize -= size;
        delete[] p;
    }
    int64_t getAllocatedSize() { return _allocatedSize; }

private:
    int64_t _allocatedSize;
};

class FBMessageWriter {
public:
    FBMessageWriter();
    ~FBMessageWriter();

private:
    FBMessageWriter(const FBMessageWriter &);
    FBMessageWriter &operator=(const FBMessageWriter &);

public:
    static void setRecycleBufferSize(int64_t size) { FB_RECYCLE_BUFFER_SIZE = size; }
    static int64_t getRecycleBufferSize() { return FB_RECYCLE_BUFFER_SIZE; }

public:
    void reset();
    bool addMessage(const std::string &data,
                    int64_t msgId = 0,
                    int64_t timestamp = 0,
                    uint16_t uint16Payload = 0,
                    uint8_t uint8MaskPayload = 0,
                    bool compress = false,
                    bool isMerged = false) {
        return addMessage(
            data.c_str(), data.size(), msgId, timestamp, uint16Payload, uint8MaskPayload, compress, isMerged);
    }

    bool addMessage(const char *data,
                    size_t dataLen,
                    int64_t msgId = 0,
                    int64_t timestamp = 0,
                    uint16_t uint16Payload = 0,
                    uint8_t uint8MaskPayload = 0,
                    bool compress = false,
                    bool isMerged = false) {
        if (isFinish()) {
            return false;
        }
        flatbuffers::Offset<flatbuffers::String> fbData = _fbBuilder->CreateString(data, dataLen);
        flatbuffers::Offset<flat::Message> rebuildMsg = flat::CreateMessage(
            *_fbBuilder, msgId, timestamp, fbData, uint16Payload, uint8MaskPayload, compress, isMerged);
        _offsetVec.push_back(rebuildMsg);
        return true;
    }

    void finish();
    bool isFinish() const { return _isFinish; }
    const char *data();
    size_t size();

private:
    SimpleFbAllocator *_fbAllocator;
    flatbuffers::FlatBufferBuilder *_fbBuilder;

    std::vector<flatbuffers::Offset<flat::Message>> _offsetVec;
    bool _isFinish;
    static int64_t FB_RECYCLE_BUFFER_SIZE;

private:
    AUTIL_LOG_DECLARE();
};

SWIFT_TYPEDEF_PTR(FBMessageWriter);

} // namespace protocol
} // namespace swift
