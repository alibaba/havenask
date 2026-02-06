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
#include "indexlib/index/inverted_index/format/skiplist/MultiLevelSkipListWriter.h"

#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <vector>

#include "indexlib/index/common/numeric_compress/VbyteCompressor.h"

namespace indexlib::index {

AUTIL_LOG_SETUP(indexlib.index, MultiLevelSkipListWriter);

MultiLevelSkipListWriter::MultiLevelSkipListWriter(autil::mem_pool::Pool* byteSlicePool)
    : _byteSlicePool(byteSlicePool) {}

MultiLevelSkipListWriter::~MultiLevelSkipListWriter() {
    for (size_t i = 0; i < _skipBuffers.size(); ++i) {
        _skipBuffers[i]->~ByteSliceWriter();
        uint8_t* mem = reinterpret_cast<uint8_t*>(_skipBuffers[i]);
        _byteSlicePool->deallocate(mem, sizeof(file_system::ByteSliceWriter));
    }
}

void MultiLevelSkipListWriter::AddSkipLevel() {
    void* mem = _byteSlicePool->allocate(sizeof(file_system::ByteSliceWriter));
    file_system::ByteSliceWriter* writer = new (mem) file_system::ByteSliceWriter(_byteSlicePool);
    _skipBuffers.emplace_back(writer);
    _lastKeys.push_back(0);
    _lastTTFs.push_back(0);
    _lastOffsets.push_back(0);
    _levelEnds[_skipLevel.load(std::memory_order_relaxed)] = 0;
    _skipLevel.fetch_add(1, std::memory_order_release);
}

void MultiLevelSkipListWriter::AddItemInternal(uint32_t key, uint32_t offset,
                                               std::optional<uint32_t> ttf) {
    _accumulateOffset += offset;
    uint32_t skipCount = ++_skipCount;
    uint8_t numLevels = 1;
    if (skipCount % SKIP_MULTIPLIER == 0) {
        numLevels++;
        skipCount /= SKIP_MULTIPLIER;
        while ((skipCount % SKIP_MULTIPLIER) == 0 && numLevels < MAX_SKIP_LEVEL) {
            numLevels++;
            skipCount /= SKIP_MULTIPLIER;
        }
    }

    // The current skip list has insufficient levels; a new skip list needs to be created. Each call
    // will create at most one new skip list level.
    if (numLevels > _skipLevel.load(std::memory_order_relaxed)) {
        AddSkipLevel();
    }

    // write level 0 data
    // Level 0 does not contain a childPointer.
    uint32_t childPointer = 0;
    {
        uint8_t output[GroupVarintCompressor::BUFFER_SIZE];
        size_t len;
        if (ttf.has_value()) {
            uint32_t ttf_value = ttf.value();
            uint32_t input[3] = {key - _lastKeys[0], ttf_value - _lastTTFs[0],
                                 _accumulateOffset - _lastOffsets[0]};
            // Level 0 does not contain a childPointer.
            len = GroupVarintCompressor::Compress(input, 3 , output);
            _lastTTFs[0] = ttf_value;
        } else {
            uint32_t input[2] = {key - _lastKeys[0], _accumulateOffset - _lastOffsets[0]};
            len = GroupVarintCompressor::Compress(input, 2, output);
        }
        _skipBuffers[0]->Write(output, len);
        _lastKeys[0] = key;
        _lastOffsets[0] = _accumulateOffset;
        childPointer = _skipBuffers[0]->GetSize();
    }

    // write higher level data
    for (size_t level = 1; level < numLevels; level++) {
        uint32_t newChildPointer = _skipBuffers[level]->GetSize();

        uint8_t output[GroupVarintCompressor::BUFFER_SIZE];
        size_t len;
        if (ttf.has_value()) {
            uint32_t ttf_value = ttf.value();
            uint32_t input[4] = {key - _lastKeys[level], ttf_value - _lastTTFs[level],
                                 _accumulateOffset - _lastOffsets[level], childPointer};
            len = GroupVarintCompressor::Compress(input,  4, output);
            _lastTTFs[level] = ttf_value;
        } else {
            uint32_t input[3] = {key - _lastKeys[level], _accumulateOffset - _lastOffsets[level],
                                 childPointer};
            len = GroupVarintCompressor::Compress(input, 3, output);
        }
        _skipBuffers[level]->Write(output, len);
        _lastKeys[level] = key;
        _lastOffsets[level] = _accumulateOffset;

        childPointer = newChildPointer;
    }
    // Once all modifications to the skiplist are complete, update the view from bottom to top,
    // making the changes to the skiplist visible externally.
    for (size_t level = 0; level < numLevels; level++) {
        _levelEnds[level].store(_skipBuffers[level]->GetSize(), std::memory_order_release);
    }
}

void MultiLevelSkipListWriter::AddItem(uint32_t key, uint32_t ttf, uint32_t offset) {
    AddItemInternal(key, offset, std::make_optional(ttf));
}

void MultiLevelSkipListWriter::AddItem(uint32_t key, uint32_t offset) {
    AddItemInternal(key, offset);
}

void MultiLevelSkipListWriter::Dump(const file_system::FileWriterPtr& file) {
    if (_skipBuffers.size() == 0) {
        return;
    } else {
        for (size_t i = 1; i < _skipBuffers.size(); ++i) {
            file->WriteVUInt32(_skipBuffers[i]->GetSize()).GetOrThrow();
        }
        for (size_t i = 0; i < _skipBuffers.size(); ++i) {
            _skipBuffers[i]->Dump(file).GetOrThrow();
        }
    }
}

size_t MultiLevelSkipListWriter::EstimateDumpSize() const {
    uint32_t skipListSize = 0;
    for (size_t i = 0; i < _skipBuffers.size(); ++i) {
        // The length of level0 is not encoded; it can be directly derived from the total length.
        if (i != 0) {
            skipListSize += VByteCompressor::GetVInt32Length(_skipBuffers[i]->GetSize());
        }
        skipListSize += _skipBuffers[i]->GetSize();
    }
    return skipListSize;
}

} // namespace indexlib::index
