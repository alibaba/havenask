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

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <vector>

#include "absl/container/inlined_vector.h"
#include "indexlib/index/inverted_index/format/BufferedByteSlice.h"
#include "indexlib/index/inverted_index/format/ShortListOptimizeUtil.h"
#include "indexlib/index/inverted_index/format/skiplist/MultiLevelSkipListReader.h"
#include "indexlib/index/inverted_index/format/skiplist/SkipListWriter.h"

namespace indexlib::index {

class MultiLevelSkipListWriter : public SkipListWriter {
public:
    MultiLevelSkipListWriter(autil::mem_pool::Pool *byteSlicePool);
    virtual ~MultiLevelSkipListWriter();

public:
    void AddItem(uint32_t offset) override {};
    void AddItem(uint32_t key, uint32_t offset) override;
    void AddItem(uint32_t key, uint32_t ttf, uint32_t offset) override;
    size_t FinishFlush() override {
        return 0;
    };
    size_t EstimateDumpSize() const override;
    void Dump(const std::shared_ptr<file_system::FileWriter> &file) override;
    template <bool EnableTTF>
    void SnapShot(MultiLevelSkipListReader<EnableTTF> *reader, autil::mem_pool::Pool *pool) const;

private:
    void AddSkipLevel();
    void AddItemInternal(uint32_t key, uint32_t offset, std::optional<uint32_t> ttf = std::nullopt);

private:
    autil::mem_pool::Pool *_byteSlicePool;

    constexpr static uint32_t WRITER_INLINE_NUM = 3;
    absl::InlinedVector<uint32_t, WRITER_INLINE_NUM> _lastKeys;
    absl::InlinedVector<uint32_t, WRITER_INLINE_NUM> _lastTTFs;
    absl::InlinedVector<uint32_t, WRITER_INLINE_NUM> _lastOffsets;
    absl::InlinedVector<file_system::ByteSliceWriter *, WRITER_INLINE_NUM> _skipBuffers;

    std::atomic<uint32_t> _levelEnds[MAX_SKIP_LEVEL];
    // Record the maximum skip list level. The skip list will not exceed 256, so uint8_t is
    // sufficient for storage.
    std::atomic<uint8_t> _skipLevel = 0;
    // The number of elements indexed in the skip list, i.e., the total number of skip lists in
    // level 0.
    uint32_t _skipCount = 0;
    // Record the current offset for writing to the skip list.
    uint32_t _accumulateOffset = 0;

private:
    friend class MultiLevelSkipListWriterTest;
    AUTIL_LOG_DECLARE();
};

template <bool EnableTTF>
void MultiLevelSkipListWriter::SnapShot(MultiLevelSkipListReader<EnableTTF> *reader,
                                        autil::mem_pool::Pool *pool) const {
    // Create a snapshot
    uint8_t currentLevel = _skipLevel.load(std::memory_order_acquire);

    // The current skiplist is empty.
    if (currentLevel == 0) {
        // Used to exit immediately after SkipTo check.
        reader->_skipLevel = 0;
        return;
    }

    // Ensure that each skip list entry at the high level can index its corresponding low level
    // entry.
    // 1. Write: Update from bottom to top, ensuring that the corresponding low level entry has
    // already been written when each high level entry is written.
    // 2. Read: Read from top to bottom, ensuring that the corresponding low level entry has already
    // been written when each high level entry is read.
    uint32_t levelEnds[MAX_SKIP_LEVEL];
    for (int32_t level = currentLevel - 1; level >= 0; level--) {
        levelEnds[level] = _levelEnds[level].load(std::memory_order_acquire);
    }

    // Check again, level 0 might be null.
    if (unlikely(levelEnds[0] == 0)) {
        reader->_skipLevel = 0;
        return;
    }

    // Start initializing the skip list
    reader->_skipLevel = currentLevel;
    reader->allocateData(pool);
    for (size_t i = 0; i < reader->_skipLevel; i++) {
        reader->_metas[i].levelEnd = levelEnds[i];
        reader->_readers[i].Open(_skipBuffers[i]->GetByteSliceList()).GetOrThrow();
    }
    reader->LoadFirstItem();
}

} // namespace indexlib::index
