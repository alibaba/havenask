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

#include <memory>

#include "indexlib/index/inverted_index/format/BufferedByteSlice.h"
#include "indexlib/index/inverted_index/format/ShortListOptimizeUtil.h"

namespace indexlib::index {

class BufferedSkipListWriter : public BufferedByteSlice
{
public:
    BufferedSkipListWriter(autil::mem_pool::Pool* byteSlicePool, autil::mem_pool::RecyclePool* bufferPool,
                           index::CompressMode compressMode = index::PFOR_DELTA_COMPRESS_MODE);
    virtual ~BufferedSkipListWriter() = default;

    void AddItem(uint32_t deltaValue1);
    void AddItem(uint32_t key, uint32_t value1);
    void AddItem(uint32_t key, uint32_t value1, uint32_t value2);

    size_t FinishFlush();

    void Dump(const std::shared_ptr<file_system::FileWriter>& file) override;
    size_t EstimateDumpSize() const override;

    // include sizeof(BufferedSkipListWriter), ByteSliceWriter & ShortBuffe use
    // number 250 is estimate by unit test
    static constexpr size_t ESTIMATE_INIT_MEMORY_USE = 250;

protected:
    size_t DoFlush(uint8_t compressMode) override;

private:
    static const uint32_t INVALID_LAST_KEY = (uint32_t)-1;
    bool IsReferenceCompress() const { return _lastKey == INVALID_LAST_KEY; }

    uint32_t _lastKey;
    uint32_t _lastValue1;

    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
