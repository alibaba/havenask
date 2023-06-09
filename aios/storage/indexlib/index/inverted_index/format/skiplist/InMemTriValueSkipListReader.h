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
#include "indexlib/index/inverted_index/format/BufferedByteSliceReader.h"
#include "indexlib/index/inverted_index/format/skiplist/TriValueSkipListReader.h"

namespace indexlib::index {

class InMemTriValueSkipListReader : public TriValueSkipListReader
{
public:
    explicit InMemTriValueSkipListReader(autil::mem_pool::Pool* sessionPool = nullptr);
    ~InMemTriValueSkipListReader();

    void Load(const util::ByteSliceList* byteSliceList, uint32_t start, uint32_t end,
              const uint32_t& itemCount) override
    {
        assert(false);
    }

    void Load(util::ByteSlice* byteSlice, uint32_t start, uint32_t end, const uint32_t& itemCount) override
    {
        assert(false);
    }

    void Load(BufferedByteSlice* postingBuffer);

    uint32_t GetLastValueInBuffer() const override;
    uint32_t GetLastKeyInBuffer() const override;

protected:
    std::pair<Status, bool> LoadBuffer() override;

private:
    autil::mem_pool::Pool* _sessionPool;
    BufferedByteSlice* _skipListBuffer;
    BufferedByteSliceReader _skipListReader;

    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index
