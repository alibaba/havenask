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

#include "indexlib/index/inverted_index/Constant.h"
#include "indexlib/index/inverted_index/format/skiplist/SkipListReader.h"

namespace indexlib::index {
class PairValueSkipListReader : public SkipListReader
{
private:
    using SkipListReader::Load;

public:
    static const uint32_t ITEM_SIZE = sizeof(uint32_t) * 2;
    using keytype_t = uint32_t;
    PairValueSkipListReader(bool isReferenceCompress = false);
    PairValueSkipListReader(const PairValueSkipListReader& other);
    virtual ~PairValueSkipListReader();

public:
    virtual void Load(const util::ByteSliceList* byteSliceList, uint32_t start, uint32_t end,
                      const uint32_t& itemCount);

    virtual void Load(util::ByteSlice* byteSlice, uint32_t start, uint32_t end, const uint32_t& itemCount);

    std::pair<Status, bool> SkipTo(uint32_t queryKey, uint32_t& key, uint32_t& prevKey, uint32_t& value,
                                   uint32_t& delta) override;
    inline std::pair<Status, bool> SkipTo(uint32_t queryKey, uint32_t& key, uint32_t& value, uint32_t& delta);

    uint32_t GetPrevKey() const { return _prevKey; }

    uint32_t GetCurrentKey() const { return _currentKey; }

    virtual std::pair<Status, bool> LoadBuffer();

    uint32_t GetLastValueInBuffer() const override;
    uint32_t GetLastKeyInBuffer() const override;

public:
    // for test
    void SetPrevKey(uint32_t prevKey) { _prevKey = prevKey; }
    void SetCurrentKey(uint32_t curKey) { _currentKey = curKey; }

private:
    void InnerLoad(uint32_t start, uint32_t end, const uint32_t& itemCount);

protected:
    uint32_t _currentKey;
    uint32_t _currentValue;
    uint32_t _prevKey;
    uint32_t _prevValue;

protected:
    uint32_t _keyBuffer[SKIP_LIST_BUFFER_SIZE];
    uint32_t _valueBuffer[SKIP_LIST_BUFFER_SIZE];
    uint32_t _currentCursor;
    uint32_t _numInBuffer;
    uint32_t* _keyBufferBase;
    uint32_t* _valueBufferBase;
    bool _isReferenceCompress;

private:
    AUTIL_LOG_DECLARE();
};

///////////////////////////////////////////////
inline std::pair<Status, bool> PairValueSkipListReader::SkipTo(uint32_t queryKey, uint32_t& key, uint32_t& value,
                                                               uint32_t& delta)
{
    return SkipTo(queryKey, key, _prevKey, value, delta);
}
} // namespace indexlib::index
