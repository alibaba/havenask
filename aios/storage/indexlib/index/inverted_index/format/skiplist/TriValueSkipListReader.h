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
class TriValueSkipListReader : public SkipListReader
{
    using SkipListReader::Load;

public:
    static const uint32_t ITEM_SIZE = sizeof(uint32_t) * 3;
    using keytype_t = uint32_t;

    TriValueSkipListReader(bool isReferenceCompress = false);
    TriValueSkipListReader(const TriValueSkipListReader& other);
    ~TriValueSkipListReader();

public:
    virtual void Load(const util::ByteSliceList* byteSliceList, uint32_t start, uint32_t end,
                      const uint32_t& itemCount);

    virtual void Load(util::ByteSlice* byteSlice, uint32_t start, uint32_t end, const uint32_t& itemCount);

    std::pair<Status, bool> SkipTo(uint32_t queryDocId, uint32_t& docId, uint32_t& prevDocId, uint32_t& offset,
                                   uint32_t& delta) override;

    inline std::pair<Status, bool> SkipTo(uint32_t queryDocId, uint32_t& docId, uint32_t& offset, uint32_t& delta);

    uint32_t GetPrevDocId() const { return _prevDocId; }

    uint32_t GetCurrentDocId() const { return _currentDocId; }

    uint32_t GetCurrentTTF() const override { return _currentTTF; }

    uint32_t GetPrevTTF() const override { return _prevTTF; }

protected:
    virtual std::pair<Status, bool> LoadBuffer();
    void InnerLoad(uint32_t start, uint32_t end, const uint32_t& itemCount);

protected:
    uint32_t _currentDocId;
    uint32_t _currentOffset;
    uint32_t _currentTTF;
    uint32_t _prevDocId;
    uint32_t _prevOffset;
    uint32_t _prevTTF;

protected:
    uint32_t _docIdBuffer[SKIP_LIST_BUFFER_SIZE];
    uint32_t _offsetBuffer[SKIP_LIST_BUFFER_SIZE];
    uint32_t _ttfBuffer[SKIP_LIST_BUFFER_SIZE];
    uint32_t _currentCursor;
    uint32_t _numInBuffer;

private:
    AUTIL_LOG_DECLARE();
};

//////////////////////////////////////////////
inline std::pair<Status, bool> TriValueSkipListReader::SkipTo(uint32_t queryDocId, uint32_t& docId, uint32_t& offset,
                                                              uint32_t& delta)
{
    return SkipTo(queryDocId, docId, _prevDocId, offset, delta);
}
} // namespace indexlib::index
