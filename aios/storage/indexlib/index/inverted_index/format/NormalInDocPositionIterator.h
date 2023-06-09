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

#include "indexlib/index/common/field_format/section_attribute/MultiSectionMeta.h"
#include "indexlib/index/inverted_index/InDocPositionIterator.h"
#include "indexlib/index/inverted_index/format/NormalInDocState.h"
#include "indexlib/index/inverted_index/format/PositionListFormatOption.h"
#include "indexlib/index/inverted_index/section_attribute/SectionAttributeReaderImpl.h"

namespace indexlib::index {

class NormalInDocPositionIterator : public InDocPositionIterator
{
public:
    explicit NormalInDocPositionIterator(PositionListFormatOption option = PositionListFormatOption());
    ~NormalInDocPositionIterator();

    void Init(const NormalInDocState& state);
    index::ErrorCode SeekPositionWithErrorCode(pos_t pos, pos_t& nextPos) override;
    pospayload_t GetPosPayload() override;
    sectionid_t GetSectionId() override;
    section_len_t GetSectionLength() override;
    section_weight_t GetSectionWeight() override;
    fieldid_t GetFieldId() override;
    int32_t GetFieldPosition() override;
    bool HasPosPayload() const override;
    pos_t GetCurrentPosition() const { return _currentPos; }

private:
    inline void SeekSectionMeta();

    pos_t _posBuffer[MAX_POS_PER_RECORD];
    pospayload_t _posPayloadBuffer[MAX_POS_PER_RECORD];

    NormalInDocState _state;

    int64_t _currentPos; // int64 for we can use -1 as initial stat
    int32_t _visitedPosInBuffer;
    int32_t _visitedPosInDoc;
    int32_t _posCountInBuffer;
    uint32_t _offsetInRecord;
    uint32_t _totalPosCount;

    uint8_t _sectionMetaBuf[MAX_SECTION_COUNT_PER_DOC * 5] __attribute__((aligned(8)));
    MultiSectionMeta* _sectionMeta;
    int32_t _visitedSectionInDoc;
    uint32_t _visitedSectionLen;
    sectionid_t _currentSectionId;
    fieldid_t _currentFieldId;

    PositionListFormatOption _option;

    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<NormalInDocPositionIterator> NormalInDocPositionIteratorPtr;
/////////////////////////////////////////////////////////////////////////////////////

inline void NormalInDocPositionIterator::SeekSectionMeta()
{
    if (_sectionMeta == nullptr) {
        _sectionMeta = new MultiSectionMeta();
    }
    if (_sectionMeta->GetSectionCount() == 0) {
        const SectionAttributeReaderImpl* sectionReader =
            static_cast<const SectionAttributeReaderImpl*>(_state.GetSectionReader());
        assert(sectionReader);
        if (0 > sectionReader->Read(_state.GetDocId(), _sectionMetaBuf, sizeof(_sectionMetaBuf))) {
            INDEXLIB_FATAL_ERROR(IndexCollapsed, "Read section attribute FAILED, docId: [%d]", _state.GetDocId());
        }

        _sectionMeta->Init(_sectionMetaBuf, sectionReader->HasFieldId(), sectionReader->HasSectionWeight());
    }

    while (_currentPos >= _visitedSectionLen) {
        _visitedSectionLen += _sectionMeta->GetSectionLen(++_visitedSectionInDoc);

        fieldid_t fieldId = _sectionMeta->GetFieldId(_visitedSectionInDoc);
        if (_currentFieldId != fieldId) {
            _currentSectionId = 0;
            _currentFieldId = fieldId;
        } else {
            _currentSectionId++;
        }
    }
}

inline bool NormalInDocPositionIterator::HasPosPayload() const { return _option.HasPositionPayload(); }
} // namespace indexlib::index
