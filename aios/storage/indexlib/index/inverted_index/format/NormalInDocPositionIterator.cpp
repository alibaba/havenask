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
#include "indexlib/index/inverted_index/format/NormalInDocPositionIterator.h"

#include "indexlib/index/inverted_index/format/PositionListSegmentDecoder.h"
#include "indexlib/util/Exception.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, NormalInDocPositionIterator);

NormalInDocPositionIterator::NormalInDocPositionIterator(PositionListFormatOption option)
    : _currentPos(-1)
    , _visitedPosInBuffer(-1)
    , _visitedPosInDoc(-1)
    , _posCountInBuffer(0)
    , _offsetInRecord(0)
    , _totalPosCount(0)
    , _sectionMeta(nullptr)
    , _visitedSectionInDoc(-1)
    , _visitedSectionLen(0)
    , _currentSectionId(INVALID_SECID)
    , _currentFieldId(-1)
    , _option(option)
{
}

NormalInDocPositionIterator::~NormalInDocPositionIterator() { DELETE_AND_SET_NULL(_sectionMeta); }

void NormalInDocPositionIterator::Init(const NormalInDocState& state)
{
    PositionListSegmentDecoder* posSegDecoder = state.GetPositionListSegmentDecoder();
    assert(posSegDecoder);

    if (_state.GetPositionListSegmentDecoder() != posSegDecoder) {
        posSegDecoder->SetNeedReopen(true);
    }

    bool needUnpack = posSegDecoder->LocateRecord(&state, _totalPosCount);
    _offsetInRecord = posSegDecoder->GetOffsetInRecord();

    _state = state;

    if (needUnpack) {
        _visitedPosInBuffer = -1;
        _posCountInBuffer = 0;
    } else {
        _visitedPosInBuffer = _offsetInRecord - 1;
        _posBuffer[_visitedPosInBuffer + 1] += 1;
    }

    _currentPos = -1;
    _visitedPosInDoc = 0;
    _currentFieldId = -1;

    _visitedSectionInDoc = -1;
    _visitedSectionLen = 0;
    _currentSectionId = INVALID_SECID;
}

index::ErrorCode NormalInDocPositionIterator::SeekPositionWithErrorCode(pos_t pos, pos_t& result)
{
    pos = pos >= (_currentPos + 1) ? pos : (_currentPos + 1);
    while (pos > _currentPos) {
        if (++_visitedPosInDoc > (int32_t)_totalPosCount) {
            result = INVALID_POSITION;
            return index::ErrorCode::OK;
        }

        if (++_visitedPosInBuffer >= _posCountInBuffer) {
            PositionListSegmentDecoder* posSegDecoder = _state.GetPositionListSegmentDecoder();
            assert(posSegDecoder);
            try {
                _posCountInBuffer =
                    posSegDecoder->DecodeRecord(_posBuffer, MAX_POS_PER_RECORD, _posPayloadBuffer, MAX_POS_PER_RECORD);
            } catch (const util::FileIOException& e) {
                return index::ErrorCode::FileIO;
            }

            if (_visitedPosInBuffer == 0) {
                _visitedPosInBuffer = _offsetInRecord;
                _posBuffer[_visitedPosInBuffer] += 1;
            } else {
                _visitedPosInBuffer = 0;
            }
        }
        _currentPos += _posBuffer[_visitedPosInBuffer];
    }
    result = _currentPos;
    return index::ErrorCode::OK;
}

pospayload_t NormalInDocPositionIterator::GetPosPayload()
{
    if (HasPosPayload()) {
        return _posPayloadBuffer[_visitedPosInBuffer];
    }
    return 0;
}

sectionid_t NormalInDocPositionIterator::GetSectionId()
{
    if (!_state.GetSectionReader()) {
        return 0;
    }
    SeekSectionMeta();
    return _currentSectionId;
}

section_len_t NormalInDocPositionIterator::GetSectionLength()
{
    if (!_state.GetSectionReader()) {
        return 0;
    }
    SeekSectionMeta();
    return (section_len_t)_sectionMeta->GetSectionLen(_visitedSectionInDoc);
}

fieldid_t NormalInDocPositionIterator::GetFieldId()
{
    if (!_state.GetSectionReader()) {
        return INVALID_FIELDID;
    }
    SeekSectionMeta();
    // NOTE: _currentFieldId is fieldIdInPack, convert to real fieldid_t
    return _state.GetFieldId(_currentFieldId);
}

section_weight_t NormalInDocPositionIterator::GetSectionWeight()
{
    if (!_state.GetSectionReader()) {
        return 0;
    }
    SeekSectionMeta();
    return _sectionMeta->GetSectionWeight(_visitedSectionInDoc);
}

int32_t NormalInDocPositionIterator::GetFieldPosition()
{
    if (!_state.GetSectionReader()) {
        return INVALID_FIELDID;
    }
    SeekSectionMeta();
    return _currentFieldId;
}
} // namespace indexlib::index
