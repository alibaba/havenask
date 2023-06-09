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

#include "indexlib/index/inverted_index/InDocPositionState.h"

namespace indexlib::index {
class PositionListSegmentDecoder;
class InDocPositionIteratorState : public InDocPositionState
{
public:
    InDocPositionIteratorState(uint8_t compressMode = PFOR_DELTA_COMPRESS_MODE,
                               const PositionListFormatOption& option = PositionListFormatOption())
        : InDocPositionState(option)
        , _posSegDecoder(nullptr)
        , _recordOffset(0)
        , _offsetInRecord(0)
        , _totalPosCount(0)
        , _compressMode(compressMode)
    {
    }

    InDocPositionIteratorState(const InDocPositionIteratorState& other)
        : InDocPositionState(other)
        , _posSegDecoder(other._posSegDecoder)
        , _recordOffset(other._recordOffset)
        , _offsetInRecord(other._offsetInRecord)
        , _totalPosCount(other._totalPosCount)
        , _compressMode(other._compressMode)
    {
    }

    virtual ~InDocPositionIteratorState() = default;

    tf_t GetTermFreq() override;

    void SetPositionListSegmentDecoder(PositionListSegmentDecoder* posSegDecoder) { _posSegDecoder = posSegDecoder; }
    PositionListSegmentDecoder* GetPositionListSegmentDecoder() const { return _posSegDecoder; }
    void SetCompressMode(uint8_t compressMode) { _compressMode = compressMode; }
    uint8_t GetCompressMode() const { return _compressMode; }
    void SetRecordOffset(int32_t recordOffset) { _recordOffset = recordOffset; }
    int32_t GetRecordOffset() const { return _recordOffset; }
    void SetOffsetInRecord(int32_t offsetInRecord) { _offsetInRecord = offsetInRecord; }
    int32_t GetOffsetInRecord() const { return _offsetInRecord; }
    void SetTotalPosCount(uint32_t totalPosCount) { _totalPosCount = totalPosCount; }
    uint32_t GetTotalPosCount() const { return _totalPosCount; }

protected:
    void Clone(InDocPositionIteratorState* state) const;

    PositionListSegmentDecoder* _posSegDecoder;
    int32_t _recordOffset;
    int32_t _offsetInRecord;
    uint32_t _totalPosCount;
    uint8_t _compressMode;

private:
    AUTIL_LOG_DECLARE();
};

//////////////////////////////////////////////////////////
inline void InDocPositionIteratorState::Clone(InDocPositionIteratorState* state) const
{
    InDocPositionState::Clone(state);
    state->_posSegDecoder = _posSegDecoder;
    state->_compressMode = _compressMode;
    state->_recordOffset = _recordOffset;
    state->_offsetInRecord = _offsetInRecord;
    state->_totalPosCount = _totalPosCount;
}
} // namespace indexlib::index
