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
#include "indexlib/index/inverted_index/format/PositionListFormat.h"

#include "indexlib/index/common/numeric_compress/EncoderProvider.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, PositionListFormat);

void PositionListFormat::Init(const PositionListFormatOption& option)
{
    uint8_t rowCount = 0;
    uint32_t offset = 0;
    bool enableP4DeltaBlockOpt = (_formatVersion >= 1);

    _posValue = new PosValue();
    _posValue->SetLocation(rowCount++);
    _posValue->SetOffset(offset);
    for (size_t i = 0; i < COMPRESS_MODE_SIZE; ++i) {
        EncoderProvider::EncoderParam param(i, false, enableP4DeltaBlockOpt);
        _posValue->SetEncoder(i, EncoderProvider::GetInstance()->GetPosListEncoder(param));
    }
    AddAtomicValue(_posValue);
    offset += sizeof(pos_t);

    if (option.HasPositionPayload()) {
        _posPayloadValue = new PosPayloadValue();
        _posPayloadValue->SetLocation(rowCount++);
        _posPayloadValue->SetOffset(offset);
        for (size_t i = 0; i < COMPRESS_MODE_SIZE; ++i) {
            EncoderProvider::EncoderParam param(i, false, enableP4DeltaBlockOpt);
            _posPayloadValue->SetEncoder(i, EncoderProvider::GetInstance()->GetPosPayloadEncoder(param));
        }
        AddAtomicValue(_posPayloadValue);
        offset += sizeof(pospayload_t);
    }
    InitPositionSkipListFormat(option);
}

void PositionListFormat::InitPositionSkipListFormat(const PositionListFormatOption& option)
{
    if (_positionSkipListFormat) {
        DELETE_AND_SET_NULL(_positionSkipListFormat);
    }
    _positionSkipListFormat = new PositionSkipListFormat(option, _formatVersion);
    assert(_positionSkipListFormat);
}

} // namespace indexlib::index
