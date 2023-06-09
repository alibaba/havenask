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
#include "indexlib/index/inverted_index/format/PositionSkipListFormat.h"

#include "indexlib/index/common/numeric_compress/EncoderProvider.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, PositionSkipListFormat);

void PositionSkipListFormat::Init(const PositionListFormatOption& option)
{
    uint8_t rowCount = 0;
    uint32_t offset = 0;
    bool enableP4DeltaBlockOpt = (_formatVersion >= 1);

    if (!option.HasTfBitmap()) {
        _totalPosValue = new TotalPosValue();
        _totalPosValue->SetLocation(rowCount++);
        _totalPosValue->SetOffset(offset);
        for (size_t i = 0; i < COMPRESS_MODE_SIZE; ++i) {
            EncoderProvider::EncoderParam param(i, false, enableP4DeltaBlockOpt);
            _totalPosValue->SetEncoder(i, EncoderProvider::GetInstance()->GetSkipListEncoder(param));
        }
        AddAtomicValue(_totalPosValue);
        offset += sizeof(uint32_t);
    }

    _offsetValue = new OffsetValue();
    _offsetValue->SetLocation(rowCount++);
    _offsetValue->SetOffset(offset);
    for (size_t i = 0; i < COMPRESS_MODE_SIZE; ++i) {
        EncoderProvider::EncoderParam param(i, false, enableP4DeltaBlockOpt);
        const Int32Encoder* encoder = nullptr;
        if (option.HasTfBitmap()) {
            param.mode = SHORT_LIST_COMPRESS_MODE;
            encoder = EncoderProvider::GetInstance()->GetSkipListEncoder(param);
        } else {
            encoder = EncoderProvider::GetInstance()->GetSkipListEncoder(param);
        }
        _offsetValue->SetEncoder(i, encoder);
    }
    AddAtomicValue(_offsetValue);
}

} // namespace indexlib::index
