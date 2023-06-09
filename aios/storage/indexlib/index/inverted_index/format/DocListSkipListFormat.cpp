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
#include "indexlib/index/inverted_index/format/DocListSkipListFormat.h"

#include "indexlib/index/common/numeric_compress/EncoderProvider.h"

namespace indexlib::index {

AUTIL_LOG_SETUP(indexlib.index, DocListSkipListFormat);

void DocListSkipListFormat::Init(const DocListFormatOption& option)
{
    uint8_t rowCount = 0;
    uint32_t offset = 0;
    bool enableP4DeltaBlockOpt = (_formatVersion >= 1);

#define ATOMIC_VALUE_INIT(value_type, atomic_value_type, encoder_func)                                                 \
    _##atomic_value_type##Value = new atomic_value_type##Value();                                                      \
    _##atomic_value_type##Value->SetLocation(rowCount++);                                                              \
    _##atomic_value_type##Value->SetOffset(offset);                                                                    \
    for (size_t i = 0; i < COMPRESS_MODE_SIZE; ++i) {                                                                  \
        EncoderProvider::EncoderParam param(i, false, enableP4DeltaBlockOpt);                                          \
        _##atomic_value_type##Value->SetEncoder(i, EncoderProvider::GetInstance()->encoder_func(param));               \
    }                                                                                                                  \
    AddAtomicValue(_##atomic_value_type##Value);                                                                       \
    offset += sizeof(value_type);

    ATOMIC_VALUE_INIT(uint32_t, DocId, GetSkipListEncoder);

    if (option.HasTfList()) {
        ATOMIC_VALUE_INIT(uint32_t, TotalTF, GetSkipListEncoder);
    }

    ATOMIC_VALUE_INIT(uint32_t, Offset, GetSkipListEncoder);
}

} // namespace indexlib::index
