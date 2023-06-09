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

#include "indexlib/base/Constant.h"
#include "indexlib/base/Types.h"
#include "indexlib/index/common/Types.h"

namespace indexlib::index {

struct SectionMeta {
    section_weight_t weight;
    uint16_t fieldId : 5; // this is fieldIdxInPack, not real fieldId of FieldSchema
    uint16_t length  : 11;
    SectionMeta() : weight(0), fieldId((uint8_t)INVALID_FIELDID & 0x1f), length(0) {}
    SectionMeta(section_weight_t w, fieldid_t fid, section_len_t len)
        : weight(w)
        , fieldId((uint8_t)fid)
        , length((uint16_t)len)
    {
    }

    bool operator==(const SectionMeta& other) const
    {
        return (weight == other.weight && length == other.length && fieldId == other.fieldId);
    }

    bool operator!=(const SectionMeta& other) const { return !(*this == other); }
};
} // namespace indexlib::index
