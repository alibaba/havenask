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

#include "indexlib/index/kv/KVTypeId.h"

namespace indexlibv2::index {

class FixedLenValueExtractorUtil
{
public:
    static bool ValueExtract(void* data, const KVTypeId& typeId, autil::mem_pool::Pool* pool, autil::StringView& value);
};

inline bool FixedLenValueExtractorUtil::ValueExtract(void* data, const KVTypeId& typeId, autil::mem_pool::Pool* pool,
                                                     autil::StringView& value)
{
    if (typeId.compressTypeFlag == ct_int8) {
        autil::StringView input((char*)data, sizeof(int8_t));
        float* rawValue = (float*)pool->allocate(sizeof(float));
        if (indexlib::util::FloatInt8Encoder::Decode(typeId.compressType.GetInt8AbsMax(), input, (char*)(rawValue)) !=
            1) {
            return false;
        }
        value = {(char*)(rawValue), sizeof(float)};
        return true;
    } else if (typeId.compressTypeFlag == ct_fp16) {
        float* rawValue = (float*)pool->allocate(sizeof(float));
        autil::StringView input((char*)data, sizeof(int16_t));
        if (indexlib::util::Fp16Encoder::Decode(input, (char*)(rawValue)) != 1) {
            return false;
        }
        value = {(char*)(rawValue), sizeof(float)};
        return true;
    } else {
        assert(typeId.compressTypeFlag == ct_other);
        value = {(char*)(data), typeId.valueLen};
        return true;
    }
}

} // namespace indexlibv2::index
