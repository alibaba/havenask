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

#include "autil/LongHashValue.h"

namespace indexlibv2::index {

class SingleEncodedNullValue
{
public:
    template <typename T>
    static void GetEncodedValue(void* ret)
    {
#define ENCODE_NULL_VALUE(type)                                                                                        \
    if (std::is_same<T, type>::value) {                                                                                \
        static type minValue = std::numeric_limits<type>::min();                                                       \
        memcpy(ret, &minValue, sizeof(type));                                                                          \
        return;                                                                                                        \
    }
        ENCODE_NULL_VALUE(uint8_t);
        ENCODE_NULL_VALUE(int8_t);
        ENCODE_NULL_VALUE(uint16_t);
        ENCODE_NULL_VALUE(int16_t);
        ENCODE_NULL_VALUE(uint32_t);
        ENCODE_NULL_VALUE(int32_t);
        ENCODE_NULL_VALUE(uint64_t);
        ENCODE_NULL_VALUE(int64_t);
        ENCODE_NULL_VALUE(float);
        ENCODE_NULL_VALUE(double);
        ENCODE_NULL_VALUE(char);
        ENCODE_NULL_VALUE(bool);
#undef ENCODE_NULL_VALUE
        if (std::is_same<T, autil::LongHashValue<2>>::value) {
            return;
        }
        assert(false);
    }

public:
    static constexpr uint32_t NULL_FIELD_BITMAP_SIZE = 64;
    static constexpr int16_t ENCODED_FP16_NULL_VALUE = std::numeric_limits<int16_t>::min();
    static constexpr int8_t ENCODED_FP8_NULL_VALUE = std::numeric_limits<int8_t>::min();
    static constexpr uint32_t BITMAP_HEADER_SIZE = sizeof(uint64_t);
};

} // namespace indexlibv2::index
