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

#include "indexlib/base/Types.h"

namespace indexlib::index {

template <typename T>
struct EquivalentCompressTraits {
    typedef T EncoderType;
    class ZigZagEncoder
    {
    public:
        static inline EncoderType Encode(T value) __attribute__((always_inline)) { return value; }
        static inline T Decode(EncoderType value) __attribute__((always_inline)) { return value; }
    };
};

#undef DECLARE_COMPRESS_TRAITS
#undef DECLARE_UNION_COMPRESS_TRAITS

#define DECLARE_UNION_COMPRESS_TRAITS(TYPE, ENCODETYPE)                                                                \
    template <>                                                                                                        \
    struct EquivalentCompressTraits<TYPE> {                                                                            \
        typedef ENCODETYPE EncoderType;                                                                                \
        union UnionValue {                                                                                             \
            TYPE oValue;                                                                                               \
            EncoderType eValue;                                                                                        \
        };                                                                                                             \
        class ZigZagEncoder                                                                                            \
        {                                                                                                              \
        public:                                                                                                        \
            static inline EncoderType Encode(TYPE value) __attribute__((always_inline))                                \
            {                                                                                                          \
                UnionValue uv;                                                                                         \
                uv.oValue = value;                                                                                     \
                return uv.eValue;                                                                                      \
            }                                                                                                          \
            static inline TYPE Decode(EncoderType value) __attribute__((always_inline))                                \
            {                                                                                                          \
                UnionValue uv;                                                                                         \
                uv.eValue = value;                                                                                     \
                return uv.oValue;                                                                                      \
            }                                                                                                          \
        };                                                                                                             \
    };

DECLARE_UNION_COMPRESS_TRAITS(float, uint32_t)
DECLARE_UNION_COMPRESS_TRAITS(double, uint64_t)

#define DECLARE_COMPRESS_TRAITS(TYPE, ENCODETYPE, BITNUM)                                                              \
    template <>                                                                                                        \
    struct EquivalentCompressTraits<TYPE> {                                                                            \
        typedef ENCODETYPE EncoderType;                                                                                \
        class ZigZagEncoder                                                                                            \
        {                                                                                                              \
        public:                                                                                                        \
            static inline EncoderType Encode(TYPE value) __attribute__((always_inline))                                \
            {                                                                                                          \
                return (value << 1) ^ (value >> BITNUM);                                                               \
            }                                                                                                          \
            static inline TYPE Decode(EncoderType value) __attribute__((always_inline))                                \
            {                                                                                                          \
                return (value >> 1) ^ -(TYPE)(value & 1);                                                              \
            }                                                                                                          \
        };                                                                                                             \
    };

DECLARE_COMPRESS_TRAITS(int8_t, uint8_t, 7)
DECLARE_COMPRESS_TRAITS(int16_t, uint16_t, 15)
DECLARE_COMPRESS_TRAITS(int32_t, uint32_t, 31)
DECLARE_COMPRESS_TRAITS(int64_t, uint64_t, 63)
} // namespace indexlib::index
