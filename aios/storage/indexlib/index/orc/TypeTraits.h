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

#include <type_traits>

#include "orc/Type.hh"
#include "orc/Vector.hh"

namespace indexlibv2::index {

template <typename T>
struct make_signed_wrapper {
    using type = typename std::make_signed<T>::type;
};

template <>
struct make_signed_wrapper<double> {
    using type = double;
};

template <>
struct make_signed_wrapper<float> {
    using type = float;
};

template <typename T>
struct OrcTypeTraits {
};

template <orc::TypeKind>
struct OrcKindTypeTraits {
};

#define DECLARE_ORC_TYPE(T, OrcTypeKind, BatchType)                                                                    \
    template <>                                                                                                        \
    struct OrcTypeTraits<T> {                                                                                          \
        static constexpr orc::TypeKind TYPE_KIND = OrcTypeKind;                                                        \
        using ColumnVectorType = BatchType;                                                                            \
    };                                                                                                                 \
    template <>                                                                                                        \
    struct OrcKindTypeTraits<OrcTypeKind> {                                                                            \
        typedef T CppType;                                                                                             \
        using VectorBatchType = BatchType;                                                                             \
    }

DECLARE_ORC_TYPE(int8_t, orc::BYTE, orc::ByteVectorBatch);
DECLARE_ORC_TYPE(int16_t, orc::SHORT, orc::ShortVectorBatch);
DECLARE_ORC_TYPE(int32_t, orc::INT, orc::IntVectorBatch);
DECLARE_ORC_TYPE(int64_t, orc::LONG, orc::LongVectorBatch);
DECLARE_ORC_TYPE(uint8_t, orc::UNSIGNED_BYTE, orc::ByteVectorBatch);
DECLARE_ORC_TYPE(uint16_t, orc::UNSIGNED_SHORT, orc::ShortVectorBatch);
DECLARE_ORC_TYPE(uint32_t, orc::UNSIGNED_INT, orc::IntVectorBatch);
DECLARE_ORC_TYPE(uint64_t, orc::UNSIGNED_LONG, orc::LongVectorBatch);
DECLARE_ORC_TYPE(float, orc::FLOAT, orc::FloatVectorBatch);
DECLARE_ORC_TYPE(double, orc::DOUBLE, orc::DoubleVectorBatch);
DECLARE_ORC_TYPE(std::string, orc::STRING, orc::StringVectorBatch);

#undef DECLARE_ORC_TYPE

} // namespace indexlibv2::index
