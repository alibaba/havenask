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

#include "autil/MultiValueType.h"
#include "table/HllCtxColumnData.h"
#include "table/ListColumnData.h"
#include "table/SimpleColumnData.h"

namespace table {

template <typename T>
struct ColumnDataTraits {};

#define BUILD_SIMPLE_COLUMN_DATA_TYPE(cppType)                                                                         \
    template <>                                                                                                        \
    struct ColumnDataTraits<cppType> {                                                                                 \
        typedef SimpleColumnData<cppType> ColumnDataType;                                                              \
    }

BUILD_SIMPLE_COLUMN_DATA_TYPE(bool);
BUILD_SIMPLE_COLUMN_DATA_TYPE(int8_t);
BUILD_SIMPLE_COLUMN_DATA_TYPE(uint8_t);
BUILD_SIMPLE_COLUMN_DATA_TYPE(int16_t);
BUILD_SIMPLE_COLUMN_DATA_TYPE(uint16_t);
BUILD_SIMPLE_COLUMN_DATA_TYPE(int32_t);
BUILD_SIMPLE_COLUMN_DATA_TYPE(uint32_t);
BUILD_SIMPLE_COLUMN_DATA_TYPE(int64_t);
BUILD_SIMPLE_COLUMN_DATA_TYPE(uint64_t);
BUILD_SIMPLE_COLUMN_DATA_TYPE(float);
BUILD_SIMPLE_COLUMN_DATA_TYPE(double);
BUILD_SIMPLE_COLUMN_DATA_TYPE(std::string);
#undef BUILD_SIMPLE_COLUMN_DATA_TYPE

template <>
struct ColumnDataTraits<autil::HllCtx> {
    typedef HllCtxColumnData ColumnDataType;
};

#define BUILD_LIST_COLUMN_DATA_TYPE(cppType)                                                                           \
    template <>                                                                                                        \
    struct ColumnDataTraits<cppType> {                                                                                 \
        typedef ListColumnData<cppType> ColumnDataType;                                                                \
    }

BUILD_LIST_COLUMN_DATA_TYPE(autil::MultiValueType<bool>);
BUILD_LIST_COLUMN_DATA_TYPE(autil::MultiValueType<int8_t>);
BUILD_LIST_COLUMN_DATA_TYPE(autil::MultiValueType<uint8_t>);
BUILD_LIST_COLUMN_DATA_TYPE(autil::MultiValueType<int16_t>);
BUILD_LIST_COLUMN_DATA_TYPE(autil::MultiValueType<uint16_t>);
BUILD_LIST_COLUMN_DATA_TYPE(autil::MultiValueType<int32_t>);
BUILD_LIST_COLUMN_DATA_TYPE(autil::MultiValueType<uint32_t>);
BUILD_LIST_COLUMN_DATA_TYPE(autil::MultiValueType<int64_t>);
BUILD_LIST_COLUMN_DATA_TYPE(autil::MultiValueType<uint64_t>);
BUILD_LIST_COLUMN_DATA_TYPE(autil::MultiValueType<autil::uint128_t>);
BUILD_LIST_COLUMN_DATA_TYPE(autil::MultiValueType<float>);
BUILD_LIST_COLUMN_DATA_TYPE(autil::MultiValueType<double>);
BUILD_LIST_COLUMN_DATA_TYPE(autil::MultiValueType<char>);
BUILD_LIST_COLUMN_DATA_TYPE(autil::MultiValueType<autil::MultiValueType<char>>);

} // namespace table
