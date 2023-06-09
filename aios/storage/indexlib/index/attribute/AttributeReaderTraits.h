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

#include "indexlib/base/Types.h"
#include "indexlib/index/attribute/MultiValueAttributeDiskIndexer.h"
#include "indexlib/index/attribute/MultiValueAttributeMemReader.h"
#include "indexlib/index/attribute/SingleValueAttributeDiskIndexer.h"
#include "indexlib/index/attribute/SingleValueAttributeMemReader.h"

namespace indexlibv2::index {

template <class T>
struct AttributeReaderTraits {
public:
    using SegmentReader = SingleValueAttributeDiskIndexer<T>;
    using SegmentReadContext = typename SegmentReader::ReadContext;
    using InMemSegmentReader = SingleValueAttributeMemReader<T>;
};

#define DECLARE_READER_TRAITS_FOR_MULTI_VALUE(type)                                                                    \
    template <>                                                                                                        \
    struct AttributeReaderTraits<autil::MultiValueType<type>> {                                                        \
    public:                                                                                                            \
        using SegmentReader = MultiValueAttributeDiskIndexer<type>;                                                    \
        using SegmentReadContext = typename SegmentReader::ReadContext;                                                \
        using InMemSegmentReader = MultiValueAttributeMemReader<type>;                                                 \
    };

DECLARE_READER_TRAITS_FOR_MULTI_VALUE(char)
DECLARE_READER_TRAITS_FOR_MULTI_VALUE(int8_t)
DECLARE_READER_TRAITS_FOR_MULTI_VALUE(uint8_t)
DECLARE_READER_TRAITS_FOR_MULTI_VALUE(int16_t)
DECLARE_READER_TRAITS_FOR_MULTI_VALUE(uint16_t)
DECLARE_READER_TRAITS_FOR_MULTI_VALUE(int32_t)
DECLARE_READER_TRAITS_FOR_MULTI_VALUE(uint32_t)
DECLARE_READER_TRAITS_FOR_MULTI_VALUE(int64_t)
DECLARE_READER_TRAITS_FOR_MULTI_VALUE(uint64_t)
DECLARE_READER_TRAITS_FOR_MULTI_VALUE(float)
DECLARE_READER_TRAITS_FOR_MULTI_VALUE(double)
DECLARE_READER_TRAITS_FOR_MULTI_VALUE(autil::MultiChar)
DECLARE_READER_TRAITS_FOR_MULTI_VALUE(autil::uint128_t)

#undef DECLARE_READER_TRAITS_FOR_MULTI_VALUE

} // namespace indexlibv2::index
