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

#include "autil/Log.h"
#include "indexlib/index/attribute/AttributeFactoryRegister.h"
#include "indexlib/index/attribute/AttributeReaderCreator.h"
#include "indexlib/index/attribute/MultiValueAttributeReader.h"
#include "indexlib/index/attribute/SingleValueAttributeReader.h"

namespace indexlibv2::index {
using AttributeReaderFactory = AttributeFactory<AttributeReader, AttributeReaderCreator>;
#undef REGISTER_SIMPLE_MULTI_VALUE_CREATOR_TO_ATTRIBUTE_FACTORY
#undef REGISTER_SIMPLE_SINGLE_VALUE_CREATOR_TO_ATTRIBUTE_FACTORY
#undef REGISTER_SPECIAL_MULTI_VALUE_CREATOR_TO_ATTRIBUTE_FACTORY
#undef REGISTER_SPECIAL_SINGLE_VALUE_CREATOR_TO_ATTRIBUTE_FACTORY

#define REGISTER_SIMPLE_SINGLE_VALUE_CREATOR_TO_ATTRIBUTE_FACTORY(TYPE, NAME)                                          \
    template <>                                                                                                        \
    __attribute__((unused)) void RegisterSingleValue##NAME(AttributeReaderFactory* factory)                            \
    {                                                                                                                  \
        factory->RegisterSingleValueCreator(std::make_unique<SingleValueAttributeReader<TYPE>::Creator>());            \
    }

#define REGISTER_SPECIAL_SINGLE_VALUE_CREATOR_TO_ATTRIBUTE_FACTORY(TYPE, NAME, FIELD_TYPE)                             \
    typedef SingleValueAttributeReader<TYPE> NAME##AttributeReader;                                                    \
    DECLARE_ATTRIBUTE_READER_CREATOR_CLASS(NAME##AttributeReader, FIELD_TYPE)                                          \
    template <>                                                                                                        \
    __attribute__((unused)) void RegisterSpecialSingleValue##NAME(AttributeReaderFactory* factory)                     \
    {                                                                                                                  \
        factory->RegisterSingleValueCreator(std::make_unique<NAME##AttributeReaderCreator>());                         \
    }

#define REGISTER_SIMPLE_MULTI_VALUE_CREATOR_TO_ATTRIBUTE_FACTORY(TYPE, NAME)                                           \
    template <>                                                                                                        \
    __attribute__((unused)) void RegisterMultiValue##NAME(AttributeReaderFactory* factory)                             \
    {                                                                                                                  \
        factory->RegisterMultiValueCreator(std::make_unique<MultiValueAttributeReader<TYPE>::Creator>());              \
    }

#define REGISTER_SPECIAL_MULTI_VALUE_CREATOR_TO_ATTRIBUTE_FACTORY(TYPE, NAME, FIELD_TYPE)                              \
    typedef MultiValueAttributeReader<TYPE> NAME##AttributeReader;                                                     \
    DECLARE_ATTRIBUTE_READER_CREATOR_CLASS(NAME##AttributeReader, FIELD_TYPE)                                          \
    template <>                                                                                                        \
    __attribute__((unused)) void RegisterSpecialMultiValue##NAME(AttributeReaderFactory* factory)                      \
    {                                                                                                                  \
        factory->RegisterMultiValueCreator(std::make_unique<NAME##AttributeReaderCreator>());                          \
    }

typedef MultiValueAttributeReader<char> StringAttributeReader;
} // namespace indexlibv2::index
