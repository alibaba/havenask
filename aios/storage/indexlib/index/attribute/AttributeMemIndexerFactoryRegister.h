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
#include "indexlib/index/attribute/AttributeMemIndexerCreator.h"
#include "indexlib/index/attribute/MultiValueAttributeMemIndexer.h"
#include "indexlib/index/attribute/SingleValueAttributeMemIndexer.h"

namespace indexlibv2::index {
using AttributeMemIndexerFactory = AttributeFactory<AttributeMemIndexer, AttributeMemIndexerCreator>;

#undef REGISTER_SIMPLE_MULTI_VALUE_CREATOR_TO_ATTRIBUTE_FACTORY
#undef REGISTER_SIMPLE_SINGLE_VALUE_CREATOR_TO_ATTRIBUTE_FACTORY
#undef REGISTER_SPECIAL_MULTI_VALUE_CREATOR_TO_ATTRIBUTE_FACTORY
#undef REGISTER_SPECIAL_SINGLE_VALUE_CREATOR_TO_ATTRIBUTE_FACTORY

#define REGISTER_SIMPLE_SINGLE_VALUE_CREATOR_TO_ATTRIBUTE_FACTORY(TYPE, NAME)                                          \
    template <>                                                                                                        \
    __attribute__((unused)) void RegisterSingleValue##NAME(AttributeMemIndexerFactory* factory)                        \
    {                                                                                                                  \
        factory->RegisterSingleValueCreator(std::make_unique<SingleValueAttributeMemIndexer<TYPE>::Creator>());        \
    }

#define REGISTER_SPECIAL_SINGLE_VALUE_CREATOR_TO_ATTRIBUTE_FACTORY(TYPE, NAME, FIELD_TYPE)                             \
    typedef SingleValueAttributeMemIndexer<TYPE> NAME##AttributeMemIndexer;                                            \
    DECLARE_ATTRIBUTE_MEM_INDEXER_CREATOR(NAME##AttributeMemIndexer, FIELD_TYPE)                                       \
    template <>                                                                                                        \
    __attribute__((unused)) void RegisterSpecialSingleValue##NAME(AttributeMemIndexerFactory* factory)                 \
    {                                                                                                                  \
        factory->RegisterSingleValueCreator(std::make_unique<NAME##AttributeMemIndexerCreator>());                     \
    }

#define REGISTER_SIMPLE_MULTI_VALUE_CREATOR_TO_ATTRIBUTE_FACTORY(TYPE, NAME)                                           \
    template <>                                                                                                        \
    __attribute__((unused)) void RegisterMultiValue##NAME(AttributeMemIndexerFactory* factory)                         \
    {                                                                                                                  \
        factory->RegisterMultiValueCreator(std::make_unique<MultiValueAttributeMemIndexer<TYPE>::Creator>());          \
    }

#define REGISTER_SPECIAL_MULTI_VALUE_CREATOR_TO_ATTRIBUTE_FACTORY(TYPE, NAME, FIELD_TYPE)                              \
    typedef MultiValueAttributeMemIndexer<TYPE> NAME##AttributeMemIndexer;                                             \
    DECLARE_ATTRIBUTE_MEM_INDEXER_CREATOR(NAME##AttributeMemIndexer, FIELD_TYPE)                                       \
    template <>                                                                                                        \
    __attribute__((unused)) void RegisterSpecialMultiValue##NAME(AttributeMemIndexerFactory* factory)                  \
    {                                                                                                                  \
        factory->RegisterMultiValueCreator(std::make_unique<NAME##AttributeMemIndexerCreator>());                      \
    }

} // namespace indexlibv2::index
