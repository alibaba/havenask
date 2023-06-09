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

#include "indexlib/index/attribute/AttributeDiskIndexerCreator.h"
#include "indexlib/index/attribute/AttributeFactoryRegister.h"
#include "indexlib/index/attribute/MultiValueAttributeDiskIndexer.h"
#include "indexlib/index/attribute/SingleValueAttributeDiskIndexer.h"

namespace indexlibv2::index {
using AttributeDiskIndexerFactory = AttributeFactory<AttributeDiskIndexer, AttributeDiskIndexerCreator>;

#undef REGISTER_SIMPLE_MULTI_VALUE_CREATOR_TO_ATTRIBUTE_FACTORY
#undef REGISTER_SIMPLE_SINGLE_VALUE_CREATOR_TO_ATTRIBUTE_FACTORY
#undef REGISTER_SPECIAL_MULTI_VALUE_CREATOR_TO_ATTRIBUTE_FACTORY
#undef REGISTER_SPECIAL_SINGLE_VALUE_CREATOR_TO_ATTRIBUTE_FACTORY

#define REGISTER_SIMPLE_SINGLE_VALUE_CREATOR_TO_ATTRIBUTE_FACTORY(TYPE, NAME)                                          \
    template <>                                                                                                        \
    __attribute__((unused)) void RegisterSingleValue##NAME(AttributeDiskIndexerFactory* factory)                       \
    {                                                                                                                  \
        factory->RegisterSingleValueCreator(std::make_unique<SingleValueAttributeDiskIndexer<TYPE>::Creator>());       \
    }

#define REGISTER_SPECIAL_SINGLE_VALUE_CREATOR_TO_ATTRIBUTE_FACTORY(TYPE, NAME, FIELD_TYPE)                             \
    typedef SingleValueAttributeDiskIndexer<TYPE> NAME##AttributeDiskIndexer;                                          \
    DECLARE_ATTRIBUTE_DISK_INDEXER_CREATOR(NAME##AttributeDiskIndexer, FIELD_TYPE)                                     \
    template <>                                                                                                        \
    __attribute__((unused)) void RegisterSpecialSingleValue##NAME(AttributeDiskIndexerFactory* factory)                \
    {                                                                                                                  \
        factory->RegisterSingleValueCreator(std::make_unique<NAME##AttributeDiskIndexerCreator>());                    \
    }

#define REGISTER_SIMPLE_MULTI_VALUE_CREATOR_TO_ATTRIBUTE_FACTORY(TYPE, NAME)                                           \
    template <>                                                                                                        \
    __attribute__((unused)) void RegisterMultiValue##NAME(AttributeDiskIndexerFactory* factory)                        \
    {                                                                                                                  \
        factory->RegisterMultiValueCreator(std::make_unique<MultiValueAttributeDiskIndexer<TYPE>::Creator>());         \
    }

#define REGISTER_SPECIAL_MULTI_VALUE_CREATOR_TO_ATTRIBUTE_FACTORY(TYPE, NAME, FIELD_TYPE)                              \
    typedef MultiValueAttributeDiskIndexer<TYPE> NAME##AttributeDiskIndexer;                                           \
    DECLARE_ATTRIBUTE_DISK_INDEXER_CREATOR(NAME##AttributeDiskIndexer, FIELD_TYPE)                                     \
    template <>                                                                                                        \
    __attribute__((unused)) void RegisterSpecialMultiValue##NAME(AttributeDiskIndexerFactory* factory)                 \
    {                                                                                                                  \
        factory->RegisterMultiValueCreator(std::make_unique<NAME##AttributeDiskIndexerCreator>());                     \
    }

} // namespace indexlibv2::index
