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
#include "indexlib/index/attribute/AttributeFactory.h"
#include "indexlib/index/attribute/merger/AttributeMergerCreator.h"
#include "indexlib/index/attribute/merger/MultiValueAttributeMerger.h"
#include "indexlib/index/attribute/merger/MultiValueAttributeMergerCreator.h"
#include "indexlib/index/attribute/merger/SingleValueAttributeMerger.h"

namespace indexlibv2::index {
using AttributeMergerFactory = AttributeFactory<AttributeMerger, AttributeMergerCreator>;
#undef REGISTER_SIMPLE_MULTI_VALUE_CREATOR_TO_ATTRIBUTE_FACTORY
#undef REGISTER_SIMPLE_SINGLE_VALUE_CREATOR_TO_ATTRIBUTE_FACTORY
#undef REGISTER_SPECIAL_MULTI_VALUE_CREATOR_TO_ATTRIBUTE_FACTORY
#undef REGISTER_SPECIAL_SINGLE_VALUE_CREATOR_TO_ATTRIBUTE_FACTORY

#define REGISTER_SIMPLE_SINGLE_VALUE_CREATOR_TO_ATTRIBUTE_FACTORY(TYPE, NAME)                                          \
    template <>                                                                                                        \
    __attribute__((unused)) void RegisterSingleValue##NAME(AttributeMergerFactory* factory)                            \
    {                                                                                                                  \
        factory->RegisterSingleValueCreator(std::make_unique<SingleValueAttributeMerger<TYPE>::Creator>());            \
    }

#define REGISTER_SPECIAL_SINGLE_VALUE_CREATOR_TO_ATTRIBUTE_FACTORY(TYPE, NAME, FIELD_TYPE)                             \
    typedef SingleValueAttributeMerger<TYPE> NAME##AttributeMerger;                                                    \
    DECLARE_ATTRIBUTE_MERGER_CREATOR_CLASS(NAME##AttributeMerger, FIELD_TYPE)                                          \
    template <>                                                                                                        \
    __attribute__((unused)) void RegisterSpecialSingleValue##NAME(AttributeMergerFactory* factory)                     \
    {                                                                                                                  \
        factory->RegisterSingleValueCreator(std::make_unique<NAME##AttributeMergerCreator>());                         \
    }

#define REGISTER_SIMPLE_MULTI_VALUE_CREATOR_TO_ATTRIBUTE_FACTORY(TYPE, NAME)                                           \
    template <>                                                                                                        \
    __attribute__((unused)) void RegisterMultiValue##NAME(AttributeMergerFactory* factory)                             \
    {                                                                                                                  \
        factory->RegisterMultiValueCreator(std::make_unique<MultiValueAttributeMergerCreator<TYPE>>());                \
    }

#define REGISTER_SPECIAL_MULTI_VALUE_CREATOR_TO_ATTRIBUTE_FACTORY(TYPE, NAME, FIELD_TYPE)                              \
    DECLARE_MULTI_VALUE_ATTRIBUTE_MERGER_CREATOR_CLASS(NAME##AttributeMerger, TYPE, FIELD_TYPE)                        \
    template <>                                                                                                        \
    __attribute__((unused)) void RegisterSpecialMultiValue##NAME(AttributeMergerFactory* factory)                      \
    {                                                                                                                  \
        factory->RegisterMultiValueCreator(std::make_unique<NAME##AttributeMergerCreator>());                          \
    }

} // namespace indexlibv2::index
