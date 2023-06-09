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

#include "indexlib/index/attribute/AttributeFactory.h"

namespace indexlibv2::index {
#undef REGISTER_SIMPLE_MULTI_VALUE_CREATOR_TO_ATTRIBUTE_FACTORY
#undef REGISTER_SIMPLE_SINGLE_VALUE_CREATOR_TO_ATTRIBUTE_FACTORY
#undef REGISTER_SPECIAL_MULTI_VALUE_CREATOR_TO_ATTRIBUTE_FACTORY
#undef REGISTER_SPECIAL_SINGLE_VALUE_CREATOR_TO_ATTRIBUTE_FACTORY

#define REGISTER_SIMPLE_SINGLE_VALUE_CREATOR_TO_ATTRIBUTE_FACTORY(TYPE, NAME)                                          \
    template <INSTANCE, CREATOR>                                                                                       \
    __attribute__((unused)) void RegisterSingleValue##NAME(AttributeFactory<INSTANCE, CREATOR>* factory)               \
    {                                                                                                                  \
        factory->RegisterSingleValueCreator(std::make_unique<INSTANCE<TYPE>::Creator>());                              \
    }

#define REGISTER_SPECIAL_SINGLE_VALUE_CREATOR_TO_ATTRIBUTE_FACTORY(TYPE, NAME, FIELD_TYPE)                             \
    template <INSTANCE, CREATOR>                                                                                       \
    __attribute__((unused)) void RegisterSpecialSingleValue##NAME(AttributeFactory<INSTANCE, CREATOR>* factory)        \
    {                                                                                                                  \
        assert(false);                                                                                                 \
    }

#define REGISTER_SIMPLE_MULTI_VALUE_CREATOR_TO_ATTRIBUTE_FACTORY(TYPE, NAME)                                           \
    template <INSTANCE, CREATOR>                                                                                       \
    __attribute__((unused)) void RegisterMultiValue##NAME(AttributeFactory<INSTANCE, CREATOR>* factory)                \
    {                                                                                                                  \
        factory->RegisterMultiValueCreator(std::make_unique<INSTANCE<TYPE>::Creator>());                               \
    }

#define REGISTER_SPECIAL_MULTI_VALUE_CREATOR_TO_ATTRIBUTE_FACTORY(TYPE, NAME, FIELD_TYPE)                              \
    template <INSTANCE, CREATOR>                                                                                       \
    __attribute__((unused)) void RegisterSpecialMultiValue##NAME(AttributeFactory<INSTANCE, CREATOR>* factory)         \
    {                                                                                                                  \
        assert(false);                                                                                                 \
    }

} // namespace indexlibv2::index
