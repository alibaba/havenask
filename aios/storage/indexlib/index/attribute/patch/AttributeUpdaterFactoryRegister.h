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
#include "indexlib/index/attribute//patch/SingleValueAttributeUpdater.h"
#include "indexlib/index/attribute/AttributeFactoryRegister.h"
#include "indexlib/index/attribute/patch/AttributeUpdaterCreator.h"
#include "indexlib/index/attribute/patch/MultiValueAttributeUpdater.h"

namespace indexlibv2::index {

using AttributeUpdaterFactoryInstance = AttributeFactory<AttributeUpdater, AttributeUpdaterCreator>;

#undef REGISTER_SIMPLE_MULTI_VALUE_CREATOR_TO_ATTRIBUTE_FACTORY
#undef REGISTER_SIMPLE_SINGLE_VALUE_CREATOR_TO_ATTRIBUTE_FACTORY
#undef REGISTER_SPECIAL_MULTI_VALUE_CREATOR_TO_ATTRIBUTE_FACTORY
#undef REGISTER_SPECIAL_SINGLE_VALUE_CREATOR_TO_ATTRIBUTE_FACTORY

#define REGISTER_SIMPLE_SINGLE_VALUE_CREATOR_TO_ATTRIBUTE_FACTORY(TYPE, NAME)                                          \
    template <>                                                                                                        \
    __attribute__((unused)) void RegisterSingleValue##NAME(AttributeUpdaterFactoryInstance* factory)                   \
    {                                                                                                                  \
        factory->RegisterSingleValueCreator(std::make_unique<SingleValueAttributeUpdater<TYPE>::Creator>());           \
    }

#define REGISTER_SPECIAL_SINGLE_VALUE_CREATOR_TO_ATTRIBUTE_FACTORY(TYPE, NAME, FIELD_TYPE)                             \
    typedef SingleValueAttributeUpdater<TYPE> NAME##AttributeUpdater;                                                  \
    DECLARE_ATTRIBUTE_UPDATER_CREATOR(NAME##AttributeUpdater, FIELD_TYPE)                                              \
    template <>                                                                                                        \
    __attribute__((unused)) void RegisterSpecialSingleValue##NAME(AttributeUpdaterFactoryInstance* factory)            \
    {                                                                                                                  \
        factory->RegisterSingleValueCreator(std::make_unique<NAME##AttributeUpdaterCreator>());                        \
    }

#define REGISTER_SIMPLE_MULTI_VALUE_CREATOR_TO_ATTRIBUTE_FACTORY(TYPE, NAME)                                           \
    template <>                                                                                                        \
    __attribute__((unused)) void RegisterMultiValue##NAME(AttributeUpdaterFactoryInstance* factory)                    \
    {                                                                                                                  \
        factory->RegisterMultiValueCreator(std::make_unique<MultiValueAttributeUpdater<TYPE>::Creator>());             \
    }

#define REGISTER_SPECIAL_MULTI_VALUE_CREATOR_TO_ATTRIBUTE_FACTORY(TYPE, NAME, FIELD_TYPE)                              \
    typedef MultiValueAttributeUpdater<TYPE> NAME##AttributeUpdater;                                                   \
    DECLARE_ATTRIBUTE_UPDATER_CREATOR(NAME##AttributeUpdater, FIELD_TYPE)                                              \
    template <>                                                                                                        \
    __attribute__((unused)) void RegisterSpecialMultiValue##NAME(AttributeUpdaterFactoryInstance* factory)             \
    {                                                                                                                  \
        factory->RegisterMultiValueCreator(std::make_unique<NAME##AttributeUpdaterCreator>());                         \
    }

} // namespace indexlibv2::index
