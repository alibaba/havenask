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

#include "autil/Log.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader_creator.h"
#include "indexlib/index/normal/attribute/accessor/attribute_reader_factory.h"
#include "indexlib/index/normal/attribute/accessor/single_value_attribute_reader.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_reader.h"

namespace indexlib::index {

#define REGISTER_SIMPLE_MULTI_VALUE_TO_ATTRIBUTE_READER_FACTORY(TYPE, NAME)                                            \
    __attribute__((unused)) void RegisterMultiValue##NAME(AttributeReaderFactory* factory)                             \
    {                                                                                                                  \
        factory->RegisterMultiValueCreator(AttributeReaderCreatorPtr(new VarNumAttributeReader<TYPE>::Creator()));     \
    }

#define REGISTER_SIMPLE_SINGLE_VALUE_TO_ATTRIBUTE_READER_FACTORY(TYPE, NAME)                                           \
    __attribute__((unused)) void RegisterSingleValue##NAME(AttributeReaderFactory* factory)                            \
    {                                                                                                                  \
        factory->RegisterCreator(AttributeReaderCreatorPtr(new SingleValueAttributeReader<TYPE>::Creator()));          \
    }

#define REGISTER_SPECIAL_MULTI_VALUE_TO_ATTRIBUTE_READER_FACTORY(TYPE, NAME, FIELD_TYPE)                               \
    typedef VarNumAttributeReader<TYPE> NAME##AttributeReader;                                                         \
    DECLARE_ATTRIBUTE_READER_CREATOR(NAME##AttributeReader, FIELD_TYPE)                                                \
    __attribute__((unused)) void RegisterSpecialMultiValue##NAME(AttributeReaderFactory* factory)                      \
    {                                                                                                                  \
        factory->RegisterMultiValueCreator(AttributeReaderCreatorPtr(new NAME##AttributeReaderCreator));               \
    }

#define REGISTER_SPECIAL_SINGLE_VALUE_TO_ATTRIBUTE_READER_FACTORY(TYPE, NAME, FIELD_TYPE)                              \
    typedef SingleValueAttributeReader<TYPE> NAME##AttributeReader;                                                    \
    DECLARE_ATTRIBUTE_READER_CREATOR(NAME##AttributeReader, FIELD_TYPE)                                                \
    __attribute__((unused)) void RegisterSpecialSingleValue##NAME(AttributeReaderFactory* factory)                     \
    {                                                                                                                  \
        factory->RegisterCreator(AttributeReaderCreatorPtr(new NAME##AttributeReaderCreator));                         \
    }

} // namespace indexlib::index
