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
#include "indexlib/config/number_index_type_transformor.h"

#include "indexlib/config/field_config.h"
using namespace std;

namespace indexlib { namespace config {
AUTIL_LOG_SETUP(indexlib.config, NumberIndexTypeTransformor);
using namespace config;

NumberIndexTypeTransformor::NumberIndexTypeTransformor() {}

NumberIndexTypeTransformor::~NumberIndexTypeTransformor() {}

void NumberIndexTypeTransformor::Transform(IndexConfigPtr indexConfig, InvertedIndexType& type)
{
    // single field in number index
    assert(indexConfig->GetFieldCount() == 1);
    IndexConfig::Iterator iter = indexConfig->CreateIterator();
    auto fieldConfig = iter.Next();
    type = TransformFieldTypeToIndexType(fieldConfig->GetFieldType());
}

InvertedIndexType NumberIndexTypeTransformor::TransformFieldTypeToIndexType(FieldType fieldType)
{
    InvertedIndexType type = it_unknown;
    switch (fieldType) {
    case ft_int8:
        type = it_number_int8;
        break;
    case ft_uint8:
        type = it_number_uint8;
        break;
    case ft_int16:
        type = it_number_int16;
        break;
    case ft_uint16:
        type = it_number_uint16;
        break;
    case ft_integer:
        type = it_number_int32;
        break;
    case ft_uint32:
        type = it_number_uint32;
        break;
    case ft_long:
        type = it_number_int64;
        break;
    case ft_uint64:
        type = it_number_uint64;
        break;
    default:
        assert(false);
        break;
    }
    return type;
}
}} // namespace indexlib::config
