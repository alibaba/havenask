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
#include "indexlib/index/normal/attribute/accessor/attribute_segment_patch_iterator_creator.h"

#include "indexlib/config/field_type_traits.h"
#include "indexlib/index/normal/attribute/accessor/single_value_attribute_patch_reader.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_patch_reader.h"

using namespace std;
using namespace indexlib::config;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, AttributeSegmentPatchIteratorCreator);

AttributeSegmentPatchIteratorCreator::AttributeSegmentPatchIteratorCreator() {}

AttributeSegmentPatchIteratorCreator::~AttributeSegmentPatchIteratorCreator() {}

AttributeSegmentPatchIterator* AttributeSegmentPatchIteratorCreator::Create(const AttributeConfigPtr& attrConfig)
{
    if (!attrConfig->IsAttributeUpdatable()) {
        return NULL;
    }

    AttributeSegmentPatchIterator* iter = NULL;
    if (attrConfig->GetFieldType() == ft_string) {
        if (attrConfig->IsMultiValue()) {
            return new VarNumAttributePatchReader<autil::MultiChar>(attrConfig);
        }
        return new VarNumAttributePatchReader<char>(attrConfig);
    }

    if (attrConfig->IsMultiValue()) {
        iter = CreateVarNumSegmentPatchIterator(attrConfig);
    } else {
        iter = CreateSingleValueSegmentPatchIterator(attrConfig);
    }
    return iter;
}

AttributeSegmentPatchIterator*
AttributeSegmentPatchIteratorCreator::CreateVarNumSegmentPatchIterator(const AttributeConfigPtr& attrConfig)
{
#define CREATE_VAR_NUM_ITER_BY_TYPE(field_type)                                                                        \
    case field_type: {                                                                                                 \
        typedef FieldTypeTraits<field_type>::AttrItemType Type;                                                        \
        return new VarNumAttributePatchReader<Type>(attrConfig);                                                       \
        break;                                                                                                         \
    }

    switch (attrConfig->GetFieldType()) {
        CREATE_VAR_NUM_ITER_BY_TYPE(ft_int32);
        CREATE_VAR_NUM_ITER_BY_TYPE(ft_uint32);
        CREATE_VAR_NUM_ITER_BY_TYPE(ft_float);
        CREATE_VAR_NUM_ITER_BY_TYPE(ft_fp8);
        CREATE_VAR_NUM_ITER_BY_TYPE(ft_fp16);
        CREATE_VAR_NUM_ITER_BY_TYPE(ft_int64);
        CREATE_VAR_NUM_ITER_BY_TYPE(ft_uint64);
        CREATE_VAR_NUM_ITER_BY_TYPE(ft_hash_64);
        CREATE_VAR_NUM_ITER_BY_TYPE(ft_hash_128);
        CREATE_VAR_NUM_ITER_BY_TYPE(ft_int8);
        CREATE_VAR_NUM_ITER_BY_TYPE(ft_uint8);
        CREATE_VAR_NUM_ITER_BY_TYPE(ft_int16);
        CREATE_VAR_NUM_ITER_BY_TYPE(ft_uint16);
        CREATE_VAR_NUM_ITER_BY_TYPE(ft_double);
    default:
        INDEXLIB_FATAL_ERROR(UnSupported,
                             "unsupported field type [%d] "
                             "for patch iterator!",
                             attrConfig->GetFieldType());
    }
    return NULL;
}

AttributeSegmentPatchIterator*
AttributeSegmentPatchIteratorCreator::CreateSingleValueSegmentPatchIterator(const AttributeConfigPtr& attrConfig)
{
#define CREATE_SINGLE_VALUE_ITER_BY_TYPE(field_type)                                                                   \
    case field_type: {                                                                                                 \
        typedef FieldTypeTraits<field_type>::AttrItemType Type;                                                        \
        return new SingleValueAttributePatchReader<Type>(attrConfig);                                                  \
        break;                                                                                                         \
    }

    switch (attrConfig->GetFieldType()) {
        CREATE_SINGLE_VALUE_ITER_BY_TYPE(ft_int32);
        CREATE_SINGLE_VALUE_ITER_BY_TYPE(ft_uint32);
        CREATE_SINGLE_VALUE_ITER_BY_TYPE(ft_float);
        CREATE_SINGLE_VALUE_ITER_BY_TYPE(ft_fp8);
        CREATE_SINGLE_VALUE_ITER_BY_TYPE(ft_fp16);
        CREATE_SINGLE_VALUE_ITER_BY_TYPE(ft_int64);
        CREATE_SINGLE_VALUE_ITER_BY_TYPE(ft_uint64);
        CREATE_SINGLE_VALUE_ITER_BY_TYPE(ft_hash_64);
        CREATE_SINGLE_VALUE_ITER_BY_TYPE(ft_hash_128);
        CREATE_SINGLE_VALUE_ITER_BY_TYPE(ft_int8);
        CREATE_SINGLE_VALUE_ITER_BY_TYPE(ft_uint8);
        CREATE_SINGLE_VALUE_ITER_BY_TYPE(ft_int16);
        CREATE_SINGLE_VALUE_ITER_BY_TYPE(ft_uint16);
        CREATE_SINGLE_VALUE_ITER_BY_TYPE(ft_double);
        CREATE_SINGLE_VALUE_ITER_BY_TYPE(ft_time);
        CREATE_SINGLE_VALUE_ITER_BY_TYPE(ft_timestamp);
        CREATE_SINGLE_VALUE_ITER_BY_TYPE(ft_date);
    default:
        INDEXLIB_FATAL_ERROR(UnSupported,
                             "unsupported field type [%d] "
                             "for patch iterator!",
                             attrConfig->GetFieldType());
    }
    return NULL;
}
}} // namespace indexlib::index
