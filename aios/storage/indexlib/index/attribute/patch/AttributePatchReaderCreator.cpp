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
#include "indexlib/index/attribute/patch/AttributePatchReaderCreator.h"

#include "indexlib/index/attribute/patch/MultiValueAttributePatchReader.h"
#include "indexlib/index/attribute/patch/SingleValueAttributePatchReader.h"
#include "indexlib/index/common/FieldTypeTraits.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, AttributePatchReaderCreator);

std::shared_ptr<AttributePatchReader>
AttributePatchReaderCreator::Create(const std::shared_ptr<config::AttributeConfig>& attrConfig)
{
    if (!attrConfig->IsAttributeUpdatable()) {
        return nullptr;
    }

    if (attrConfig->GetFieldType() == ft_string) {
        if (attrConfig->IsMultiValue()) {
            return std::make_shared<MultiValueAttributePatchReader<autil::MultiChar>>(attrConfig);
        }
        return std::make_shared<MultiValueAttributePatchReader<char>>(attrConfig);
    }

    if (attrConfig->IsMultiValue()) {
        return CreateMultiValuePatchReader(attrConfig);
    } else {
        return CreateSingleValuePatchReader(attrConfig);
    }
}

std::shared_ptr<AttributePatchReader>
AttributePatchReaderCreator::CreateMultiValuePatchReader(const std::shared_ptr<config::AttributeConfig>& attrConfig)
{
#define CREATE_MULTI_VALUE_PATCH_READER_BY_TYPE(field_type)                                                            \
    case field_type: {                                                                                                 \
        typedef indexlib::index::FieldTypeTraits<field_type>::AttrItemType Type;                                       \
        return std::make_shared<MultiValueAttributePatchReader<Type>>(attrConfig);                                     \
        break;                                                                                                         \
    }

    switch (attrConfig->GetFieldType()) {
        CREATE_MULTI_VALUE_PATCH_READER_BY_TYPE(ft_int32);
        CREATE_MULTI_VALUE_PATCH_READER_BY_TYPE(ft_uint32);
        CREATE_MULTI_VALUE_PATCH_READER_BY_TYPE(ft_float);
        CREATE_MULTI_VALUE_PATCH_READER_BY_TYPE(ft_fp8);
        CREATE_MULTI_VALUE_PATCH_READER_BY_TYPE(ft_fp16);
        CREATE_MULTI_VALUE_PATCH_READER_BY_TYPE(ft_int64);
        CREATE_MULTI_VALUE_PATCH_READER_BY_TYPE(ft_uint64);
        CREATE_MULTI_VALUE_PATCH_READER_BY_TYPE(ft_hash_64);
        CREATE_MULTI_VALUE_PATCH_READER_BY_TYPE(ft_hash_128);
        CREATE_MULTI_VALUE_PATCH_READER_BY_TYPE(ft_int8);
        CREATE_MULTI_VALUE_PATCH_READER_BY_TYPE(ft_uint8);
        CREATE_MULTI_VALUE_PATCH_READER_BY_TYPE(ft_int16);
        CREATE_MULTI_VALUE_PATCH_READER_BY_TYPE(ft_uint16);
        CREATE_MULTI_VALUE_PATCH_READER_BY_TYPE(ft_double);
    default:
        AUTIL_LOG(ERROR,
                  "unsupported field type [%d] "
                  "for patch iterator!",
                  attrConfig->GetFieldType());
        return nullptr;
    }
    return nullptr;
}

std::shared_ptr<AttributePatchReader>
AttributePatchReaderCreator::CreateSingleValuePatchReader(const std::shared_ptr<config::AttributeConfig>& attrConfig)
{
#define CREATE_SINGLE_VALUE_PATCH_READER_BY_TYPE(field_type)                                                           \
    case field_type: {                                                                                                 \
        typedef indexlib::index::FieldTypeTraits<field_type>::AttrItemType Type;                                       \
        return std::make_shared<SingleValueAttributePatchReader<Type>>(attrConfig);                                    \
        break;                                                                                                         \
    }

    switch (attrConfig->GetFieldType()) {
        CREATE_SINGLE_VALUE_PATCH_READER_BY_TYPE(ft_int32);
        CREATE_SINGLE_VALUE_PATCH_READER_BY_TYPE(ft_uint32);
        CREATE_SINGLE_VALUE_PATCH_READER_BY_TYPE(ft_float);
        CREATE_SINGLE_VALUE_PATCH_READER_BY_TYPE(ft_fp8);
        CREATE_SINGLE_VALUE_PATCH_READER_BY_TYPE(ft_fp16);
        CREATE_SINGLE_VALUE_PATCH_READER_BY_TYPE(ft_int64);
        CREATE_SINGLE_VALUE_PATCH_READER_BY_TYPE(ft_uint64);
        CREATE_SINGLE_VALUE_PATCH_READER_BY_TYPE(ft_hash_64);
        CREATE_SINGLE_VALUE_PATCH_READER_BY_TYPE(ft_hash_128);
        CREATE_SINGLE_VALUE_PATCH_READER_BY_TYPE(ft_int8);
        CREATE_SINGLE_VALUE_PATCH_READER_BY_TYPE(ft_uint8);
        CREATE_SINGLE_VALUE_PATCH_READER_BY_TYPE(ft_int16);
        CREATE_SINGLE_VALUE_PATCH_READER_BY_TYPE(ft_uint16);
        CREATE_SINGLE_VALUE_PATCH_READER_BY_TYPE(ft_double);
        CREATE_SINGLE_VALUE_PATCH_READER_BY_TYPE(ft_time);
        CREATE_SINGLE_VALUE_PATCH_READER_BY_TYPE(ft_timestamp);
        CREATE_SINGLE_VALUE_PATCH_READER_BY_TYPE(ft_date);
    default:
        AUTIL_LOG(ERROR,
                  "unsupported field type [%d] "
                  "for patch iterator!",
                  attrConfig->GetFieldType());
        return nullptr;
    }
    return nullptr;
}
} // namespace indexlibv2::index
