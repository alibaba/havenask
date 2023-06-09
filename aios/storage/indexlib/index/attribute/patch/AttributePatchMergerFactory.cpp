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
#include "indexlib/index/attribute/patch/AttributePatchMergerFactory.h"

#include "indexlib/index/attribute/AttributeUpdateBitmap.h"
#include "indexlib/index/attribute/patch/AttributePatchMerger.h"
#include "indexlib/index/attribute/patch/MultiValueAttributePatchMerger.h"
#include "indexlib/index/attribute/patch/SingleValueAttributePatchMerger.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, AttributePatchMergerFactory);

#define CREATE_ATTRIBUTE_PATCH_MERGER(fieldType, valueType)                                                            \
    case fieldType: {                                                                                                  \
        if (isMultiValue) {                                                                                            \
            return std::make_unique<MultiValueAttributePatchMerger<valueType>>(attributeConfig, updateBitmap);         \
        } else {                                                                                                       \
            return std::make_unique<SingleValueAttributePatchMerger<valueType>>(attributeConfig, updateBitmap);        \
        }                                                                                                              \
    }

std::unique_ptr<AttributePatchMerger>
AttributePatchMergerFactory::Create(const std::shared_ptr<config::AttributeConfig>& attributeConfig,
                                    const std::shared_ptr<SegmentUpdateBitmap>& updateBitmap)
{
    FieldType fieldType = attributeConfig->GetFieldType();
    bool isMultiValue = attributeConfig->IsMultiValue();
    switch (fieldType) {
        CREATE_ATTRIBUTE_PATCH_MERGER(ft_int8, int8_t);
        CREATE_ATTRIBUTE_PATCH_MERGER(ft_uint8, uint8_t);
        CREATE_ATTRIBUTE_PATCH_MERGER(ft_int16, int16_t);
        CREATE_ATTRIBUTE_PATCH_MERGER(ft_uint16, uint16_t);
        CREATE_ATTRIBUTE_PATCH_MERGER(ft_int32, int32_t);
        CREATE_ATTRIBUTE_PATCH_MERGER(ft_uint32, uint32_t);
        CREATE_ATTRIBUTE_PATCH_MERGER(ft_int64, int64_t);
        CREATE_ATTRIBUTE_PATCH_MERGER(ft_hash_64, uint64_t);
        CREATE_ATTRIBUTE_PATCH_MERGER(ft_uint64, uint64_t);
        CREATE_ATTRIBUTE_PATCH_MERGER(ft_float, float);
        CREATE_ATTRIBUTE_PATCH_MERGER(ft_fp8, int8_t);
        CREATE_ATTRIBUTE_PATCH_MERGER(ft_fp16, int16_t);
        CREATE_ATTRIBUTE_PATCH_MERGER(ft_double, double);
        CREATE_ATTRIBUTE_PATCH_MERGER(ft_hash_128, autil::uint128_t);
    case ft_string: {
        if (isMultiValue) {
            return std::make_unique<MultiValueAttributePatchMerger<autil::MultiChar>>(attributeConfig, updateBitmap);
        }
        return std::make_unique<MultiValueAttributePatchMerger<char>>(attributeConfig, updateBitmap);
    }
    default:
        assert(false);
    }
    return nullptr;
}

} // namespace indexlibv2::index
