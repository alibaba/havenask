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
#include "indexlib/index/common/data_structure/AttributeCompressInfo.h"

#include "indexlib/config/CompressTypeOption.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"

using indexlib::config::CompressTypeOption;

namespace indexlibv2::index {

AttributeCompressInfo::AttributeCompressInfo() {}

AttributeCompressInfo::~AttributeCompressInfo() {}

bool AttributeCompressInfo::NeedCompressData(const std::shared_ptr<index::AttributeConfig>& attrConfig)
{
    if (!attrConfig) {
        return false;
    }

    CompressTypeOption compressedType = attrConfig->GetCompressType();
    return compressedType.HasEquivalentCompress() && !attrConfig->IsMultiValue() &&
           attrConfig->GetFieldType() != ft_string;
}

bool AttributeCompressInfo::NeedCompressOffset(const std::shared_ptr<index::AttributeConfig>& attrConfig)
{
    if (!attrConfig) {
        return false;
    }

    CompressTypeOption compressedType = attrConfig->GetCompressType();
    return compressedType.HasEquivalentCompress() &&
           (attrConfig->GetFieldType() == ft_string || attrConfig->IsMultiValue());
}

bool AttributeCompressInfo::NeedEnableAdaptiveOffset(const std::shared_ptr<index::AttributeConfig>& attrConfig)
{
    if (!attrConfig) {
        return false;
    }
    return !NeedCompressOffset(attrConfig) || !attrConfig->IsAttributeUpdatable();
}
} // namespace indexlibv2::index
