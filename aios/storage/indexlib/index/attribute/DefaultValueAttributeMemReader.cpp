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
#include "indexlib/index/attribute/DefaultValueAttributeMemReader.h"

#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/attribute/patch/DefaultValueAttributePatch.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, DefaultValueAttributeMemReader);

Status DefaultValueAttributeMemReader::Open(const std::shared_ptr<AttributeConfig>& attrConfig)
{
    auto [status, defaultValue] = DefaultValueAttributePatch::GetDecodedDefaultValue(attrConfig, &_pool);
    _fixedValueCount = attrConfig->GetFixedMultiValueCount();
    _defaultValue = defaultValue;
    return status;
}

} // namespace indexlibv2::index
