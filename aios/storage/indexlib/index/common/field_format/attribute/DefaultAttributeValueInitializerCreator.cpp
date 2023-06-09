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
#include "indexlib/index/common/field_format/attribute/DefaultAttributeValueInitializerCreator.h"

#include "indexlib/config/FieldConfig.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/common/field_format/attribute/AttributeConvertorFactory.h"
#include "indexlib/index/common/field_format/attribute/DefaultAttributeValueInitializer.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, DefaultAttributeValueInitializerCreator);

DefaultAttributeValueInitializerCreator::DefaultAttributeValueInitializerCreator(
    const std::shared_ptr<config::AttributeConfig>& attrConfig)
{
    assert(attrConfig);
    auto fieldConfigs = attrConfig->GetFieldConfigs();
    assert(fieldConfigs.size() == 1);
    const auto& fieldConfig = fieldConfigs[0];
    assert(fieldConfig);
    _fieldConfig = fieldConfig;
    assert(_fieldConfig);
    if (_fieldConfig->GetFieldType() == ft_raw) {
        return;
    }
    std::shared_ptr<AttributeConvertor> convertor(
        AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(attrConfig));
    assert(convertor);
    convertor->SetEncodeEmpty(true);
    _convertor = convertor;
}

DefaultAttributeValueInitializerCreator::~DefaultAttributeValueInitializerCreator() {}

std::unique_ptr<AttributeValueInitializer> DefaultAttributeValueInitializerCreator::Create()
{
    if (_fieldConfig->GetFieldType() == ft_raw) {
        return std::make_unique<DefaultAttributeValueInitializer>(config::FieldConfig::FIELD_DEFAULT_STR_VALUE);
    }

    std::string defaultValueStr = _fieldConfig->GetDefaultValue();
    if (defaultValueStr.empty()) {
        defaultValueStr = DefaultAttributeValueInitializer::GetDefaultValueStr(_fieldConfig);
        if (defaultValueStr.empty()) {
            AUTIL_LOG(DEBUG, "field [%s] not support empty default value string, use [%s] instead",
                      _fieldConfig->GetFieldName().c_str(), defaultValueStr.c_str());
        }
    }
    return std::make_unique<DefaultAttributeValueInitializer>(_convertor->Encode(defaultValueStr));
}

} // namespace indexlibv2::index
