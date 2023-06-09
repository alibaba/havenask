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
#include "indexlib/index/common/field_format/attribute/AttributeConvertor.h"
#include "indexlib/index/common/field_format/attribute/AttributeValueInitializerCreator.h"

namespace indexlibv2::config {
class AttributeConfig;
class FieldConfig;
} // namespace indexlibv2::config

namespace indexlibv2::index {

class DefaultAttributeValueInitializerCreator : public AttributeValueInitializerCreator
{
public:
    explicit DefaultAttributeValueInitializerCreator(const std::shared_ptr<config::AttributeConfig>& attrConfig);

    ~DefaultAttributeValueInitializerCreator();

public:
    std::unique_ptr<AttributeValueInitializer> Create() override;

private:
    std::shared_ptr<indexlibv2::config::FieldConfig> _fieldConfig;
    std::shared_ptr<AttributeConvertor> _convertor;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
