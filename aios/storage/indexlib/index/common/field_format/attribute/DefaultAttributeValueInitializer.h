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

#include "autil/StringView.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/index/common/field_format/attribute/AttributeValueInitializer.h"

namespace indexlibv2::config {
class FieldConfig;
}

namespace indexlibv2::index {

class DefaultAttributeValueInitializer : public AttributeValueInitializer
{
public:
    explicit DefaultAttributeValueInitializer(const std::string& valueStr);
    ~DefaultAttributeValueInitializer();

public:
    bool GetInitValue(docid_t docId, char* buffer, size_t bufLen) const override;

    bool GetInitValue(docid_t docId, autil::StringView& value, autil::mem_pool::Pool* memPool) const override;

    static std::string GetDefaultValueStr(const std::shared_ptr<config::FieldConfig>& fieldConfig);

public:
    autil::StringView GetDefaultValue(autil::mem_pool::Pool* memPool) const;

private:
    std::string _valueStr;
};

} // namespace indexlibv2::index
