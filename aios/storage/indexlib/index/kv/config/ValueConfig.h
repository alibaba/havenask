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
#include "indexlib/base/FieldType.h"
#include "indexlib/base/Status.h"

namespace indexlibv2::config {
class AttributeConfig;
class FieldConfig;
class PackAttributeConfig;

class ValueConfig
{
public:
    ValueConfig();
    ValueConfig(const ValueConfig& other);
    ~ValueConfig();

public:
    Status Init(const std::vector<std::shared_ptr<AttributeConfig>>& attrConfigs);
    size_t GetAttributeCount() const;
    const std::shared_ptr<AttributeConfig>& GetAttributeConfig(size_t idx) const;
    const std::vector<std::shared_ptr<AttributeConfig>>& GetAttributeConfigs() const;

    std::pair<Status, std::shared_ptr<PackAttributeConfig>> CreatePackAttributeConfig() const;
    void EnableCompactFormat(bool toSetCompactFormat);
    bool IsCompactFormatEnabled() const;
    int32_t GetFixedLength() const;
    FieldType GetFixedLengthValueType() const;
    FieldType GetActualFieldType() const;
    bool IsSimpleValue() const;
    void DisableSimpleValue();

    bool IsValueImpact() const;
    void EnableValueImpact(bool flag);
    bool IsPlainFormat() const;
    void EnablePlainFormat(bool flag);

    Status AppendField(const std::shared_ptr<FieldConfig>& field);

private:
    FieldType FixedLenToFieldType(int32_t size) const;
    Status CheckIfAttributesSupportNull(const std::vector<std::shared_ptr<AttributeConfig>>& attrConfigs) const;
    void SetActualFieldType(const std::vector<std::shared_ptr<AttributeConfig>>& attrConfigs);
    void SetFixedValueLen(const std::vector<std::shared_ptr<AttributeConfig>>& attrConfigs);

private:
    struct Impl;
    std::unique_ptr<Impl> _impl;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::config
