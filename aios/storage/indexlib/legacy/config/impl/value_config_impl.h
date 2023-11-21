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

#include "autil/Log.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/legacy/indexlib.h"

namespace indexlib { namespace config {

class ValueConfigImpl
{
public:
    ValueConfigImpl()
        : mEnableCompactPackAttribute(false)
        , mValueImpact(false)
        , mPlainFormat(false)
        , mDisableSimpleValue(false)
        , mFixedValueLen(-1)
        , mActualFieldType(FieldType::ft_unknown)
    {
    }
    ~ValueConfigImpl() {}

public:
    void Init(const std::vector<AttributeConfigPtr>& attrConfigs);
    size_t GetAttributeCount() const { return mAttrConfigs.size(); }
    const config::AttributeConfigPtr& GetAttributeConfig(size_t idx) const
    {
        assert(idx < GetAttributeCount());
        return mAttrConfigs[idx];
    }

    config::PackAttributeConfigPtr CreatePackAttributeConfig() const;
    void EnableCompactFormat(bool toSetCompactFormat) { mEnableCompactPackAttribute = toSetCompactFormat; }
    bool IsCompactFormatEnabled() const { return mEnableCompactPackAttribute; }
    int32_t GetFixedLength() const { return mEnableCompactPackAttribute ? mFixedValueLen : -1; }
    FieldType GetFixedLengthValueType() const;
    FieldType GetActualFieldType() const;

    bool IsSimpleValue() const;
    void DisableSimpleValue();

    void EnableValueImpact(bool flag)
    {
        mValueImpact = flag;
        mPlainFormat = mPlainFormat && (!mValueImpact);
    }

    void EnablePlainFormat(bool flag)
    {
        mPlainFormat = flag;
        mValueImpact = mValueImpact && (!mPlainFormat);
    }

private:
    FieldType CalculateFixLenValueType(int32_t size) const;
    void CheckSupportNullFields() const;

private:
    std::vector<config::AttributeConfigPtr> mAttrConfigs;
    bool mEnableCompactPackAttribute;
    bool mValueImpact;
    bool mPlainFormat;
    bool mDisableSimpleValue;
    int32_t mFixedValueLen;
    FieldType mActualFieldType;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<ValueConfigImpl> ValueConfigImplPtr;
}} // namespace indexlib::config
