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
#include "autil/NoCopyable.h"
#include "indexlib/common/field_format/pack_attribute/pack_attribute_formatter.h"
#include "indexlib/config/pack_attribute_config.h"

namespace indexlib::common {

class PackValueAdapter : private autil::NoCopyable
{
public:
    class ValueState
    {
    public:
        bool Init(const config::PackAttributeConfigPtr& packAttr);
        void GetAttributesWithStoreOrder(std::vector<std::string>& attributes) const;
        bool GetStoreOrder(const std::string& attribute, size_t& idx) const;
        bool UnpackValues(const autil::StringView& packValue,
                          std::vector<PackAttributeFormatter::AttributeFieldData>& values) const;

        autil::StringView FormatValues(const std::vector<PackAttributeFormatter::AttributeFieldData>& values,
                                       autil::mem_pool::Pool* pool) const;

        PackAttributeFormatter::AttributeFieldData GetDefaultValue(size_t idx) const;

        void DisablePlainFormat();

    private:
        PackAttributeFormatterPtr mPackFormatter;
        std::shared_ptr<PlainFormatEncoder> mPlainFormatEncoder;
        std::vector<PackAttributeFormatter::AttributeFieldData> mDefaultValues;
        autil::mem_pool::Pool mPool;
    };

public:
    PackValueAdapter();
    ~PackValueAdapter();

public:
    bool Init(const config::PackAttributeConfigPtr& current, const config::PackAttributeConfigPtr& target,
              const std::vector<std::string>& ignoreFieldsInCurrent = {});

    autil::StringView ConvertIndexPackValue(const autil::StringView& packValue, autil::mem_pool::Pool* pool);

    bool NeedConvert() const { return mNeedConvert; }

    void DisablePlainFormat();

private:
    bool CheckPackAttributeEqual(const config::PackAttributeConfigPtr& current,
                                 const config::PackAttributeConfigPtr& target,
                                 const std::vector<std::string>& ignoreFieldsInCurrent);

    bool IsAttrEqual(const config::AttributeConfigPtr& current, const config::AttributeConfigPtr& target) const;

private:
    bool mNeedConvert;
    ValueState mCurrentState;
    ValueState mTargetState;
    std::set<std::string> mIgnoreFieldsInCurrent;
    std::vector<int32_t> mConvOrderMap;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::common
