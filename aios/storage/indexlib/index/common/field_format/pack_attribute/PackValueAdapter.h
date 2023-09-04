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
#include "indexlib/index/common/field_format/pack_attribute/PackAttributeFormatter.h"

namespace indexlibv2::index {

class PackValueAdapter : private autil::NoCopyable
{
public:
    class ValueState
    {
    public:
        bool Init(const std::shared_ptr<indexlibv2::index::PackAttributeConfig>& packAttr);
        void GetAttributesWithStoreOrder(std::vector<std::string>& attributes) const;
        bool GetStoreOrder(const std::string& attribute, size_t& idx) const;
        bool UnpackValues(const autil::StringView& packValue,
                          std::vector<PackAttributeFormatter::AttributeFieldData>& values) const;

        autil::StringView FormatValues(const std::vector<PackAttributeFormatter::AttributeFieldData>& values,
                                       autil::mem_pool::Pool* pool) const;

        PackAttributeFormatter::AttributeFieldData GetDefaultValue(size_t idx) const;

        void DisablePlainFormat();

    private:
        std::shared_ptr<PackAttributeFormatter> _packFormatter;
        std::shared_ptr<PlainFormatEncoder> _plainFormatEncoder;
        std::vector<PackAttributeFormatter::AttributeFieldData> _defaultValues;
        autil::mem_pool::Pool _pool;
    };

public:
    PackValueAdapter();
    ~PackValueAdapter();

public:
    bool Init(const std::shared_ptr<indexlibv2::index::PackAttributeConfig>& current,
              const std::shared_ptr<indexlibv2::index::PackAttributeConfig>& target,
              const std::vector<std::string>& ignoreFieldsInCurrent = {});

    autil::StringView ConvertIndexPackValue(const autil::StringView& packValue, autil::mem_pool::Pool* pool);

    bool NeedConvert() const { return _needConvert; }

    void DisablePlainFormat();

private:
    bool CheckPackAttributeEqual(const std::shared_ptr<indexlibv2::index::PackAttributeConfig>& current,
                                 const std::shared_ptr<indexlibv2::index::PackAttributeConfig>& target,
                                 const std::vector<std::string>& ignoreFieldsInCurrent);

    bool IsAttrEqual(const std::shared_ptr<indexlibv2::index::AttributeConfig>& current,
                     const std::shared_ptr<indexlibv2::index::AttributeConfig>& target) const;

private:
    bool _needConvert;
    ValueState _currentState;
    ValueState _targetState;
    std::set<std::string> _ignoreFieldsInCurrent;
    std::vector<int32_t> _convOrderMap;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index
