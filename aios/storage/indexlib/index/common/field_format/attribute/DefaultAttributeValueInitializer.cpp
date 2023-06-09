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
#include "indexlib/index/common/field_format/attribute/DefaultAttributeValueInitializer.h"

#include "indexlib/config/FieldConfig.h"

namespace indexlibv2::index {

DefaultAttributeValueInitializer::DefaultAttributeValueInitializer(const std::string& valueStr) : _valueStr(valueStr) {}

DefaultAttributeValueInitializer::~DefaultAttributeValueInitializer() {}

bool DefaultAttributeValueInitializer::GetInitValue(docid_t docId, char* buffer, size_t bufLen) const
{
    assert(bufLen >= _valueStr.size());
    memcpy(buffer, _valueStr.c_str(), _valueStr.size());
    return true;
}

bool DefaultAttributeValueInitializer::GetInitValue(docid_t docId, autil::StringView& value,
                                                    autil::mem_pool::Pool* memPool) const
{
    value = GetDefaultValue(memPool);
    return true;
}

autil::StringView DefaultAttributeValueInitializer::GetDefaultValue(autil::mem_pool::Pool* memPool) const
{
    return autil::MakeCString(_valueStr.c_str(), _valueStr.length(), memPool);
}

std::string
DefaultAttributeValueInitializer::GetDefaultValueStr(const std::shared_ptr<config::FieldConfig>& fieldConfig)
{
    std::string defaultValueStr;
    FieldType ft = fieldConfig->GetFieldType();
    if (!fieldConfig->IsMultiValue()) {
        if (ft == ft_int8 || ft == ft_int16 || ft == ft_int32 || ft == ft_int64 || ft == ft_uint8 || ft == ft_uint16 ||
            ft == ft_uint32 || ft == ft_uint64 || ft == ft_float || ft == ft_fp8 || ft == ft_fp16 || ft == ft_double) {
            defaultValueStr = "0";
        } else if (ft == ft_time) {
            defaultValueStr = "00:00:00.000";
        } else if (ft == ft_date) {
            defaultValueStr = "1970-01-01";
        } else if (ft == ft_timestamp) {
            defaultValueStr = "1970-01-01 00:00:00.000";
        }
    } else if (fieldConfig->GetFixedMultiValueCount() != -1) {
        if (ft == ft_int8 || ft == ft_int16 || ft == ft_int32 || ft == ft_int64 || ft == ft_uint8 || ft == ft_uint16 ||
            ft == ft_uint32 || ft == ft_uint64 || ft == ft_float || ft == ft_double) {
            std::vector<std::string> tmpVec(fieldConfig->GetFixedMultiValueCount(), "0");
            defaultValueStr = autil::StringUtil::toString(tmpVec, fieldConfig->GetSeparator());
        }
    }
    return defaultValueStr;
}
} // namespace indexlibv2::index
