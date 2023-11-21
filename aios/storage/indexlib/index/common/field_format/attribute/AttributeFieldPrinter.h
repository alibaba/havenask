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

#include "autil/DataBuffer.h"
#include "autil/MultiValueType.h"
#include "autil/NoCopyable.h"
#include "indexlib/config/FieldConfig.h"
#include "indexlib/index/common/field_format/attribute/ShapeAttributeUtil.h"
#include "indexlib/util/TimestampUtil.h"

namespace indexlibv2::index {

class AttributeFieldPrinter : private autil::NoCopyable
{
public:
    AttributeFieldPrinter() = default;
    ~AttributeFieldPrinter() = default;

    void Init(const std::shared_ptr<config::FieldConfig>& fieldConfig)
    {
        assert(fieldConfig);
        _fieldConfig = fieldConfig;
        _fieldType = fieldConfig->GetFieldType();
    }

    template <typename T>
    bool Print(bool isNull, const autil::MultiValueType<T>& value, std::string* output) const;

    template <typename T>
    bool Print(bool isNull, const T& value, std::string* output) const;

private:
    template <typename T>
    bool SingleValuePrint(bool isNull, const T& value, std::string* output) const;

    template <typename T>
    bool MultiValuePrint(const autil::MultiValueType<T>& value, std::string* output) const;

private:
    std::shared_ptr<config::FieldConfig> _fieldConfig;
    FieldType _fieldType;
};

template <typename T>
inline bool AttributeFieldPrinter::Print(bool isNull, const autil::MultiValueType<T>& value, std::string* output) const
{
    return MultiValuePrint(value, output);
}

template <typename T>
inline bool AttributeFieldPrinter::Print(bool isNull, const T& value, std::string* output) const
{
    return SingleValuePrint(isNull, value, output);
}

template <typename T>
inline bool AttributeFieldPrinter::SingleValuePrint(bool isNull, const T& value, std::string* output) const
{
    if (isNull) {
        *output = _fieldConfig->GetNullFieldLiteralString();
    } else {
        *output = autil::StringUtil::toString<T>(value);
    }
    return true;
}

template <typename T>
inline bool AttributeFieldPrinter::MultiValuePrint(const autil::MultiValueType<T>& value, std::string* output) const
{
    assert(_fieldConfig);
    if (value.isNull()) {
        *output = _fieldConfig->GetNullFieldLiteralString();
        return true;
    }
    uint32_t size = value.size();
    output->clear();
    bool isBinary = _fieldConfig->IsBinary();
    if (isBinary) {
        // only multi string
        autil::DataBuffer dataBuffer;
        dataBuffer.write(size);
        for (uint32_t i = 0; i < size; i++) {
            std::string item = autil::StringUtil::toString<T>(value[i]);
            dataBuffer.write(item);
        }
        output->assign(dataBuffer.getData(), dataBuffer.getDataLen());
    } else {
        for (uint32_t i = 0; i < size; i++) {
            std::string item = autil::StringUtil::toString<T>(value[i]);
            if (i != 0) {
                *output += _fieldConfig->GetSeparator();
            }
            *output += item;
        }
    }
    return true;
}

template <>
inline bool AttributeFieldPrinter::Print<uint64_t>(bool isNull, const uint64_t& value, std::string* output) const
{
    if (isNull) {
        *output = _fieldConfig->GetNullFieldLiteralString();
        return true;
    }
    if (_fieldType == ft_timestamp) {
        uint64_t alignedValue = ((int64_t)value < _fieldConfig->GetDefaultTimeZoneDelta())
                                    ? 0
                                    : (value - _fieldConfig->GetDefaultTimeZoneDelta());
        *output = indexlib::util::TimestampUtil::TimestampToString(alignedValue);
        return true;
    }
    return SingleValuePrint(isNull, value, output);
}

template <>
inline bool AttributeFieldPrinter::Print<uint32_t>(bool isNull, const uint32_t& value, std::string* output) const
{
    if (isNull) {
        *output = _fieldConfig->GetNullFieldLiteralString();
        return true;
    }
    switch (_fieldType) {
    case ft_time:
        *output = indexlib::util::TimestampUtil::TimeToString(value);
        return true;
    case ft_date:
        *output = indexlib::util::TimestampUtil::DateIndexToString(value);
        return true;
    default:
        return SingleValuePrint(isNull, value, output);
    }
    assert(false);
    return false;
}

template <>
inline bool AttributeFieldPrinter::Print<double>(bool isNull, const autil::MultiValueType<double>& value,
                                                 std::string* output) const
{
    switch (_fieldType) {
    case ft_line:
        ShapeAttributeUtil::DecodeAttrValueToString(indexlib::index::Shape::ShapeType::LINE, value,
                                                    _fieldConfig->GetSeparator(), *output);
        return true;
    case ft_polygon:
        return ShapeAttributeUtil::DecodeAttrValueToString(indexlib::index::Shape::ShapeType::POLYGON, value,
                                                           _fieldConfig->GetSeparator(), *output);
    case ft_location: {
        uint32_t size = value.size();
        output->clear();
        for (uint32_t i = 0; i < size; i++) {
            std::string item = autil::StringUtil::toString<double>(value[i]);
            if (i != 0) {
                if (i & 1) {
                    *output += " ";
                } else {
                    *output += _fieldConfig->GetSeparator();
                }
            }
            *output += item;
        }
        return true;
    }
    default:
        return MultiValuePrint(value, output);
    }
    assert(false);
    return false;
}

template <>
inline bool AttributeFieldPrinter::Print<char>(bool isNull, const autil::MultiValueType<char>& value,
                                               std::string* output) const
{
    switch (_fieldType) {
    case ft_string:
        if (value.isNull()) {
            *output = _fieldConfig->GetNullFieldLiteralString();
            return true;
        }
        // TODO: use MultiValueReader for performance
        *output = std::string(value.data(), value.size());
        return true;
    default:
        return MultiValuePrint(value, output);
    }
    assert(false);
    return false;
}

} // namespace indexlibv2::index
