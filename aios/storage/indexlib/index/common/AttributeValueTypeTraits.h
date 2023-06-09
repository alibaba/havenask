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

#include "autil/LongHashValue.h"
#include "autil/MultiValueType.h"
#include "autil/StringUtil.h"
#include "indexlib/base/Constant.h"

namespace indexlib { namespace index {

template <class T>
struct AttributeValueTypeTraits {
public:
    typedef T IndexValueType;
};

#define DECLARE_ATTRIBUTE_TYPE_TRAITS_FOR_MULTI_VALUE(type)                                                            \
    template <>                                                                                                        \
    struct AttributeValueTypeTraits<autil::MultiValueType<type>> {                                                     \
    public:                                                                                                            \
        typedef autil::MultiValueType<type> IndexValueType;                                                            \
    }

DECLARE_ATTRIBUTE_TYPE_TRAITS_FOR_MULTI_VALUE(char);
DECLARE_ATTRIBUTE_TYPE_TRAITS_FOR_MULTI_VALUE(int8_t);
DECLARE_ATTRIBUTE_TYPE_TRAITS_FOR_MULTI_VALUE(uint8_t);
DECLARE_ATTRIBUTE_TYPE_TRAITS_FOR_MULTI_VALUE(int16_t);
DECLARE_ATTRIBUTE_TYPE_TRAITS_FOR_MULTI_VALUE(uint16_t);
DECLARE_ATTRIBUTE_TYPE_TRAITS_FOR_MULTI_VALUE(int32_t);
DECLARE_ATTRIBUTE_TYPE_TRAITS_FOR_MULTI_VALUE(uint32_t);
DECLARE_ATTRIBUTE_TYPE_TRAITS_FOR_MULTI_VALUE(int64_t);
DECLARE_ATTRIBUTE_TYPE_TRAITS_FOR_MULTI_VALUE(uint64_t);
DECLARE_ATTRIBUTE_TYPE_TRAITS_FOR_MULTI_VALUE(float);
DECLARE_ATTRIBUTE_TYPE_TRAITS_FOR_MULTI_VALUE(double);
DECLARE_ATTRIBUTE_TYPE_TRAITS_FOR_MULTI_VALUE(autil::MultiChar);
DECLARE_ATTRIBUTE_TYPE_TRAITS_FOR_MULTI_VALUE(autil::uint128_t);

#undef DECLARE_ATTRIBUTE_TYPE_TRAITS_FOR_MULTI_VALUE

template <typename T>
class AttributeValueTypeToString
{
public:
    static std::string ToString(const T& value) { return autil::StringUtil::toString(value); }
};

#define DECLARE_ATTRIBUTE_TYPE_TO_STRING_FOR_MULTI_VALUE(type)                                                         \
    template <>                                                                                                        \
    class AttributeValueTypeToString<autil::MultiValueType<type>>                                                      \
    {                                                                                                                  \
    public:                                                                                                            \
        static std::string ToString(const autil::MultiValueType<type>& value)                                          \
        {                                                                                                              \
            std::vector<std::string> strVec;                                                                           \
            strVec.reserve(value.size());                                                                              \
            for (size_t i = 0; i < value.size(); ++i) {                                                                \
                strVec.push_back(autil::StringUtil::toString(value[i]));                                               \
            }                                                                                                          \
            return autil::StringUtil::toString(strVec, INDEXLIB_MULTI_VALUE_SEPARATOR_STR);                            \
        }                                                                                                              \
    }

DECLARE_ATTRIBUTE_TYPE_TO_STRING_FOR_MULTI_VALUE(int8_t);
DECLARE_ATTRIBUTE_TYPE_TO_STRING_FOR_MULTI_VALUE(int16_t);
DECLARE_ATTRIBUTE_TYPE_TO_STRING_FOR_MULTI_VALUE(int32_t);
DECLARE_ATTRIBUTE_TYPE_TO_STRING_FOR_MULTI_VALUE(int64_t);
DECLARE_ATTRIBUTE_TYPE_TO_STRING_FOR_MULTI_VALUE(uint8_t);
DECLARE_ATTRIBUTE_TYPE_TO_STRING_FOR_MULTI_VALUE(uint16_t);
DECLARE_ATTRIBUTE_TYPE_TO_STRING_FOR_MULTI_VALUE(uint32_t);
DECLARE_ATTRIBUTE_TYPE_TO_STRING_FOR_MULTI_VALUE(uint64_t);
DECLARE_ATTRIBUTE_TYPE_TO_STRING_FOR_MULTI_VALUE(float);
DECLARE_ATTRIBUTE_TYPE_TO_STRING_FOR_MULTI_VALUE(double);

template <>
class AttributeValueTypeToString<autil::MultiChar>
{
public:
    static std::string ToString(const autil::MultiChar& value) { return std::string(value.data(), value.size()); }
};

template <>
class AttributeValueTypeToString<autil::MultiString>
{
public:
    static std::string ToString(const autil::MultiString& value)
    {
        std::vector<std::string> strVec;
        strVec.reserve(value.size());
        for (size_t i = 0; i < value.size(); ++i) {
            strVec.push_back(std::string(value[i].data(), value[i].size()));
        }
        return autil::StringUtil::toString(strVec, INDEXLIB_MULTI_VALUE_SEPARATOR_STR);
    }
};
}} // namespace indexlib::index
