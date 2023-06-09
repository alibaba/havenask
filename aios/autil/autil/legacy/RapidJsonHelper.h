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

#include <assert.h>
#include <stdint.h>
#include <stddef.h>
#include <limits>
#include <string>
#include <type_traits>
#include <vector>
#include <algorithm>

#include "autil/Log.h"
#include "autil/MultiValueCreator.h"
#include "autil/MultiValueFormatter.h"
#include "autil/MultiValueType.h"
#include "autil/MultiValueWriter.h"
#include "autil/legacy/RapidJsonCommon.h"
#include "rapidjson/document.h"
#include "rapidjson/rapidjson.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"
#include "autil/mem_pool/Pool.h"

namespace autil {

template<typename T, typename = void>
struct IsStdVectorType : std::false_type {};

template<typename T>
struct IsStdVectorType<T, typename std::enable_if<std::is_same<T, std::vector<typename T::value_type>>::value>::type> : std::true_type {};

class RapidJsonHelper
{
private:
    RapidJsonHelper(const RapidJsonHelper &);
    RapidJsonHelper& operator=(const RapidJsonHelper &);
public:
    static std::string SimpleValue2Str(const SimpleValue& value);

    template <typename T>
    static T getWithDefault(const SimpleDocument &doc,
                            const std::string &key,
                            const T &defaultValue = T());
    template <typename T>
    static T getWithDefault(const SimpleValue &doc,
                            const std::string &key,
                            const T &defaultValue = T());
    template <typename T>
    static T getValue(const SimpleValue &value);

    template <typename T>
    static std::vector<T> getVectorValue(const SimpleValue &value);

    template <typename T>
    static autil::MultiValueType<T> getMultiValue(const SimpleValue &value, autil::mem_pool::Pool *pool);

    template <typename T>
    static T invalidValue();

    template <typename T>
    static T getValue(const SimpleValue &value, autil::mem_pool::Pool *pool);
private:
    template <typename T>
    static T getValue(const SimpleValue &value, autil::mem_pool::Pool *pool, std::true_type);

    template <typename T>
    static T getValue(const SimpleValue &value, autil::mem_pool::Pool *pool, std::false_type);

    template <typename T>
    static T getValueMaybeVec(const SimpleValue &value, std::true_type) {
        return getVectorValue<typename T::value_type>(value);
    }

private:
    AUTIL_LOG_DECLARE();
};

template <>
inline double RapidJsonHelper::invalidValue<double>() {
    return std::numeric_limits<double>::quiet_NaN();
}

template <typename T>
inline T RapidJsonHelper::invalidValue() {
    return std::numeric_limits<T>::max();
}

template <typename T>
T RapidJsonHelper::getWithDefault(const SimpleDocument &doc,
                                  const std::string &key,
                                  const T &defaultValue)
{
    if (!doc.IsObject()) {
        return defaultValue;
    }
    auto iter = doc.FindMember(key);
    if (iter == doc.MemberEnd()) {
        return defaultValue;
    }
    return getValue<T>(iter->value);
}

template <typename T>
T RapidJsonHelper::getWithDefault(const SimpleValue &doc,
                                  const std::string &key,
                                  const T &defaultValue)
{
    if (!doc.IsObject()) {
        return defaultValue;
    }
    auto iter = doc.FindMember(key);
    if (iter == doc.MemberEnd()) {
        return defaultValue;
    }
    return getValue<T>(iter->value);
}

template <>
inline bool RapidJsonHelper::getValue<bool>(const SimpleValue &value) {
    if (!value.IsBool()) {
        AUTIL_LOG(DEBUG, "get bool value failed");
        return false;
    }
    return value.GetBool();
}

template <>
inline int8_t RapidJsonHelper::getValue<int8_t>(const SimpleValue &value) {
    if (!value.IsInt()) {
        AUTIL_LOG(DEBUG, "get int8 value failed");
        return invalidValue<int8_t>();
    }
    return (int8_t)value.GetInt();
}

template <>
inline uint8_t RapidJsonHelper::getValue<uint8_t>(const SimpleValue &value) {
    if (!value.IsUint()) {
        AUTIL_LOG(DEBUG, "get uint8 value failed");
        return invalidValue<uint8_t>();
    }
    return (uint8_t)value.GetUint();
}

template <>
inline int16_t RapidJsonHelper::getValue<int16_t>(const SimpleValue &value) {
    if (!value.IsInt()) {
        AUTIL_LOG(DEBUG, "get int16 value failed");
        return invalidValue<int16_t>();
    }
    return (int16_t)value.GetInt();
}

template <>
inline uint16_t RapidJsonHelper::getValue<uint16_t>(const SimpleValue &value) {
    if (!value.IsUint()) {
        AUTIL_LOG(DEBUG, "get uint16 value failed");
        return invalidValue<uint16_t>();
    }
    return (uint16_t)value.GetUint();
}

template <>
inline int32_t RapidJsonHelper::getValue<int32_t>(const SimpleValue &value) {
    if (!value.IsInt()) {
        AUTIL_LOG(DEBUG, "get int32 value failed");
        return invalidValue<int32_t>();
    }
    return value.GetInt();
}

template <>
inline uint32_t RapidJsonHelper::getValue<uint32_t>(const SimpleValue &value) {
    if (!value.IsUint()) {
        AUTIL_LOG(DEBUG, "get uint32 value failed");
        return invalidValue<uint32_t>();
    }
    return value.GetUint();
}

template <>
inline int64_t RapidJsonHelper::getValue<int64_t>(const SimpleValue &value) {
    if (!value.IsInt64()) {
        AUTIL_LOG(DEBUG, "get uint64 value failed");
        return invalidValue<int64_t>();
    }
    return value.GetInt64();
}

template <>
inline long long int RapidJsonHelper::getValue<long long int>(const SimpleValue &value) {
    if (!value.IsInt64()) {
        AUTIL_LOG(DEBUG, "get uint64 value failed");
        return invalidValue<int64_t>();
    }
    return value.GetInt64();
}

template <>
inline uint64_t RapidJsonHelper::getValue<uint64_t>(const SimpleValue &value) {
    if (!value.IsUint64()) {
        AUTIL_LOG(DEBUG, "get uint64 value failed");
        return invalidValue<uint64_t>();
    }
    return value.GetUint64();
}

template <>
inline double RapidJsonHelper::getValue<double>(const SimpleValue &value) {
    if (!value.IsDouble()) {
        AUTIL_LOG(DEBUG, "get double value failed");
        return invalidValue<double>();
    }
    return value.GetDouble();
}

template <>
inline float RapidJsonHelper::getValue<float>(const SimpleValue &value) {
    if (!value.IsDouble()) {
        AUTIL_LOG(DEBUG, "get float value failed");
        return (float)invalidValue<double>();
    }
    return (float)value.GetDouble();
}

template <>
inline std::string RapidJsonHelper::getValue<std::string>(const SimpleValue &value) {
    if (!value.IsString()) {
        AUTIL_LOG(DEBUG, "get string value failed");
        return "";
    }
    return std::string(value.GetString(), value.GetStringLength());
}

template <typename T>
T RapidJsonHelper::getValue(const SimpleValue &value) {
    return getValueMaybeVec<T>(value, typename IsStdVectorType<T>::type());
}

template <typename T>
inline std::vector<T> RapidJsonHelper::getVectorValue(const SimpleValue &value) {
    std::vector<T> values;
    if (value.IsArray()) {
        size_t size = value.Size();
        for (size_t i = 0; i < size; i++) {
            values.push_back(getValue<T>(value[i]));
        }
    }
    return values;
}

template <>
inline autil::MultiString RapidJsonHelper::getMultiValue<autil::MultiChar>(
        const SimpleValue &value, autil::mem_pool::Pool *pool)
{
    typedef autil::MultiString MultiString;
    if (!value.IsArray()) {
        return MultiString();
    }
    std::vector<std::string> strVec;
    for (size_t i = 0; i < value.Size(); i++) {
        assert(value[i].IsString());
        strVec.push_back(getValue<std::string>(value[i]));
    }
    char* buffer = autil::MultiValueCreator::createMultiStringBuffer(strVec, pool);
    MultiString ms;
    ms.init(buffer);
    return ms;
}

template <>
inline autil::MultiChar RapidJsonHelper::getMultiValue<char>(
        const SimpleValue &value, autil::mem_pool::Pool *pool)
{
    typedef autil::MultiChar MultiChar;
    std::string str = getValue<std::string>(value);
    char* buffer = autil::MultiValueCreator::createMultiValueBuffer<char>(
            (char*)str.data(), str.length(), pool);
    MultiChar mc;
    mc.init(buffer);
    return mc;
}

template <typename T>
inline autil::MultiValueType<T> RapidJsonHelper::getMultiValue(
        const SimpleValue &value, autil::mem_pool::Pool *pool)
{
    typedef autil::MultiValueType<T> MultiValueTyped;
    MultiValueTyped ret;
    if (!value.IsArray()) {
        return ret;
    }

    size_t size = value.Size();
    size_t bufLen = autil::MultiValueFormatter::calculateBufferLen(size, sizeof(T));
    char* buffer = (char *)pool->allocate(bufLen);
    autil::MultiValueWriter<T> mvWriter;
    bool initFlag = mvWriter.init(buffer, bufLen, size);
    assert(initFlag);
    (void)initFlag;
    for (size_t i = 0; i < value.Size(); i++) {
        mvWriter.set(i, getValue<T>(value[i]));
    }
    ret.init(buffer);
    return ret;
}

inline std::string RapidJsonHelper::SimpleValue2Str(const SimpleValue& value)
{
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    value.Accept(writer);
    return buffer.GetString();
}

template <typename T>
T RapidJsonHelper::getValue(const SimpleValue &value, autil::mem_pool::Pool *pool) {
    return getValue<T>(value, pool, typename autil::IsMultiType<T>::type());
}

template <typename T>
T RapidJsonHelper::getValue(const SimpleValue &value, autil::mem_pool::Pool *pool, std::true_type) {
    return getMultiValue<typename T::type>(value, pool);
}

template <typename T>
T RapidJsonHelper::getValue(const SimpleValue &value, autil::mem_pool::Pool *pool, std::false_type) {
    return getValue<T>(value);
}

}
