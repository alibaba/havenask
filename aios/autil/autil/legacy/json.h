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

#include <stddef.h>
#include <stdint.h>
#include <iosfwd>
#include <map>
#include <string>
#include <typeinfo>
#include <vector>

#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/string_tools.h"

namespace autil{ namespace legacy { namespace json
{
    /// quick hand for json related type
    typedef std::vector<Any> JsonArray;
    typedef std::map<std::string, Any> JsonMap;

    /// Json now internally express a number in the json expression as a JsonNumber.
    struct JsonNumber : public std::string
    {
        /// constructor from string
        JsonNumber(const std::string& s, size_t start = 0, size_t n = std::string::npos) : std::string(s, start, n)
        {}

        /// constructor from number type
        template<typename T>
        JsonNumber(const T& data)
        {
            *this = data;
        }

        /// Cast to required number type
        template<typename T>
        T Cast() const
        {
            /// StringTo handles uint8_t and int8_t as a number not a char
            return StringTo<T>(this->AsString());
        }

        std::string& AsString()
        {
            return dynamic_cast<std::string&>(*this);
        }

        const std::string& AsString() const
        {
            return dynamic_cast<const std::string&>(*this);
        }

        /// operator=
        template<typename T>
        JsonNumber& operator=(const T& data);

        template<typename T>
        bool operator==(const T& rhs) const;
    };

    /// templates
    template<typename T>
    bool JsonNumber::operator==(const T& rhs) const
    {
        try
        {
            return this->Cast<T>() == rhs;
        }
        catch (...)
        {
        }
        return false;
    }
    template<>
    bool JsonNumber::operator==(const Any& rhs) const;
    template<>
    bool JsonNumber::operator==(const JsonNumber& rhs) const;

    template<typename T>
    JsonNumber& JsonNumber::operator=(const T& data)
    {
        this->AsString() = autil::legacy::ToString(data);
        return *this;
    }

    template<>
    inline JsonNumber& JsonNumber::operator=(const uint8_t& data)
    {
        this->AsString() = autil::legacy::ToString(static_cast<uint32_t>(data));
        return *this;
    }

    template<>
    inline JsonNumber& JsonNumber::operator=(const int8_t& data)
    {
        this->AsString() = autil::legacy::ToString(static_cast<int32_t>(data));
        return *this;
    }

    /// conversion functions
    Any ParseJson(std::istream &is);

    /// Convert an Any expected being a number in a json-Any to be T.
    /// Supported T: uint8_t, int8_t, uint16_t, int16_t, uint32_t, int32_t, uint64_t, int64_t
    ///              float and double
    template<typename T>
    T JsonNumberCast(const Any& a)
    {
        if (a.GetType() == typeid(T))               /// Normal AnyCast functionality
        {
            return AnyCast<T>(a);
        }
        else if (a.GetType() == typeid(JsonNumber)) /// A JsonNumber seen? Try number cast
        {
            JsonNumber nl = AnyCast<JsonNumber>(a);
            return nl.Cast<T>();
        } else {
            return AnyNumberCast<T>(a);
        }
    }

    /// string interface for ease to  use, contributed by pen.hang
    void ParseJson(const std::string& str, size_t& pos, Any& ret);
    void ParseJson(const std::string& str, Any& ret);

    inline Any ParseJson(const std::string& str)
    {
        Any a;
        ParseJson(str, a);
        return a;
    }
    inline std::string ToString(const Any& a, bool isCompact = false, const std::string& prefix="")
    {
        std::string ret;
        ToString(a, ret, isCompact, prefix);
        return ret;
    }

    /// for ease to probe Any from json parser
    inline bool IsJsonNull(const Any& any)
    {
        return any.GetType() == typeid(void);
    }

    inline bool IsJsonBool(const Any& any)
    {
        return any.GetType() == typeid(bool);
    }

    inline bool IsJsonArray(const Any& any)
    {
        return any.GetType() == typeid(JsonArray);
    }

    inline bool IsJsonMap(const Any& any)
    {
        return any.GetType() == typeid(JsonMap);
    }

    inline bool IsJsonString(const Any& any)
    {
        return any.GetType() == typeid(std::string);
    }

    inline bool IsJsonNumber(const Any& any)
    {
        return any.GetType() == typeid(JsonNumber);
    }

    inline std::string GetJsonTypeName(const Any& any)
    {
        const std::type_info& type = any.GetType();
        return type == typeid(void) ? "null" :
               type == typeid(bool) ? "bool" :
               type == typeid(std::string) ? "string" :
               type == typeid(JsonArray) ? "array" :
               type == typeid(JsonMap) ? "map" :
               type == typeid(JsonNumber) ||
               type == typeid(uint8_t) ||
               type == typeid(int8_t) ||
               type == typeid(uint16_t) ||
               type == typeid(int16_t) ||
               type == typeid(uint32_t) ||
               type == typeid(int32_t) ||
               type == typeid(uint64_t) ||
               type == typeid(int64_t) ||
               type == typeid(float) ||
               type == typeid(double) ? "number" :
               "unknown";
    }

    /// exceptions
    class JsonParserError : public ParameterInvalidException
    {
        public:
            JsonParserError(const std::string& e) : ParameterInvalidException(e)
            {}
    };

    class JsonNothingError : public JsonParserError
    {
        public:
            AUTIL_LEGACY_DEFINE_EXCEPTION(JsonNothingError, JsonParserError);
    };

    /// operator ==
    bool Equal(const JsonArray& lhs, const JsonArray& rhs);
    bool Equal(const JsonMap& lhs, const JsonMap& rhs);
    bool Equal(const Any& lhs, const Any& rhs);

}}} // end of namespace legacy::json
