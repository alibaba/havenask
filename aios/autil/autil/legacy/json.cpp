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
#include <stddef.h>
#include <stdint.h>
#include <map>
#include <string>
#include <typeinfo>
#include <utility>
#include <vector>
#include <iosfwd>

#include "autil/legacy/any.h"
#include "autil/legacy/astream.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/json.h"
#include "autil/legacy/string_tools.h"

namespace autil{ namespace legacy { namespace json
{
    static void SkipSpace(a_Istream& is);
    static void SkipExact(a_Istream& is, std::string str);

    static std::vector<Any> ParseJsonArray(a_Istream& is);
    static std::map<std::string, Any> ParseJsonMap(a_Istream& is);
    static JsonNumber ParseJsonNum(a_Istream& is);
    static std::string ParseJsonString(a_Istream& is);
    static bool ParseJsonBoolTrue(a_Istream& is);
    static bool ParseJsonBoolFalse(a_Istream& is);
    static Any ParseJsonNull(a_Istream& is);




    inline static void trueThenThrow(bool test, const char *s)
    {
        if (test) AUTIL_LEGACY_THROW(JsonParserError, s);
    }

    inline static void trueThenThrow(bool test, const std::string& s)
    {
        if (test) AUTIL_LEGACY_THROW(JsonParserError, s);
    }

    // exportable function
    Any ParseJson(a_Istream& is)
    {
        SkipSpace(is);
        char byte = static_cast<char>(is.peek());
        if (is.good())
        {
            switch (byte)
            {
                case '[':
                    return ParseJsonArray(is);
                case '{':
                    return ParseJsonMap(is);
                case '"':
                    return ParseJsonString(is);
                case 't':
                    return ParseJsonBoolTrue(is);
                case 'f':
                    return ParseJsonBoolFalse(is);
                case 'n':
                    return ParseJsonNull(is);
                case '+': case '-': case '.':
                case '0': case '1': case '2': case '3': case '4':
                case '5': case '6': case '7': case '8': case '9':
                    return ParseJsonNum(is);
                default:
                    trueThenThrow(true, std::string("unknown char ")+byte);
            }
        }
        AUTIL_LEGACY_THROW(JsonNothingError, "");
        return Any();
    }
/*
    Any ParseJson(const std::string& str)
    {
        a_IstreamBuffer iss(str);
        Any a = ParseJson(iss);
        SkipSpace(iss);
        if (iss.peek() != -1)
        {
            AUTIL_LEGACY_THROW(
                JsonParserError, 
                std::string("Expect EOF but see more chars: ") + 
                    static_cast<char>(iss.peek())
            );
        }
        return a;
    }
*/
    void ToStream(const Any& a, a_Ostream &os, bool isCompact, const std::string& prefix)
    {
        decltype(a.GetType()) &type = a.GetType();
        if (type == typeid(void))                               /// void (null)
        {
            os << prefix << "null";
        }
        else if (type == typeid(std::vector<Any>))              /// list
        {
            os << prefix << "[";
            std::vector<Any> v = AnyCast<std::vector<Any> >(a);
            for(std::vector<Any>::const_iterator it = v.begin();
                it != v.end();
                ++it)
            {
                if (it != v.begin()) os << ",";
                if (!isCompact) os << aendl;
                ToStream(*it, os, isCompact, prefix + (isCompact ? "" : "  "));
            }
            if (!isCompact) os << aendl;
            os << prefix << "]";
        }
        else if (type == typeid(std::map<std::string, Any>))    /// map
        {
            os << prefix << "{";
            std::map<std::string, Any> m =
                    AnyCast<std::map<std::string, Any> >(a);
            for(std::map<std::string, Any>::const_iterator it = m.begin();
                it != m.end();
                ++it)
            {
                if (it != m.begin()) os << ",";
                if (!isCompact) os << aendl;
                os << prefix << "\"" << QuoteString(it->first) << "\":";
                if (!isCompact) os << aendl;
                ToStream(it->second, os, isCompact, prefix + (isCompact ? "" : "  "));
            }
            if (!isCompact) os << aendl;
            os << prefix << "}";
        }
        else if (type == typeid(std::string))               /// string
        {
            os << prefix
               << "\"" << QuoteString(AnyCast<std::string>(a)) << "\"";
        } else if (type == typeid(bool))                    /// bool
        {
            os << prefix << autil::legacy::ToString(AnyCast<bool>(a));
        }
        /// different numberic types
#define WRITE_DATA(tp) \
        else if (type == typeid(tp)) \
        { \
            os << prefix << autil::legacy::ToString(autil::legacy::AnyCast<tp>(a)); \
        }
        WRITE_DATA(double)
        WRITE_DATA(float)
        WRITE_DATA(uint16_t)
        WRITE_DATA(int16_t)
        WRITE_DATA(uint32_t)
        WRITE_DATA(int32_t)
        WRITE_DATA(uint64_t)
        WRITE_DATA(int64_t)
#undef WRITE_DATA
#define WRITE_DATA_AS(tp, as) \
        else if (type == typeid(tp)) \
        { \
            os << prefix << autil::legacy::ToString(static_cast<as>(autil::legacy::AnyCast<tp>(a)));\
        }
        WRITE_DATA_AS(uint8_t, uint32_t)
        WRITE_DATA_AS(int8_t, int32_t)
#undef WRITE_DATA_AS
        /// end of different numberic types
        else if (type == typeid(JsonNumber))         /// number literal
        {
            os << prefix << AnyCast<JsonNumber>(a).AsString();
        }
        else                                            /// other un-supported type
        {
            AUTIL_LEGACY_THROW(
                autil::legacy::ParameterInvalidException, 
                std::string("See unsupported data in Any:") + type.name()
            );
        }
    }
/*
    std::string ToString(
                         const Any& a,
                         bool isCompact,
                         const std::string& prefix)
    {
        a_OstreamBuffer ostr;
        ToStream(a, ostr, isCompact, prefix);
        return ostr.str();
    }
*/

    // below are local private functions (static)


    static void SkipMultiLineComment(a_Istream &is)
    {
        bool prevIsStar = false;
        for(char b = static_cast<char>(is.peek());
            is.good();
            is.get(), b = static_cast<char>(is.peek()))
        {
            if (b == '*')
            {
                prevIsStar = true;
            }
            else
            {
                if (prevIsStar && b == '/')
                {
                    is.get();
                    return;
                }
                prevIsStar = false;
            }
        }
        trueThenThrow(true, "Expect end of comment");
    }

    static void SkipSingleLineComment(a_Istream& is)
    {
        for(char b = static_cast<char>(is.peek());
            is.good();
            is.get(), b = static_cast<char>(is.peek()))
        {
            if (b == '\r' || b == '\n')
            {
                is.get();
                return;
            }
        }
        // program goes here if the stream becomes bad (usually eof) before
        // a newline is seen. SkipSingleLineComment regards this situation
        // as the end of the comment and returns. If there are any missing
        // elements, corresponding ParseXXX will handle this unexpected end
        // and report error.
    }
    static void SkipComment(a_Istream& is)
    {
        char b = static_cast<char>(is.peek());
        trueThenThrow(!is.good() || (b != '*' && b != '/'),
            "expect / or * to begin a comment block after /");
        is.get();
        if (b == '*') SkipMultiLineComment(is);
        else SkipSingleLineComment(is);
    }

    static void SkipSpace(a_Istream& is)
    {
        for(char b = static_cast<char>(is.peek());
            is.good();
            b = static_cast<char>(is.peek()))
        {
            switch (b)
            {
                case ' ':
                case '\t':
                case '\r':
                case '\n':
                    is.get();
                    break;
                case '/':
                    is.get();
                    SkipComment(is);
                    break;
                default:
                    return;
            }
        }
        // stream crash (usually end) when skipping spaces, ignore.
    }
    static void SkipExact(a_Istream& is, std::string str)
    {
        std::string::const_iterator it = str.begin();
        for(char byte = static_cast<char>(is.peek());
            is.good() && it != str.end() && byte == *it;
            is.get(), byte = static_cast<char>(is.peek()), ++it)
            ;
        if (it == str.end()) return;
        trueThenThrow(true, std::string("expected ")+str);
    }
    static bool ParseJsonBoolTrue(a_Istream &is)
    {
        SkipExact(is, "true");
        return true;
    }
    static bool ParseJsonBoolFalse(a_Istream &is)
    {
        SkipExact(is, "false");
        return false;
    }
    static Any ParseJsonNull(a_Istream &is)
    {
        SkipExact(is, "null");
        return Any();
    }
    static std::string ParseJsonString(a_Istream& is)
    {
        SkipExact(is, "\"");

        std::string str;
        try
        {
            str = UnquoteString(is);
        }
        catch (const autil::legacy::BadQuoteString& e)
        {
            AUTIL_LEGACY_THROW(
                JsonParserError, 
                std::string("BadQuoteString:") + e.what()
            );
        }

        SkipExact(is, "\"");
        return str;
    }

    static JsonNumber ParseJsonNum(a_Istream& is)
    {
        /*
    Acceptable num format:
        [+|-] x [e|E [+|-] digit+]
        x = digit+ [. digit*]
          = digit* . dight+
        */
        std::string s;
        bool signOk = true;  // current status accepts sign + or -
        bool dotOk = true;   // current status accpets dots
        bool eOk = false;    // cuurent status accepts e/E
        bool ed = false;     // e/E has already seen
        bool activeExit = false;
        for(char byte = static_cast<char>(is.peek());
            !activeExit && is.good();
           )
        {
            switch(byte)
            {
                case '+': case '-':
                    trueThenThrow(!signOk, "unexpected sign +/-");
                    signOk = false; eOk = false; dotOk = !ed;
                    break;
                case '.':
                    trueThenThrow(!dotOk, "unexpected .");
                    signOk = dotOk = false;
                    break;
                case 'e': case 'E':
                    trueThenThrow(!eOk, "unexpected e/E");
                    signOk = true; dotOk = false; eOk = false;
                    ed = true;
                    break;
                case '0': case '1': case '2': case '3': case '4':
                case '5': case '6': case '7': case '8': case '9':
                    signOk = false; eOk = !ed; // dot remains
                    break;
                default:
                    activeExit = true;
                    continue; // skip s+=byte;
            }
            s += byte;
            is.get();
            byte = static_cast<char>(is.peek());
        }
        char last = *s.rbegin();
        trueThenThrow(
            last == 'e' || last == 'E' ||
            last == '+' || last == '-' ||
            s == "+." || s == "-.",
            std::string("unexpected end: " + s)
        );
        return JsonNumber(s);
    }

    static std::map<std::string, Any> ParseJsonMap(a_Istream& is)
    {
        SkipExact(is, "{");

        std::map<std::string, Any> dict;
        decltype(dict.begin()) insPos = dict.begin();
        size_t sizeBeforeIns = 0;
        bool expectComma = false;

        for(char byte = (SkipSpace(is), static_cast<char>(is.peek()));
            is.good();
            SkipSpace(is), byte = static_cast<char>(is.peek())
           )
        {
            switch(byte)
            {
                case '}':
                    trueThenThrow(
                        !expectComma && dict.size() > 0,
                        "element required after , in dict");
                    is.get();
                    return dict;
                case ',':
                    trueThenThrow(!expectComma, ", not expected");
                    is.get();
                    expectComma = false;
                    break;
                default:
                    trueThenThrow(expectComma, ", expected in a dict");
                    std::string name = ParseJsonString(is);
                    //trueThenThrow(
                    //    dict.find(name) != dict.end(), 
                    //    "name not unique: " + name);
                    SkipSpace(is);
                    SkipExact(is, ":");

                    Any value;
                    try
                    {
                        value  = ParseJson(is);
                    }
                    catch(JsonNothingError& )
                    {
                        trueThenThrow(true, "Expect value in map for key " + name);
                    }
                    //dict[name]=value;
                    insPos = dict.insert(insPos, std::make_pair(name, value));
                    trueThenThrow(
                        dict.size() == sizeBeforeIns, "name not unique: " + name);
                    ++ sizeBeforeIns;

                    expectComma = true;
            }
        }
        trueThenThrow(true, "} expected");
        return std::map<std::string, Any>();
    }


    static std::vector<Any> ParseJsonArray(a_Istream& is)
    {
        SkipExact(is, "[");
        std::vector<Any> array;
        bool expectComma = false;
        for(char byte = (SkipSpace(is), static_cast<char>(is.peek()));
            is.good();
            SkipSpace(is), byte = static_cast<char>(is.peek()))
        {
            switch (byte)
            {
                case ',':
                    trueThenThrow(!expectComma, ", unexpected in array");
                    expectComma = false;
                    is.get();
                    break;
                case ']':
                    trueThenThrow(
                            !expectComma && array.size() > 0,
                            "item missing between , and ]");
                    is.get();
                    return array;
                default:
                    trueThenThrow(expectComma, ", or ] expected");
                    array.push_back(ParseJson(is));
                    expectComma = true;
            }
        }
        trueThenThrow(true, "] expected for an array");
        return std::vector<Any>();
    }

    template<>
    bool JsonNumber::operator==(const JsonNumber& rhs) const
    {
        return this->AsString() == rhs.AsString();
    }

    template<>
    bool JsonNumber::operator==(const Any& rhs) const
    {
        if (IsJsonNumber(rhs))
        {
            return *this == AnyCast<JsonNumber>(rhs);
        }
        try  // to avoid cast error thrown outside
        {
#define COMPARE_AS(type) \
            if (rhs.GetType() == typeid(type)) \
            { \
                return this->Cast<type>() == AnyCast<type>(rhs); \
            }

           COMPARE_AS(uint8_t);
           COMPARE_AS(int8_t);
           COMPARE_AS(uint16_t);
           COMPARE_AS(int16_t);
           COMPARE_AS(uint32_t);
           COMPARE_AS(int32_t);
           COMPARE_AS(uint64_t);
           COMPARE_AS(int64_t);
           COMPARE_AS(float);
           COMPARE_AS(double);
#undef COMPARE_AS
        }
        catch (...)
        {
        }
        return false;
    }

    bool Equal(const JsonArray& lhs, const JsonArray& rhs)
    {
        if (lhs.size() != rhs.size())
        {
            return false;
        }
        decltype(lhs.begin()) it1 = lhs.begin();
        decltype(rhs.begin()) it2 = rhs.begin();
        for (; it1 != lhs.end(); ++it1, ++it2)
        {
            if (Equal(*it1,*it2))
               ;
            else
            {
                return false;
            }
        }
        return true;
    }

    bool Equal(const JsonMap& lhs, const JsonMap& rhs)
    {
        if (lhs.size() != rhs.size())
        {
            return false;
        }
        decltype(lhs.begin()) it1 = lhs.begin();
        decltype(rhs.begin()) it2 = rhs.begin();
        for (; it1 != lhs.end(); ++it1, ++it2)
        {
            if (it1->first == it2->first &&
                Equal(it1->second, it2->second)
               )
               ;
            else
            {
                 return false;
            }
        }
        return true;
    }

    bool Equal(const Any& lhs, const Any& rhs)
    {
        if (IsJsonNumber(lhs))
        {
            return AnyCast<JsonNumber>(lhs) == rhs;
        }
        else if (IsJsonNumber(rhs))
        {
            return AnyCast<JsonNumber>(rhs) == lhs;
        }

        if (lhs.GetType() != rhs.GetType())
        {
            return false;
        }

        if (IsJsonNull(lhs))
        {
            return true;
        }
        else if (IsJsonBool(lhs))
        {
            return AnyCast<bool>(lhs) == AnyCast<bool>(rhs);
        }
        else if (IsJsonString(lhs))
        {
            return AnyCast<std::string>(lhs) == AnyCast<std::string>(rhs);
        }
        else if (IsJsonArray(lhs))
        {
            return Equal(AnyCast<JsonArray>(lhs), AnyCast<JsonArray>(rhs));
        }
        else if (IsJsonMap(lhs))
        {
            return Equal(AnyCast<JsonMap>(lhs), AnyCast<JsonMap>(rhs));
        }
        else
        {
            AUTIL_LEGACY_THROW(InvalidOperation, "Equal for Any only supports a json-Any.");
        }
    }

Any ParseJson(std::istream &is)
{
    a_IstreamStd is2(is);
    return ParseJson(is2);
}


}}} // end of namespace autil::legacy::json
