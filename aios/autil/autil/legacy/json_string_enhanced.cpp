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
#include <exception>

#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/json.h"
#include "autil/legacy/string_tools.h"

using namespace autil::legacy;
using namespace autil::legacy::json;

namespace
{
    void SkipSpace(const std::string& is, size_t& pos);
    void SkipExact(const std::string& is, size_t& pos, const std::string& str);

    void ParseJsonArray(const std::string& is, size_t& pos, JsonArray& ret);
    void ParseJsonMap(const std::string& is, size_t& pos, JsonMap& ret);
    void ParseJsonNum(const std::string& is, size_t& pos, Any& ret);
    void ParseJsonString(const std::string& is, size_t& pos, std::string& ret);
    void ParseJsonBoolTrue(const std::string& is, size_t& pos);
    void ParseJsonBoolFalse(const std::string& is, size_t& pos);
    void ParseJsonNull(const std::string& is, size_t& pos);

    inline static void trueThenThrow(bool test, const char *s)
    {
        if (test) AUTIL_LEGACY_THROW(JsonParserError, s);
    }

    inline static void trueThenThrow(bool test, const std::string& s)
    {
        if (test) AUTIL_LEGACY_THROW(JsonParserError, s.c_str());
    }
}

namespace autil{ namespace legacy
{
namespace json
{
    void ParseJson(const std::string& is, size_t& pos, Any& ret)
    {
        SkipSpace(is, pos);
        if (pos < is.size())
        {
            char byte = is[pos];
            switch (byte)
            {
                case '[':
                    ret = JsonArray();
                    ParseJsonArray(is, pos, *(AnyCast<JsonArray>(&ret)));
                    break;
                case '{':
                    ret = JsonMap();
                    ParseJsonMap(is, pos, *(AnyCast<JsonMap>(&ret)));
                    break;
                case '"':
                    ret = std::string();
                    ParseJsonString(is, pos, *(AnyCast<std::string>(&ret)));
                    break;
                case 't':
                    ParseJsonBoolTrue(is, pos);
                    ret = true;
                    break;
                case 'f':
                    ParseJsonBoolFalse(is, pos);
                    ret = false;
                    break;
                case 'n':
                    ParseJsonNull(is, pos);
                    ret = Any();
                    break;
                case '+': case '-': case '.':
                case '0': case '1': case '2': case '3': case '4':
                case '5': case '6': case '7': case '8': case '9':
                    ParseJsonNum(is, pos, ret);
                    break;
                default:
                    trueThenThrow(true, std::string("unknown char ") + byte);
            }
            return;
        }
        AUTIL_LEGACY_THROW(JsonNothingError, "");
    }
 
    void ParseJson(const std::string& is, Any& ret)
    {
        size_t pos = 0;
        ParseJson(is, pos, ret);
        SkipSpace(is, pos);
        if (pos < is.size())
        {
            AUTIL_LEGACY_THROW(JsonParserError, "Expect EOF but see more chars");
        }
    }

    void ToString(const Any& a, std::string &os, bool isCompact, const std::string& prefix)
    {
        decltype(a.GetType()) &type = a.GetType();
        if (type == typeid(void))                               /// void (null)
        {
            os.append(prefix);
            os.append("null");
        }
        else if (type == typeid(std::vector<Any>))              /// list
        {
            os.append(prefix);
            os.push_back('[');
            const JsonArray& v = *(AnyCast<JsonArray>(&a));
            for(std::vector<Any>::const_iterator it = v.begin();
                it != v.end();
                ++it)
            {
                if (it != v.begin())
                {
                    os.push_back(',');
                }
                if (!isCompact)
                {
                    os.push_back('\n');
                }
                ToString(*it, os, isCompact, prefix + (isCompact ? "" : "  "));
            }
            if (!isCompact)
            {
                os.push_back('\n');
            }
            os.append(prefix);
            os.push_back(']');
        }
        else if (type == typeid(std::map<std::string, Any>))    /// map
        {
            os.append(prefix);
            os.push_back('{');
            const JsonMap& m = *(
                    AnyCast<JsonMap>(&a)
            );
            for(std::map<std::string, Any>::const_iterator it = m.begin();
                it != m.end();
                ++it)
            {
                if (it != m.begin())
                {
                    os.push_back(',');
                }
                if (!isCompact)
                { 
                    os.push_back('\n');
                }
                os.append(prefix);
                os.push_back('\"');
                QuoteString(it->first, os);
                os.append("\":");
                if (!isCompact)
                {
                    os.push_back('\n');
                }
                ToString(it->second, os, isCompact, prefix + (isCompact ? "" : "  "));
            }
            if (!isCompact)
            {
                os.push_back('\n');
            }
            os.append(prefix);
            os.push_back('}');
        }
        else if (type == typeid(std::string))               /// string
        {
            os.append(prefix);
            os.push_back('\"');
            QuoteString(*AnyCast<std::string>(&a), os);
            os.push_back('\"');
        } else if (type == typeid(bool))                    /// bool
        {
            os.append(prefix);
            os.append(autil::legacy::ToString(AnyCast<bool>(a)));
        }
        /// different numberic types
#define WRITE_DATA(tp)                                             \
        else if (type == typeid(tp))                               \
        {                                                          \
            os.append(prefix);                                     \
            os.append(autil::legacy::ToString(autil::legacy::AnyCast<tp>(a)));   \
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
#define WRITE_DATA_AS(tp, as)                                                         \
        else if (type == typeid(tp))                                                  \
        {                                                                             \
            os.append(prefix);                                                        \
            os.append(autil::legacy::ToString(static_cast<as>(autil::legacy::AnyCast<tp>(a))));     \
        }
        WRITE_DATA_AS(uint8_t, uint32_t)
        WRITE_DATA_AS(int8_t, int32_t)
#undef WRITE_DATA_AS
        /// end of different numberic types
        else if (type == typeid(JsonNumber))         /// number literal
        {
            os.append(prefix);
            os.append(AnyCast<JsonNumber>(a).AsString());
        }
        else                                            /// other un-supported type
        {
            AUTIL_LEGACY_THROW(
                autil::legacy::ParameterInvalidException, 
                std::string("See unsupported data in Any:") + type.name()
            );
        }
    }
}}}

namespace
{
    void SkipMultiLineComment(const std::string&is, size_t& pos)
    {
        bool previsstar = false;
        for(; pos < is.size(); ++pos)
        {
            char b = is[pos];
            if (b == '*')
            {
                previsstar = true;
            }
            else
            {
                if (previsstar && b == '/')
                {
                    ++pos;
                    return;
                }
                previsstar = false;
            }
        }
        trueThenThrow(true, "expect end of comment");
    }
    
    void SkipSingleLineComment(const std::string& is, size_t& pos)
    {
        for (; pos < is.size(); ++pos)
        {
            char b = is[pos];
            if (b == '\r' || b == '\n')
            {
                ++pos;
                return;
            }
        }
        // program goes here if the stream becomes bad (usually eof) before
        // a newline is seen. skipsinglelinecomment regards this situation
        // as the end of the comment and returns. if there are any missing
        // elements, corresponding parsexxx will handle this unexpected end
        // and report error.
    }
    void SkipComment(const std::string& is, size_t& pos)
    {
        trueThenThrow(pos >= is.size(), "expect / or * to begin a comment block after /");
        char b = is[pos];
        
        if (b == '*')
        {
            ++pos;
            SkipMultiLineComment(is, pos);
        }
        else if (b == '/')
        {
            ++pos;
            SkipSingleLineComment(is, pos);
        }
        else
        {
            trueThenThrow(true, "expect / or * to begin a comment block after /");
        }
    }

    void SkipSpace(const std::string& is, size_t& pos)
    {
        while (pos < is.size())
        {
            char b = is[pos];
            switch (b)
            {
                case ' ':
                case '\t':
                case '\r':
                case '\n':
                    ++pos;
                    break;
                case '/':
                    ++pos;
                    SkipComment(is, pos);
                    break;
                default:
                    return;
            }
        }
        // stream crash (usually end) when skipping spaces, ignore.
    }

    void SkipExact(const std::string& is, size_t& pos, const std::string& str)
    {
        int result;
        try
        {
            result = is.compare(pos, str.size(), str);
        }
        catch (std::exception& ex)
        {
            result = -2;
        }
        trueThenThrow(result != 0, std::string("expected ") + str);
        pos += str.size();
    }

    void ParseJsonBoolTrue(const std::string &is, size_t& pos)
    {
        SkipExact(is, pos, "true");
    }

    void ParseJsonBoolFalse(const std::string& is, size_t& pos)
    {
        SkipExact(is, pos, "false");
    }

    void ParseJsonNull(const std::string& is, size_t& pos)
    {
        SkipExact(is, pos, "null");
    }

    void ParseJsonString(const std::string& is, size_t& pos, std::string& ret)
    {
        SkipExact(is, pos, "\"");

        try
        {
            UnquoteString(is, pos, ret);
        }
        catch (const autil::legacy::BadQuoteString& e)
        {
            AUTIL_LEGACY_THROW(JsonParserError, std::string("BadQuoteString:") + e.what());
        }
        SkipExact(is, pos, "\"");
    }

    void ParseJsonNum(const std::string& is, size_t& pos, autil::legacy::Any& ret)
    {
        /*
    Acceptable num format:
        [+|-] x [e|E [+|-] digit+]
        x = digit+ [. digit*]
          = digit* . dight+
        */
        size_t start = pos;
        bool signOk = true;  // current status accepts sign + or -
        bool dotOk = true;   // current status accpets dots
        bool eOk = false;    // cuurent status accepts e/E
        bool ed = false;     // e/E has already seen
        for(; pos < is.size(); ++pos)
        {
            char byte = is[pos];
            switch(byte)
            {
                case '+': case '-':
                    trueThenThrow(!signOk, "unexpected sign +/-");
                    signOk = false; eOk = false; dotOk = !ed;
                    continue;
                case '.':
                    trueThenThrow(!dotOk, "unexpected .");
                    signOk = dotOk = false;
                    continue;
                case 'e': case 'E':
                    trueThenThrow(!eOk, "unexpected e/E");
                    signOk = true; dotOk = false; eOk = false;
                    ed = true;
                    continue;
                case '0': case '1': case '2': case '3': case '4':
                case '5': case '6': case '7': case '8': case '9':
                    signOk = false; eOk = !ed; // dot remains
                    continue;
                default:
                    ;
            }
            break;
        }
        char last = is[pos - 1];
        trueThenThrow(
            last == 'e' || last == 'E' ||
            last == '+' || last == '-' ||
            (start + 2 == pos && last == '.' && (is[start] == '+' || is[start] == '-')),
            "unexpected end");
        ret = JsonNumber(is, start, pos - start);
    }

    void ParseJsonMap(const std::string& is, size_t& pos, std::map<std::string, Any>& ret)
    {
        decltype(ret.begin()) insPos = ret.begin();
        size_t sizeBeforeIns = ret.size();

        SkipExact(is, pos, "{");
        SkipSpace(is, pos);

        bool expectComma = false;
        for (; pos < is.size(); SkipSpace(is, pos))
        {
            char byte = is[pos];
            switch(byte)
            {
                case '}':
                    trueThenThrow(
                        !expectComma && ret.size() > 0,
                        "element required after ,");
                    ++pos;
                    return;
                case ',':
                    trueThenThrow(!expectComma, ", not expected");
                    ++pos;
                    expectComma = false;
                    break;
                default:
                    trueThenThrow(expectComma, ", expected");
                    std::string name;
                    ParseJsonString(is, pos, name);
                    //std::pair<decltype(ret.begin()), bool> it = 
                    //    ret.insert(std::make_pair(name, Any()));
                    insPos = ret.insert(insPos, std::make_pair(name, Any()));
                    trueThenThrow(ret.size() == sizeBeforeIns, "name not unique:" + name);
                    sizeBeforeIns ++;
                    SkipSpace(is, pos);
                    SkipExact(is, pos, ":");

                    try
                    {
                        //ParseJson(is, pos, it.first->second);
                        ParseJson(is, pos, insPos->second);
                    }
                    catch(JsonNothingError& )
                    {
                        trueThenThrow(true, "Expect value in map for key " + name);
                    }
                    expectComma = true;
            }
        }
        trueThenThrow(true, "} expected");
    }


    void ParseJsonArray(const std::string& is, size_t& pos, JsonArray& ret)
    {
        SkipExact(is, pos, "[");
        SkipSpace(is, pos);
        bool expectComma = false;
        for (; pos < is.size(); SkipSpace(is, pos))
        {
            char byte = is[pos];
            switch (byte)
            {
                case ',':
                    trueThenThrow(!expectComma, ", unexpected");
                    expectComma = false;
                    ++pos;
                    break;
                case ']':
                    trueThenThrow(
                            !expectComma && ret.size() > 0,
                            "item missing between , and ]");
                    ++pos;
                    return;
                default:
                    trueThenThrow(expectComma, ", or ] expected");
                    ret.resize(ret.size() + 1);
                    ParseJson(is, pos, ret.back());
                    expectComma = true;
            }
        }
        trueThenThrow(true, "] expected for an array");
    }
} // end of anonymous namespace
