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
#include <ctype.h>
#include <stddef.h>
#include <stdint.h>
#include <algorithm>
#include <string>

#include "autil/legacy/exception.h"
#include "autil/legacy/string_tools.h"

namespace autil{ namespace legacy
{
int CountCharUtf8(const std::string& str)
{
    std::string::size_type byteCount = str.length();
    int charCount = 0;
    std::string::size_type currPos = 0;
    while (currPos < byteCount)
    {
        if((str[currPos] & 0x80))
        {
            int charBytes = 0;
            if ((str[currPos] & 0xE0) == 0xC0)
            {
                charBytes = 2;
            }
            else if((str[currPos] & 0xF0) == 0xE0)
            {
                charBytes = 3;
            }
            else if((str[currPos] & 0xF8) == 0xF0)
            {
                charBytes = 4;
            }
            else if((str[currPos] & 0xFC) == 0xF8)
            {
                charBytes = 5;
            }
            else if((str[currPos] & 0xFE) == 0xFC)
            {
                charBytes = 6;
            }
            else
            {
                AUTIL_LEGACY_THROW(BadUtf8Format, "");
            }
            int i = 0;
            while (++i < charBytes)
            {
                if (++currPos == byteCount)
                {
                    AUTIL_LEGACY_THROW(BadUtf8Format, "");
                }
                if((str[currPos] & 0xC0) != 0x80)
                {
                    AUTIL_LEGACY_THROW(BadUtf8Format, "");
                }
            }
        }
        ++currPos;
        ++charCount;
    }//while
    return charCount;
}

std::string ToUpperCaseUtf8(const std::string& str)
{
    std::string upperCaseString(str);
    std::string::size_type byteCount = upperCaseString.length();
    std::string::size_type currPos = 0;
    for (; currPos < byteCount; ++currPos)
    {
        if (!(upperCaseString[currPos] & 0x80))
        {
            upperCaseString[currPos] = toupper(upperCaseString[currPos]);
        }
    }
    return upperCaseString;
}

std::string ToLowerCaseUtf8(const std::string& str)
{
    std::string lowerCaseString(str);
    std::string::size_type byteCount = lowerCaseString.length();
    std::string::size_type currPos = 0;
    for (; currPos < byteCount; ++currPos)
    {
        if(!(lowerCaseString[currPos] & 0x80))
        {
            lowerCaseString[currPos] = tolower(lowerCaseString[currPos]);
        }
    }
    return lowerCaseString;
}

int StrCaseCmpUtf8(const std::string& str1, const std::string& str2)
{
    std::string::size_type smallerLen = std::min(str1.length(), str2.length());
    std::string::size_type currPos = static_cast<std::string::size_type>(-1);
    while (++currPos < smallerLen)
    {
        if (str1[currPos] == str2[currPos])
        {
            continue;
        }
        else if (!(str1[currPos] & 0x80)
              && !(str2[currPos] & 0x80)
              && tolower(str1[currPos]) == tolower(str2[currPos]))
        {
            continue;
        }
        else
        {
            return ((unsigned char)str1[currPos] & 0xFF) > ((unsigned char)str2[currPos] & 0xFF) ? 1 : -1;
        }
    }//while
    if (str1.length() == str2.length())
    {
        return 0;
    }
    return str1.length() > str2.length() ? 1 : -1;
}

std::string::size_type FindNextCharUtf8(const std::string& str, std::string::size_type pos)
{
    std::string::size_type byteCount = str.length();
    std::string::size_type currPos = pos + 1;
    if (currPos > byteCount)
    {
        AUTIL_LEGACY_THROW(IndexOutOfRange, "");
    }
    while (currPos < byteCount)
    {
        if (!(str[currPos] & 0x80)
          || (str[currPos] & 0xE0) == 0xC0
          || (str[currPos] & 0xF0) == 0xE0
          || (str[currPos] & 0xF8) == 0xF0
          || (str[currPos] & 0xFC) == 0xF8
          || (str[currPos] & 0xFE) == 0xFC)
        {
            break;
        }
        if ((str[currPos] & 0xC0) != 0x80)
        {
            AUTIL_LEGACY_THROW(BadUtf8Format, "");
        }
        ++currPos;
    }//while
    return currPos;
}
}}//end of namespace autil::legacy

autil::legacy::Utf8StringIterator::Utf8StringIterator(std::string string) : mString(string), currPos(0)
{        
}

std::string autil::legacy::Utf8StringIterator::NextChar()
{
    if (currPos < mString.size())
    {
        if((mString[currPos] & 0x80))
        {
            if ((mString[currPos] & 0xE0) == 0xC0)
            {
                // 2 bytes                
                std::string ret = mString.substr(currPos, 2);
                currPos += 2;
                return ret;
            }
            else if((mString[currPos] & 0xF0) == 0xE0)
            {
                // 3 bytes
                std::string ret = mString.substr(currPos, 3);
                currPos += 3;
                return ret;
            }
            else if((mString[currPos] & 0xF8) == 0xF0)
            {
                // 4 bytes
                std::string ret = mString.substr(currPos, 4);
                currPos += 4;
                return ret;
            }
            else
            {
                AUTIL_LEGACY_THROW(ParameterInvalidException, "string with illegal uft8 character");
            }
        }
        else
        {
            // 1 bytes
            std::string ret = mString.substr(currPos, 1);
            currPos += 1;
            return ret;            
        }
    }
    else
    {
        return "";
    }
}       

std::string autil::legacy::Utf8Substr(const std::string& str, size_t pos, size_t length)
{
    Utf8StringIterator itor(str);
    size_t currPos = 0;
    std::string ret;
    std::string tmp;
    while ((tmp = itor.NextChar()) != "" && (currPos < pos || currPos - pos < length))
    {
        if (currPos >= pos)
        {
            ret.append(tmp);
        }
        currPos ++;
    }
    return ret;
}

/**
 * @author: xiaobao
 */
namespace autil{ namespace legacy
{
    static const char UTF8_3BYTE_PREFIX = 0xE0;
    static const char UTF8_2BYTE_PREFIX = 0xC0;
    static const char UTF8_BYTE_PREFIX = 0x80;

    static const char UTF8_3BYTE_MASK = 0xF0; /// first 4 bits are defined for 3 bytes: 1110
    static const char UTF8_2BYTE_MASK = 0xE0; /// first 3 bits are defined for 2 bytes: 110
    static const char UTF8_BYTE_MASK = 0xC0;  /// first 2 bits are defined for tailing bytes: 10

    /**
     * UTF8 Encoding of Unicode(2 byte)
     * =========================================================
     * Data  -- Encoding way (each byte)
     * length-- 
     * =========================================================
     * 7bit  -> one byte: 0 + 7bit
     * 11bit -> two bytes: 110 + 5bit (msb), 10 + 6bit (lsb)
     * 16bit -> three bytes: 1110 + 4bit (msb), 10 + 6bit, 10 + 6bit (lsb)
     */
    std::string UnicodeToUtf8(uint16_t unicode)
    {

        std::string utf8;
        if(unicode <= 0x7F)
        {
            utf8 += static_cast<char>(unicode);
        }
        else if(unicode <= 0x7FF)
        {
            utf8 += static_cast<char>(
                    ((unicode & 0x7C0) >> 6) | UTF8_2BYTE_PREFIX);
            utf8 += static_cast<char>((unicode & 0x3F) | UTF8_BYTE_PREFIX);
        }
        else /*if(unicode <= 0xFFFF)*/ 
        {
            utf8 += static_cast<char>(
                    ((unicode & 0xF000) >> 12) | UTF8_3BYTE_PREFIX);
            utf8 += static_cast<char>(
                    ((unicode & 0x0FC0) >> 6) | UTF8_BYTE_PREFIX);
            utf8 += static_cast<char>((unicode & 0x003F) | UTF8_BYTE_PREFIX);
        }
        return utf8;
    }
    
    /**
     * The inverse function of UnicodeToUtf8. 
     * @param it is a reference so that when the program exists, caller can
     * learn how many chars are parsed through the changed it. The param it stops exactly one byte after
     * those bytes converted. If the sequence is not valid for utf-8, param it will remain unchanged.
     */
    uint16_t Utf8ToUnicode(
        const std::string &s,
        std::string::const_iterator& it)
    {
#define trueThenThrow(test, str) if (test) AUTIL_LEGACY_THROW(autil::legacy::ParameterInvalidException, str);
        trueThenThrow(it == s.end(), "unexpected end in utf8");
        uint16_t unicode = 0;
        char c = *it;
        if (UTF8_3BYTE_PREFIX == (c & UTF8_3BYTE_MASK))
        {   // 3 byte
            trueThenThrow((it+2 > s.end()), "unexpected end in utf8");
            trueThenThrow(((*(it+1)) & UTF8_BYTE_MASK) != UTF8_BYTE_PREFIX, "not utf8, expected prefix missing");
            trueThenThrow(((*(it+2)) & UTF8_BYTE_MASK) != UTF8_BYTE_PREFIX, "not utf8, expected prefix missing");
            unicode = (((c & 0x0F) << 12) |
                       ((*++it & 0x3F) << 6) |
                       ((*++it & 0x3F)));
        }
        else if (UTF8_2BYTE_PREFIX == (c & UTF8_2BYTE_MASK))
        {
            trueThenThrow(it+1 > s.end(), "unexpected end in utf8");
            trueThenThrow(((*(it+1)) & UTF8_BYTE_MASK) != UTF8_BYTE_PREFIX, "not utf8, expected prefix missing");
            unicode = (((c & 0x1F) << 6) |
                       ((*++it & 0x3F)));
        }
        /**
         * Here is not
         *   if ( (c & UTF8_BYTE_MASK) != UTF8_BYTE_PREFIX )
         * since we only accept 0xxxxxxx. 11xxxxxx is also rejected.
         */
        else if ( (c & UTF8_BYTE_PREFIX) != UTF8_BYTE_PREFIX )
        {
            unicode = *it;
        }
        else
        {
            /// 10xx xxxx, or 11xx xxxx
            trueThenThrow(true, "not utf8 - bad byte");
        }
        ++it;   /// Move iterator one byte after involved bytes, for convention.
        return unicode;
#undef trueThenThrow
    }
}}
