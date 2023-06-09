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
#include "autil/StringUtil.h"

#include <assert.h>
#include <ctype.h>
#include <errno.h>
#include <limits.h>
#include <pthread.h>
#include <stdarg.h>
#include <string.h>
#include <time.h>
#include <bits/types/struct_tm.h>
#include <algorithm>

#include "autil/Lock.h"
#include "autil/ShortString.h"
#include "autil/TimeUtility.h"

using namespace std;

namespace autil {

const std::string StringUtil::NULL_STRING = "";

StringUtil::StringUtil() {
}

StringUtil::~StringUtil() {
}

class StringStreamPool {
public:
    StringStreamPool() {
        _ssVec.reserve(8);
    }
    ~StringStreamPool() {
        for (size_t i = 0; i < _ssVec.size(); ++i) {
            delete _ssVec[i];
        }
    }
    stringstream* getStringStream() {
        stringstream* ret = NULL;
        {
            ScopedLock lock(_lock);
            if (!_ssVec.empty()) {
                ret = _ssVec.back();
                _ssVec.pop_back();
            }
        }
        if (ret == NULL) {
            ret = new stringstream();
        }
        return ret;
    }
    void putStringStream(stringstream* ss) {
        ss->clear();
        ScopedLock lock(_lock);
        _ssVec.push_back(ss);
    }
private:
    ThreadMutex _lock;
    vector<stringstream *> _ssVec;
};

static const size_t POOL_SIZE = 127;
static StringStreamPool sPool[POOL_SIZE];

stringstream* StringUtil::getStringStream() {
    size_t offset = pthread_self() % POOL_SIZE;
    return sPool[offset].getStringStream();
}

void StringUtil::putStringStream(stringstream* ss) {
    size_t offset = pthread_self() % POOL_SIZE;
    sPool[offset].putStringStream(ss);
}

void StringUtil::trim(std::string& str) {
    str.erase(str.find_last_not_of(' ') + 1);
    str.erase(0, str.find_first_not_of(' '));
}

void StringUtil::trim(std::string_view& str) {
    auto endPos = str.find_last_not_of(' ');
    auto beginPos = str.find_first_not_of(' ');
    if (beginPos == std::string_view::npos) {
        beginPos = 0;
    }
    str = str.substr(beginPos, endPos - beginPos + 1);
}

bool StringUtil::parseTrueFalse(const string &str, bool &value) {
    string trimStr = str;
    trim(trimStr);
    toLowerCase(trimStr);
    if (trimStr == "t"
        || trimStr == "true"
        || trimStr == "y"
        || trimStr == "yes"
        || trimStr == "1")
    {
        value = true;
        return true;
    } else if (trimStr == "f"
               || trimStr == "false"
               || trimStr == "n"
               || trimStr == "no"
               || trimStr == "0")
    {
        value = false;
        return true;
    } else {
        return false;
    }
}

bool StringUtil::startsWith(const std::string &str, const std::string &prefix) {
    return (str.size() >= prefix.size())
        && (str.compare(0, prefix.size(), prefix) == 0);
}

bool StringUtil::endsWith(const std::string &str, const std::string &suffix) {
    size_t s1 = str.size();
    size_t s2 = suffix.size();
    return (s1 >= s2) && (str.compare(s1 - s2, s2, suffix) == 0);
}

std::vector<std::string> StringUtil::split(const std::string& text,
        const std::string &sepStr, bool ignoreEmpty)
{
    std::vector<std::string> vec;
    split(vec, text, sepStr, ignoreEmpty);
    return vec;
}

std::vector<std::string> StringUtil::split(const std::string& text,
        const char &sepChar, bool ignoreEmpty)
{
    std::vector<std::string> vec;
    split(vec, text, sepChar, ignoreEmpty);
    return vec;
}

void StringUtil::split(std::vector<std::string> &vec, const std::string& text,
                       const char &sepChar, bool ignoreEmpty)
{
    split(vec, text, std::string(1, sepChar), ignoreEmpty);
}

void StringUtil::split(std::vector<std::string> &vec, const std::string& text,
                       const std::string &sepStr, bool ignoreEmpty)
{
    std::string str(text);
    std::string sep(sepStr);
    size_t n = 0, old = 0;
    while (n != std::string::npos)
    {
        n = str.find(sep,n);
        if (n != std::string::npos)
        {
            if (!ignoreEmpty || n != old)
                vec.emplace_back(str.substr(old, n-old));
            n += sep.length();
            old = n;
        }
    }

    if (!ignoreEmpty || old < str.length()) {
        vec.emplace_back(str.substr(old, str.length() - old));
    }
}

bool StringUtil::isSpace(const string& text) {
    if (text == string("　")) {
        return true;
    }
    if (text.length() > 1) {
        return false;
    }
    return isspace(text[0]);
}

bool StringUtil::isSpace(const ShortString& text) {
    if (text == "　") {
        return true;
    }

    if (text.length() > 1) {
        return false;
    }

    return isspace(text[0]);
}

void StringUtil::toUpperCase(char *str) {
    if (str) {
        while (*str) {
            if (*str >= 'a' && *str <= 'z') {
                *str += 'A' - 'a';
            }
            str++;
        }
    }
}

void StringUtil::toUpperCase(string &str) {
    for(size_t i = 0; i < str.size(); i++) {
        str[i] = toupper(str[i]);
    }
}

void StringUtil::toUpperCase(const char *str, std::string &retStr) {
    retStr = str;
    for(size_t i = 0; i < retStr.size(); i++) {
        retStr[i] = toupper(str[i]);
    }
}

void StringUtil::toLowerCase(string &str) {
    for(size_t i = 0; i < str.size(); i++) {
        str[i] = tolower(str[i]);
    }
}

void StringUtil::toLowerCase(char *str) {
    if (str) {
        while (*str) {
            if (*str >= 'A' && *str <= 'Z') {
                *str -= 'A' - 'a';
            }
            str++;
        }
    }
}

void StringUtil::toLowerCase(const char *str, std::string &retStr) {
    retStr = str;
    toLowerCase(retStr);
}

bool StringUtil::strToInt8(const char* str, int8_t& value)
{
    int32_t v32 = 0;
    bool ret = strToInt32(str, v32);
    value = (int8_t)v32;

    return ret && v32 >= INT8_MIN && v32 <= INT8_MAX;
}

bool StringUtil::strToUInt8(const char* str, uint8_t& value)
{
    uint32_t v32 = 0;
    bool ret = strToUInt32(str, v32);
    value = (uint8_t)v32;

    return ret && v32 <= UINT8_MAX;
}

bool StringUtil::strToInt16(const char* str, int16_t& value)
{
    int32_t v32 = 0;
    bool ret = strToInt32(str, v32);
    value = (int16_t)v32;
    return ret && v32 >= INT16_MIN && v32 <= INT16_MAX;
}

bool StringUtil::strToUInt16(const char* str, uint16_t& value)
{
    uint32_t v32 = 0;
    bool ret = strToUInt32(str, v32);
    value = (uint16_t)v32;
    return ret && v32 <= UINT16_MAX;
}

bool StringUtil::strToInt32(const char* str, int32_t& value)
{
    if (NULL == str || *str == 0)
    {
        return false;
    }
    char* endPtr = NULL;
    errno = 0;

# if __WORDSIZE == 64
    int64_t value64 = strtol(str, &endPtr, 10);
    if (value64 < INT32_MIN || value64 > INT32_MAX)
    {
        return false;
    }
    value = (int32_t)value64;
# else
    value = (int32_t)strtol(str, &endPtr, 10);
# endif

    if (errno == 0 && endPtr && *endPtr == 0)
    {
        return true;
    }
    return false;
}

bool StringUtil::strToUInt32(const char* str, uint32_t& value)
{
    if (NULL == str || *str == 0 || *str == '-')
    {
        return false;
    }
    char* endPtr = NULL;
    errno = 0;

# if __WORDSIZE == 64
    uint64_t value64 = strtoul(str, &endPtr, 10);
    if (value64 > UINT32_MAX)
    {
        return false;
    }
    value = (int32_t)value64;
# else
    value = (uint32_t)strtoul(str, &endPtr, 10);
# endif

    if (errno == 0 && endPtr && *endPtr == 0)
    {
        return true;
    }
    return false;
}

bool StringUtil::strToUInt64(const char* str, uint64_t& value)
{
    if (NULL == str || *str == 0 || *str == '-')
    {
        return false;
    }
    char* endPtr = NULL;
    errno = 0;
    value = (uint64_t)strtoull(str, &endPtr, 10);
    if (errno == 0 && endPtr && *endPtr == 0)
    {
        return true;
    }
    return false;
}

bool StringUtil::strToInt64(const char* str, int64_t& value)
{
    if (NULL == str || *str == 0)
    {
        return false;
    }
    char* endPtr = NULL;
    errno = 0;
    value = (int64_t)strtoll(str, &endPtr, 10);
    if (errno == 0 && endPtr && *endPtr == 0)
    {
        return true;
    }
    return false;
}

bool StringUtil::hexStrToUint64(const char* str, uint64_t& value)
{
    if (NULL == str || *str == 0)
     {
         return false;
     }
     char* endPtr = NULL;
     errno = 0;
     value = (uint64_t)strtoull(str, &endPtr, 16);
     if (errno == 0 && endPtr && *endPtr == 0)
     {
         return true;
     }
     return false;
}

bool StringUtil::hexStrToInt64(const char* str, int64_t& value) {
    if (NULL == str || *str == 0)
     {
         return false;
     }
     char* endPtr = NULL;
     errno = 0;
     value = (int64_t)strtoll(str, &endPtr, 16);
     if (errno == 0 && endPtr && *endPtr == 0)
     {
         return true;
     }
     return false;
}

uint32_t StringUtil::deserializeUInt32(const std::string& str)
{
    assert(str.length() == sizeof(uint32_t));

    uint32_t value= 0;
    for (size_t i = 0; i < str.length(); ++i)
    {
        value <<= 8;
        value |= (unsigned char)str[i];
    }
    return value;
}

void StringUtil::serializeUInt32(uint32_t value, std::string& str)
{
    char key[4];
    for (int i = (int)sizeof(uint32_t) - 1; i >= 0; --i)
    {
        key[i] = (char)(value & 0xFF);
        value >>= 8;
    }
    str.assign(key, sizeof(uint32_t));
}

void StringUtil::serializeUInt64(uint64_t value, std::string& str)
{
    char key[8];
    for (int i = (int)sizeof(uint64_t) - 1; i >= 0; --i)
    {
        key[i] = (char)(value & 0xFF);
        value >>= 8;
    }
    str.assign(key, sizeof(uint64_t));
}

uint64_t  StringUtil::deserializeUInt64(const std::string& str)
{
    assert(str.length() == sizeof(uint64_t));

    uint64_t value= 0;
    for (size_t i = 0; i < str.length(); ++i)
    {
        value <<= 8;
        value |= (unsigned char)str[i];
    }
    return value;
}

bool StringUtil::strToFloat(const char* str, float& value)
{
    if (NULL == str || *str == 0)
    {
        return false;
    }
    errno = 0;
    char* endPtr = NULL;
    value = strtof(str, &endPtr);
    if (errno == 0 && endPtr && *endPtr == 0)
    {
        return true;
    }
    return false;
}

bool StringUtil::strToDouble(const char* str, double& value)
{
    if (NULL == str || *str == 0)
    {
        return false;
    }
    errno = 0;
    char* endPtr = NULL;
    value = strtod(str, &endPtr);
    if (errno == 0 && endPtr && *endPtr == 0)
    {
        return true;
    }
    return false;
}

void StringUtil::uint64ToHexStr(uint64_t value, char* hexStr, int len)
{
    assert(len > 16);
    snprintf(hexStr, len, "%016lx", value);
}

int8_t StringUtil::strToInt8WithDefault(const char* str, int8_t defaultValue)
{
    int8_t tmp;
    if(strToInt8(str, tmp))
    {
        return tmp;
    }
    return defaultValue;
}

uint8_t StringUtil::strToUInt8WithDefault(const char* str, uint8_t defaultValue)
{
    uint8_t tmp;
    if(strToUInt8(str, tmp))
    {
        return tmp;
    }
    return defaultValue;
}

int16_t StringUtil::strToInt16WithDefault(const char* str, int16_t defaultValue)
{
    int16_t tmp;
    if(strToInt16(str, tmp))
    {
        return tmp;
    }
    return defaultValue;
}

uint16_t StringUtil::strToUInt16WithDefault(const char* str, uint16_t defaultValue)
{
    uint16_t tmp;
    if(strToUInt16(str, tmp))
    {
        return tmp;
    }
    return defaultValue;
}

int32_t StringUtil::strToInt32WithDefault(const char* str, int32_t defaultValue)
{
    int32_t tmp;
    if(strToInt32(str, tmp))
    {
        return tmp;
    }
    return defaultValue;
}

uint32_t StringUtil::strToUInt32WithDefault(const char* str, uint32_t defaultValue)
{
    uint32_t tmp;
    if(strToUInt32(str, tmp))
    {
        return tmp;
    }
    return defaultValue;
}

int64_t StringUtil::strToInt64WithDefault(const char* str, int64_t defaultValue)
{
    int64_t tmp;
    if(strToInt64(str, tmp))
    {
        return tmp;
    }
    return defaultValue;
}

uint64_t StringUtil::strToUInt64WithDefault(const char* str, uint64_t defaultValue)
{
    uint64_t tmp;
    if(strToUInt64(str, tmp))
    {
        return tmp;
    }
    return defaultValue;
}

float StringUtil::strToFloatWithDefault(const char* str, float defaultValue)
{
    float tmp;
    if(strToFloat(str, tmp))
    {
        return tmp;
    }
    return defaultValue;
}

double StringUtil::strToDoubleWithDefault(const char* str, double defaultValue)
{
    double tmp;
    if(strToDouble(str, tmp))
    {
        return tmp;
    }
    return defaultValue;
}

void StringUtil::sundaySearch(const string &text, const string &key, vector<size_t> &posVec, bool caseSensitive) {
    size_t textSize = text.size();
    size_t keySize = key.size();
    if (caseSensitive) {
        sundaySearch(text.c_str(), textSize, key.c_str(), keySize, posVec);
    } else {
        string str(text);
        string keyword(key);
        toUpperCase(str);
        toUpperCase(keyword);
        sundaySearch(str.c_str(), textSize, keyword.c_str(), keySize, posVec);
    }
}

void StringUtil::sundaySearch(const char *text, const char *key, vector<size_t> &posVec) {
    size_t textSize = strlen(text);
    size_t keySize = strlen(key);
    sundaySearch(text, textSize, key, keySize, posVec);
}

void StringUtil::sundaySearch(const char *text, size_t textSize, const char *key, size_t keySize, vector<size_t> &posVec) {
    posVec.clear();
    if (textSize < keySize || keySize == 0) {
        return;
    }

    if (keySize == 1) {
        for (size_t i = 0; i < textSize; ++i) {
            if (text[i] == *key) {
                posVec.push_back(i);
            }
        }
        return;
    }

    uint32_t next[256];
    for(size_t i = 0; i < 256; ++i) {
        next[i] = keySize + 1;
    }

    for(size_t i = 0; i < keySize; ++i) {
        next[(unsigned char)(key[i])] = keySize - i;
    }

    size_t maxPos = textSize - keySize;
    size_t pos;
    for(pos = 0; pos < maxPos;) {
        size_t i;
        size_t j;
        for(i = pos, j = 0; j < keySize; ++j, ++i) {
            if(text[i] != key[j]) {
                pos += next[(unsigned char)(text[pos + keySize])];
                break;
            }
        }
        if(j == keySize) {
            posVec.push_back(pos);
            pos += next[(unsigned char)(text[pos + keySize])];
        }
    }

    if (pos == maxPos) {
        size_t i;
        size_t j;
        for(i = pos, j = 0; j < keySize; ++j, ++i) {
            if(text[i] != key[j]) {
                break;
            }
        }
        if(j == keySize) {
            posVec.push_back(pos);
        }
    }
}

void StringUtil::replaceLast(string &str, const string& oldStr,
                             const string& newStr)
{
    size_t pos = str.rfind(oldStr);
    if (pos != string::npos) {
        str.replace(pos, oldStr.size(), newStr);
    }
}

void StringUtil::replaceFirst(string &str, const string& oldStr,
                             const string& newStr)
{
    size_t pos = str.find(oldStr);
    if (pos != string::npos) {
        str.replace(pos, oldStr.size(), newStr);
    }
}


void StringUtil::replaceAll(string &str, const string& oldStr,
                            const string& newStr)
{
    size_t pos = str.find(oldStr);
    while (pos != std::string::npos) {
        str.replace(pos, oldStr.size(), newStr);
        pos = str.find(oldStr, pos + newStr.size());
    }
}

void StringUtil::replace(string &str, char oldValue, char newValue) {
    ::replace(str.begin(), str.end(), oldValue, newValue);
}

string StringUtil::strToHexStr(const std::string &str)
{
    string hexStr;
    hexStr.resize(2 * str.length());

    for (size_t i = 0; i < str.length(); ++i) {
        uint8_t c = (uint8_t)str[i];
        sprintf(&hexStr[i*2], "%02x", c);
    }

    return hexStr;
}

string StringUtil::hexStrToStr(const std::string &hexStr)
{
    assert(hexStr.length() % 2 == 0);
    string str;
    string lowCasedStr;
    StringUtil::toLowerCase(hexStr.c_str(), lowCasedStr);
    for (size_t i = 0; i < lowCasedStr.length(); i += 2) {
        int ascii;
        char a = lowCasedStr[i], b = lowCasedStr[i+1];
        if (a >= 'a') {
            ascii = (a - 'a' + 10) * 16;
        } else {
            ascii = (a - '0') * 16;
        }

        if (b >= 'a') {
            ascii += (b - 'a' + 10);
        } else {
            ascii += (b - '0');
        }

        char tempChar = (char)ascii;
        str += tempChar;
    }
    return str;
}

void StringUtil::getKVValue(const string& text, string& key, string& value,
                            const string& sep, bool isTrim)
{
    size_t n;

    if (!sep.empty() && (n = text.find(sep, 0)) != string::npos) {
        key = text.substr(0, n);
        value = text.substr(n + sep.length());
    } else {
        key = text;
        value.clear();
    }

    if (isTrim) {
        trim(key);
        trim(value);
    }
}

bool StringUtil::tryConvertToDateInMonth(int64_t timeInSecond, std::string& str)
{
    int64_t curTimeInSecond = autil::TimeUtility::currentTimeInSeconds();
    int64_t interval = 3600 * 24 * 30; // 1 month
    if (timeInSecond > (curTimeInSecond - interval) && timeInSecond <= curTimeInSecond) {
        time_t tmp = timeInSecond;
        struct tm cvt;
        localtime_r(&tmp, &cvt);
        char dataBuf[64];
        size_t len = strftime(dataBuf, 64, "%F %T", &cvt);
        str = string(dataBuf, len);
        return true;
    }
    return false;
}

string StringUtil::join(const vector<string> &array, const string &seperator) {
    string result;
    for (vector<string>::const_iterator itr = array.begin(); itr != array.end(); ++itr) {
        result.append(*itr);
        if (itr + 1 != array.end()) {
            result.append(seperator);
        }
    }
    return result;
}


}
