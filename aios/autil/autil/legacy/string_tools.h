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
/** @file string_tools.h
 * Part I: String utility functions for utf-8 format.
 *  String utility functions for utf-8 format.
 *  Currently support functions includes:
 *  1. return utf8 character counts
 *  2. uppercase to lowercase or lowercase to uppercase
 *  3. case_insensitive comparison for two utf-8 strings
 *  4. return the starting offset of next character after an given position
 *  of a string
 *
 * Part II: string convertion from and additon with bool, int, and double
 *
 * Part III: string pattern map string to string vector
 * Part IIII: string regex function wrapper, test whether a string is matched with certain expression
 */


#include <regex.h>
#include <stddef.h>
#include <stdint.h>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include "autil/StringUtil.h"
#include "autil/legacy/exception.h"

/// Part I: UTF8 Support
namespace autil{ namespace legacy
{
class a_Istream;

    /**
     * Exception throw when string format is not utf-8
     */
    class BadUtf8Format : public ExceptionBase {
    public:
        AUTIL_LEGACY_DEFINE_EXCEPTION(BadUtf8Format, ExceptionBase);
    };


    class IndexOutOfRange : public ExceptionBase {
    public:
        AUTIL_LEGACY_DEFINE_EXCEPTION(IndexOutOfRange, ExceptionBase);
    };

    /** Count the number of utf-8 characters in utf-8 encoded string,
     *  Throw utf8 format exception if input is not an uft-8 string
     *
     *  @param str input string, utf-8 format assumed
     *
     *  @return number of utf-8 characters
     */
    int CountCharUtf8(const std::string& str);

    /**
     * Transform all lowercase characters of inputstring into uppercase,
     * and result is stored in the return string object
     *
     *  @param str input string, utf-8 format assumed
     *
     *  @return result string object, with all lowercase characters transformed
     */
    std::string ToUpperCaseUtf8(const std::string& str);

    /**
    * Transform all uppercase characters of inputstring into lowercase,
    * and result is stored in the return string object
    *
    *  @param str input string, utf-8 format assumed
    *
    *  @return result string object, with all uppercase characters transformed
    */

    std::string ToLowerCaseUtf8(const std::string& str);

    /**
    * Case insensitive two utf-8 strings compare function.For different input
    * string, the sequence of the first different character between two strings
    * determine the sequence of the two strings
    *
    *  @param str1 first input string
    *  @param str2 second input string
    *
    *  @return int 1 if str1 great than str2
    *              0 if str1 equalto str2
    *              -1 if str1 less than str2
    */
    int StrCaseCmpUtf8(const std::string& str1, const std::string& str2);

    /** Find the starting offset of the next character after pos.
    *@code
    * for (string::size_type i = 0; i < s.size();)
    * {
    * 	string:size_type j = FindNexCharUtf8(s, i);
    *   //process the char in [i, j)
    *   i = j;
    * }
    *
    * @endcode
    *  @param str string inputstring
    *  @param pos string::size_type position in bytes, not in character
    *
    *  @return string::size_type the start pos of the next utf8 character.
    *   means the next character is the first character of substring[pos + 1, ...)
    *   of inputstring
    *   return str.size() if pos belongs to [latsCharPos, str.size())
    *   throw IndexOutofRange exception if there is no character at pos in the str,
    *   means that pos greater than or equalto str.size().
    *   Note: this function also throws BadUtf8Format Exception if string format is
    *   not utf8 format.
    */
    std::string::size_type FindNextCharUtf8(const std::string& str, std::string::size_type pos);
}}


/**
 * @author xiaobao
 */
namespace autil{ namespace legacy
{
    /**
     * UTF8 Encoding of Unicode(2 byte)
     * =========================================================
     * Data  -- Encoding way (each byte)
     * length--
     * =========================================================
     * 7bit  -> one byte: 0 + 7bit
     * 11bit -> two bytes: 110 + 5bit, 10 + 6bit
     * 16bit -> three bytes: 11100 + 4bit, 10 + 6bit, 10 + 6bit
     */
    std::string UnicodeToUtf8(uint16_t unicode);

    /**
     * The inverse function of UnicodeToUtf8.
     * @param it is a reference so that when the method returns, caller can
     * learn how many chars are parsed through the changed it.
     */
    uint16_t Utf8ToUnicode(
            const std::string &s,
            std::string::const_iterator& it);

    inline uint16_t Utf8ToUnicode(const std::string &s)
    {
        std::string::const_iterator it = s.begin();
        return Utf8ToUnicode(s, it);
    }
}}


/// Part II: String operations including conversion, splitting, trimming.

/**
 * @author xiaobao
 */
namespace autil{ namespace legacy
{
    class BadQuoteString : public ParameterInvalidException
    {
        public:
            BadQuoteString(const std::string& e) : ParameterInvalidException(e)
            {}
    };

    /**
     * covert num to its hex expression in string format,
     * such as ToHexDigit(3) == '3', ToHexDigit(12) == 'C'
     */
    char ToHexDigit(uint8_t data);

    /**
     * covert numerical data to its hex expression in string format
     * such as ToHexString(0x375f) == "375f", ToHexString(0x0000ff) == "ff"
     */
    template<typename T>
    std::string ToHexString(const T &data)
    {
        uint32_t size = sizeof(T) * 8;
        T dataCopy = data;
        std::string str;
        do {
            uint8_t n = dataCopy & 0x0f;
            char c = static_cast<char>(n < 10 ? ('0' + n) : ('A' + n - 10));
            str.insert(str.begin(), c);
        } while ((dataCopy >>= 4) && (size -= 4));
        return str;
    }

    /** Quote string using json format
     * @param textToEscape exact chars to be quoted
     * @return Quoted version of the string
     */
    std::string QuoteString(const std::string& textToEscape);
    void QuoteString(const std::string& textToEscape, std::string& ret);

    /** Un-quote string using json format
     * @param escapedText string of char quoted by QuoteString
     * @return exact chars represented
     */
    std::string UnquoteString(const std::string& escapedText);
    std::string UnquoteString(a_Istream& is);
    void UnquoteString(const std::string& escapedText, size_t& pos, std::string& ret);

    /** Split string by delimeter */
    std::vector<std::string> SplitString(
        const std::string& str,
        const std::string& delimeter=" ");

    /**
     * This method's behaviors is not like autil::legacy::SplitString(string, string),
     * The difference is below method use the whole delim as a separator,
     * and will scan the target str from begin to end and we drop "".
     * @Return: vector of substring split by delim, without ""
     */
    std::vector<std::string> StringSpliter(
        const std::string& str,
        const std::string& delim);

    /** Remove whitespaces in the beginning and the end of a string */
    std::string TrimString(
            const std::string& str,
            const char leftTrimChar = ' ',
            const char rightTrimChar = ' ');
    std::string LeftTrimString(const std::string& str, const char trimChar = ' ');
    std::string RightTrimString(const std::string& str, const char trimChar = ' ');

    std::string ToLowerCaseString(const std::string& orig);
    std::string ToUpperCaseString(const std::string& orig);

    /** String Replacement Functions **/

    /** Replace the specific string segments with new string.
        Notice: In this process, each processment will not base on the result of
        previous replacement result. (please notice the difference with replace string
        step by step)
        e.g. ReplaceString("122212","12","1") == "1221"
        ReplaceStringIteratively("122212", "12", "1") == "11"
    */
    std::string ReplaceString(const std::string& origin_string,
        const std::string& old_value, const std::string& new_value);

    /**
     * Convert from string into T.
     * Example: int n = StringTo<int>("100")
     * Current Support T:
     * int8_t, uint8_t, int16_t, uint16_t, int32_t, uin32_t, int64_t, uint64_t,
     * float, double, std::string
     */
    template<typename T>
    T StringTo(const std::string& str)
    {
        std::string s = RightTrimString(str);
        T t;
        if ( StringUtil::fromString(s, t))
        {
            return t;
        }

        AUTIL_LEGACY_THROW(ParameterInvalidException, "No valid cast given the string:" + str);
    }

    /**
     * @brief this function is used to convert the key in the cell to hex representation
     */
    inline std::string StringToHex(const std::string& str)
    {
        static const char* table = "0123456789ABCDEF";
        std::string result(str.size() * 2, '\0');
        for(size_t i = 0; i < str.length(); i++)
        {
            result[i * 2] = table[(str[i] & 0xF0) >> 4];
            result[i * 2 + 1] = table[str[i] & 0x0F];
        }
        return result;
    }
    /**
     * @note FromHexString is the corresponding method to StringToHex.
     */
    std::string FromHexString(const std::string& str);

    template<> inline std::string StringTo(const std::string& string) { return string; }

    template<> uint8_t StringTo(const std::string& string);

    template<> int8_t StringTo(const std::string& string);

    template<> bool StringTo(const std::string& string);

    /**
    * Convert from string into std::vector<T>
    * @see StringTo for valid types of T
    */
    template<typename T>
    std::vector<T> StringToVector(const std::string& string, const std::string& delim = ",")
    {
        if (delim.size() == 0)
        {
            AUTIL_LEGACY_THROW(autil::legacy::ParameterInvalidException, "Empty delim");
        }
        if (string.size() == 0)
        {
            return std::vector<T>();
        }
        std::vector<T> part;
        size_t pos = 0;
        while (pos <= string.size())
        {
            size_t pos2 = string.find_first_of(delim, pos);
            if (pos2 == std::string::npos)
            {
                pos2 = string.size();
            }
            part.push_back(StringTo<T>(string.substr(pos, pos2-pos)));
            pos = pos2 + 1;
        }
        return part;
    }

    /**
    * Convert from string into std::pair<K, V>
    * @see StringTo for valid types of K, V
    */
    template<typename K, typename V>
    std::pair<K, V> StringToPair(const std::string& str, const std::string& delim = ":")
    {
        size_t pos = str.find(delim);
        if (pos == std::string::npos)
        {
            AUTIL_LEGACY_THROW(ParameterInvalidException, "Missing delim expected inside pair");
        }
        return std::make_pair(
                StringTo<K>(str.substr(0, pos)),
                StringTo<V>(str.substr(pos + delim.size())));
    }

    /**
    * Convert from string into std::map<K, V>
    * @see StringTo for valid types of K, V
    */
    template<typename K, typename V>
    std::map<K, V> StringToMap(
            const std::string& string,
            const std::string& itemDelim = ",",
            const std::string& keyValueDelim = ":")
    {
        std::map<K, V> result;
        std::vector<std::string> part = StringToVector<std::string>(string, itemDelim);
        for (std::vector<std::string>::const_iterator it = part.begin();
             it != part.end(); ++it)
        {
            if (!result.insert(StringToPair<K,V>(*it, keyValueDelim)).second)
            {
                AUTIL_LEGACY_THROW(ParameterInvalidException, "Duplicate keys in map");
            }
        }
        return result;
    }


    /**
     * Convert from T to string.
     * @Example: std::string s = ToString(100);
     *
     * Be careful with uint8_t and int8_t: They are equalevent with usigned char and signed char.
     * Therefore, ToString(uint8_t(48)) is the same as ToString('0'), and thus yields
     * std::string("0") instead of std::string("48"). To get the latter result, please explicitly
     * cast the parameter from uint8_t (or int8_t) to uint32_t (or int32_t respectively).
     */
    template<typename T>
    std::string ToString(const T& obj)
    {
        return autil::StringUtil::toString(obj);
    }

    template<> inline std::string ToString(const std::string& data) { return data; }

    template<> std::string ToString(const bool& data);

    template<> std::string ToString(const float& data);

    template<> std::string ToString(const double& data);

    /**
    * Convert from std::vector<T> into string
    * @see ToSting for valid types of T
    * @see StringToVector for converting from string to std::vector<T>
    */
    template<typename T>
    std::string ToString(const std::vector<T>& vec, const std::string& delim = ",")
     {
         return StringUtil::toString(vec, delim);
     }

    std::string ToString(double data, uint32_t precison);

    /**
     * Convert integer to binary, it is more efficient for positive integer number,
     * since integer 123 will take 3 bytes when ToString() but only need 1 byte actually.
     *
     * The function does not depend on the endian-ness of the machine.
     * It always place the lsb in the first byte of the result, and msb in the last byte.
     */
    template<typename IntegerType>
    std::string IntegerToBinary(IntegerType n)
    {
        uint8_t size = sizeof(IntegerType);
        std::string s;
        do
        {
            s += static_cast<char>(n & 0xff);
        } while ((n >>= 8) && s.size() < size);
        return s;
    }

    template<typename IntegerType>
    IntegerType BinaryToInteger(const std::string& s)
    {
        if (s.empty() || s.size() > sizeof(IntegerType))
        {
            AUTIL_LEGACY_THROW(ParameterInvalidException, "can't convert empty string to integer");
        }

        IntegerType n = 0;
        for (size_t pos = 0; pos < s.size(); pos++)
        {
            n |= static_cast<IntegerType>(static_cast<uint8_t>(s[pos])) << (pos * 8);
        }
        return n;
    }

    /**
     * Addition with std::string
     * Still pay attention to uint8_t and int8_t. For more detail, see comments in ToString()
     */
    std::string operator+(const std::string& left, int32_t data);
    std::string operator+(const std::string& left, uint32_t data);
    std::string operator+(const std::string& left, int64_t data);
    std::string operator+(const std::string& left, uint64_t data);
    std::string operator+(const std::string& left, float data);
    std::string operator+(const std::string& left, double data);
    std::string operator+(const std::string& left, bool data);

    /**
     * To escape the "input" string so that it can be written to a c++ program, not voilate
     * the string literal rule in c++ languages. For example, it escape " to \".
     */
    std::string EscapeForCCode(const std::string& input);


    /**
     * @brief Returns whether the std::string begins with the pattern passed in.
     * @param input the input string
     * @param pattern The pattern to compare with.
     * @return result
     */
    bool StartWith(const std::string& input, const std::string& pattern);

    /**
     * @brief Returns whether the std::string ends with the pattern passed in.
     * @param input the input string
     * @param pattern The pattern to compare with.
     * @return result
     */
    bool EndWith(const std::string& input, const std::string& pattern);
}}

/**
 * Part III: string pattern map string to string list (by ly)
 * GRAMMAR: python style (the content between {} is optional, but its order must follow the sample)
 * @Example: "pangu://file1_{instance_id1}_any_{range(1, 10)}_any_{["xxx", "yyy", "zzz"]}.dat;
 *            pangu://file2_{instance_id2}_any_{range(150, 501)}_any_{["xxx", "yyy"]}.dat"
 * UNITTEST:
 * CASE 1: Basic logic of string pattern
 * CASE 2: Exception handling for invalid input grammar
 */

namespace autil{ namespace legacy
{
class StringPattern
{
public:
    StringPattern();

    /**
    * Set pattern user want to transform
    */
    void SetPattern(const std::string& pattern);

    /**
     * Set map of map<variable, value>
     */
    void SetVariableMap(const std::map<std::string, std::string>& variableMap);

    /**
     * @return pattern
     */
    std::string GetPattern() const;

    /**
     * @return instance_id map
     */
    std::map<std::string, std::string> GetVariableMap() const;

    /**
     * Pattern transform result.
     * @return strings transformed from pattern
     */
    std::vector<std::string> GetStrings();

    /**
     * Get the idx string in result strings.
     * @return the idx string in result strings.
     */
    std::string operator [](size_t idx);

    /**
     * Get the result strings' size
     */
    size_t size();

private:
    /**
     * Prepare for operator []
     */
    void Prepare();

    /**
     * Reset patternInfos for new pattern or variableMap
     */
    void Reset();

    /**
     * PatternSplit split mPattern by (char)delim which is ';' by default
     * @Example:
     * mPattern "pangu://file1_{instance_id1}_any_{range(1, 2)}_any_{["xxx", "yyy", "zzz"]}.dat;
     *           pangu://file2_{instance_id2}_any_{range(150, 151)}_any_{["xxx"]}.dat"
     * @return string vector as: "pangu://file1_{instance_id1}_any_{range(1, 2)}_any_{["xxx", "yyy", "zzz"]}.dat
     *                           pangu://file2_{instance_id2}_any_{range(150, 151)}_any_{["xxx"]}.dat
     */
    std::vector<std::string> PatternSplit(const char& delim = ';') const;

    /**
     * QuoteStrip get the content string between ""
     * @Example;
     * input: " \"xxx\" "
     * output: "xxx"
     */
    std::string QuoteStrip(const std::string& str) const;

    /**
     * InternalSplit split the string between cLeft and cRight by ','
     * cLeft,cRight can be '(' and ')' or '[' and ']', when cLeft and cRight is '(' ')'
     * the content between () must be (xx, yy), this is for range(x, y)
     * @Example:
     * input: (xxx, yyy)
     * output: xxx
     *         yyy
     * input: [xxx, yyy, zzz]
     * output: xxx
     *         yyy
     *         zzz
     */
    std::vector<std::string> InternalSplit(const std::string& str, const char& cLeft, const char& cRight) const;

    /**
     * Given a pattern string, strip and return all patterns.
     * @Example:
     * input: pangu://file1_{instance_id1}_any_{range(1, 2)}_any_{["xxx", "yyy", "zzz"]}.dat
     * output: {{pangu://file1_}, {a}, {_any_}, {1,2}, {_any_}, {xxx, yyy, zzz}, {.dat}}
     */
    std::vector<std::vector<std::string> > SplitStringIntoPatterns(const std::string& str) const;

    /**
     * Span the Content pattern to a string vector.
     * @Example:
     * input: {["xxx", "yyy", "zzz"]}
     * output: xxx
     *         yyy
     *         zzz
     */
    std::vector<std::string> ContentSpan(const std::string& str) const;

    /**
     * Span the Range pattern to a string vector.
     * @Example:
     * input: {range(1, 3)}
     * output:        1
     *                2
     *                3
     */
    std::vector<std::string> RangeSpan(const std::string& str) const;

    std::vector<std::string> RangeSpanInternal(const std::string& lo, const std::string& hi) const;

    /**
     * @brief Span the Range pattern to a string vector. Resulting string length can be specified.
     * @param str the input string pattern; 3rd param - width (default: width_of_(2nd param)); 4th param - fill char (default: '0')
     * @return result
     * @exception specified width not enough for all items; pattern syntax error
     * @Example:
     * input: {range(8, 12, 3, #)}      {range(8, 12)}
     * output:        ##8                   08
     *                ##9                   09
     *                #10                   10
     *                #11                   11
     *                #12                   12
     */
    std::vector<std::string> RangeSpanAligned(const std::string& str) const;
    bool AlignStrings(std::vector<std::string>& vecStrings, int width = 0, char fill = '0') const;

    /**
     * private member
     */
    std::string mPattern;
    std::map<std::string, std::string> mVariableMap;
    /**
     * private member for iterator and []
     */
    struct PatternInfo
    {
        std::vector<std::vector<std::string> > partialPatterns;
        std::vector<size_t> subStringNumber;
        /*  patternString = "pangu://file1_{instance_id1}_any_{range(1, 2)}_any_{["xxx", "yyy", "zzz"]}.dat"
         *  mIdMap[instance_id1] = "id1"
         *  partialPatterns = [["file1_"]
         *                     ["id1"]
         *                     ["_any_"]
         *                     ["1", "2"]
         *                     ["_any_"]
         *                     ["xxx", "yyy", "zzz"]
         *                     [".dat"]]
         *  subStringNumber = [6, 6, 6, 6, 3, 3, 1, 1];
         */
    };
    std::vector<PatternInfo> mPatternInfos;
    size_t mSize;

}; // class StringPattern
}} // namespace autil::legacy


/** PART IV: Regular expressions
*/
namespace autil{ namespace legacy
{
/**
 *@brief match the string with regular expression
 *@param str origin string
 *@param pattern regular expression
 *@param regex_mode match mode
 *@return match success or not
*/
bool RegexMatch(const std::string& str, const std::string& pattern, const int& regex_mode=REG_NOSUB);

/**
 *@brief match the string with regular expression (extension mode)
 *@param str origin string
 *@param pattern regular expression
 *@return match success or not
*/
bool RegexMatchExtension(const std::string& str, const std::string& pattern);

/**
 *@brief match the string, and get the matched item into group
 *@param str origin string
 *@param pattern regular expression
 *@param mItems the return of matched groups
 *@return match success or not
*/
bool RegexGroupMatch(const std::string &originStr, const std::string& pattern, std::vector<std::string>& matchedItems);

class Utf8StringIterator
{
    std::string mString;
    std::string::size_type currPos;
public:
    Utf8StringIterator(std::string string);
    std::string NextChar();
};

std::string Utf8Substr(const std::string& str, size_t pos, size_t length = std::string::npos);

}} // namespace autil::legacy
