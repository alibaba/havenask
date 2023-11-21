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
/**
 * =====================================================================================
 *
 *       Filename:  str_util.h
 *
 *    Description:  字符串解析库
 *
 *        Version:  0.1
 *        Created:  2013-05-07 15:11:09
 *       Revision:  none
 *       Compiler:  glibc
 *
 *         Author:  tiechou (search engine group), tiechou@taobao.com
 *        Company:  www.taobao.com
 *
 * =====================================================================================
 */

#ifndef SRC_STR_UTIL_H_
#define SRC_STR_UTIL_H_
#include <assert.h>
#include <stdarg.h>
#include <stdio.h>
#include <string.h>
#include <string>
#include <vector>

#include "aios/apps/facility/cm2/cm_basic/common/common.h"
#include "autil/CommonMacros.h"

namespace cm_basic {

void splitStr(const char* pszAttribute, char cSep, std::vector<std::string>& vProtoPortAttr);

template <typename Vec>
std::string joinStr(const Vec& vec, const std::string& sep = ",")
{
    std::string ret;
    for (typename Vec::const_iterator it = vec.begin(); it != vec.end(); ++it) {
        if (ret.length() == 0) {
            ret = *it;
        } else {
            ret += sep + *it;
        }
    }
    return ret;
}

char* trim(const char* szStr);
char* replicate(const char* szStr);

/*
 *  判断是否为空格,包括单字节的'\t','\r','\n'等字符
 */
inline bool isSpace(char ch) { return (ch) == ' ' || (ch) == '\t' || (ch) == '\r' || (ch) == '\n'; }
/*
 *  判断是否为单字节的字母
 */
inline bool isAlpha(char ch) { return (((ch) >= 'a' && (ch) <= 'z') || ((ch) >= 'A' && (ch) <= 'Z')); }
/*
 *  判断是否为双字节的字母
 */
inline bool isChineseAlpha(unsigned char c1, unsigned char c2)
{
    return (c1 == 0xA3) && (((c2 >= 0xC1) && (c2 <= 0xDA)) || ((c2 >= 0xE1) && (c2 <= 0xFA)));
}
/*
 *  判断是否为单字节的数字
 */
inline bool isNum(char ch) { return ((ch) >= '0' && (ch) <= '9'); }

/*
 *   判断字符串是否为数字表达
 */
inline bool isAllNumber(const char* szStr)
{
    if (!szStr) {
        return false;
    }
    int iLen = strlen(szStr);
    for (int i = 0; i < iLen; i++) {
        if ((!isNum(szStr[i])) && (szStr[i] != '.')) {
            if ((i == 0) && ((szStr[i] == '+') || (szStr[i] == '-'))) {
                continue;
            }
            return false;
        }
    }
    return true;
}

/*
 *  判断是否为字母(包括单字节和双字节)
 */
inline bool isWholeAlpha(const char* pStr) { return (isAlpha(*pStr) || isChineseAlpha(pStr[0], pStr[1])); }

/*
 *  判断是否为单字节的字母和数字
 */
inline bool isAlphaNum(const char ch) { return (isAlpha(ch) || isNum(ch)); }

inline char char2num(char ch)
{
    if (ch >= '0' && ch <= '9')
        return (char)(ch - '0');
    if (ch >= 'a' && ch <= 'f')
        return (char)(ch - 'a' + 10);
    if (ch >= 'A' && ch <= 'F')
        return (char)(ch - 'A' + 10);
    return '0';
}

inline int safe_snprintf(char* s, int n, const char* format, ...)
{
    if (n <= 0) {
        return 0;
    }
    int ret = 0;
    va_list args;
    va_start(args, format);
    ret = vsnprintf(s, 0, format, args);
    va_end(args);
    if (n <= 0) {
        return 0;
    }
    return (ret >= n) ? n - 1 : ret;
};

/**
 * @brief lowercase the string
 * @param src input string, will be changed by this function
 * @return lowercased string
 */
char* str_lowercase(char* src);

/**
 * @brief uppercase the string
 * @param src input string, will be changed by this function
 * @return uppercased string
 */
char* str_uppercase(char* src);

/**
 * @brief trim leading spaces ( \\t\\r\\n)
 * @param str the string to trim
 * @return: the left trimed string porinter as str
 */
char* str_ltrim(char* str);

/**
 * @brief trim tail spaces ( \\t\\r\\n)
 * @param str the string to trim
 * @return the right trimed string porinter as str
 */
char* str_rtrim(char* str);

/**
 * @brief trim leading and tail spaces ( \\t\\r\\n)
 * @param str the string to trim
 * @return the trimed string porinter as str
 */
char* str_trim(char* str);

/**
 * 用特定的分隔符 将字符串  的一部分  分开 （源字符串会被修改掉）
 *
 * @param begin            起始字符位置
 * @param end              结束字符位置
 * @param delim            分隔符
 * @param arr              用于存储数据结果的数组
 * @param arr_len          输入的数组的长度
 *
 * @return    切分出的子串个数
 */
inline int str_part_split(char* begin, char* end, char delim, char* arr[], int arr_len)
{
    int cnt = 0;

    while ('\0' != *begin) {
        arr[cnt] = begin;
        ++cnt;

        while (('\0' != *begin) && (delim != *begin)) {
            ++begin; /* 循环一直到  delim 或者  \0 结束 */
        }

        *begin = '\0'; /* 找到了分隔符 截断 */

        ++begin;

        if (unlikely((begin >= end) || (cnt >= arr_len)))
            break;
    }

    return cnt;
}

/**
 * 用特定的分隔符 将字符串分开 （源字符串会被修改掉）
 *
 * @param begin            起始字符位置
 * @param delim            分隔符
 * @param arr              用于存储数据结果的数组
 * @param arr_len          输入的数组的长度
 *
 * @return    切分出的子串个数
 */
inline int str_split(char* begin, char delim, char* arr[], int arr_len)
{
    int cnt = 0;

    while ('\0' != *begin) {
        arr[cnt] = begin;

        while (('\0' != *begin) && (delim != *begin)) {
            ++begin; /* 循环一直到  delim 或者  \0 结束 */
        }

        if (unlikely(*begin == '\0')) {
            ++cnt;

            break;
        } else {
            *begin = '\0'; /* 找到了分隔符 截断 */

            ++begin;
            ++cnt;
        }

        if (unlikely(cnt >= arr_len))
            break;
    }

    return cnt;
}

/**
 * 用特定的分隔符 将字符串 的一部分 分开 （源字符串会被修改掉）
 * 并将切开的字符串的长度返回
 *
 * @param begin            起始字符位置
 * @param end              结束字符位置
 * @param delim            分隔符
 * @param sub_arr          用于存储数据结果的数组
 * @param sub_len_arr      用于存储 切开的字串的长度,
 * @param arr_len          输入的2个数组  的长度， 必须一致
 *
 * @return    切分出的子串个数
 */
inline int str_part_split_len(char* begin, char* end, char delim, char* sub_arr[], int sub_len_arr[], int arr_len)
{
    int cnt = 0;

    while ('\0' != *begin) {
        sub_arr[cnt] = begin;

        while (('\0' != *begin) && (delim != *begin)) {
            ++begin; /* 循环一直到  delim 或者  \0 结束 */
        }

        if (unlikely(*begin == '\0')) {
            sub_len_arr[cnt] = begin - sub_arr[cnt];
            ++cnt;

            break;
        } else {
            *begin = '\0'; /* 找到了分隔符 截断 */
            sub_len_arr[cnt] = begin - sub_arr[cnt];

            ++begin;
            ++cnt;
        }

        if (unlikely((begin >= end) || (cnt >= arr_len)))
            break;
    }

    return cnt;
}

/**
 * 用特定的分隔符 将字符串分开 （源字符串会被修改掉）
 * 并将切开的字符串的长度返回
 *
 * @param begin            起始字符位置
 * @param delim            分隔符
 * @param sub_arr          用于存储数据结果的数组
 * @param sub_len_arr      用于存储 切开的字串的长度,
 * @param arr_len          输入的2个数组  的长度， 必须一致
 *
 * @return    切分出的子串个数
 */
inline int str_split_len(char* begin, char delim, char* sub_arr[], int sub_len_arr[], int arr_len)
{
    int cnt = 0;

    while ('\0' != *begin) {
        sub_arr[cnt] = begin;

        while (('\0' != *begin) && (delim != *begin)) {
            ++begin; /* 循环一直到  delim 或者  \0 结束 */
        }

        if (unlikely(*begin == '\0')) {
            sub_len_arr[cnt] = begin - sub_arr[cnt];
            ++cnt;

            break;
        } else {
            *begin = '\0'; /* 找到了分隔符 截断 */
            sub_len_arr[cnt] = begin - sub_arr[cnt];

            ++begin;
            ++cnt;
        }

        if (unlikely(cnt >= arr_len))
            break;
    }

    return cnt;
}

/**
 * 根据特殊字符将字符串划分为多个子串 （源字符串不会被修改掉）
 *
 * @param begin            起始字符位置
 * @param size             长度
 * @param delim            分隔符
 * @param arr              用于存储各个子串起始位置的数组
 * @param arr_ele_cnt      用于存储各个子串的长度的数组
 * @param arr_len          输入的数组的长度
 *
 * @return    切分出的子串个数
 */
inline int const_str_split(const char* begin, const int size, char delim, const char* arr[], int arr_ele_cnt[],
                           int arr_len)
{
    int cnt = 0;
    int pos = 0;

    while ('\0' != *begin) {
        arr[cnt] = begin;

        int ele_cnt = 0;
        while ((pos < size) && (delim != *begin)) {
            ++begin;   /* 循环一直到  delim 或者  \0 结束 */
            ++ele_cnt; /*  记录子串的长度 */
            ++pos;
        }
        arr_ele_cnt[cnt] = ele_cnt;

        ++cnt;
        ++begin;
        ++pos;

        if ((pos >= size) || (cnt >= arr_len)) {
            break;
        }
    }

    return cnt;
}

} // namespace cm_basic
#endif
