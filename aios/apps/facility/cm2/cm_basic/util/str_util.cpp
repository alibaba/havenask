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
#include "aios/apps/facility/cm2/cm_basic/util/str_util.h"

namespace cm_basic {

static const char* getSection(const char* pszBegin, char cSeparator, char* pszBuffer, unsigned uiLength)
{
    if ((NULL == pszBegin) || (NULL == pszBuffer) || (0 == uiLength))
        return (const char*)-1;

    pszBuffer[0] = '\0';

    // 去除头部的空格
    for (; (*pszBegin != '\0') && isspace(*pszBegin); ++pszBegin)
        ;

    if ('\0' == *pszBegin)
        return NULL;

    if (cSeparator == *pszBegin)
        return pszBegin + 1;

    // 留出一个空字符空间
    --uiLength;

    // 取数据
    unsigned uiDataLen = 0;
    for (; (pszBegin[uiDataLen] != '\0') && (pszBegin[uiDataLen] != cSeparator); ++uiDataLen) {
        if (uiDataLen >= uiLength)
            return (const char*)-1;
        pszBuffer[uiDataLen] = pszBegin[uiDataLen];
    }

    // 移动开始位置到当前字段的结束符处
    pszBegin += uiDataLen;

    // 去除尾部的空格
    for (--uiDataLen; (uiDataLen > 0) && isspace(pszBuffer[uiDataLen]); --uiDataLen)
        ;

    pszBuffer[uiDataLen + 1] = '\0';

    // 计算返回值
    if (cSeparator == *pszBegin)
        return pszBegin + 1;

    return NULL;
}

void splitStr(const char* pszAttribute, char cSep, std::vector<std::string>& vProtoPortAttr)
{
    while ((pszAttribute != NULL) && (pszAttribute != (void*)-1)) {
        char szFlag[256];
        pszAttribute = getSection(pszAttribute, cSep, szFlag, 256);
        if (((void*)-1 == pszAttribute) || ('\0' == szFlag[0])) {
            continue;
        }
        vProtoPortAttr.push_back(szFlag);
    }
}

/* 整齐字符串,去掉字符串前后的空格,调用者要自己释放返回字符串delete[] result; */
char* trim(const char* szStr)
{
    const char* szTmp;
    char *szRet, *szTrim;
    szTmp = szStr;
    while (isSpace(*szTmp))
        szTmp++;
    size_t len = strlen(szTmp);
    szTrim = replicate(szTmp); //最后有空格
    if (len > 0 && isSpace(szTrim[len - 1])) {
        szRet = &szTrim[len - 1];
        while (isSpace(*szRet))
            szRet--;
        *(++szRet) = '\0';
    }
    return szTrim;
}

/* 复制字符串,函数内部分配内存空间,调用者必须负责释放返回的字符串 */
char* replicate(const char* szStr)
{
    if (szStr == NULL)
        return NULL;
    size_t len = strlen(szStr);
    char* szRet = new char[len + 1];
    assert(len + 1 > 0);
    strcpy(szRet, szStr);
    return szRet;
}

char* str_ltrim(char* str)
{
    char* p;
    char* end_ptr;
    int dest_len;

    end_ptr = str + strlen(str);
    for (p = str; p < end_ptr; p++) {
        if (!(' ' == *p || '\n' == *p || '\r' == *p || '\t' == *p)) {
            break;
        }
    }

    if (p == str) {
        return str;
    }

    dest_len = (end_ptr - p) + 1; // including \0
    memmove(str, p, dest_len);

    return str;
}

char* str_rtrim(char* str)
{
    int len;
    char* p;
    char* last_ptr;

    len = strlen(str);
    if (len == 0) {
        return str;
    }

    last_ptr = str + len - 1;
    for (p = last_ptr; p >= str; p--) {
        if (!(' ' == *p || '\n' == *p || '\r' == *p || '\t' == *p)) {
            break;
        }
    }

    if (p != last_ptr) {
        *(p + 1) = '\0';
    }

    return str;
}

char* str_trim(char* str)
{
    str_rtrim(str);
    str_ltrim(str);
    return str;
}

char* str_lowercase(char* src)
{
    char* p;

    p = src;
    while (*p != '\0') {
        if (*p >= 'A' && *p <= 'Z') {
            *p += 32;
        }

        p++;
    }

    return src;
}

char* str_uppercase(char* src)
{
    char* p;

    p = src;
    while (*p != '\0') {
        if (*p >= 'a' && *p <= 'z') {
            *p -= 32;
        }

        p++;
    }

    return src;
}

} // namespace cm_basic
