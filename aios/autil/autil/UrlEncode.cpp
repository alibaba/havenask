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
#include <stdio.h>
#include <stdlib.h>
#include <string>

#include "autil/UrlEncode.h"

namespace autil {

/* Converts a hex character to its integer value */
char from_hex(char ch) {
    return isdigit(ch) ? ch - '0' : tolower(ch) - 'a' + 10;
}

/* Converts an integer value to its hex character*/
char to_hex(char code) {
    static char hex[] = "0123456789ABCDEF";
    return hex[code & 15];
}

/* Returns a url-encoded version of str */
/* IMPORTANT: be sure to free() the returned string after use */
int UrlEncode::encode(const char *str, const int strSize, char *buf, const int resultSize) {
    int writeSize = resultSize - 3;
    const char *pstr = str;
    char *pbuf = buf;
    int count = 0;
    while (pstr < str + strSize  && count < writeSize) {
        if (isalnum(*pstr) || *pstr == '-' || *pstr == '_' || *pstr == '.' || *pstr == '~') {
            count++;
            *pbuf++ = *pstr;
        } else if (*pstr == ' ') {
            count++;
            *pbuf++ = '+';
        } else {
            count += 3;
            *pbuf++ = '%', *pbuf++ = to_hex(*pstr >> 4), *pbuf++ = to_hex(*pstr & 15);
        }
        pstr++;
    }
    *pbuf = '\0';
    return count;
}

int UrlEncode::decode(const char *str, const int strSize, char *buf, int bufsize) {
    int writeSize = (bufsize - 1);
    const char *pstr = str;
    const char *pstrEnd = str + strSize;
    char *pbuf = buf;
    int count = 0;
    while (pstr < pstrEnd && count < writeSize) {
        if (*pstr == '%') {
            if (pstr + 2 < pstrEnd && pstr[1] && pstr[2]) {
                *pbuf++ = from_hex(pstr[1]) << 4 | from_hex(pstr[2]);
                pstr += 2;
                count++;
            }
        } else if (*pstr == '+') {
            *pbuf++ = ' ';
            count++;
        } else {
            *pbuf++ = *pstr;
            count++;
        }
        pstr++;
    }
    *pbuf = '\0';
    return count;
}

std::string UrlEncode::decode(const std::string &s) {
    size_t size = s.size();
    std::string res;
    if (size > 0) {
        char *pdata = (char *)malloc(size + 1);
        if (NULL != pdata) {
            int count = decode(s.c_str(), s.size(), pdata, size + 1);
            res.assign(pdata, count);
        }
        free(pdata);
    }
    return res;
}

std::string UrlEncode::encode(const std::string &s) {
    size_t size = s.size();
    std::string res;
    if (size > 0) {
        char *pdata = (char *)malloc(size * 3 + 1);
        if (NULL != pdata) {
            int count = encode(s.c_str(), s.size(), pdata, size * 3 + 1);
            res.assign(pdata, count);
        }
        free(pdata);
    }
    return res;
}

}
