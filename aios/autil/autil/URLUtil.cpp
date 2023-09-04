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
#include "autil/URLUtil.h"

#include <assert.h>
#include <ctype.h>
#include <iosfwd>
#include <stdlib.h>
#include <strings.h>

using namespace std;

namespace autil {

URLUtil::URLUtil() {}

URLUtil::~URLUtil() {}

string URLUtil::decode(const string &str) {
    size_t nlen = str.length();
    char *dstBuf = (char *)malloc(nlen + 1);
    assert(dstBuf);
    bzero(dstBuf, nlen + 1);
    char *dst = dstBuf;
    const char *src = str.c_str();
    char temp[3]; // store two char for strtol, end with '\0'
    temp[2] = '\0';

    for (size_t i = 0; i <= nlen;) {
        if (src[i] == '+') {
            *dst++ = ' ';
            ++i;
        } else if (src[i] == '%' && i + 2 <= nlen && isxdigit(src[i + 1]) && isxdigit(src[i + 2])) {
            // convert hex string to integer
            temp[0] = src[i + 1];
            temp[1] = src[i + 2];
            *dst++ = strtol(temp, NULL, 16);
            i += 3;
        } else {
            *dst++ = src[i];
            ++i;
        }
    }
    string ss(dstBuf);
    free(dstBuf);
    return ss;
}

} // namespace autil
