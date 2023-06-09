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
#include "autil/UrlDecoder.h"

#include <ctype.h>
#include <cstddef>

#include "string.h"

using namespace std;

namespace autil {

UrlDecoder::UrlDecoder() {
}

UrlDecoder::~UrlDecoder() {
}

string UrlDecoder::Decode(const string& str) {
    string result;
    size_t index = 0;
    const char *data = str.c_str();
    for (size_t len = str.length(); len > 0; --len, ++index) {
        if (*data == '+') {
            result.append(" ");
        } else if (*data == '%' && len >= 2 &&
                   isxdigit(*(data + 1)) && isxdigit(*(data + 2))) {
            result.append(1, (char)DecodeChar(data + 1));
            data += 2;
            len -= 2;
        } else {
            result.append(1, *data);
        }
        ++data;
    }
    return result;
}

char *UrlDecoder::Decode(const char *str) {
    char *res = new char[strlen(str) + 1];
    char *cur = res;

    for (; *str != '\0'; ++str, ++cur) {
        if (*str == '+') {
            *cur = ' ';
        } else if (*str == '%' && *(str + 1) != '\0'
                   && *(str + 2) != '\0') {
            *cur = DecodeChar(str + 1);
            str += 2;
        } else {
            *cur = *str;
        }
    }
    *cur = '\0';
    return res;
}

char UrlDecoder::DecodeChar(const char *bytes) {
    int ch = tolower(*bytes++);
    int value = (ch >= '0' && ch <= '9' ? ch - '0' : ch - 'a' + 10) << 4;
    ch = tolower(*bytes);
    value += (ch >= '0' && ch <= '9') ? ch - '0' : ch - 'a' + 10;
    return (char)value;
}

} // namespace autil
