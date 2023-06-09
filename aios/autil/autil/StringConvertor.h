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

#include <stdint.h>
#include <string>

namespace autil {

class StringConvertor {
public:
    StringConvertor();
    ~StringConvertor();

private:
    StringConvertor(const StringConvertor &);
    StringConvertor& operator=(const StringConvertor &);

public:
    template<typename T>
    inline __attribute__((always_inline))
    static T atoi(const char *p, int32_t size) {
        T x = 0;
        bool neg = false;
        const char *end = p + size;
        if (*p == '-') {
            neg = true;
            ++p;
        }
        while (*p >= '0' && *p <= '9' && p < end) {
            x = (x << 3) + (x << 1) + (*p - '0');
            ++p;
        }
        return neg ? -x : x;
    }
};

static const int32_t STRBUF_UNIT_SIZE = 512;

class StringAppender {
public:
    explicit StringAppender(int32_t size = STRBUF_UNIT_SIZE);
    ~StringAppender();

public:
    StringAppender& appendString(const std::string &data);
    StringAppender& appendChar(const char ch);
    StringAppender& appendBool(bool bVal);
    StringAppender& appendInt64(int64_t number);
    std::string toString();
    void copyToString(std::string &data);
    const char* buf() { return buf_; }
    int32_t idx() { return idx_; }
    int32_t size() { return size_; }
    void clear() { idx_ = 0; }
    void decIdx() { if (idx_ > 0) { idx_--; }}

private:
    int32_t normalizeSize(int32_t size);
    void ensureBufSize(int32_t needSize);

private:
    char *buf_;
    int32_t idx_;
    int32_t size_;
};

}

