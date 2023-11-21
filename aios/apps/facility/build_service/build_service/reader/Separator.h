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

#include "build_service/common_define.h"
#include "build_service/util/Log.h"

namespace build_service { namespace reader {

class Separator
{
public:
    Separator(const std::string& sep);
    ~Separator();

private:
    Separator(const Separator&);
    Separator& operator=(const Separator&);

public:
    const char* findInBuffer(const char* buffer, const char* end) const;

public:
    size_t size() const { return _sep.size(); }
    bool isEmpty() { return _sep.empty(); }
    const std::string& getSeperator() const { return _sep; }

private:
    std::string _sep;
    int32_t _next[256];
};

inline const char* Separator::findInBuffer(const char* buffer, const char* end) const
{
    size_t sepLen = _sep.size();
    const char* sepBegin = _sep.data();
    if (sepLen <= 4) {
        const char* ret = std::search(buffer, end, sepBegin, sepBegin + sepLen);
        return ret != end ? ret : NULL;
    }

    const char* curBack = buffer + sepLen - 1;

    while (curBack < end) {
        int32_t next = _next[(unsigned char)*curBack];
        curBack += sepLen;
        if (next == 1) {
            continue;
        }
        curBack += next;
        if (curBack > end) {
            return NULL;
        }
        const char* cur = curBack - sepLen;
        if (std::equal(cur, curBack, sepBegin)) {
            return cur;
        }
    }

    return NULL;
}

}} // namespace build_service::reader
