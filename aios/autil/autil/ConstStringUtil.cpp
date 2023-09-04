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
#include "autil/ConstStringUtil.h"

#include <iosfwd>
#include <stdio.h>

using namespace std;
namespace autil {

std::vector<autil::StringView>
ConstStringUtil::split(const autil::StringView &text, const std::string &sepStr, bool ignoreEmpty) {
    std::vector<autil::StringView> vec;
    split(vec, text, sepStr, ignoreEmpty);
    return vec;
}

std::vector<autil::StringView>
ConstStringUtil::split(const autil::StringView &text, const char &sepChar, bool ignoreEmpty) {
    std::vector<autil::StringView> vec;
    split(vec, text, sepChar, ignoreEmpty);
    return vec;
}

void ConstStringUtil::split(std::vector<autil::StringView> &vec,
                            const autil::StringView &text,
                            const char &sepChar,
                            bool ignoreEmpty) {
    split(vec, text, std::string(1, sepChar), ignoreEmpty);
}

void ConstStringUtil::split(std::vector<autil::StringView> &vec,
                            const autil::StringView &text,
                            const std::string &sepStr,
                            bool ignoreEmpty) {
    size_t n = 0, old = 0;
    while (n != std::string::npos) {
        n = text.find(sepStr, n);
        if (n != std::string::npos) {
            if (!ignoreEmpty || n != old)
                vec.push_back(text.substr(old, n - old));
            n += sepStr.length();
            old = n;
        }
    }

    if (!ignoreEmpty || old < text.length()) {
        vec.push_back(text.substr(old, text.length() - old));
    }
}

} // namespace autil
