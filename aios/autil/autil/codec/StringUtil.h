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

#include <limits.h>
#include <stdint.h>
#include <string>
#include <vector>

#define AUTIL_CODE_MAX_BOLD_TERMS 64
#define AUTIL_CODE_BOLD_BEGIN "<b>"
#define AUTIL_CODE_BOLD_END "</b>"

namespace autil {
namespace codec {

struct cn_result {
    std::string cn_str;
    bool first_flag;
};

class StringUtil {
public:
    static char *trim(char *szStr);
    static std::string removeSpace(const std::string &str);

    static unsigned int
    split(std::vector<std::string> &v, const std::string &s, char delimiter, unsigned int maxSegments = INT_MAX);
    template <typename T>
    static unsigned int split(T &output, const std::string &s, char delimiter, unsigned int maxSegments = INT_MAX);

    static std::string join(const std::vector<std::string> &array, const std::string &seperator);

    static bool strToKV32(const char *str, uint64_t &value);
    static bool strToKVFloat(const char *str, uint64_t &value);

    // 是否全是非中文字符
    static bool isAllChar(const std::string &str);
    // 是否包含非中文字符
    static bool containsChar(const std::string &str);
    // 是否是中文和英文的混合
    static bool isCnCharSemi(const std::string &str, cn_result &cn_obj);
    // 根据分词将src 字符串加粗
    static int32_t boldLineStr(
        const std::vector<std::string> &segments, const char *src, int32_t src_len, char *dst, int32_t dest_len);
};

template <typename T>
inline unsigned int StringUtil::split(T &output, const std::string &s, char delimiter, unsigned int maxSegments) {
    std::string::size_type left = 0;
    unsigned int i = 0;
    unsigned int num = 0;
    for (i = 1; i < maxSegments; i++) {
        std::string::size_type right = s.find(delimiter, left);
        if (right == std::string::npos) {
            break;
        }
        *output++ = s.substr(left, right - left);
        left = right + 1;
        ++num;
    }
    std::string lastSubStr = s.substr(left);
    if (lastSubStr != "") {
        *output++ = lastSubStr;
        ++num;
    }
    return num;
}

} // namespace codec
} // namespace autil
