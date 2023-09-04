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
#include "autil/codec/StringUtil.h"

#include <ctype.h>
#include <iosfwd>
#include <iterator>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <strings.h>

#include "autil/CommonMacros.h"
#include "autil/StringUtil.h"

namespace autil {
namespace codec {

#define SAFE_COPY(dst, src, len)                                                                                       \
    if (len > 0) {                                                                                                     \
        snprintf(dst, len, "%s", src);                                                                                 \
    }

#define AUTIL_CODE_MAX_BOLD_TERMS 64

using namespace std;

char *StringUtil::trim(char *szStr) {
    char *szTmp;
    char *szRet, *szTrim;

    szTmp = szStr;
    while ((*szTmp) == ' ' || (*szTmp) == '\t' || (*szTmp) == '\r' || (*szTmp) == '\n')
        szTmp++;
    size_t len = strlen(szTmp);
    szTrim = szTmp;
    if (len > 0 &&
        (szTrim[len - 1] == ' ' || szTrim[len - 1] == '\t' || szTrim[len - 1] == '\r' || szTrim[len - 1] == '\n')) {
        szRet = &szTrim[len - 1];
        while ((*szRet) == ' ' || (*szRet) == '\t' || (*szRet) == '\r' || (*szRet) == '\n')
            szRet--;
        *(++szRet) = '\0';
    }

    return szTrim;
}

string StringUtil::removeSpace(const string &str) {
    string result;
    result.reserve(str.size());
    for (string::const_iterator itr = str.begin(); itr != str.end(); ++itr) {
        if (*itr != ' ' && *itr != '\t') {
            result.append(1, *itr);
        }
    }
    return result;
}

unsigned int StringUtil::split(vector<string> &v, const string &s, char delimiter, unsigned int maxSegments) {
    v.clear();
    back_insert_iterator<vector<string>> it(v);
    return split(it, s, delimiter, maxSegments);
}

string StringUtil::join(const vector<string> &array, const string &seperator) {
    string result;
    for (vector<string>::const_iterator itr = array.begin(); itr != array.end(); ++itr) {
        result.append(*itr);
        if (itr + 1 != array.end()) {
            result.append(seperator);
        }
    }
    return result;
}

bool StringUtil::strToKV32(const char *str, uint64_t &value) {
    if (NULL == str || *str == 0) {
        return false;
    }
    const char *pvalue = strchr(str, ':');
    if (!pvalue || !*pvalue) {
        return false;
    }
    pvalue++;

    union kv_struct {
        struct {
            uint32_t k;
            uint32_t v;
        } kv;
        uint64_t value;
    } data;
    // key和value转换都正确
    autil::StringUtil::strToUInt32(str, data.kv.k);
    autil::StringUtil::strToUInt32(pvalue, data.kv.v);
    value = data.value;
    return true;
}

bool StringUtil::strToKVFloat(const char *str, uint64_t &value) {
    if (NULL == str || *str == 0) {
        return false;
    }
    const char *pvalue = strchr(str, ':');
    if (!pvalue || !*pvalue) {
        return false;
    }
    pvalue++;

    union kv_struct {
        struct {
            uint32_t k;
            float v;
        } kv;
        uint64_t value;
    } data;
    // key和value转换都正确
    autil::StringUtil::strToUInt32(str, data.kv.k);
    autil::StringUtil::strToFloat(pvalue, data.kv.v);
    value = data.value;
    return true;
}

bool StringUtil::isAllChar(const string &str) {
    size_t str_size = str.size();
    for (uint32_t i = 0; i < str_size; ++i) {
        if (0 != (str[i] & 0x80)) {
            return false;
        }
    }
    return true;
}

bool StringUtil::containsChar(const string &str) {
    bool flag = false;
    size_t str_size = str.size();
    for (uint32_t i = 0; i < str_size; ++i) {
        if (0 == (str[i] & 0x80)) {
            flag = true;
            break;
        }
    }
    return flag;
}

/**
 * 是否是中英文的混合,只能是"aa中文" 或者"中文aa"两种格式否则返回false
 * cn.firt_flag 用于标记是否是英文打头
 * cn.cn_str 保存中文字符串
 * @param str:input
 * @param cn_obj:output
 *
 * @return
 */
bool StringUtil::isCnCharSemi(const string &str, cn_result &cn_obj) {
    size_t str_size = str.size();
    cn_obj.first_flag = false;
    if (1 >= str_size) {
        return false;
    }

    if (1 < str_size) {
        if (0 == (str[0] & 0x80) && isalpha(str[0])) {
            cn_obj.first_flag = true;
        }
    }

    // first
    if (true == cn_obj.first_flag) {
        int end_flag = 0;
        for (uint32_t i = 1; i < str_size; i++) {
            if (0 != (str[i] & 0x80)) {
                end_flag = i - 1;
                break;
            }
        }
        for (uint32_t i = end_flag + 1; i < str_size; i++) {
            if (0 == (str[i] & 0x80)) {
                return false;
            }
        }
        cn_obj.cn_str = str.substr(end_flag + 1);

        return true;
    } else {
        uint32_t end_flag = 0;
        for (uint32_t i = 1; i < str_size; i++) {
            if (0 == (str[i] & 0x80)) {
                end_flag = i - 1;
                break;
            }
        }

        for (uint32_t i = end_flag + 1; i < str_size; i++) {
            if (0 != (str[i] & 0x80)) {
                return false;
            }
        }
        cn_obj.cn_str = str.substr(0, end_flag + 1);
        return true;
    }
}

int32_t StringUtil::boldLineStr(
    const vector<string> &segments, const char *src, int32_t src_len, char *dest, int32_t dest_len) {
    if (unlikely(0 == segments.size())) {
        strncpy(dest, src, src_len < dest_len ? src_len : dest_len);
        return src_len < dest_len ? src_len : dest_len;
    }

    bool match = true;
    bool need_end = false;
    bool need_begin = true;
    const char *p = src;
    const char *end = p + src_len;
    int32_t index = 0;

    while (*p) {
        match = false;
        int32_t len = 0;
        if (' ' != *p) {
            int i = 0;
            for (vector<string>::const_iterator it = segments.begin();
                 it != segments.end() && i < AUTIL_CODE_MAX_BOLD_TERMS;
                 ++it, ++i) {
                const string &segment = (*it);
                len = segment.size();
                if (end - p < len)
                    continue;
                if (0 == strncasecmp(p, segment.c_str(), len)) {
                    if (p != src && !need_begin) {
                        need_end = true;
                    }
                    match = true;
                    break;
                }
            }
        } else {
            // 跳过空格
            match = true;
            const char *blank_str = p;
            while (*++blank_str && *blank_str == ' ') {}
            len = blank_str - p;
        }

        if (!match) {
            if (need_begin) {
                SAFE_COPY(dest + index, AUTIL_CODE_BOLD_BEGIN, dest_len - index);
                index += strlen(AUTIL_CODE_BOLD_BEGIN);
                need_begin = false;
                need_end = true;
            }
            dest[index++] = *p++;
        } else {
            if (need_end) {
                SAFE_COPY(dest + index, AUTIL_CODE_BOLD_END, dest_len - index);
                index += strlen(AUTIL_CODE_BOLD_END);
                need_end = false;
                need_begin = true;
            }
            memcpy(dest + index, p, len);
            index += len;
            dest[index] = '\0';
            p += len;
        }
    }
    if (need_end) {
        SAFE_COPY(dest + index, AUTIL_CODE_BOLD_END, dest_len - index);
        index += strlen(AUTIL_CODE_BOLD_END);
    }
    return index;
}

} // namespace codec
} // namespace autil
