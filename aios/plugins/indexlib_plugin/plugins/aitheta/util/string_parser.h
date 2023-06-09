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
#ifndef INDEXLIB_PLUGIN_PLUGINS_UTIL_STRING_PARSER_H
#define INDEXLIB_PLUGIN_PLUGINS_UTIL_STRING_PARSER_H

#include "indexlib_plugin/plugins/aitheta/common_define.h"

namespace indexlib {
namespace aitheta_plugin {

class StringParser {
 public:
    // position range is [src, tail)
    // declare parameter with string& to speed up parse
    // as parser temporarily changes val of src
    static bool Parse(std::string& src, size_t front, size_t tail, char sep, uint32_t dim, EmbeddingPtr& emb);

    template <typename T>
    static bool Parse(std::string& src, size_t front, size_t tail, char sep, std::vector<T>& res);

    template <typename T>
    static bool Parse(std::string& src, size_t front, size_t tail, T& res);

 private:
    template <typename T>
    static bool FromString(const char* src, T& res);

 private:
    IE_LOG_DECLARE();
};

template <typename T>
inline bool StringParser::FromString(const char* src, T& res) {
    return false;
}

template <>
inline bool StringParser::FromString(const char* src, int8_t& res) {
    bool ret = autil::StringUtil::strToInt8(src, res);
    return ret;
}

template <>
inline bool StringParser::FromString(const char* src, uint8_t& res) {
    bool ret = autil::StringUtil::strToUInt8(src, res);
    return ret;
}

template <>
inline bool StringParser::FromString(const char* src, int16_t& res) {
    bool ret = autil::StringUtil::strToInt16(src, res);
    return ret;
}

template <>
inline bool StringParser::FromString(const char* src, uint16_t& res) {
    bool ret = autil::StringUtil::strToUInt16(src, res);
    return ret;
}

template <>
inline bool StringParser::FromString(const char* src, int32_t& res) {
    bool ret = autil::StringUtil::strToInt32(src, res);
    return ret;
}

template <>
inline bool StringParser::FromString(const char* src, uint32_t& res) {
    bool ret = autil::StringUtil::strToUInt32(src, res);
    return ret;
}

template <>
inline bool StringParser::FromString(const char* src, int64_t& res) {
    bool ret = autil::StringUtil::strToInt64(src, res);
    return ret;
}

template <>
inline bool StringParser::FromString(const char* src, uint64_t& res) {
    bool ret = autil::StringUtil::strToUInt64(src, res);
    return ret;
}

template <>
inline bool StringParser::FromString(const char* src, float& res) {
    bool ret = autil::StringUtil::strToFloat(src, res);
    return ret;
}

template <>
inline bool StringParser::FromString(const char* src, double& res) {
    bool ret = autil::StringUtil::strToDouble(src, res);
    return ret;
}

template <>
inline bool StringParser::FromString(const char* src, bool& res) {
    // TODO(richard.sy)
    return autil::StringUtil::parseTrueFalse(std::string(src), res);
}

template <>
inline bool StringParser::FromString(const char* src, std::string& res) {
    res = std::string(src);
    return true;
}

template <typename T>
bool StringParser::Parse(std::string& src, size_t front, size_t tail, T& res) {
    const char* fp = const_cast<const char*>(src.data() + front);
    char* const tp = const_cast<char* const>(src.data() + tail);

    char bk = *tp;
    *tp = '\0';
    if (!FromString(fp, res)) {
        IE_LOG(DEBUG, "failed to parse with str[%s]", fp);
        *tp = bk;
        return false;
    }
    *tp = bk;
    return true;
}

template <typename T>
bool StringParser::Parse(std::string& src, size_t front, size_t tail, char sep, std::vector<T>& res) {
    assert(tail >= front);
    assert(tail <= src.size());

    char* const fp = const_cast<char* const>(src.data() + front);
    char* const tp = const_cast<char* const>(src.data() + tail);

    // efp -> element front pointer
    for (char *next = fp, *efp = fp; next <= tp;++next) {
        if (*next == sep || next == tp) {
            T t;
            char bk = *next;
            *next = '\0';
            if (!FromString(efp, t)) {
                IE_LOG(DEBUG, "failed to parse with str[%s]", efp);
                *next = bk;
                return false;
            }
            *next = bk;

            res.push_back(std::move(t));
            efp = next + 1;
        }
    }
    return true;
}

inline bool StringParser::Parse(std::string& src, size_t front, size_t tail, char sep, uint32_t dim,
                                EmbeddingPtr& emb) {
    assert(tail >= front);
    assert(tail <= src.size());

    MALLOC_CHECK(emb, dim, float);

    char* const fp = const_cast<char* const>(src.data() + front);
    char* const tp = const_cast<char* const>(src.data() + tail);

    uint32_t elemNum = 0u;
    for (char *next = fp, *efp = fp; next <= tp; ++next) {
        if (*next == sep || next == tp) {
            float val = 0.0f;
            char bk = *next;
            *next = '\0';
            if (!FromString(efp, val)) {
                IE_LOG(DEBUG, "failed to parse embedding with str[%s]", efp);
                *next = bk;
                emb.reset();
                return false;
            }
            *next = bk;

            if (elemNum >= dim) {
                IE_LOG(DEBUG, "elem num larger than[%u], str[%lu]", dim, front);
                emb.reset();
                return false;
            }
            (emb.get())[elemNum++] = val;
            efp = next + 1;
        }
    }
    if (elemNum != dim) {
        IE_LOG(DEBUG, "elem num smaller than[%u], str[%lu]", dim, front);
        emb.reset();
        return false;
    }
    return true;
}

}
}

#endif  // INDEXLIB_PLUGIN_PLUGINS_UTIL_SEARCH_UTIL_H
