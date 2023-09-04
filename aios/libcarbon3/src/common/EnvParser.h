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
#ifndef COMMON_ENV_PARSER_H
#define COMMON_ENV_PARSER_H

#include "common/common.h"
#include "autil/StringUtil.h"
#include "autil/EnvUtil.h"

BEGIN_CARBON_NAMESPACE(common);

struct KVObject
{
    virtual bool fromString(const std::map<std::string, std::string>& kvs) = 0;

    std::string val(const std::map<std::string, std::string>& kvs, const std::string& k) {
        auto it = kvs.find(k);
        return it == kvs.end() ? std::string() : it->second;
    }
};

struct ListObject
{
    virtual bool fromString(const std::vector<std::string>& list) = 0;

    std::string val(const std::vector<std::string>& list, size_t idx) {
        return list.size() > idx ? list.at(idx) : std::string();
    }
};

inline KVObject& operator>> (std::stringstream& ss, KVObject& def) {
    std::string s = ss.str();
    if (s.empty()) {
        return def;
    }
    std::string sep = "=";
    if (!isalnum(s[0])) { 
        // for these platforms like AOP which parse env confs error, i have to support custom seperator.
        sep[0] = s[0];
        s.erase(s.begin());
    }
    auto vec = autil::StringUtil::split(ss.str(), ";");
    std::map<std::string, std::string> kvs;
    for (const auto& s : vec) {
        auto kv = autil::StringUtil::split(s, sep);
        if (kv.size() >= 2) {
            kvs[kv[0]] = autil::StringUtil::toString(std::vector<std::string>(kv.begin() + 1, kv.end()), sep);
        }
    }
    def.fromString(kvs);
    return def;
}

inline ListObject& operator>> (std::stringstream& ss, ListObject& def) {
    auto vec = autil::StringUtil::split(ss.str(), ","); // `,` to be compatible with old carbon config
    def.fromString(vec);
    return def;
}

class EnvParser
{
public:
    template <typename T>
    static T get(const char* k, T def = T());

    template <typename T>
    static T &getRef(const char* k, T& def);
};

template <typename T>
T EnvParser::get(const char* k, T def) {
    std::string v = autil::EnvUtil::getEnv(k);
    if (v.empty()) {
        return def;
    }
    if (!autil::StringUtil::fromString(v, def)) {
        return def;
    }
    return def;
}

template <typename T>
T& EnvParser::getRef(const char* k, T& def) {
    return get<T&>(k, def);
}

#define FROM_STRING(kvs, k, var) autil::StringUtil::fromString(val(kvs, k), var)
#define FROM_STRING_IDX(list, idx, var) autil::StringUtil::fromString(val(list, idx), var)

END_CARBON_NAMESPACE(common);

#endif
