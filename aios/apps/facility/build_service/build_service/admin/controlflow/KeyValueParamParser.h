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
#ifndef ISEARCH_BS_KEYVALUEPARAMPARSER_H
#define ISEARCH_BS_KEYVALUEPARAMPARSER_H

#include "autil/StringUtil.h"
#include "build_service/admin/controlflow/Eluna.h"
#include "build_service/common_define.h"
#include "build_service/util/Log.h"

namespace build_service { namespace admin {

class KeyValueParamParser
{
public:
    KeyValueParamParser() {}
    ~KeyValueParamParser() {}

public:
    bool fromJsonString(const std::string& str);
    std::string toJsonString();

    bool fromKVPairString(const std::string& str, const std::string& kvDelim = "=", const std::string& pairDelim = ";");
    std::string toKVPairString(const std::string& kvDelim = "=", const std::string& pairDelim = ";");

    void setValue(const std::string& key, const std::string& value);
    bool getValue(const std::string& key, std::string& value);

    size_t getKeySize();
    bool getKey(size_t idx, std::string& key);

    // return "" if not exist
    const std::string& getValue(const std::string& key) const;

    template <typename T>
    void setTypedValue(const std::string& key, T value)
    {
        setValue(key, autil::StringUtil::toString(value));
    }

    template <typename T>
    bool getTypedValue(const std::string& key, T& value)
    {
        std::string strValue;
        if (!getValue(key, strValue)) {
            return false;
        }
        return autil::StringUtil::fromString(strValue, value);
    }

public:
    static bool parseFromJsonString(const char* str, KeyValueMap& kvMap);
    static bool parseFromKVPairString(const char* str, KeyValueMap& kvMap, const std::string& kvDelim = "=",
                                      const std::string& pairDelim = ";");

    static std::string toKVPairString(const KeyValueMap& kvMap, const std::string& kvDelim = "=",
                                      const std::string& pairDelim = ";");

private:
    KeyValueMap _kvMap;
    std::vector<std::string> _keyVector;

private:
    BS_LOG_DECLARE();
};

///////////////////////////////////////////////////////////////////////////
class KeyValueParamWrapper
{
public:
    KeyValueParamWrapper() {}

public:
    bool fromJsonString(const char* str);
    const char* toJsonString();

    bool fromKVPairString(const char* str, const char* kvDelim, const char* pairDelim);
    const char* toKVPairString(const char* kvDelim, const char* pairDelim);

    void setValue(const char* key, const char* value);
    const char* getValue(const char* key);
    int64_t getIntValue(const char* key, int64_t defaultValue);
    uint64_t getUIntValue(const char* key, uint64_t defaultValue);

    int getKeySize();
    const char* getKey(int idx);

public:
    static void bindLua(lua_State* state);

private:
    KeyValueParamParser _parser;
    std::string _kvStr;
};

}} // namespace build_service::admin

#endif // ISEARCH_BS_KEYVALUEPARAMPARSER_H
