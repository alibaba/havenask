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
#include "build_service/admin/controlflow/KeyValueParamParser.h"

#include "autil/legacy/jsonizable.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;

namespace build_service { namespace admin {

BS_LOG_SETUP(admin, KeyValueParamParser);

bool KeyValueParamParser::fromJsonString(const string& str) { return parseFromJsonString(str.c_str(), _kvMap); }

string KeyValueParamParser::toJsonString() { return ToJsonString(_kvMap, true); }

bool KeyValueParamParser::fromKVPairString(const string& str, const string& kvDelim, const string& pairDelim)
{
    return parseFromKVPairString(str.c_str(), _kvMap, kvDelim, pairDelim);
}

string KeyValueParamParser::toKVPairString(const string& kvDelim, const string& pairDelim)
{
    return toKVPairString(_kvMap, kvDelim, pairDelim);
}

void KeyValueParamParser::setValue(const string& key, const string& value) { _kvMap[key] = value; }

bool KeyValueParamParser::getValue(const string& key, string& value)
{
    KeyValueMap::const_iterator iter = _kvMap.find(key);
    if (iter != _kvMap.end()) {
        value = iter->second;
        return true;
    }
    return false;
}

const string& KeyValueParamParser::getValue(const string& key) const
{
    KeyValueMap::const_iterator iter = _kvMap.find(key);
    if (iter != _kvMap.end()) {
        return iter->second;
    }
    static string emptyStr;
    return emptyStr;
}

bool KeyValueParamParser::parseFromJsonString(const char* str, KeyValueMap& kvMap)
{
    kvMap.clear();
    if (!str) {
        return true;
    }

    try {
        autil::legacy::FromJsonString(kvMap, str);
    } catch (autil::legacy::ExceptionBase& e) {
        BS_LOG(ERROR, "FromJsonString [%s] catch exception: [%s]", str, e.ToString().c_str());
        return false;
    } catch (...) {
        BS_LOG(ERROR, "exception happen when call FromJsonString [%s] ", str);
        return false;
    }
    return true;
}

bool KeyValueParamParser::parseFromKVPairString(const char* str, KeyValueMap& kvMap, const string& kvDelim,
                                                const string& pairDelim)
{
    kvMap.clear();
    if (!str) {
        return true;
    }

    vector<vector<string>> tmpInfoVec;
    StringUtil::fromString(str, tmpInfoVec, kvDelim, pairDelim);
    for (size_t i = 0; i < tmpInfoVec.size(); i++) {
        if (tmpInfoVec[i].size() != 2) {
            BS_LOG(ERROR, "string [%s] not valid kv pair format!", str);
            return false;
        }
        kvMap[tmpInfoVec[i][0]] = tmpInfoVec[i][1];
    }
    return true;
}

string KeyValueParamParser::toKVPairString(const KeyValueMap& kvMap, const string& kvDelim, const string& pairDelim)
{
    vector<vector<string>> tmpVec;
    tmpVec.reserve(kvMap.size());
    for (auto& item : kvMap) {
        vector<string> tmp;
        tmp.resize(2);
        tmp[0] = item.first;
        tmp[1] = item.second;
        tmpVec.push_back(tmp);
    }
    return StringUtil::toString(tmpVec, kvDelim, pairDelim);
}

size_t KeyValueParamParser::getKeySize()
{
    if (_keyVector.size() == _kvMap.size()) {
        return _keyVector.size();
    }
    _keyVector.clear();
    _keyVector.reserve(_kvMap.size());
    for (auto& item : _kvMap) {
        _keyVector.push_back(item.first);
    }
    return _keyVector.size();
}

bool KeyValueParamParser::getKey(size_t idx, string& key)
{
    size_t count = getKeySize();
    if (idx >= count) {
        return false;
    }
    key = _keyVector[idx];
    return true;
}

///////////////////////////////////////////////////////////////////////////////////////////////////

bool KeyValueParamWrapper::fromJsonString(const char* str) { return _parser.fromJsonString(str); }

const char* KeyValueParamWrapper::toJsonString()
{
    _kvStr = _parser.toJsonString();
    return _kvStr.c_str();
}

void KeyValueParamWrapper::setValue(const char* key, const char* value) { _parser.setValue(key, value); }

const char* KeyValueParamWrapper::getValue(const char* key) { return _parser.getValue(key).c_str(); }

int64_t KeyValueParamWrapper::getIntValue(const char* key, int64_t defaultValue)
{
    int64_t value = defaultValue;
    _parser.getTypedValue(key, value);
    return value;
}

uint64_t KeyValueParamWrapper::getUIntValue(const char* key, uint64_t defaultValue)
{
    uint64_t value = defaultValue;
    _parser.getTypedValue(key, value);
    return value;
}

bool KeyValueParamWrapper::fromKVPairString(const char* str, const char* kvDelim, const char* pairDelim)
{
    return _parser.fromKVPairString(str, kvDelim, pairDelim);
}

const char* KeyValueParamWrapper::toKVPairString(const char* kvDelim, const char* pairDelim)
{
    _kvStr = _parser.toKVPairString(kvDelim, pairDelim);
    return _kvStr.c_str();
}

int KeyValueParamWrapper::getKeySize() { return (int)_parser.getKeySize(); }

const char* KeyValueParamWrapper::getKey(int idx)
{
    if (_parser.getKey(idx, _kvStr)) {
        return _kvStr.c_str();
    }
    return "";
}

void KeyValueParamWrapper::bindLua(lua_State* state)
{
    assert(state);
    ELuna::registerClass<KeyValueParamWrapper>(state, "KVParamUtil", ELuna::constructor<KeyValueParamWrapper>);
    ELuna::registerMethod<KeyValueParamWrapper>(state, "fromJsonString", &KeyValueParamWrapper::fromJsonString);
    ELuna::registerMethod<KeyValueParamWrapper>(state, "toJsonString", &KeyValueParamWrapper::toJsonString);
    ELuna::registerMethod<KeyValueParamWrapper>(state, "fromKVPairString", &KeyValueParamWrapper::fromKVPairString);
    ELuna::registerMethod<KeyValueParamWrapper>(state, "toKVPairString", &KeyValueParamWrapper::toKVPairString);
    ELuna::registerMethod<KeyValueParamWrapper>(state, "setValue", &KeyValueParamWrapper::setValue);
    ELuna::registerMethod<KeyValueParamWrapper>(state, "getValue", &KeyValueParamWrapper::getValue);
    ELuna::registerMethod<KeyValueParamWrapper>(state, "getIntValue", &KeyValueParamWrapper::getIntValue);
    ELuna::registerMethod<KeyValueParamWrapper>(state, "getUIntValue", &KeyValueParamWrapper::getUIntValue);
    ELuna::registerMethod<KeyValueParamWrapper>(state, "getKeySize", &KeyValueParamWrapper::getKeySize);
    ELuna::registerMethod<KeyValueParamWrapper>(state, "getKey", &KeyValueParamWrapper::getKey);
}

}} // namespace build_service::admin
