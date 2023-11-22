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
#include "build_service/admin/controlflow/ListParamParser.h"

#include <assert.h>
#include <cstddef>

#include "alog/Logger.h"
#include "autil/StringUtil.h"
#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "build_service/admin/controlflow/Eluna.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;

namespace build_service { namespace admin {
BS_LOG_SETUP(admin, ListParamParser);

ListParamParser::ListParamParser() {}

ListParamParser::~ListParamParser() {}

bool ListParamParser::fromJsonString(const string& str) { return parseFromJsonString(str.c_str(), _list); }

string ListParamParser::toJsonString() { return ToJsonString(_list, true); }

bool ListParamParser::fromListString(const string& str, const string& delim)
{
    return parseFromListString(str.c_str(), _list, delim);
}

string ListParamParser::toListString(const string& delim) { return toListString(_list, delim); }

void ListParamParser::pushBack(const string& value) { _list.push_back(value); }

bool ListParamParser::getValue(size_t idx, string& value)
{
    if (idx >= _list.size()) {
        return false;
    }
    value = _list[idx];
    return true;
}

size_t ListParamParser::getSize() const { return _list.size(); }

bool ListParamParser::parseFromJsonString(const char* str, vector<string>& list)
{
    list.clear();
    if (!str) {
        return true;
    }

    try {
        autil::legacy::FromJsonString(list, str);
    } catch (autil::legacy::ExceptionBase& e) {
        BS_LOG(ERROR, "FromJsonString [%s] catch exception: [%s]", str, e.ToString().c_str());
        return false;
    } catch (...) {
        BS_LOG(ERROR, "exception happen when call FromJsonString [%s] ", str);
        return false;
    }
    return true;
}

bool ListParamParser::parseFromListString(const char* str, vector<string>& list, const string& delim)
{
    list.clear();
    if (!str) {
        return true;
    }

    StringUtil::fromString(str, list, delim);
    return true;
}

string ListParamParser::toListString(const vector<string>& list, const string& delim)
{
    return StringUtil::toString(list, delim);
}

bool ListParamWrapper::fromJsonString(const char* str) { return _parser.fromJsonString(str); }

const char* ListParamWrapper::toJsonString()
{
    _str = _parser.toJsonString();
    return _str.c_str();
}

bool ListParamWrapper::fromListString(const char* str, const char* delim) { return _parser.fromListString(str, delim); }

const char* ListParamWrapper::toListString(const char* delim)
{
    _str = _parser.toListString(delim);
    return _str.c_str();
}

void ListParamWrapper::pushBack(const char* value) { _parser.pushBack(value); }

const char* ListParamWrapper::getValue(int idx)
{
    if (!_parser.getValue(idx, _str)) {
        return "";
    }
    return _str.c_str();
}

int ListParamWrapper::getSize() { return _parser.getSize(); }

void ListParamWrapper::bindLua(lua_State* state)
{
    assert(state);
    ELuna::registerClass<ListParamWrapper>(state, "ListParamUtil", ELuna::constructor<ListParamWrapper>);
    ELuna::registerMethod<ListParamWrapper>(state, "fromJsonString", &ListParamWrapper::fromJsonString);
    ELuna::registerMethod<ListParamWrapper>(state, "toJsonString", &ListParamWrapper::toJsonString);
    ELuna::registerMethod<ListParamWrapper>(state, "fromListString", &ListParamWrapper::fromListString);
    ELuna::registerMethod<ListParamWrapper>(state, "toListString", &ListParamWrapper::toListString);
    ELuna::registerMethod<ListParamWrapper>(state, "pushBack", &ListParamWrapper::pushBack);
    ELuna::registerMethod<ListParamWrapper>(state, "getValue", &ListParamWrapper::getValue);
    ELuna::registerMethod<ListParamWrapper>(state, "getSize", &ListParamWrapper::getSize);
}

}} // namespace build_service::admin
