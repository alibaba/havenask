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
#include "matchdoc/toolkit/Schema.h"

#include <map>
#include <memory>
#include <stddef.h>
#include <utility>

#include "alog/Logger.h"
#include "autil/StringUtil.h"
#include "autil/legacy/exception.h"

namespace matchdoc {

using namespace std;
using namespace autil;
using namespace matchdoc;

AUTIL_LOG_SETUP(turing, Doc);

void Field::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    json.Jsonize("field_name", _name);
    if (_name.empty()) {
        throw autil::legacy::ExceptionBase("field name is empty");
    }

    string type;
    if (Mode::TO_JSON == json.GetMode()) {
        type = toString(_type);
    }
    json.Jsonize("field_type", type);
    if (Mode::FROM_JSON == json.GetMode()) {
        _type = fromString(type);
    }
    if (_type == bt_unknown) {
        throw autil::legacy::ExceptionBase(string("unknown built-in type: ") + type);
    }

    json.Jsonize("multi_value", _isMulti, false);
    if (_isMulti) {
        map<string, string> userDefinedParam = {{"user_defined_param", _multiValSep}};
        json.Jsonize("user_defined_param", userDefinedParam, {});
        if (Mode::FROM_JSON == json.GetMode()) {
            auto fileDataIter = userDefinedParam.find("seperator");
            if (fileDataIter != userDefinedParam.end()) {
                _multiValSep = fileDataIter->second;
            }
            if (_multiValSep.empty()) {
                _multiValSep = kMultiValSep;
            }
        }
    }
    json.Jsonize("field_vals", _fieldVals, {});
}

void Doc::Jsonize(autil::legacy::Jsonizable::JsonWrapper &json) {
    json.Jsonize("name", _name, std::string());
    if (_name.empty()) {
        throw autil::legacy::ExceptionBase("name is empty in the schema");
    }
    json.Jsonize("fields", _fields, {});
    json.Jsonize("docs", _docVals, {});
    json.Jsonize("field_seperator", _fieldValSep, kFieldValSep);
    for (const auto &field : _fields) {
        if (field._isMulti && field._multiValSep == _fieldValSep) {
            AUTIL_LOG(ERROR,
                      "multi-value separator[%s] in field[%s] same with field value separator[%s]",
                      field._multiValSep.c_str(),
                      field._name.c_str(),
                      _fieldValSep.c_str());
            throw autil::legacy::ExceptionBase("conflicts between fieldValSep and multiValSep");
        }
    }
    for (size_t i = 0; i < _docVals.size(); ++i) {
        auto fieldVals = StringUtil::split(_docVals[i], _fieldValSep);
        if (fieldVals.size() != _fields.size()) {
            AUTIL_LOG(ERROR, "docVal size:[%zu] != field size:[%zu]", _docVals.size(), _fields.size());
            throw autil::legacy::ExceptionBase();
        }
        for (size_t j = 0; j < fieldVals.size(); ++j) {
            StringUtil::trim(fieldVals[j]);
            _fields[j]._fieldVals.push_back(fieldVals[j]);
        }
    }
}
} // namespace matchdoc
