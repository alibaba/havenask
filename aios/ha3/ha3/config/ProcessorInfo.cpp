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
#include "ha3/config/ProcessorInfo.h"

#include <iosfwd>
#include <map>
#include <utility>

#include "alog/Logger.h"
#include "autil/legacy/jsonizable.h"

#include "ha3/isearch.h"
#include "autil/Log.h"

using namespace std;
namespace isearch {
namespace config {
AUTIL_LOG_SETUP(ha3, ProcessorInfo);

ProcessorInfo::ProcessorInfo() {
}

ProcessorInfo::ProcessorInfo(string processorName, string moduleName)
{
    _processorName = processorName;
    _moduleName = moduleName;
}

ProcessorInfo::~ProcessorInfo() {
}

string ProcessorInfo::getProcessorName() const {
    return _processorName;
}

void ProcessorInfo::setProcessorName(const string &processorName) {
    _processorName = processorName;
}

string ProcessorInfo::getModuleName() const {
    return _moduleName;
}

void ProcessorInfo::setModuleName(const string &moduleName) {
    _moduleName = moduleName;
}

string ProcessorInfo::getParam(const string &key) const {
    KeyValueMap::const_iterator it = _params.find(key);
    if (it != _params.end()) {
        return it->second;
    } else {
        return "";
    }
}

void ProcessorInfo::addParam(const string &key, const string &value) {
    KeyValueMap::const_iterator it = _params.find(key);
    if (it == _params.end()) {
        _params.insert(make_pair(key, value));
    } else {
        AUTIL_LOG(WARN, "already exist this key[%s], old_value[%s], insert_value[%s]",
            key.c_str(), it->second.c_str(), value.c_str());
    }
}

const KeyValueMap& ProcessorInfo::getParams() const {
    return _params;
}

void ProcessorInfo::setParams(const KeyValueMap &params) {
    _params = params;
}

void ProcessorInfo::Jsonize(JsonWrapper& json) {
    json.Jsonize("module_name", _moduleName, _moduleName);
    json.Jsonize("parameters", _params, _params);
    json.Jsonize("processor_name", _processorName, _processorName);
    if (json.GetMode() == FROM_JSON) {
        // compatible with document processor
        string className;
        json.Jsonize("class_name", className, std::string());
        if (!className.empty()) {
            _processorName = className;
        }
    }
}

bool ProcessorInfo::operator==(const ProcessorInfo &other) const {
    if (&other == this) {
        return true;
    }
    return _processorName == other._processorName
        && _moduleName == other._moduleName
        && _params == other._params;
}

} // namespace config
} // namespace isearch
