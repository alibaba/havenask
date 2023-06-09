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
#include "build_service/proto/TaskIdentifier.h"

#include "autil/StringUtil.h"

using namespace std;
using namespace autil;

namespace build_service { namespace proto {
BS_LOG_SETUP(proto, TaskIdentifier);

const std::string TaskIdentifier::FIELD_SEPARATOR = "-";
const std::string TaskIdentifier::FIELDNAME_CLUSTERNAME = "clusterName";
const std::string TaskIdentifier::FIELDNAME_TASKNAME = "taskName";
const std::string TaskIdentifier::FIELDNAME_SEPARATOR = "=";

TaskIdentifier::TaskIdentifier() {}

TaskIdentifier::~TaskIdentifier() {}

void TaskIdentifier::setValue(const string& key, const string& value) { _kvMap[key] = value; }

bool TaskIdentifier::getValue(const string& key, string& value) const
{
    KeyValueMap::const_iterator iter = _kvMap.find(key);
    if (iter != _kvMap.end()) {
        value = iter->second;
        return true;
    }
    return false;
}

bool TaskIdentifier::fromString(const string& content)
{
    vector<string> strVec = StringUtil::split(content, FIELD_SEPARATOR, false);
    if (strVec.empty()) {
        return false;
    }
    for (size_t i = 0; i < strVec.size(); i++) {
        vector<string> kvStr = StringUtil::split(strVec[i], FIELDNAME_SEPARATOR, false);
        if (kvStr.size() != 2) {
            return false;
        }
        if (i == 0) {
            if (kvStr[0] != "taskId") {
                return false;
            }
            _taskId = kvStr[1];
            continue;
        }
        _kvMap[kvStr[0]] = kvStr[1];
    }
    return true;
}

string TaskIdentifier::toString() const
{
    vector<string> strInfos;
    strInfos.reserve(_kvMap.size() + 1);
    strInfos.push_back(string("taskId") + FIELDNAME_SEPARATOR + _taskId);
    for (auto& kv : _kvMap) {
        strInfos.push_back(kv.first + FIELDNAME_SEPARATOR + kv.second);
    }
    return StringUtil::toString(strInfos, FIELD_SEPARATOR);
}

}} // namespace build_service::proto
