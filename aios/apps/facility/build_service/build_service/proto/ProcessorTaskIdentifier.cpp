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
#include "build_service/proto/ProcessorTaskIdentifier.h"

#include "autil/StringUtil.h"

using namespace std;
using namespace autil;

namespace build_service { namespace proto {
BS_LOG_SETUP(proto, ProcessorTaskIdentifier);

ProcessorTaskIdentifier::ProcessorTaskIdentifier() : _dataDescriptionId(0) { _taskId = "0"; }

ProcessorTaskIdentifier::~ProcessorTaskIdentifier() {}

bool ProcessorTaskIdentifier::fromString(const string& content)
{
    vector<string> strVec = StringUtil::split(content, FIELD_SEPARATOR, false);
    if (strVec.empty()) {
        return false;
    }
    if (!StringUtil::fromString(strVec[0], _dataDescriptionId)) {
        return false;
    }
    for (size_t i = 1; i < strVec.size(); i++) {
        vector<string> kvStr = StringUtil::split(strVec[i], FIELDNAME_SEPARATOR, false);
        if (kvStr.size() == 1 && i == 1) {
            int32_t taskId = 0;
            if (!StringUtil::fromString(kvStr[0], taskId)) {
                return false;
            }
            _taskId = StringUtil::toString(taskId);
            continue;
        }

        if (kvStr.size() != 2) {
            return false;
        }
        _kvMap[kvStr[0]] = kvStr[1];
    }
    return true;
}

string ProcessorTaskIdentifier::toString() const
{
    vector<string> strInfos;
    strInfos.push_back(StringUtil::toString(_dataDescriptionId));

    if (_taskId != "0" && !_taskId.empty()) {
        strInfos.push_back(_taskId);
    }
    for (auto& kv : _kvMap) {
        strInfos.push_back(kv.first + FIELDNAME_SEPARATOR + kv.second);
    }
    return StringUtil::toString(strInfos, FIELD_SEPARATOR);
}

}} // namespace build_service::proto
