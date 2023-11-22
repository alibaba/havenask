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
#include "build_service/config/CheckpointList.h"

#include <ostream>

#include "alog/Logger.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable.h"

using namespace std;

namespace build_service { namespace config {
BS_LOG_SETUP(config, CheckpointList);

void CheckpointList::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("checkpoint_list", _idSet, _idSet);
}

bool CheckpointList::loadFromString(const string& content)
{
    try {
        FromJsonString(*this, content);
    } catch (const autil::legacy::ExceptionBase& e) {
        stringstream ss;
        ss << "jsonize CheckpointList from [" << content << "] failed, exception[" << string(e.what()) << "]";
        BS_LOG(ERROR, "%s", ss.str().c_str());
        return false;
    }
    return true;
}

bool CheckpointList::remove(const versionid_t& versionId)
{
    auto it = _idSet.find(versionId);
    if (it == _idSet.end()) {
        return false;
    }
    _idSet.erase(it);
    return true;
}

}} // namespace build_service::config
