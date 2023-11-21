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
#include "indexlib/file_system/LifecycleTable.h"

#include <iosfwd>
#include <utility>

#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"

using namespace std;
using namespace autil;

namespace indexlib { namespace file_system {
AUTIL_LOG_SETUP(indexlib.file_system, LifecycleTable);

const string LifecycleTable::SEGMENT_DIR_MAP = "segment_dir_map";

string LifecycleTable::GetLifecycle(const std::string& path) const
{
    static const string EMPTY_STRING;
    if (_lifecycleMap.empty()) {
        return EMPTY_STRING;
    }
    auto iter = _lifecycleMap.upper_bound(path);
    if (iter == _lifecycleMap.begin()) {
        return EMPTY_STRING;
    }
    --iter;
    if (StringUtil::startsWith(path, iter->first)) {
        return iter->second;
    }
    return EMPTY_STRING;
}

// this lifecycle table only store segment level path and it can be change
bool LifecycleTable::InnerAdd(const std::string& path, const std::string& lifecycle)
{
    _lifecycleMap[path] = lifecycle;
    return true;
}

bool LifecycleTable::IsEmpty() const
{
    auto iter = _lifecycleMap.begin();
    while (iter != _lifecycleMap.end()) {
        if (!iter->second.empty()) {
            return false;
        }
        iter++;
    }
    return true;
}

void LifecycleTable::Jsonize(JsonWrapper& json) { json.Jsonize(SEGMENT_DIR_MAP, _lifecycleMap, _lifecycleMap); }

bool LifecycleTable::operator==(const LifecycleTable& other) const
{
    if (_lifecycleMap.size() != other._lifecycleMap.size()) {
        return false;
    }
    auto lhs = Begin();
    auto rhs = other.Begin();

    while (lhs != End()) {
        if (*lhs != *rhs) {
            return false;
        }
        lhs++;
        rhs++;
    }
    return true;
}

}} // namespace indexlib::file_system
