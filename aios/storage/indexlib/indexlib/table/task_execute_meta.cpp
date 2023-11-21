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
#include "indexlib/table/task_execute_meta.h"

#include <cstddef>
#include <cstdint>

#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"
#include "autil/legacy/exception.h"

using namespace std;
using namespace autil;

namespace indexlib { namespace table {

std::string TaskExecuteMeta::TASK_IDENTIFY_PREFIX = "__merge_task_";

void TaskExecuteMeta::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize("planIdx", planIdx);
    json.Jsonize("taskIdx", taskIdx);
    json.Jsonize("cost", cost);
}

bool TaskExecuteMeta::operator==(const TaskExecuteMeta& other) const
{
    return planIdx == other.planIdx && taskIdx == other.taskIdx && cost == other.cost;
}

string TaskExecuteMeta::GetIdentifier() const
{
    return "__merge_task_" + autil::StringUtil::toString(planIdx) + "#" + autil::StringUtil::toString(taskIdx);
}

bool TaskExecuteMeta::GetIdxFromIdentifier(const std::string& taskName, uint32_t& planIdx, uint32_t& taskIdx)
{
    size_t posBegin = taskName.find(TASK_IDENTIFY_PREFIX);
    if (posBegin == string::npos) {
        return false;
    }
    StringTokenizer st(taskName.substr(posBegin + TASK_IDENTIFY_PREFIX.size()), "#",
                       StringTokenizer::TOKEN_TRIM | StringTokenizer::TOKEN_IGNORE_EMPTY);

    if (st.getNumTokens() != 2) {
        return false;
    }
    if (!StringUtil::fromString(st[0], planIdx)) {
        return false;
    }
    if (!StringUtil::fromString(st[1], taskIdx)) {
        return false;
    }
    return true;
}
}} // namespace indexlib::table
