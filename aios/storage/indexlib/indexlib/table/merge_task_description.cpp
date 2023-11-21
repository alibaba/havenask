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
#include "indexlib/table/merge_task_description.h"

#include <cstdint>
#include <iosfwd>
#include <map>

using namespace std;

namespace indexlib { namespace table {
IE_LOG_SETUP(table, MergeTaskDescription);

std::string MergeTaskDescription::MERGE_TASK_NAME_CONFIG = "name";
std::string MergeTaskDescription::MERGE_TASK_COST_CONFIG = "cost";
std::string MergeTaskDescription::MERGE_TASK_PARAM_CONFIG = "parameters";

MergeTaskDescription::MergeTaskDescription() : name(""), cost(0) {}

MergeTaskDescription::~MergeTaskDescription() {}

void MergeTaskDescription::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    json.Jsonize(MERGE_TASK_NAME_CONFIG, name);
    json.Jsonize(MERGE_TASK_COST_CONFIG, cost);
    json.Jsonize(MERGE_TASK_PARAM_CONFIG, parameters);
}
}} // namespace indexlib::table
