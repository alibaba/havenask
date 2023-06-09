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
#include "indexlib/framework/index_task/IndexTaskPlan.h"

namespace indexlibv2::framework {
AUTIL_LOG_SETUP(indexlib.framework, IndexTaskPlan);

IndexTaskPlan::IndexTaskPlan(const std::string& taskName, const std::string& taskType)
    : _taskName(taskName)
    , _taskType(taskType)
{
}

IndexTaskPlan::~IndexTaskPlan() {}

void IndexTaskPlan::Jsonize(JsonWrapper& json)
{
    json.Jsonize("op_descs", _opDescs, _opDescs);
    json.Jsonize("end_task_op", _endTaskOp, _endTaskOp);
    json.Jsonize("task_name", _taskName, _taskName);
    json.Jsonize("task_type", _taskType, _taskType);
}

} // namespace indexlibv2::framework
