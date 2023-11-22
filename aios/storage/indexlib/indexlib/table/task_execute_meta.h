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
#pragma once

#include <stdint.h>
#include <string>
#include <vector>

#include "autil/legacy/jsonizable.h"
#include "indexlib/misc/common.h"

namespace indexlib { namespace table {

class TaskExecuteMeta : public autil::legacy::Jsonizable
{
public:
    TaskExecuteMeta(uint32_t _planIdx, uint32_t _taskIdx, uint32_t _cost)
        : planIdx(_planIdx)
        , taskIdx(_taskIdx)
        , cost(_cost)
    {
    }
    TaskExecuteMeta() : planIdx(0), taskIdx(0), cost(0) {}
    ~TaskExecuteMeta() {}

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    bool operator==(const TaskExecuteMeta& other) const;

    std::string GetIdentifier() const;
    static bool GetIdxFromIdentifier(const std::string& taskName, uint32_t& planIdx, uint32_t& taskIdx);

private:
    static std::string TASK_IDENTIFY_PREFIX;

public:
    uint32_t planIdx;
    uint32_t taskIdx;
    uint32_t cost;
};
typedef std::vector<TaskExecuteMeta> TaskExecuteMetas;

DEFINE_SHARED_PTR(TaskExecuteMeta);
}} // namespace indexlib::table
