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

#include <algorithm>
#include <stddef.h>
#include <vector>

#include "indexlib/merger/merge_plan.h"
#include "indexlib/misc/common.h"
#include "indexlib/misc/log.h"

namespace indexlib { namespace merger {

class MergeTask
{
public:
    MergeTask();
    ~MergeTask();

public:
    void AddPlan(const MergePlan& item) { mMergePlans.push_back(item); }

    const MergePlan& operator[](size_t index) const { return mMergePlans[index]; }

    MergePlan& operator[](size_t index) { return mMergePlans[index]; }

    size_t GetPlanCount() const { return mMergePlans.size(); }

private:
    std::vector<MergePlan> mMergePlans;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MergeTask);
}} // namespace indexlib::merger
