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

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/primary_key_index_config.h"
#include "indexlib/index/normal/primarykey/primary_key_load_plan.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class PrimaryKeyLoadStrategy
{
public:
    PrimaryKeyLoadStrategy() {}
    virtual ~PrimaryKeyLoadStrategy() {}

public:
    virtual void CreatePrimaryKeyLoadPlans(const index_base::PartitionDataPtr& partitionData,
                                           PrimaryKeyLoadPlanVector& plans) = 0;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PrimaryKeyLoadStrategy);
}} // namespace indexlib::index
