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
#include "indexlib/table/aggregation/index_task/AggregationResourceCreator.h"

#include "indexlib/table/index_task/IndexTaskConstant.h"
#include "indexlib/table/index_task/merger/MergePlan.h"

namespace indexlibv2::table {

AUTIL_LOG_SETUP(indexlib.table, AggregationResourceCreator);

std::unique_ptr<framework::IndexTaskResource>
AggregationResourceCreator::CreateResource(const std::string& name, const framework::IndexTaskResourceType& type)
{
    if (type == MERGE_PLAN) {
        return std::make_unique<MergePlan>(name, type);
    } else {
        return nullptr;
    }
}

} // namespace indexlibv2::table
