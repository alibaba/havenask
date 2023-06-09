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
#include "indexlib/table/aggregation/index_task/AggregationTaskPlanCreator.h"

#include "indexlib/table/aggregation/index_task/AggregationTaskOperationCreator.h"
#include "indexlib/table/kv_table/index_task/KVTableMergePlanCreator.h"

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, AggregationTaskPlanCreator);

AggregationTaskPlanCreator::AggregationTaskPlanCreator() { RegisterSimpleCreator<KVTableMergePlanCreator>(); }

AggregationTaskPlanCreator::~AggregationTaskPlanCreator() = default;

std::shared_ptr<framework::IIndexOperationCreator>
AggregationTaskPlanCreator::CreateIndexOperationCreator(const std::shared_ptr<config::TabletSchema>& tabletSchema)
{
    return std::make_shared<AggregationTaskOperationCreator>(tabletSchema);
}

} // namespace indexlibv2::table
