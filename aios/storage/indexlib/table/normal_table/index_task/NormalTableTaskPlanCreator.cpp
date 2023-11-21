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
#include "indexlib/table/normal_table/index_task/NormalTableTaskPlanCreator.h"

#include "indexlib/config/TabletOptions.h"
#include "indexlib/table/normal_table/index_task/NormalTableAlterTablePlanCreator.h"
#include "indexlib/table/normal_table/index_task/NormalTableCompactPlanCreator.h"
#include "indexlib/table/normal_table/index_task/NormalTableReclaimPlanCreator.h"
#include "indexlib/table/normal_table/index_task/NormalTableSplitPlanCreator.h"
#include "indexlib/table/normal_table/index_task/NormalTableTaskOperationCreator.h"

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, NormalTableTaskPlanCreator);

NormalTableTaskPlanCreator::NormalTableTaskPlanCreator()
{
    RegisterSimpleCreator<NormalTableSplitPlanCreator>();
    RegisterSimpleCreator<NormalTableCompactPlanCreator>();
    RegisterSimpleCreator<NormalTableAlterTablePlanCreator>();
    RegisterSimpleCreator<NormalTableReclaimPlanCreator>();
}

Status NormalTableTaskPlanCreator::SelectTaskConfigs(const framework::IndexTaskContext* taskContext,
                                                     std::vector<config::IndexTaskConfig>* configs)
{
    *configs = taskContext->GetTabletOptions()->GetAllIndexTaskConfigs();
    RETURN_IF_STATUS_ERROR(NormalTableSplitPlanCreator::AppendDefaultConfigIfNecessary(taskContext, configs),
                           "split plan creator append default task plan failed");
    RETURN_IF_STATUS_ERROR(NormalTableReclaimPlanCreator::AppendDefaultConfigIfNecessary(taskContext, configs),
                           "reclaim plan creator append default task plan failed");
    return Status::OK();
}
NormalTableTaskPlanCreator::~NormalTableTaskPlanCreator() {}

} // namespace indexlibv2::table
