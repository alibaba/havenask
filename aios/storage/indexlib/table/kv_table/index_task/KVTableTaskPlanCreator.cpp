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
#include "indexlib/table/kv_table/index_task/KVTableTaskPlanCreator.h"

#include "indexlib/table/kv_table/index_task/KVTableAlterTablePlanCreator.h"
#include "indexlib/table/kv_table/index_task/KVTableBulkloadPlanCreator.h"
#include "indexlib/table/kv_table/index_task/KVTableMergePlanCreator.h"
#include "indexlib/table/kv_table/index_task/KVTableTaskOperationCreator.h"

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, KVTableTaskPlanCreator);

KVTableTaskPlanCreator::KVTableTaskPlanCreator()
{
    RegisterSimpleCreator<KVTableMergePlanCreator>();
    RegisterSimpleCreator<KVTableAlterTablePlanCreator>();
    RegisterSimpleCreator<KVTableBulkloadPlanCreator>();
}

KVTableTaskPlanCreator::~KVTableTaskPlanCreator() {}

} // namespace indexlibv2::table
