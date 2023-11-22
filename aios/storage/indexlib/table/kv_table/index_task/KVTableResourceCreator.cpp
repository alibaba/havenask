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
#include "indexlib/table/kv_table/index_task/KVTableResourceCreator.h"

#include "indexlib/table/index_task/IndexTaskConstant.h"
#include "indexlib/table/index_task/VersionResource.h"
#include "indexlib/table/index_task/merger/MergePlan.h"
#include "indexlib/table/kv_table/index_task/KVTableBulkloadSegmentCreatePlan.h"

namespace indexlibv2::table {

std::unique_ptr<framework::IndexTaskResource>
KVTableResourceCreator::CreateResource(const std::string& name, const framework::IndexTaskResourceType& type)
{
    if (type == MERGE_PLAN) {
        return std::make_unique<MergePlan>(name, type);
    } else if (type == VERSION_RESOURCE) {
        return std::make_unique<VersionResource>(name, type);
    } else if (type == BULKLOAD_SEGMENT_CREATE_PLAN) {
        return std::make_unique<KVTableBulkloadSegmentCreatePlan>(name, type);
    } else {
        return nullptr;
    }
}

} // namespace indexlibv2::table
