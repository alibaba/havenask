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

#include <stddef.h>
#include <vector>

#include "indexlib/index_base/index_meta/segment_info.h"
#include "indexlib/misc/common.h"
#include "indexlib/misc/log.h"
#include "indexlib/table/segment_meta.h"
#include "indexlib/table/table_merge_plan.h"

namespace indexlib { namespace table {

class TableMergePlanResource
{
public:
    TableMergePlanResource();
    virtual ~TableMergePlanResource();

public:
    virtual bool Init(const TableMergePlanPtr& mergePlan, const std::vector<SegmentMetaPtr>& segmentMetas) = 0;
    virtual bool Store(const file_system::DirectoryPtr& root) const = 0;
    virtual bool Load(const file_system::DirectoryPtr& root) = 0;

    virtual size_t EstimateMemoryUse() const = 0;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TableMergePlanResource);
}} // namespace indexlib::table
