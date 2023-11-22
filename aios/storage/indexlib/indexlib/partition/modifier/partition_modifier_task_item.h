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
#include "indexlib/index/normal/util/segment_modifier_container.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(index, AttributeUpdateBitmap);
DECLARE_REFERENCE_CLASS(index, BuiltAttributeSegmentModifier);
DECLARE_REFERENCE_CLASS(partition, PatchModifier);
DECLARE_REFERENCE_CLASS(partition, PartitionModifier);
DECLARE_REFERENCE_CLASS(util, BuildResourceMetrics);

namespace indexlib { namespace partition {

enum class PartitionModifierDumpTaskType {
    InvalidModifierTask,
    InplaceModifierTask,
    SubDocModifierTask,
    PatchModifierTask,
};

class PartitionModifierDumpTaskItem
{
public:
    PartitionModifierDumpTaskItem() = default;
    ~PartitionModifierDumpTaskItem() = default;

public:
    void Dump(const config::IndexPartitionSchemaPtr& schema, const file_system::DirectoryPtr& directory,
              segmentid_t srcSegmentId, uint32_t threadCnt);

public:
    using PackAttrUpdateBitmapVec = std::vector<index::AttributeUpdateBitmapPtr>;

    bool isOnline = true;
    PartitionModifierDumpTaskType taskType = PartitionModifierDumpTaskType::InvalidModifierTask;
    util::BuildResourceMetricsPtr buildResourceMetrics;
    // if type is SubDocModifierTask, sub modifier must dump first.
    std::vector<PackAttrUpdateBitmapVec> packAttrUpdataBitmapItems; // used for inplace modifier
    std::vector<PartitionModifierPtr> patchModifiers;               // used for patch modifier

    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PartitionModifierDumpTaskItem);
}} // namespace indexlib::partition
