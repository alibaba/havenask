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
#include "indexlib/config/pack_attribute_config.h"
#include "indexlib/index_base/index_meta/segment_merge_info.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(index_base, PartitionData);
DECLARE_REFERENCE_CLASS(file_system, Directory);
namespace indexlib { namespace index {

class DedupPackAttributePatchFileMerger
{
public:
    DedupPackAttributePatchFileMerger(const config::PackAttributeConfigPtr& packAttrConfig)
        : mPackAttrConfig(packAttrConfig)
    {
    }

    ~DedupPackAttributePatchFileMerger() {}

public:
    void Merge(const index_base::PartitionDataPtr& partitionData, const index_base::SegmentMergeInfos& segMergeInfos,
               const file_system::DirectoryPtr& targetAttrDir);

    uint32_t EstimateMemoryUse(const index_base::PartitionDataPtr& partitionData,
                               const index_base::SegmentMergeInfos& segMergeInfos);

private:
    config::PackAttributeConfigPtr mPackAttrConfig;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DedupPackAttributePatchFileMerger);
}} // namespace indexlib::index
