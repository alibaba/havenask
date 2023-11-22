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

#include "autil/ThreadPool.h"
#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/index/normal/util/patch_work_item.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/modifier/partition_modifier.h"

DECLARE_REFERENCE_CLASS(index, PatchIterator);
DECLARE_REFERENCE_CLASS(index, AttributeReader);
DECLARE_REFERENCE_CLASS(index, IndexPatchIterator);
DECLARE_REFERENCE_CLASS(index_base, PartitionData);
DECLARE_REFERENCE_CLASS(index_base, Version);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(config, AttributeConfig);
DECLARE_REFERENCE_CLASS(tools, PatchTools);

namespace indexlib { namespace partition {

class PatchLoader
{
public:
    PatchLoader(const config::IndexPartitionSchemaPtr& schema, const config::OnlineConfig& onlineConfig);
    ~PatchLoader();

public:
    void Init(const index_base::PartitionDataPtr& partitionData, bool ignorePatchToOldIncSegment,
              const index_base::Version& lastLoadedVersion, segmentid_t startLoadSegment, bool loadPatchForOpen);

    void Load(const index_base::Version& patchLoadedVersion, const PartitionModifierPtr& modifier);

    size_t CalculatePatchLoadExpandSize();

    // only for test
    static void LoadAttributePatch(index::AttributeReader& attrReader, const config::AttributeConfigPtr& attrConfig,
                                   const index_base::PartitionDataPtr& partitionData);

private:
    bool NeedLoadPatch(const index_base::Version& currentVersion, const index_base::Version& loadedVersion) const;
    static bool HasUpdatableAttribute(const config::IndexPartitionSchemaPtr& schema);
    static bool HasUpdatableIndex(const config::IndexPartitionSchemaPtr& schema);

    std::pair<size_t, size_t> SingleThreadLoadPatch(const PartitionModifierPtr& modifier);

    std::pair<size_t, size_t> MultiThreadLoadPatch(const PartitionModifierPtr& modifier);
    bool InitPatchWorkItems(const PartitionModifierPtr& modifier, std::vector<index::PatchWorkItem*>* patchWorkItems);
    std::pair<size_t, size_t> EstimatePatchWorkItemCost(std::vector<index::PatchWorkItem*>* patchWorkItems,
                                                        const autil::ThreadPoolPtr& threadPool);
    void SortPatchWorkItemsByCost(std::vector<index::PatchWorkItem*>* patchWorkItems);

private:
    static int64_t PATCH_WORK_ITEM_COST_SAMPLE_COUNT;

private:
    index_base::PartitionDataPtr _partitionData;
    config::IndexPartitionSchemaPtr _schema;
    config::OnlineConfig _onlineConfig;
    index::PatchIteratorPtr _attrPatchIter;
    index::IndexPatchIteratorPtr _indexPatchIter;
    std::vector<index::PatchWorkItem*> _patchWorkItems;

private:
    friend class indexlib::tools::PatchTools;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PatchLoader);
}} // namespace indexlib::partition
