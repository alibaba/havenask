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
#include "indexlib/index/normal/inverted_index/builtin_index/pack/pack_index_merger.h"

#include "indexlib/config/package_index_config.h"
#include "indexlib/index/inverted_index/format/PostingFormatOption.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_merger_creator.h"
#include "indexlib/index/normal/inverted_index/builtin_index/pack/on_disk_pack_index_iterator.h"

using namespace std;

using namespace indexlib::config;
using namespace indexlib::util;

using namespace indexlib::index_base;

namespace indexlib { namespace index { namespace legacy {
IE_LOG_SETUP(legacy, PackIndexMerger);

PackIndexMerger::PackIndexMerger() {}

PackIndexMerger::~PackIndexMerger() {}

void PackIndexMerger::Init(const IndexConfigPtr& indexConfig)
{
    NormalIndexMerger::Init(indexConfig);

    PackageIndexConfigPtr packIndexConf = DYNAMIC_POINTER_CAST(PackageIndexConfig, indexConfig);
    assert(packIndexConf);
    mSectionMerger.reset();
    if (packIndexConf->GetShardingType() != IndexConfig::IST_IS_SHARDING && packIndexConf->HasSectionAttribute()) {
        SectionAttributeConfigPtr sectionAttrConf = packIndexConf->GetSectionAttributeConfig();
        assert(sectionAttrConf);
        AttributeConfigPtr attrConfig = sectionAttrConf->CreateAttributeConfig(packIndexConf->GetIndexName());

        VarNumAttributeMergerCreator<char> mergerCreator;
        mSectionMerger.reset(mergerCreator.Create(attrConfig->IsUniqEncode(), false, false));
        mSectionMerger->Init(attrConfig, mMergeHint, mTaskResources);
    }
}

void PackIndexMerger::BeginMerge(const SegmentDirectoryBasePtr& segDir)
{
    NormalIndexMerger::BeginMerge(segDir);
    if (mSectionMerger) {
        mSectionMerger->BeginMerge(segDir);
    }
}

void PackIndexMerger::SortByWeightMerge(const index::MergerResource& resource,
                                        const index_base::SegmentMergeInfos& segMergeInfos,
                                        const index_base::OutputSegmentMergeInfos& outputSegMergeInfos)
{
    NormalIndexMerger::SortByWeightMerge(resource, segMergeInfos, outputSegMergeInfos);
    if (mSectionMerger) {
        // Merge section attribute
        mSectionMerger->SortByWeightMerge(resource, segMergeInfos, outputSegMergeInfos);
    }
}

void PackIndexMerger::Merge(const index::MergerResource& resource, const index_base::SegmentMergeInfos& segMergeInfos,
                            const index_base::OutputSegmentMergeInfos& outputSegMergeInfos)
{
    NormalIndexMerger::Merge(resource, segMergeInfos, outputSegMergeInfos);
    if (mSectionMerger) {
        // Merge section attribute
        mSectionMerger->Merge(resource, segMergeInfos, outputSegMergeInfos);
    }
}

std::shared_ptr<OnDiskIndexIteratorCreator> PackIndexMerger::CreateOnDiskIndexIteratorCreator()
{
    return std::shared_ptr<OnDiskIndexIteratorCreator>(
        new OnDiskPackIndexIterator::Creator(GetPostingFormatOption(), mIOConfig, mIndexConfig));
}

int64_t PackIndexMerger::EstimateMemoryUse(const SegmentDirectoryBasePtr& segDir, const MergerResource& resource,
                                           const index_base::SegmentMergeInfos& segMergeInfos,
                                           const index_base::OutputSegmentMergeInfos& outputSegMergeInfos,
                                           bool isSortedMerge) const
{
    int64_t estMemUse =
        NormalIndexMerger::EstimateMemoryUse(segDir, resource, segMergeInfos, outputSegMergeInfos, isSortedMerge);
    if (mSectionMerger) {
        estMemUse =
            max(mSectionMerger->EstimateMemoryUse(segDir, resource, segMergeInfos, outputSegMergeInfos, isSortedMerge),
                estMemUse);
    }
    return estMemUse;
}
}}} // namespace indexlib::index::legacy
