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
#include "indexlib/config/SortDescription.h"
#include "indexlib/index/merger_util/reclaim_map/reclaim_map_creator.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(index, ReclaimMap);
DECLARE_REFERENCE_CLASS(index::legacy, MultiComparator);
DECLARE_REFERENCE_CLASS(index::legacy, DocInfoAllocator);
DECLARE_REFERENCE_CLASS(index::legacy, MultiAttributeEvaluator);
DECLARE_REFERENCE_CLASS(index, OfflineAttributeSegmentReaderContainer);

namespace indexlib { namespace index {

class SortedReclaimMapCreator : public ReclaimMapCreator
{
public:
    SortedReclaimMapCreator(bool hasTruncate, const OfflineAttributeSegmentReaderContainerPtr& attrReaderContainer,
                            const config::IndexPartitionSchemaPtr& schema,
                            const indexlibv2::config::SortDescriptions& sortDescs);
    ~SortedReclaimMapCreator();

public:
    ReclaimMapPtr Create(const index_base::SegmentMergeInfos& segMergeInfos, const DeletionMapReaderPtr& delMapReader,
                         const SegmentSplitHandler& segmentSplitHandler) const override;

private:
    ReclaimMapPtr SortByWeightInit(const index_base::SegmentMergeInfos& segMergeInfos,
                                   const DeletionMapReaderPtr& delMapReader,
                                   const std::vector<config::AttributeConfigPtr>& attrConfigs,
                                   const std::vector<indexlibv2::config::SortPattern>& sortPatterns,
                                   const SegmentSplitHandler& segmentSplitHandler) const;
    void InitMultiComparator(const std::vector<config::AttributeConfigPtr>& attrConfigs,
                             const std::vector<indexlibv2::config::SortPattern>& sortPatterns,
                             const legacy::MultiComparatorPtr& multiComparator,
                             const legacy::DocInfoAllocatorPtr& docInfoAllocator) const;
    void InitMultiAttrEvaluator(const index_base::SegmentMergeInfo& segMergeInfo,
                                const std::vector<config::AttributeConfigPtr>& attrConfigs,
                                const legacy::DocInfoAllocatorPtr& docInfoAllocator,
                                const legacy::MultiAttributeEvaluatorPtr& multiEvaluatorPtr) const;

private:
    OfflineAttributeSegmentReaderContainerPtr mAttrReaderContainer;
    const config::IndexPartitionSchemaPtr mSchema;
    const indexlibv2::config::SortDescriptions mSortDescs;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SortedReclaimMapCreator);
}} // namespace indexlib::index
