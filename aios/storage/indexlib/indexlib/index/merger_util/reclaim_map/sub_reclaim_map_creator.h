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
#ifndef __INDEXLIB_SUB_RECLAIM_MAP_CREATOR_H
#define __INDEXLIB_SUB_RECLAIM_MAP_CREATOR_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/field_type_traits.h"
#include "indexlib/config/merge_config.h"
#include "indexlib/index/merger_util/reclaim_map/reclaim_map_creator.h"
#include "indexlib/index/normal/attribute/accessor/single_value_attribute_segment_reader.h"
#include "indexlib/index_base/index_meta/segment_merge_info.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(index, DeletionMapReader);
DECLARE_REFERENCE_CLASS(index, AttributeSegmentReader);
DECLARE_REFERENCE_CLASS(index, ReclaimMap);
DECLARE_REFERENCE_CLASS(index, SegmentDirectoryBase);
DECLARE_REFERENCE_CLASS(config, AttributeConfig);

namespace indexlib { namespace index {

class SubReclaimMapCreator : public ReclaimMapCreator
{
public:
    using ItemType = config::FieldTypeTraits<ft_int32>::AttrItemType;
    using SegmentReader = SingleValueAttributeSegmentReader<ItemType>;
    using SegmentReaderPtr = std::shared_ptr<SegmentReader>;
    using SegmentReaderPair = std::pair<SegmentReaderPtr, SegmentReader::ReadContext>;

private:
    class State : public ReclaimMapCreator::State
    {
    public:
        State(uint32_t maxDocCount, uint32_t mergedDocCount, ReclaimMap* mainReclaimMap,
              const std::vector<int>& segIdxMap, std::vector<SegmentReaderPair>& mainJoinAttrSegReaders);
        ~State();

    public:
        void ReclaimOneDoc(segmentid_t segId, docid_t baseDocId, docid_t localId, docid_t globalMainDocId,
                           const DeletionMapReaderPtr& deletionMapReader);
        ReclaimMapPtr ConstructSubReclaimMap(const index_base::SegmentMergeInfos& segMergeInfos,
                                             bool needReserveMapping);

    private:
        void GetMainJoinValues(SegmentReaderPair& segReaderPair, docid_t localDocId, docid_t& lastDocJoinValue,
                               docid_t& curDocJoinValue) const;

    private:
        std::vector<docid_t> mJoinValueIdArray;
        std::vector<docid_t> mMainJoinValues;
        ReclaimMap* mMainReclaimMap;
        const std::vector<int>& mSegIdxMap;
        std::vector<SegmentReaderPair>& mMainJoinAttrSegReaders;
        uint32_t mMergedDocCount;
    };

public:
    SubReclaimMapCreator(bool hasTruncate, const config::AttributeConfigPtr& mainJoinAttrConfig);
    virtual ~SubReclaimMapCreator();

public:
    ReclaimMapPtr CreateSubReclaimMap(ReclaimMap* mainReclaimMap,
                                      const index_base::SegmentMergeInfos& mainSegMergeInfos,
                                      const index_base::SegmentMergeInfos& segMergeInfos,
                                      const SegmentDirectoryBasePtr& segDir,
                                      const DeletionMapReaderPtr& delMapReader) const;

private:
    void CheckMainAndSubMergeInfos(const index_base::SegmentMergeInfos& mainSegMergeInfos,
                                   const index_base::SegmentMergeInfos& subSegMergeInfos) const;
    std::vector<int32_t> InitSegmentIdxs(const index_base::SegmentMergeInfos& mainSegMergeInfos) const;
    std::vector<SegmentReaderPair>
    InitJoinAttributeSegmentReaders(const index_base::SegmentMergeInfos& mainSegMergeInfos,
                                    const SegmentDirectoryBasePtr& segDir, bool multiTargetSegment) const;
    static bool MultiTargetSegment(const ReclaimMap* mainReclaimMap)
    {
        return mainReclaimMap->GetTargetSegmentCount() > 1u;
    }

private:
    ReclaimMapPtr ReclaimJoinedSubDocs(const index_base::SegmentMergeInfos& subSegMergeInfos,
                                       ReclaimMap* mainReclaimMap, const std::vector<int>& segIdxMap,
                                       std::vector<SegmentReaderPair>& mainJoinAttrSegReaders,
                                       const DeletionMapReaderPtr& subDeletionMapReader) const;

private:
    config::AttributeConfigPtr mMainJoinAttrConfig;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ReclaimMapCreator);
}} // namespace indexlib::index

#endif //__INDEXLIB_SUB_RECLAIM_MAP_CREATOR_H
