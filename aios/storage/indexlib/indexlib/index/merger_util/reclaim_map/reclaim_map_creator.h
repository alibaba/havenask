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
#include "indexlib/config/merge_config.h"
#include "indexlib/index/util/reclaim_map.h"
#include "indexlib/index_base/index_meta/segment_merge_info.h"
#include "indexlib/index_base/segment_mapper.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(index, DeletionMapReader);
DECLARE_REFERENCE_CLASS(index, OfflineAttributeSegmentReaderContainer);

namespace indexlib { namespace index {

class ReclaimMapCreator
{
public:
    typedef std::function<segmentindex_t(segmentid_t, docid_t)> SegmentSplitHandler;

private:
    typedef std::pair<segmentid_t, docid_t> GlobalId;
    typedef util::VectorTyped<GlobalId> GlobalIdVector;
    typedef std::shared_ptr<GlobalIdVector> GlobalIdVectorPtr;
    typedef std::vector<docid_t> DocOrder;
    typedef std::shared_ptr<DocOrder> DocOrderPtr;

public: // public for SubReclaimMapCreator
    class State
    {
    public:
        State(const SegmentSplitHandler& segmentSplitHandler, uint32_t maxDocCount);
        State(bool multiTargetSegment, uint32_t maxDocCount); // for sub
        ~State();

    public:
        void ReclaimOneDoc(segmentid_t segId, docid_t baseDocId, docid_t localId,
                           const DeletionMapReaderPtr& deletionMapReader);
        ReclaimMapPtr ConstructReclaimMap(const index_base::SegmentMergeInfos& segMergeInfos, bool needReserveMapping,
                                          const DeletionMapReaderPtr& deletionMapReader, bool isSort);

    protected:
        void RewriteDocIdArray(const index_base::SegmentMergeInfos& segMergeInfos,
                               const index_base::SegmentMapper& segMapper);
        void MakeTargetBaseDocIds(const index_base::SegmentMapper& segMapper, segmentindex_t maxSegIdx);
        bool NeedSplitSegment() const { return mSegmentSplitHandler.operator bool(); }

        void ConstructDocOrder(const index_base::SegmentMergeInfos& segMergeInfos, ReclaimMapPtr& reclaimMap,
                               const index_base::SegmentMapper& mapper, const DeletionMapReaderPtr& deletionMapReader);

    protected:
        SegmentSplitHandler mSegmentSplitHandler;

        index_base::SegmentMapper mSegMapper;

        // may use reclaim_map directly.
        uint32_t mDeletedDocCount;
        docid_t mNewDocId;
        std::vector<docid_t> mDocIdArray;
        std::vector<docid_t> mTargetBaseDocIds;
        DocOrderPtr mDocIdOrder;
    };

public:
    ReclaimMapCreator(bool hasTruncate);
    virtual ~ReclaimMapCreator();

public:
    virtual ReclaimMapPtr Create(const index_base::SegmentMergeInfos& segMergeInfos,
                                 const DeletionMapReaderPtr& delMapReader,
                                 const SegmentSplitHandler& segmentSplitHandler) const;

public:
    // simplify test
    static ReclaimMapPtr Create(bool hasTruncate, const index_base::SegmentMergeInfos& segMergeInfos,
                                const DeletionMapReaderPtr& delMapReader,
                                const SegmentSplitHandler& segmentSplitHandler = nullptr)
    {
        ReclaimMapCreator creator(hasTruncate);
        return creator.Create(segMergeInfos, delMapReader, segmentSplitHandler);
    }

protected:
    bool NeedReverseMapping() const { return mHasTruncate; }

protected:
    bool mHasTruncate;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ReclaimMapCreator);
}} // namespace indexlib::index
