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
#include "indexlib/index/normal/attribute/accessor/attribute_patch_iterator.h"
#include "indexlib/index/normal/attribute/accessor/attribute_patch_reader.h"
#include "indexlib/index_base/patch/patch_file_finder.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class SingleFieldPatchIterator : public AttributePatchIterator
{
public:
    typedef std::pair<docid_t, AttributeSegmentPatchIteratorPtr> SegmentPatchIteratorInfo;

public:
    SingleFieldPatchIterator(const config::AttributeConfigPtr& attrConfig, bool isSub);

    ~SingleFieldPatchIterator();

public:
    void Init(const index_base::PartitionDataPtr& partitionData, bool ignorePatchToOldIncSegment,
              segmentid_t startLoadSegment);
    void Next(AttrFieldValue& value) override;
    void Reserve(AttrFieldValue& value) override;
    size_t GetPatchLoadExpandSize() const override;

    fieldid_t GetFieldId() const { return mAttrIdentifier; }

    attrid_t GetAttributeId() const;

    void CreateIndependentPatchWorkItems(std::vector<PatchWorkItem*>& workItems) override;

    size_t GetPatchItemCount() const { return mPatchItemCount; }

private:
    AttributeSegmentPatchIteratorPtr CreateSegmentPatchIterator(const index_base::PatchFileInfoVec& patchInfoVec);

    docid_t CalculateGlobalDocId() const;

    index_base::PatchFileInfoVec FilterLoadedPatchFileInfos(index_base::PatchFileInfoVec& patchInfosVec,
                                                            segmentid_t lastLoadedSegment);

    docid_t GetNextDocId() const
    {
        return mCursor < mSegmentPatchIteratorInfos.size() ? CalculateGlobalDocId() : INVALID_DOCID;
    }

private:
    std::vector<SegmentPatchIteratorInfo> mSegmentPatchIteratorInfos;

    config::AttributeConfigPtr mAttrConfig;
    size_t mCursor;
    size_t mPatchLoadExpandSize;
    size_t mPatchItemCount;

private:
    friend class SingleFieldPatchIteratorTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SingleFieldPatchIterator);
}} // namespace indexlib::index
