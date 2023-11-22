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
#include <queue>

#include "autil/ConstString.h"
#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/index/normal/attribute/accessor/attribute_patch_iterator.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class AttributePatchIteratorComparator
{
public:
    bool operator()(AttributePatchIterator*& lft, AttributePatchIterator*& rht) const
    {
        assert(lft);
        assert(rht);
        return (*rht) < (*lft);
    }
};

class MultiFieldPatchIterator : public PatchIterator
{
public:
    MultiFieldPatchIterator(const config::IndexPartitionSchemaPtr& schema);
    ~MultiFieldPatchIterator();

public:
    void Init(const index_base::PartitionDataPtr& partitionData, bool ignorePatchToOldIncSegment,
              const index_base::Version& lastLoadVersion, segmentid_t startLoadSegment = INVALID_SEGMENTID,
              bool isSub = false);

    void Next(AttrFieldValue& value) override;
    void Reserve(AttrFieldValue& value) override;
    bool HasNext() const override { return !mHeap.empty(); }
    size_t GetPatchLoadExpandSize() const override { return mPatchLoadExpandSize; };

    docid_t GetCurrentDocId() const
    {
        if (!HasNext()) {
            return INVALID_DOCID;
        }
        AttributePatchIterator* patchIter = mHeap.top();
        assert(patchIter);
        return patchIter->GetCurrentDocId();
    }

    void CreateIndependentPatchWorkItems(std::vector<PatchWorkItem*>& workItems) override;

private:
    AttributePatchIterator* CreateSingleFieldPatchIterator(const index_base::PartitionDataPtr& partitionData,
                                                           bool ignorePatchToOldIncSegment,
                                                           segmentid_t startLoadSegment,
                                                           const config::AttributeConfigPtr& attrConf);

    AttributePatchIterator* CreatePackFieldPatchIterator(const index_base::PartitionDataPtr& partitionData,
                                                         bool ignorePatchToOldIncSegment,
                                                         const index_base::Version& lastLoadVersion,
                                                         segmentid_t startLoadSegment,
                                                         const config::PackAttributeConfigPtr& packAttrConf);

private:
    typedef std::priority_queue<AttributePatchIterator*, std::vector<AttributePatchIterator*>,
                                AttributePatchIteratorComparator>
        AttributePatchReaderHeap;

private:
    config::IndexPartitionSchemaPtr mSchema;
    AttributePatchReaderHeap mHeap;
    docid_t mCurDocId;
    bool mIsSub;
    size_t mPatchLoadExpandSize;

private:
    friend class MultiFieldPatchIteratorTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(MultiFieldPatchIterator);
}} // namespace indexlib::index
