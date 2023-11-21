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

#include "autil/mem_pool/Pool.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/attribute_modifier.h"
#include "indexlib/index/normal/util/segment_modifier_container.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(util, CounterMap);
DECLARE_REFERENCE_CLASS(util, AccumulativeCounter);
DECLARE_REFERENCE_CLASS(index, BuiltAttributeSegmentModifier);

namespace indexlib { namespace index {

class PatchAttributeModifier : public AttributeModifier
{
private:
    typedef std::vector<util::AccumulativeCounterPtr> AttrCounterVector;
    typedef std::map<segmentid_t, BuiltAttributeSegmentModifierPtr> BuiltSegmentModifierMap;

public:
    PatchAttributeModifier(const config::IndexPartitionSchemaPtr& schema,
                           util::BuildResourceMetrics* buildResourceMetrics, bool isOffline = false);
    ~PatchAttributeModifier();

public:
    void Init(const index_base::PartitionDataPtr& partitionData);
    bool Update(docid_t docId, const document::AttributeDocumentPtr& attrDoc) override;
    bool UpdateField(docid_t docId, fieldid_t fieldId, const autil::StringView& value, bool isNull) override;

    void Dump(const file_system::DirectoryPtr& dir, segmentid_t srcSegment = INVALID_SEGMENTID,
              uint32_t dumpThreadNum = 1);

    bool IsDirty() const { return mSegmentModifierContainer.IsDirty(); }
    void GetPatchedSegmentIds(std::vector<segmentid_t>* patchSegIds) const
    {
        mSegmentModifierContainer.GetPatchedSegmentIds(patchSegIds);
    }
    const BuiltAttributeSegmentModifierPtr& GetSegmentModifier(docid_t globalId, docid_t& localId)
    {
        return mSegmentModifierContainer.GetSegmentModifier(globalId, localId);
    }
    const SegmentBaseDocIdInfo& GetSegmentBaseInfo(docid_t globalId) const
    {
        return mSegmentModifierContainer.GetSegmentBaseInfo(globalId);
    }

    SegmentModifierContainer<BuiltAttributeSegmentModifierPtr> GetSegmentModifierContainer() const
    {
        return mSegmentModifierContainer;
    }

private:
    void InitCounters(const util::CounterMapPtr& counterMap);

private:
    SegmentModifierContainer<BuiltAttributeSegmentModifierPtr> mSegmentModifierContainer;
    AttrCounterVector mAttrIdToCounters;
    bool mIsOffline;
    bool mEnableCounters;

private:
    friend class PatchAttributeModifierTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PatchAttributeModifier);
}} // namespace indexlib::index
