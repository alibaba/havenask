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
#include "indexlib/index/normal/inverted_index/accessor/index_modifier.h"
#include "indexlib/index/normal/inverted_index/accessor/index_segment_modifier.h"
#include "indexlib/index/normal/util/segment_modifier_container.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(util, AccumulativeCounter);

namespace indexlib { namespace index {

class PatchIndexModifier : public IndexModifier
{
private:
    typedef std::vector<util::AccumulativeCounterPtr> IndexCounterVector;

public:
    PatchIndexModifier(const config::IndexPartitionSchemaPtr& schema, util::BuildResourceMetrics* buildResourceMetrics,
                       bool isOffline = false);
    ~PatchIndexModifier();

public:
    void Init(const index_base::PartitionDataPtr& partitionData);
    void Update(docid_t docId, const document::IndexDocumentPtr& indexDoc) override;
    bool UpdateField(docid_t docId, const document::ModifiedTokens& modifiedTokens) override;
    bool UpdateIndex(IndexUpdateTermIterator* iterator) override
    {
        assert(false);
        return false;
    }

    void Dump(const file_system::DirectoryPtr& dir, segmentid_t srcSegment = INVALID_SEGMENTID,
              uint32_t dumpThreadNum = 1);

    bool IsDirty() const { return _segmentModifierContainer.IsDirty(); }

private:
    void InitCounters(const util::CounterMapPtr& counterMap);

private:
    SegmentModifierContainer<IndexSegmentModifierPtr> _segmentModifierContainer;
    IndexCounterVector _indexIdToCounters;
    bool _isOffline;
    bool _enableCounters;

private:
    friend class PatchIndexModifierTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PatchIndexModifier);
}} // namespace indexlib::index
