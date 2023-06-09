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

#include <limits>
#include <memory>

#include "autil/Lock.h"
#include "autil/TimeUtility.h"
#include "autil/WorkItem.h"
#include "indexlib/common_define.h"
#include "indexlib/config/field_config.h"
#include "indexlib/config/index_config.h"
#include "indexlib/index/normal/util/build_work_item.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(document, Document);
DECLARE_REFERENCE_CLASS(document, DocumentCollector);
DECLARE_REFERENCE_CLASS(document, NormalDocument);
DECLARE_REFERENCE_CLASS(index, InplaceIndexModifier);
DECLARE_REFERENCE_CLASS(index, InMemoryIndexSegmentWriter);

namespace indexlib { namespace index::legacy {

// User of work item should ensure lifetime of index writer and data out live the work item.
// Each IndexBuildWorkItem maps to one index writer in InMemoryIndexSegmentWriter, index id or one index config.
// Each IndexBuildWorkItem maps to one index reader in InplaceIndexModifier, index id or one index config.
class IndexBuildWorkItem : public legacy::BuildWorkItem
{
public:
    // For multi-shard work item, indexConfig should be of type IndexConfig::ISI_NEED_SHARDING.
    IndexBuildWorkItem(config::IndexConfig* indexConfig, size_t shardId, index::InplaceIndexModifier* inplaceModifier,
                       index::InMemoryIndexSegmentWriter* inMemorySegmentWriter, bool isSub,
                       docid_t buildingSegmentBaseDocId, const document::DocumentCollectorPtr& docCollector);
    ~IndexBuildWorkItem() {};

public:
    void doProcess() override;

private:
    void BuildOneDoc(DocOperateType opType, const document::NormalDocumentPtr& doc);
    void AddOneDoc(const document::NormalDocumentPtr& doc);
    void UpdateDocInBuiltSegment(const document::DocumentPtr& document);
    void UpdateDocInBuildingSegment(const document::DocumentPtr& document);

private:
    config::IndexConfig* _indexConfig;
    indexid_t _indexId;
    size_t _shardId;

    // Update in built segment(code path: InplaceModifier->InplaceIndexModifier->InvertedIndexReader(e.g. normal index
    // reader))
    index::InplaceIndexModifier* _inplaceModifier;
    // Add, update in building segment
    index::InMemoryIndexSegmentWriter* _inMemorySegmentWriter;

private:
    IE_LOG_DECLARE();
};

}} // namespace indexlib::index::legacy
