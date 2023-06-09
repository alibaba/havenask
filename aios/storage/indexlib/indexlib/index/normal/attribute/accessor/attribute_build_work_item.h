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

#include "indexlib/common_define.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/config/pack_attribute_config.h"
#include "indexlib/index/normal/util/build_work_item.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(document, NormalDocument);
DECLARE_REFERENCE_CLASS(document, DocumentCollector);
DECLARE_REFERENCE_CLASS(index, InMemoryAttributeSegmentWriter);
DECLARE_REFERENCE_CLASS(index, AttributeModifier);

namespace indexlib { namespace index::legacy {

// User of work item should ensure lifetime of index writer and data out live the work item.
class AttributeBuildWorkItem : public legacy::BuildWorkItem
{
public:
    AttributeBuildWorkItem(const config::AttributeConfig* attributeConfig, AttributeModifier* modifier,
                           InMemoryAttributeSegmentWriter* segmentWriter, bool isSub, docid_t buildingSegmentBaseDocId,
                           const document::DocumentCollectorPtr& docCollector);
    AttributeBuildWorkItem(const config::PackAttributeConfig* packAttributeConfig, AttributeModifier* modifier,
                           InMemoryAttributeSegmentWriter* segmentWriter, bool isSub, docid_t buildingSegmentBaseDocId,
                           const document::DocumentCollectorPtr& docCollector);
    ~AttributeBuildWorkItem() {};

public:
    void doProcess() override;

private:
    inline bool IsPackAttribute() { return _attributeId == INVALID_ATTRID; }
    void BuildOneDoc(DocOperateType opType, const document::NormalDocumentPtr& doc);
    void AddOneDoc(const document::NormalDocumentPtr& doc);
    void UpdateDocInBuiltSegment(const document::NormalDocumentPtr& document);
    void UpdateDocInBuildingSegment(const document::NormalDocumentPtr& document);

private:
    attrid_t _attributeId;
    packattrid_t _packAttributeId;

    InMemoryAttributeSegmentWriter* _segmentWriter;
    AttributeModifier* _modifier;

private:
    IE_LOG_DECLARE();
};

}} // namespace indexlib::index::legacy
