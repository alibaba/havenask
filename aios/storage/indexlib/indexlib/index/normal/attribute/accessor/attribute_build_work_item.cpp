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
#include "indexlib/index/normal/attribute/accessor/attribute_build_work_item.h"

#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/index/normal/attribute/accessor/attribute_modifier.h"
#include "indexlib/index/normal/attribute/accessor/in_memory_attribute_segment_writer.h"

namespace indexlib { namespace index::legacy {
IE_LOG_SETUP(index, AttributeBuildWorkItem);

AttributeBuildWorkItem::AttributeBuildWorkItem(const config::AttributeConfig* attributeConfig,
                                               AttributeModifier* modifier,
                                               InMemoryAttributeSegmentWriter* segmentWriter, bool isSub,
                                               docid_t buildingSegmentBaseDocId,
                                               const document::DocumentCollectorPtr& docCollector)
    : BuildWorkItem((isSub ? "_SUB_ATTRIBUTE_" : "_ATTRIBUTE_") + attributeConfig->GetAttrName(),
                    BuildWorkItemType::ATTR, isSub, buildingSegmentBaseDocId, docCollector)
    , _attributeId(attributeConfig->GetAttrId())
    , _packAttributeId(INVALID_PACK_ATTRID)
    , _segmentWriter(segmentWriter)
    , _modifier(modifier)
{
}

AttributeBuildWorkItem::AttributeBuildWorkItem(const config::PackAttributeConfig* packAttributeConfig,
                                               AttributeModifier* modifier,
                                               InMemoryAttributeSegmentWriter* segmentWriter, bool isSub,
                                               docid_t buildingSegmentBaseDocId,
                                               const document::DocumentCollectorPtr& docCollector)
    : BuildWorkItem(packAttributeConfig->GetPackName(), BuildWorkItemType::ATTR, isSub, buildingSegmentBaseDocId,
                    docCollector)
    , _attributeId(INVALID_ATTRID)
    , _packAttributeId(packAttributeConfig->GetPackAttrId())
    , _segmentWriter(segmentWriter)
    , _modifier(modifier)
{
}

void AttributeBuildWorkItem::doProcess()
{
    assert(_docs != nullptr);
    for (const document::DocumentPtr& document : *_docs) {
        document::NormalDocumentPtr doc = DYNAMIC_POINTER_CAST(document::NormalDocument, document);
        DocOperateType opType = doc->GetDocOperateType();
        if (_isSub) {
            for (const document::NormalDocumentPtr& subDoc : doc->GetSubDocuments()) {
                BuildOneDoc(opType, subDoc);
            }
        } else {
            BuildOneDoc(opType, doc);
        }
    }
}

void AttributeBuildWorkItem::BuildOneDoc(DocOperateType opType, const document::NormalDocumentPtr& doc)
{
    if (opType == ADD_DOC) {
        return AddOneDoc(doc);
    } else if (opType == UPDATE_FIELD) {
        if (doc->GetDocId() < _buildingSegmentBaseDocId) {
            return UpdateDocInBuiltSegment(doc);
        } else {
            return UpdateDocInBuildingSegment(doc);
        }
    } // else { do nothing; }
}

void AttributeBuildWorkItem::AddOneDoc(const document::NormalDocumentPtr& doc)
{
    const document::AttributeDocumentPtr& attrDoc = doc->GetAttributeDocument();
    if (IsPackAttribute()) {
        _segmentWriter->AddDocumentPackAttribute(attrDoc, _packAttributeId);
    } else {
        _segmentWriter->AddDocumentAttribute(attrDoc, _attributeId);
    }
}

// Currently for UPDATE_FIELD in attribute work item, each attribute will try to apply the update if available.
void AttributeBuildWorkItem::UpdateDocInBuiltSegment(const document::NormalDocumentPtr& doc)
{
    const document::AttributeDocumentPtr& attrDoc = doc->GetAttributeDocument();
    docid_t docId = doc->GetDocId();
    if (IsPackAttribute()) {
        _modifier->UpdatePackAttribute(docId, attrDoc, _packAttributeId);
    } else {
        _modifier->UpdateAttribute(docId, attrDoc, _attributeId);
    }
}

void AttributeBuildWorkItem::UpdateDocInBuildingSegment(const document::NormalDocumentPtr& doc)
{
    const document::AttributeDocumentPtr& attrDoc = doc->GetAttributeDocument();
    docid_t docId = doc->GetDocId() - _buildingSegmentBaseDocId;
    if (IsPackAttribute()) {
        _segmentWriter->UpdateDocumentPackAttribute(docId, attrDoc, _packAttributeId);
    } else {
        _segmentWriter->UpdateDocumentAttribute(docId, attrDoc, _attributeId);
    }
}

}} // namespace indexlib::index::legacy
