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
#include "indexlib/index/normal/inverted_index/accessor/index_build_work_item.h"

#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/index/inverted_index/InvertedIndexReader.h"
#include "indexlib/index/normal/framework/index_writer.h"
#include "indexlib/index/normal/inverted_index/accessor/in_memory_index_segment_writer.h"
#include "indexlib/index/normal/inverted_index/accessor/inplace_index_modifier.h"

namespace indexlib { namespace index::legacy {
IE_LOG_SETUP(index, IndexBuildWorkItem);

IndexBuildWorkItem::IndexBuildWorkItem(config::IndexConfig* indexConfig, size_t shardId,
                                       index::InplaceIndexModifier* inplaceModifier,
                                       index::InMemoryIndexSegmentWriter* inMemorySegmentWriter, bool isSub,
                                       docid_t buildingSegmentBaseDocId,
                                       const document::DocumentCollectorPtr& docCollector)
    : BuildWorkItem((isSub ? "_SUB_INVERTEDINDEX_" : "_INVERTEDINDEX_") + indexConfig->GetIndexName() + "_@_" +
                        std::to_string(shardId),
                    BuildWorkItemType::INDEX, isSub, buildingSegmentBaseDocId, docCollector)
    , _indexConfig(indexConfig)
    , _indexId(indexConfig->GetIndexId())
    , _shardId(shardId)
    , _inplaceModifier(inplaceModifier)
    , _inMemorySegmentWriter(inMemorySegmentWriter)
{
}

void IndexBuildWorkItem::doProcess()
{
    assert(_docs != nullptr);
    for (const document::DocumentPtr& document : *_docs) {
        document::NormalDocumentPtr doc = std::dynamic_pointer_cast<document::NormalDocument>(document);
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

void IndexBuildWorkItem::BuildOneDoc(DocOperateType opType, const document::NormalDocumentPtr& doc)
{
    if (opType == ADD_DOC) {
        return AddOneDoc(doc);
    } else if (opType == UPDATE_FIELD) {
        if (!_indexConfig->IsIndexUpdatable()) {
            return;
        }
        if (!_indexConfig->GetNonTruncateIndexName().empty()) {
            return;
        }
        // docs in built segment
        if (doc->GetDocId() < _buildingSegmentBaseDocId) {
            return UpdateDocInBuiltSegment(doc);
        }
        return UpdateDocInBuildingSegment(doc);
    }
    assert(opType != UNKNOWN_OP);
}

void IndexBuildWorkItem::AddOneDoc(const document::NormalDocumentPtr& doc)
{
    _inMemorySegmentWriter->AddDocumentByIndex(doc, _indexId, _shardId);
    const document::IndexDocumentPtr& indexDoc = doc->GetIndexDocument();
    _inMemorySegmentWriter->EndDocumentByIndex(indexDoc, _indexId, _shardId);
}

void IndexBuildWorkItem::UpdateDocInBuiltSegment(const document::DocumentPtr& document)
{
    document::NormalDocumentPtr doc = std::dynamic_pointer_cast<document::NormalDocument>(document);
    const document::IndexDocumentPtr& indexDoc = doc->GetIndexDocument();
    docid_t docId = doc->GetDocId();
    _inplaceModifier->UpdateByIndex(docId, indexDoc, _indexId, _shardId);
}

void IndexBuildWorkItem::UpdateDocInBuildingSegment(const document::DocumentPtr& document)
{
    document::NormalDocumentPtr doc = std::dynamic_pointer_cast<document::NormalDocument>(document);
    docid_t docId = doc->GetDocId() - _buildingSegmentBaseDocId;
    _inMemorySegmentWriter->UpdateDocumentByIndex(docId, doc, _indexId, _shardId);
}

}} // namespace indexlib::index::legacy
