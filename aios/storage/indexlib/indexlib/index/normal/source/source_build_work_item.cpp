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
#include "indexlib/index/normal/source/source_build_work_item.h"

#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/index/normal/source/source_writer.h"

namespace indexlib { namespace index::legacy {
IE_LOG_SETUP(index, SourceBuildWorkItem);

SourceBuildWorkItem::SourceBuildWorkItem(index::SourceWriter* sourceWriter,
                                         const document::DocumentCollectorPtr& docCollector, bool isSub)
    : BuildWorkItem(/*name=*/isSub ? "_SUB_SOURCE_" : "_SOURCE_", BuildWorkItemType::SOURCE, isSub,
                    /*buildingSegmentBaseDocId=*/INVALID_DOCID, docCollector)
    , _sourceWriter(sourceWriter)
{
}

void SourceBuildWorkItem::doProcess()
{
    assert(_docs != nullptr);
    for (const document::DocumentPtr& document : *_docs) {
        assert(document->GetDocOperateType() == ADD_DOC);
        document::NormalDocumentPtr doc = DYNAMIC_POINTER_CAST(document::NormalDocument, document);
        if (_isSub) {
            const document::NormalDocument::DocumentVector& subDocs = doc->GetSubDocuments();
            for (size_t i = 0; i < subDocs.size(); i++) {
                const document::NormalDocumentPtr& subDoc = subDocs[i];
                _sourceWriter->AddDocument(subDoc->GetSourceDocument());
            }
        } else {
            _sourceWriter->AddDocument(doc->GetSourceDocument());
        }
    }
}

}} // namespace indexlib::index::legacy
