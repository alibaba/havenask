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
#include "indexlib/document/normal/rewriter/NormalDocumentRewriterBase.h"

#include "indexlib/document/DocumentIterator.h"
#include "indexlib/document/IDocumentBatch.h"
#include "indexlib/document/normal/NormalDocument.h"

namespace indexlibv2::document {
AUTIL_LOG_SETUP(indexlib.document, NormalDocumentRewriterBase);

Status NormalDocumentRewriterBase::Rewrite(IDocumentBatch* batch)
{
    auto iter = indexlibv2::document::DocumentIterator<indexlibv2::document::IDocument>::Create(batch);
    while (iter->HasNext()) {
        std::shared_ptr<IDocument> doc = iter->Next();
        assert(doc);
        auto normalDoc = std::dynamic_pointer_cast<NormalDocument>(doc);
        if (!normalDoc) {
            AUTIL_LOG(ERROR, "cast normal doc failed");
            return Status::Corruption("cast normal doc failed");
        }
        RETURN_IF_STATUS_ERROR(RewriteOneDoc(normalDoc), "rewrite doc[%d] failed in batch", doc->GetDocId());
    }
    return Status::OK();
}

} // namespace indexlibv2::document
