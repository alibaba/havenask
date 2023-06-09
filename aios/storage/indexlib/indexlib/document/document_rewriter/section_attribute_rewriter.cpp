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
#include "indexlib/document/document_rewriter/section_attribute_rewriter.h"

#include "indexlib/document/index_document/normal_document/normal_document.h"

using namespace std;
using namespace indexlib::config;

namespace indexlib { namespace document {
IE_LOG_SETUP(document, SectionAttributeRewriter);

SectionAttributeRewriter::SectionAttributeRewriter() {}

SectionAttributeRewriter::~SectionAttributeRewriter() {}

bool SectionAttributeRewriter::Init(const IndexPartitionSchemaPtr& schema)
{
    SectionAttributeAppenderPtr mainAppender(new SectionAttributeAppender());
    if (mainAppender->Init(schema)) {
        mMainDocAppender = mainAppender;
    }
    IndexPartitionSchemaPtr subSchema = schema->GetSubIndexPartitionSchema();
    if (subSchema) {
        SectionAttributeAppenderPtr subAppender(new SectionAttributeAppender());
        if (subAppender->Init(subSchema)) {
            mSubDocAppender = subAppender;
        }
    }
    return mMainDocAppender || mSubDocAppender;
}

void SectionAttributeRewriter::Rewrite(const DocumentPtr& document)
{
    if (document->GetDocOperateType() != ADD_DOC) {
        return;
    }
    NormalDocumentPtr doc = DYNAMIC_POINTER_CAST(NormalDocument, document);
    if (mMainDocAppender) {
        RewriteIndexDocument(mMainDocAppender, doc);
    }

    if (mSubDocAppender) {
        const NormalDocument::DocumentVector& subDocs = doc->GetSubDocuments();
        for (size_t i = 0; i < subDocs.size(); ++i) {
            RewriteIndexDocument(mSubDocAppender, subDocs[i]);
        }
    }
}

void SectionAttributeRewriter::RewriteIndexDocument(const SectionAttributeAppenderPtr& appender,
                                                    const NormalDocumentPtr& document)
{
    const IndexDocumentPtr& indexDocument = document->GetIndexDocument();
    if (!indexDocument) {
        INDEXLIB_FATAL_ERROR(UnSupported, "indexDocument is NULL!");
    }
    appender->AppendSectionAttribute(indexDocument);
}
}} // namespace indexlib::document
