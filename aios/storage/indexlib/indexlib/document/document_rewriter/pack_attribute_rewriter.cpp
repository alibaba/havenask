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
#include "indexlib/document/document_rewriter/pack_attribute_rewriter.h"

#include "indexlib/document/index_document/normal_document/normal_document.h"

using namespace std;
using namespace indexlib::config;

namespace indexlib { namespace document {
IE_LOG_SETUP(document, PackAttributeRewriter);

PackAttributeRewriter::PackAttributeRewriter() {}

PackAttributeRewriter::~PackAttributeRewriter() {}

bool PackAttributeRewriter::Init(const IndexPartitionSchemaPtr& schema)
{
    MultiRegionPackAttributeAppenderPtr mainAppender(new MultiRegionPackAttributeAppender);
    if (mainAppender->Init(schema)) {
        mMainDocAppender = mainAppender;
    }
    IndexPartitionSchemaPtr subSchema = schema->GetSubIndexPartitionSchema();
    if (subSchema) {
        MultiRegionPackAttributeAppenderPtr subAppender(new MultiRegionPackAttributeAppender);
        if (subAppender->Init(subSchema)) {
            mSubDocAppender = subAppender;
        }
    }
    return mMainDocAppender || mSubDocAppender;
}

void PackAttributeRewriter::Rewrite(const DocumentPtr& document)
{
    if (document->GetDocOperateType() != ADD_DOC || document->GetOriginalOperateType() == DELETE_DOC) {
        return;
    }
    NormalDocumentPtr doc = DYNAMIC_POINTER_CAST(NormalDocument, document);
    if (mMainDocAppender) {
        RewriteAttributeDocument(mMainDocAppender, doc);
    }

    if (mSubDocAppender) {
        const NormalDocument::DocumentVector& subDocs = doc->GetSubDocuments();
        for (size_t i = 0; i < subDocs.size(); ++i) {
            RewriteAttributeDocument(mSubDocAppender, subDocs[i]);
        }
    }
}

void PackAttributeRewriter::RewriteAttributeDocument(const MultiRegionPackAttributeAppenderPtr& appender,
                                                     const NormalDocumentPtr& document)
{
    const AttributeDocumentPtr& attributeDocument = document->GetAttributeDocument();
    if (!attributeDocument) {
        return;
    }

    if (!appender->AppendPackAttribute(attributeDocument, document->GetPool(), document->GetRegionId())) {
        document->SetDocOperateType(SKIP_DOC);
        IE_LOG(WARN, "AppendPackAttribute fail, skip current doc!");
        ERROR_COLLECTOR_LOG(WARN, "AppendPackAttribute fail, skip current doc!");
    }
}
}} // namespace indexlib::document
