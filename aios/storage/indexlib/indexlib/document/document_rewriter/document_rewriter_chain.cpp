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
#include "indexlib/document/document_rewriter/document_rewriter_chain.h"

#include "indexlib/config/index_partition_schema.h"
#include "indexlib/document/document_rewriter/document_rewriter.h"
#include "indexlib/document/document_rewriter/document_rewriter_creator.h"

using namespace std;
using namespace indexlib::config;

namespace indexlib { namespace document {
IE_LOG_SETUP(document, DocumentRewriterChain);

DocumentRewriterChain::DocumentRewriterChain(const IndexPartitionSchemaPtr& schema,
                                             const IndexPartitionOptions& options,
                                             const indexlibv2::config::SortDescriptions& sortDesc)
    : mSchema(schema)
    , mOptions(options)
    , mSortDescriptions(sortDesc)
{
    assert(schema);
}

DocumentRewriterChain::~DocumentRewriterChain() {}

void DocumentRewriterChain::Init()
{
    mRewriters.clear();
    if (mSchema->GetTableType() == tt_kkv || mSchema->GetTableType() == tt_kv) {
        PushDeleteToAddDocumentRewriter();
        PushPackAttributeRewriter();
    } else {
        PushAddToUpdateRewriter();
        PushTimestampRewriter();
        PushSectionAttributeRewriter();
        PushPackAttributeRewriter();
        PushHashIdAttributeRewriter();
    }
}

void DocumentRewriterChain::Rewrite(const DocumentPtr& doc)
{
    for (size_t i = 0; i < mRewriters.size(); i++) {
        mRewriters[i]->Rewrite(doc);
    }
}

void DocumentRewriterChain::PushAddToUpdateRewriter()
{
    DocumentRewriterPtr add2UpdateRewriter(
        DocumentRewriterCreator::CreateAddToUpdateDocumentRewriter(mSchema, mOptions, mSortDescriptions));
    if (add2UpdateRewriter) {
        mRewriters.push_back(add2UpdateRewriter);
    }
}

void DocumentRewriterChain::PushTimestampRewriter()
{
    DocumentRewriterPtr timestampRewriter(DocumentRewriterCreator::CreateTimestampRewriter(mSchema));
    if (timestampRewriter) {
        mRewriters.push_back(timestampRewriter);
    }
}

void DocumentRewriterChain::PushSectionAttributeRewriter()
{
    // todo: only be used for lagacy document version
    // delete after DOCUMENT_BINARY_VERSION is 3 (current is 2)
    DocumentRewriterPtr sectionAttributeRewriter(DocumentRewriterCreator::CreateSectionAttributeRewriter(mSchema));
    if (sectionAttributeRewriter) {
        mRewriters.push_back(sectionAttributeRewriter);
    }
}

void DocumentRewriterChain::PushPackAttributeRewriter()
{
    DocumentRewriterPtr packAttributeRewriter(DocumentRewriterCreator::CreatePackAttributeRewriter(mSchema));
    if (packAttributeRewriter) {
        mRewriters.push_back(packAttributeRewriter);
    }
}

void DocumentRewriterChain::PushHashIdAttributeRewriter()
{
    DocumentRewriterPtr hashIdAttributeRewriter(DocumentRewriterCreator::CreateHashIdDocumentRewriter(mSchema));
    if (hashIdAttributeRewriter) {
        mRewriters.push_back(hashIdAttributeRewriter);
    }
}

void DocumentRewriterChain::PushDeleteToAddDocumentRewriter()
{
    DocumentRewriterPtr rewriter(DocumentRewriterCreator::CreateDeleteToAddDocumentRewriter());
    assert(rewriter);
    mRewriters.push_back(rewriter);
}
}} // namespace indexlib::document
