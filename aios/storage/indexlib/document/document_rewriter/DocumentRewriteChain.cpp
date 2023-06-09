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
#include "indexlib/document/document_rewriter/DocumentRewriteChain.h"

#include "indexlib/document/document_rewriter/IDocumentRewriter.h"

namespace indexlibv2::document {
AUTIL_LOG_SETUP(indexlib.document, DocumentRewriteChain);

DocumentRewriteChain::DocumentRewriteChain() {}

DocumentRewriteChain::~DocumentRewriteChain() {}
Status DocumentRewriteChain::Rewrite(document::IDocumentBatch* batch)
{
    for (auto rewriter : _rewriters) {
        // TODO(xiaohao.yxh) more info about which rewriter failed
        RETURN_IF_STATUS_ERROR(rewriter->Rewrite(batch), "rewrite document failed");
    }
    return Status::OK();
}
void DocumentRewriteChain::AppendRewriter(const std::shared_ptr<IDocumentRewriter>& rewriter)
{
    _rewriters.push_back(rewriter);
}

} // namespace indexlibv2::document
