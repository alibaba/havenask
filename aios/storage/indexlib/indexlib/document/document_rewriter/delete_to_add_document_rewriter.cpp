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
#include "indexlib/document/document_rewriter/delete_to_add_document_rewriter.h"

#include "indexlib/document/document.h"

using namespace std;

namespace indexlib { namespace document {
IE_LOG_SETUP(document, DeleteToAddDocumentRewriter);

void DeleteToAddDocumentRewriter::Rewrite(const DocumentPtr& doc)
{
    DocOperateType type = doc->GetDocOperateType();
    if (type == ADD_DOC || type == SKIP_DOC || type == CHECKPOINT_DOC) {
        return;
    }

    if (type == DELETE_DOC) {
        doc->ModifyDocOperateType(ADD_DOC);
        return;
    }

    // TODO: supportupdate
    assert(false);
}
}} // namespace indexlib::document
