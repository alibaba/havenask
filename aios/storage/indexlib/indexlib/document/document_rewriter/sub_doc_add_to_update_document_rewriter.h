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
#ifndef __INDEXLIB_SUB_DOC_ADD_TO_UPDATE_DOCUMENT_REWRITER_H
#define __INDEXLIB_SUB_DOC_ADD_TO_UPDATE_DOCUMENT_REWRITER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/document/document_rewriter/add_to_update_document_rewriter.h"
#include "indexlib/document/document_rewriter/document_rewriter.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(document, NormalDocument);

namespace indexlib { namespace document {

class SubDocAddToUpdateDocumentRewriter : public document::DocumentRewriter
{
public:
    SubDocAddToUpdateDocumentRewriter();
    ~SubDocAddToUpdateDocumentRewriter();

public:
    void Init(const config::IndexPartitionSchemaPtr& schema, AddToUpdateDocumentRewriter* mainRewriter,
              AddToUpdateDocumentRewriter* subRewriter);

    void Rewrite(const document::DocumentPtr& doc) override;

private:
    bool ValidateModifiedField(const document::NormalDocumentPtr& doc) const;
    static bool IsExistSubFieldId(const document::NormalDocumentPtr& doc, fieldid_t fid);
    void ClearModifiedFields(const document::NormalDocumentPtr& doc);

private:
    AddToUpdateDocumentRewriter* mMainRewriter;
    AddToUpdateDocumentRewriter* mSubRewriter;
    fieldid_t mSubPkFieldId;

private:
    friend class SubDocAddToUpdateDocumentRewriterTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SubDocAddToUpdateDocumentRewriter);
}} // namespace indexlib::document

#endif //__INDEXLIB_SUB_DOC_ADD_TO_UPDATE_DOCUMENT_REWRITER_H
