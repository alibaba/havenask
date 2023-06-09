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
#include "indexlib/document/document_rewriter/sub_doc_add_to_update_document_rewriter.h"

#include "indexlib/document/index_document/normal_document/normal_document.h"

using namespace std;
using namespace indexlib::config;

namespace indexlib { namespace document {
IE_LOG_SETUP(document, SubDocAddToUpdateDocumentRewriter);

SubDocAddToUpdateDocumentRewriter::SubDocAddToUpdateDocumentRewriter()
{
    mMainRewriter = NULL;
    mSubRewriter = NULL;
    mSubPkFieldId = INVALID_FIELDID;
}

SubDocAddToUpdateDocumentRewriter::~SubDocAddToUpdateDocumentRewriter()
{
    DELETE_AND_SET_NULL(mMainRewriter);
    DELETE_AND_SET_NULL(mSubRewriter);
}

void SubDocAddToUpdateDocumentRewriter::Init(const config::IndexPartitionSchemaPtr& schema,
                                             AddToUpdateDocumentRewriter* mainRewriter,
                                             AddToUpdateDocumentRewriter* subRewriter)
{
    assert(mainRewriter);
    assert(subRewriter);
    mMainRewriter = mainRewriter;
    mSubRewriter = subRewriter;

    IndexPartitionSchemaPtr subSchema = schema->GetSubIndexPartitionSchema();
    assert(subSchema);
    IndexSchemaPtr subIndexSchema = subSchema->GetIndexSchema();
    assert(subIndexSchema);
    mSubPkFieldId = subIndexSchema->GetPrimaryKeyIndexFieldId();
    assert(mSubPkFieldId != INVALID_FIELDID);
}

void SubDocAddToUpdateDocumentRewriter::Rewrite(const document::DocumentPtr& document)
{
    if (document->GetDocOperateType() != ADD_DOC) {
        return;
    }
    NormalDocumentPtr doc = DYNAMIC_POINTER_CAST(NormalDocument, document);
    const NormalDocument::FieldIdVector& modifiedFields = doc->GetModifiedFields();
    const NormalDocument::FieldIdVector& subModifiedFields = doc->GetSubModifiedFields();
    if (modifiedFields.empty() && subModifiedFields.empty()) {
        return;
    }

    if (!ValidateModifiedField(doc)) {
        ClearModifiedFields(doc);
        return;
    }

    // can both main and sub be rewrited?
    std::function<void()> mainCommitter;
    if (!modifiedFields.empty()) {
        NormalDocument::FieldIdVector bufferedSubModifiedFields = doc->GetSubModifiedFields();
        doc->ClearSubModifiedFields();
        mainCommitter = mMainRewriter->TryRewrite(doc);
        if (!mainCommitter) {
            ClearModifiedFields(doc);
            return;
        }
    }

    NormalDocument::DocumentVector toUpdateSubDocuments;
    std::vector<std::function<void()>> subCommitters;
    const NormalDocument::DocumentVector& subDocuments = doc->GetSubDocuments();
    for (size_t i = 0; i < subDocuments.size(); ++i) {
        const NormalDocument::FieldIdVector& subModifiedFields = subDocuments[i]->GetModifiedFields();
        if (subModifiedFields.empty()) {
            continue;
        }
        auto subCommitter = mSubRewriter->TryRewrite(subDocuments[i]);
        if (!subCommitter) {
            ClearModifiedFields(doc);
            return;
        }
        subCommitters.push_back(subCommitter);
        toUpdateSubDocuments.push_back(subDocuments[i]);
    }
    if (mainCommitter) {
        mainCommitter();
    } else {
        mMainRewriter->RewriteIndexDocument(doc);
        doc->SetSummaryDocument(SerializedSummaryDocumentPtr());
        doc->SetAttributeDocument(AttributeDocumentPtr(new AttributeDocument));
        doc->ModifyDocOperateType(UPDATE_FIELD);
    }
    doc->ClearSubDocuments();
    assert(subCommitters.size() == toUpdateSubDocuments.size());
    for (size_t i = 0; i < toUpdateSubDocuments.size(); ++i) {
        subCommitters[i]();
        doc->AddSubDocument(toUpdateSubDocuments[i]);
    }
    ClearModifiedFields(doc);
}

bool SubDocAddToUpdateDocumentRewriter::ValidateModifiedField(const document::NormalDocumentPtr& doc) const
{
    // validate pk
    // validate other fields
    const NormalDocument::FieldIdVector& subModifiedFields = doc->GetSubModifiedFields();
    for (size_t i = 0; i < subModifiedFields.size(); ++i) {
        fieldid_t subFid = subModifiedFields[i];
        // pk changed, no rewrite
        if (subFid == mSubPkFieldId) {
            return false;
        }
        if (!IsExistSubFieldId(doc, subFid)) {
            // TODO: many logs
            IE_LOG(DEBUG,
                   "modified fields not consistent, "
                   "fid [%d] exist in main, not exist in sub",
                   subFid);
            ERROR_COLLECTOR_LOG(ERROR,
                                "modified fields not consistent, "
                                "fid [%d] exist in main, not exist in sub",
                                subFid);
            return false;
        }
    }
    return true;
}

bool SubDocAddToUpdateDocumentRewriter::IsExistSubFieldId(const document::NormalDocumentPtr& doc, fieldid_t fid)
{
    const NormalDocument::DocumentVector& subDocs = doc->GetSubDocuments();
    for (size_t j = 0; j < subDocs.size(); ++j) {
        const NormalDocument::FieldIdVector& modifiedFields = subDocs[j]->GetModifiedFields();
        NormalDocument::FieldIdVector::const_iterator it = find(modifiedFields.begin(), modifiedFields.end(), fid);
        if (it != modifiedFields.end()) {
            return true;
        }
    }
    return false;
}

void SubDocAddToUpdateDocumentRewriter::ClearModifiedFields(const NormalDocumentPtr& doc)
{
    assert(doc);
    doc->ClearModifiedFields();
    doc->ClearSubModifiedFields();

    const NormalDocument::DocumentVector& subDocuments = doc->GetSubDocuments();
    for (size_t i = 0; i < subDocuments.size(); i++) {
        subDocuments[i]->ClearModifiedFields();
        subDocuments[i]->ClearSubModifiedFields();
    }
}
}} // namespace indexlib::document
