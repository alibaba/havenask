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
#include "indexlib/document/document_rewriter/timestamp_document_rewriter.h"

#include "autil/StringUtil.h"
#include "indexlib/document/index_document/normal_document/field.h"
#include "indexlib/document/index_document/normal_document/index_tokenize_field.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/util/KeyHasherTyped.h"

using namespace std;
using namespace autil;
using namespace indexlib::util;

namespace indexlib { namespace document {
IE_LOG_SETUP(document, TimestampDocumentRewriter);

TimestampDocumentRewriter::TimestampDocumentRewriter(fieldid_t timestampFieldId) : mTimestampFieldId(timestampFieldId)
{
}

TimestampDocumentRewriter::~TimestampDocumentRewriter() {}

void TimestampDocumentRewriter::Rewrite(const DocumentPtr& document)
{
    DocOperateType opType = document->GetDocOperateType();
    if (opType != ADD_DOC) {
        return;
    }
    NormalDocumentPtr doc = DYNAMIC_POINTER_CAST(NormalDocument, document);
    string timestamp = StringUtil::toString(doc->GetTimestamp());
    IndexDocumentPtr indexDoc = doc->GetIndexDocument();
    if (!indexDoc) {
        return;
    }

    Field* field = indexDoc->GetField(mTimestampFieldId);
    if (field) {
        IE_INTERVAL_LOG2(60, INFO, "field [%d] will be reset for timestamp field rewrite.", mTimestampFieldId);
        indexDoc->ClearField(mTimestampFieldId);
    }

    field = indexDoc->CreateField(mTimestampFieldId, Field::FieldTag::TOKEN_FIELD);
    assert(field);
    auto tokenizeField = dynamic_cast<IndexTokenizeField*>(field);
    assert(tokenizeField);
    tokenizeField->Reset();
    tokenizeField->SetFieldId(mTimestampFieldId);
    Section* section = tokenizeField->CreateSection(1);
    dictkey_t hashKey;
    Int64NumberHasher::GetHashKey(timestamp.data(), timestamp.size(), hashKey);
    section->CreateToken(hashKey);
}
}} // namespace indexlib::document
