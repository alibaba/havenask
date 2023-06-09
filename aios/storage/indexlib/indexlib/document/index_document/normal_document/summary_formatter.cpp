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
#include "indexlib/document/index_document/normal_document/summary_formatter.h"

#include <assert.h>
#include <memory>

#include "indexlib/document/index_document/normal_document/group_field_formatter.h"
#include "indexlib/util/Exception.h"
#include "indexlib/util/Status2Exception.h"

using namespace std;
using namespace autil;
using namespace indexlib::config;
using namespace indexlib::document;

namespace indexlib { namespace document {
IE_LOG_SETUP(document, SummaryFormatter);

SummaryFormatter::SummaryFormatter(const SummarySchemaPtr& summarySchema) : mSummarySchema(summarySchema)
{
    if (mSummarySchema && mSummarySchema->NeedStoreSummary()) {
        for (summarygroupid_t groupId = 0; groupId < summarySchema->GetSummaryGroupConfigCount(); ++groupId) {
            const SummaryGroupConfigPtr& summaryGroupConfig = summarySchema->GetSummaryGroupConfig(groupId);
            if (summaryGroupConfig->NeedStoreSummary()) {
                mGroupFormatterVec.push_back(SummaryGroupFormatterPtr(new SummaryGroupFormatter(summaryGroupConfig)));
            } else {
                mGroupFormatterVec.push_back(SummaryGroupFormatterPtr());
            }
        }
        mLengthVec.resize(mGroupFormatterVec.size());
    }
}

size_t SummaryFormatter::GetSerializeLength(const SummaryDocumentPtr& document)
{
    if (mGroupFormatterVec.size() == 1) {
        // only default group
        assert(mGroupFormatterVec[0]);
        auto [status, len] = mGroupFormatterVec[0]->GetSerializeLength(document);
        THROW_IF_STATUS_ERROR(status);
        return len;
    }
    size_t totalLength = 0;
    for (size_t i = 0; i < mGroupFormatterVec.size(); ++i) {
        if (!mGroupFormatterVec[i]) {
            continue;
        }
        auto [status, len] = mGroupFormatterVec[i]->GetSerializeLength(document);
        THROW_IF_STATUS_ERROR(status);
        mLengthVec[i] = len;
        totalLength += GroupFieldFormatter::GetVUInt32Length(mLengthVec[i]);
        totalLength += mLengthVec[i];
    }
    return totalLength;
}

void SummaryFormatter::SerializeSummary(const SummaryDocumentPtr& document, char* value, size_t length) const
{
    if (mGroupFormatterVec.size() == 1) {
        // only default group
        assert(mGroupFormatterVec[0]);
        auto status = mGroupFormatterVec[0]->SerializeSummary(document, value, length);
        THROW_IF_STATUS_ERROR(status);
        return;
    }

    char* cursor = value;
    for (size_t i = 0; i < mGroupFormatterVec.size(); ++i) {
        if (!mGroupFormatterVec[i]) {
            continue;
        }
        uint32_t groupLength = mLengthVec[i];
        assert(length >= (uint32_t)(cursor - value) + groupLength + GroupFieldFormatter::GetVUInt32Length(groupLength));
        GroupFieldFormatter::WriteVUInt32(groupLength, cursor);
        auto status = mGroupFormatterVec[i]->SerializeSummary(document, cursor, groupLength);
        THROW_IF_STATUS_ERROR(status);
        cursor += groupLength;
    }
}

void SummaryFormatter::DeserializeSummaryDoc(const document::SerializedSummaryDocumentPtr& serDoc,
                                             document::SearchSummaryDocument* document)
{
    const char* value = serDoc->GetValue();
    size_t length = serDoc->GetLength();
    if (mGroupFormatterVec.size() == 1) {
        // only default group
        assert(mGroupFormatterVec[0]);
        mGroupFormatterVec[0]->DeserializeSummary(document, value, length);
        return;
    }

    for (size_t i = 0; i < mGroupFormatterVec.size(); ++i) {
        if (!mGroupFormatterVec[i]) {
            continue;
        }
        uint32_t groupLength = GroupFieldFormatter::ReadVUInt32(value);
        mGroupFormatterVec[i]->DeserializeSummary(document, value, groupLength);
        value += groupLength;
    }
}

bool SummaryFormatter::TEST_DeserializeSummary(document::SearchSummaryDocument* document, const char* value,
                                               size_t length) const
{
    assert(mGroupFormatterVec.size() == 1 && mGroupFormatterVec[0]);
    return mGroupFormatterVec[0]->DeserializeSummary(document, value, length);
}

// summary doc -> serialized doc,
// bs processor use it serialize summary doc came from raw doc
void SummaryFormatter::SerializeSummaryDoc(const SummaryDocumentPtr& doc, SerializedSummaryDocumentPtr& serDoc)
{
    size_t length = GetSerializeLength(doc);
    char* value = new char[length];

    SerializeSummary(doc, value, length);
    if (!serDoc) {
        serDoc.reset(new SerializedSummaryDocument());
    }
    serDoc->SetSerializedDoc(value, length);
    serDoc->SetDocId(doc->GetDocId());
}

void SummaryFormatter::TEST_DeserializeSummaryDoc(const SerializedSummaryDocumentPtr& serDoc,
                                                  SearchSummaryDocument* document)
{
    assert(document);
    TEST_DeserializeSummary(document, serDoc->GetValue(), serDoc->GetLength());
}
}} // namespace indexlib::document
