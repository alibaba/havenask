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
#include "indexlib/document/index_document/normal_document/source_document_formatter.h"

#include "autil/ConstString.h"
#include "indexlib/config/source_group_config.h"
#include "indexlib/util/Status2Exception.h"

using namespace std;
using namespace autil;

using namespace indexlib::config;

namespace indexlib { namespace document {
IE_LOG_SETUP(document, SourceDocumentFormatter);

void SourceDocumentFormatter::Init(const SourceSchemaPtr& sourceSchema)
{
    mGroupFormatterVec.clear();
    if (sourceSchema) {
        mGroupCount = sourceSchema->GetSourceGroupCount();
        for (auto iter = sourceSchema->Begin(); iter != sourceSchema->End(); iter++) {
            GroupFieldFormatterPtr formatter(new GroupFieldFormatter);
            const SourceGroupConfigPtr& groupConfig = *iter;
            formatter->Init(groupConfig->GetParameter().GetDocCompressor());
            mGroupFormatterVec.push_back(formatter);
        }
    }
    mMetaFormatter.reset(new GroupFieldFormatter);
    mMetaFormatter->Init("");

    mAccessaryFormatter.reset(new GroupFieldFormatter);
    mAccessaryFormatter->Init("");
}

void SourceDocumentFormatter::SerializeSourceDocument(const SourceDocumentPtr& document, autil::mem_pool::Pool* pool,
                                                      SerializedSourceDocumentPtr& serDoc)
{
    Reset();
    SerializeMeta(document, pool, serDoc);
    SerializeData(document, pool, serDoc);
    SerializeAccessary(document, pool, serDoc);
    serDoc->_nonExistFieldInfo = document->EncodeNonExistFieldInfo(pool);
}

void SourceDocumentFormatter::DeserializeSourceDocument(const SerializedSourceDocumentPtr& serDoc,
                                                        SourceDocument* document)
{
    Reset();
    const StringView& accessaryMeta = serDoc->GetAccessaryMeta();
    const StringView& accessaryData = serDoc->GetAccessaryData();
    auto [status, accessaryIter] = mAccessaryFormatter->Deserialize(accessaryData.data(), accessaryData.size());
    THROW_IF_STATUS_ERROR(status);
    if (accessaryIter) {
        auto [status, metaIter] = mAccessaryFormatter->Deserialize(accessaryMeta.data(), accessaryMeta.size());
        THROW_IF_STATUS_ERROR(status);
        while (metaIter->HasNext()) {
            assert(accessaryIter->HasNext());
            StringView fieldName = metaIter->Next();
            StringView data = accessaryIter->Next();
            document->AppendAccessaryField(fieldName, data, true);
        }
    }

    const StringView metaValue = serDoc->GetMeta();
    if (metaValue.size() == 0) {
        return;
    }
    const char* cursor = metaValue.data();
    for (size_t i = 0; i < mGroupCount; i++) {
        uint32_t metaLen = GroupFieldFormatter::ReadVUInt32(cursor);
        auto [status, metaIter] = mMetaFormatter->Deserialize(cursor, metaLen);
        THROW_IF_STATUS_ERROR(status);
        cursor += metaLen;
        const StringView groupValue = serDoc->GetGroupValue(i);
        auto [dataStatus, dataIter] = mGroupFormatterVec[i]->Deserialize(groupValue.data(), groupValue.size());
        THROW_IF_STATUS_ERROR(dataStatus);
        if (!dataIter) {
            continue;
        }
        while (metaIter->HasNext()) {
            assert(dataIter->HasNext());
            StringView fieldName = metaIter->Next();
            StringView data = dataIter->Next();
            document->Append((index::sourcegroupid_t)i, fieldName, data, true);
        }
    }

    const StringView nonExistFieldInfo = serDoc->_nonExistFieldInfo;
    if (nonExistFieldInfo.size() != 0) {
        auto status = document->DecodeNonExistFieldInfo(nonExistFieldInfo);
        THROW_IF_STATUS_ERROR(status);
    }
}

void SourceDocumentFormatter::SerializeData(const SourceDocumentPtr& document, autil::mem_pool::Pool* pool,
                                            SerializedSourceDocumentPtr& serDoc)
{
    for (size_t i = 0; i < mGroupCount; i++) {
        SourceDocument::SourceGroupFieldIter iter = document->CreateGroupFieldIter(i);
        auto [status, len] = mGroupFormatterVec[i]->GetSerializeLength(iter);
        THROW_IF_STATUS_ERROR(status);
        char* value = (char*)pool->allocate(len);
        iter.Reset();
        status = mGroupFormatterVec[i]->SerializeFields(iter, value, len);
        THROW_IF_STATUS_ERROR(status);
        StringView groupValue(value, len);
        serDoc->SetGroupValue(i, groupValue);
    }
}

void SourceDocumentFormatter::SerializeMeta(const SourceDocumentPtr& document, autil::mem_pool::Pool* pool,
                                            SerializedSourceDocumentPtr& serDoc)
{
    // write meta
    size_t totalMetaLen = 0;
    vector<size_t> metaLengths;
    metaLengths.resize(mGroupCount);
    for (size_t i = 0; i < mGroupCount; i++) {
        SourceDocument::SourceMetaIter iter = document->CreateGroupMetaIter(i);
        auto [status, len] = mMetaFormatter->GetSerializeLength(iter);
        THROW_IF_STATUS_ERROR(status);
        metaLengths[i] = len;
        totalMetaLen += GroupFieldFormatter::GetVUInt32Length(metaLengths[i]);
        totalMetaLen += metaLengths[i];
    }

    char* metaValue = nullptr;
    if (totalMetaLen > 0) {
        metaValue = (char*)pool->allocate(totalMetaLen);
    }

    char* cursor = metaValue;
    for (size_t i = 0; i < mGroupCount; i++) {
        SourceDocument::SourceMetaIter iter = document->CreateGroupMetaIter(i);
        GroupFieldFormatter::WriteVUInt32(metaLengths[i], cursor);
        auto status = mMetaFormatter->SerializeFields(iter, cursor, metaLengths[i]);
        THROW_IF_STATUS_ERROR(status);
        cursor += metaLengths[i];
    }
    StringView meta(metaValue, totalMetaLen);
    serDoc->SetMeta(meta);
}

void SourceDocumentFormatter::SerializeAccessary(const SourceDocumentPtr& document, autil::mem_pool::Pool* pool,
                                                 SerializedSourceDocumentPtr& serDoc)
{
    SourceDocument::SourceMetaIter metaIter = document->CreateAccessaryMetaIter();
    auto [status, metaLen] = mAccessaryFormatter->GetSerializeLength(metaIter);
    THROW_IF_STATUS_ERROR(status);
    metaIter.Reset();
    char* metaBuffer = nullptr;
    if (metaLen > 0) {
        metaBuffer = (char*)pool->allocate(metaLen);
    }
    status = mAccessaryFormatter->SerializeFields(metaIter, metaBuffer, metaLen);
    THROW_IF_STATUS_ERROR(status);
    StringView meta(metaBuffer, metaLen);

    SourceDocument::SourceGroupFieldIter dataIter = document->CreateAccessaryFieldIter();
    auto [status1, len] = mAccessaryFormatter->GetSerializeLength(dataIter);
    THROW_IF_STATUS_ERROR(status1);
    char* value = nullptr;
    if (len > 0) {
        value = (char*)pool->allocate(len);
    }
    dataIter.Reset();
    status = mAccessaryFormatter->SerializeFields(dataIter, value, len);
    THROW_IF_STATUS_ERROR(status);
    StringView groupValue(value, len);
    serDoc->AddAccessaryGroupValue(meta, groupValue);
}

void SourceDocumentFormatter::Reset()
{
    // clear state in group field formatter
    mMetaFormatter->Reset();
    for (auto& groupFormatter : mGroupFormatterVec) {
        groupFormatter->Reset();
    }
    mAccessaryFormatter->Reset();
}
}} // namespace indexlib::document
