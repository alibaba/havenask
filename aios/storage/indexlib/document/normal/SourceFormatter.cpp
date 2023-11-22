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
#include "indexlib/document/normal/SourceFormatter.h"

#include "indexlib/config/GroupDataParameter.h"
#include "indexlib/document/normal/GroupFieldFormatter.h"
#include "indexlib/document/normal/SerializedSourceDocument.h"
#include "indexlib/document/normal/SourceDocument.h"
#include "indexlib/index/source/config/SourceGroupConfig.h"
#include "indexlib/index/source/config/SourceIndexConfig.h"
#include "indexlib/util/Status2Exception.h"

using namespace std;
using namespace autil;
using namespace indexlib::config;
using namespace indexlib::document;
using indexlib::document::GroupFieldFormatter;
using indexlib::document::SerializedSourceDocument;
using indexlib::document::SourceDocument;

namespace indexlibv2 { namespace document {
AUTIL_LOG_SETUP(indexlib.document, SourceFormatter);

// TODO(xiaohao.yxh) impl this
void SourceFormatter::Init(const std::shared_ptr<config::SourceIndexConfig>& sourceConfig)
{
    _groupFormatterVec.clear();
    if (sourceConfig) {
        _groupCount = sourceConfig->GetSourceGroupCount();
        auto groupConfigs = sourceConfig->GetGroupConfigs();
        for (const auto& groupConfig : groupConfigs) {
            std::shared_ptr<GroupFieldFormatter> formatter(new GroupFieldFormatter);
            formatter->Init(groupConfig->GetParameter().GetDocCompressor());
            _groupFormatterVec.push_back(formatter);
        }
    }
    _metaFormatter.reset(new GroupFieldFormatter);
    _metaFormatter->Init("");

    _accessaryFormatter.reset(new GroupFieldFormatter);
    _accessaryFormatter->Init("");
}

Status SourceFormatter::SerializeSourceDocument(const std::shared_ptr<SourceDocument>& document,
                                                autil::mem_pool::Pool* pool,
                                                std::shared_ptr<SerializedSourceDocument>& serDoc)
{
    Reset();
    RETURN_IF_STATUS_ERROR(SerializeMeta(document, pool, serDoc), "serialize meta failed");
    RETURN_IF_STATUS_ERROR(SerializeData(document, pool, serDoc), "serialize data failed");
    RETURN_IF_STATUS_ERROR(SerializeAccessary(document, pool, serDoc), "serialize accessary failed");
    serDoc->_nonExistFieldInfo = document->EncodeNonExistFieldInfo(pool);
    return Status::OK();
}

Status SourceFormatter::DeserializeSourceDocument(
    SerializedSourceDocument* serDoc,
    const std::function<Status(const autil::StringView&, const autil::StringView&)>& callback)
{
    Reset();
    const StringView& accessaryMeta = serDoc->GetAccessaryMeta();
    const StringView& accessaryData = serDoc->GetAccessaryData();
    auto [status, accessaryIter] = _accessaryFormatter->Deserialize(accessaryData.data(), accessaryData.size());
    RETURN_IF_STATUS_ERROR(status, "deserialize failed");
    if (accessaryIter) {
        auto [status, metaIter] = _accessaryFormatter->Deserialize(accessaryMeta.data(), accessaryMeta.size());
        RETURN_IF_STATUS_ERROR(status, "deserialize failed");
        while (metaIter->HasNext()) {
            assert(accessaryIter->HasNext());
            StringView fieldName = metaIter->Next();
            StringView data = accessaryIter->Next();
            RETURN_IF_STATUS_ERROR(callback(fieldName, data), "fill value failed");
        }
    }

    const StringView metaValue = serDoc->GetMeta();
    if (metaValue.size() == 0) {
        return Status::OK();
    }
    const char* cursor = metaValue.data();
    for (size_t i = 0; i < _groupCount; i++) {
        uint32_t metaLen = GroupFieldFormatter::ReadVUInt32(cursor);
        auto [status, metaIter] = _metaFormatter->Deserialize(cursor, metaLen);
        RETURN_IF_STATUS_ERROR(status, "deserialize meta failed");
        cursor += metaLen;
        const StringView groupValue = serDoc->GetGroupValue(i);
        auto [dataStatus, dataIter] = _groupFormatterVec[i]->Deserialize(groupValue.data(), groupValue.size());
        RETURN_IF_STATUS_ERROR(dataStatus, "deserialize group data failed");
        if (!dataIter) {
            continue;
        }
        while (metaIter->HasNext()) {
            assert(dataIter->HasNext());
            StringView fieldName = metaIter->Next();
            StringView data = dataIter->Next();
            RETURN_IF_STATUS_ERROR(callback(fieldName, data), "fill value failed");
        }
    }
    return Status::OK();
}

Status SourceFormatter::DeserializeSourceDocument(const SerializedSourceDocument* serDoc, SourceDocument* document)
{
    Reset();
    const StringView& accessaryMeta = serDoc->GetAccessaryMeta();
    const StringView& accessaryData = serDoc->GetAccessaryData();
    auto [status, accessaryIter] = _accessaryFormatter->Deserialize(accessaryData.data(), accessaryData.size());
    RETURN_IF_STATUS_ERROR(status, "deserialize failed");
    if (accessaryIter) {
        auto [status, metaIter] = _accessaryFormatter->Deserialize(accessaryMeta.data(), accessaryMeta.size());
        RETURN_IF_STATUS_ERROR(status, "deserialize failed");
        while (metaIter->HasNext()) {
            assert(accessaryIter->HasNext());
            StringView fieldName = metaIter->Next();
            StringView data = accessaryIter->Next();
            document->AppendAccessaryField(fieldName, data, true);
        }
    }

    const StringView metaValue = serDoc->GetMeta();
    if (metaValue.size() == 0) {
        return Status::OK();
    }
    const char* cursor = metaValue.data();
    for (size_t i = 0; i < _groupCount; i++) {
        uint32_t metaLen = GroupFieldFormatter::ReadVUInt32(cursor);
        auto [status, metaIter] = _metaFormatter->Deserialize(cursor, metaLen);
        RETURN_IF_STATUS_ERROR(status, "deserialize meta failed");
        cursor += metaLen;
        const StringView groupValue = serDoc->GetGroupValue(i);
        auto [dataStatus, dataIter] = _groupFormatterVec[i]->Deserialize(groupValue.data(), groupValue.size());
        RETURN_IF_STATUS_ERROR(dataStatus, "deserialize group data failed");
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
        RETURN_IF_STATUS_ERROR(document->DecodeNonExistFieldInfo(nonExistFieldInfo),
                               "decode non exist field info failed");
    }
    return Status::OK();
}

Status SourceFormatter::SerializeData(const std::shared_ptr<SourceDocument>& document, autil::mem_pool::Pool* pool,
                                      std::shared_ptr<SerializedSourceDocument>& serDoc)
{
    for (size_t i = 0; i < _groupCount; i++) {
        SourceDocument::SourceGroupFieldIter iter = document->CreateGroupFieldIter(i);
        auto [status, len] = _groupFormatterVec[i]->GetSerializeLength(iter);
        RETURN_IF_STATUS_ERROR(status, "get serialize length failed");
        char* value = (char*)pool->allocate(len);
        iter.Reset();
        status = _groupFormatterVec[i]->SerializeFields(iter, value, len);
        RETURN_IF_STATUS_ERROR(status, "serialize data failed");
        StringView groupValue(value, len);
        serDoc->SetGroupValue(i, groupValue);
    }
    return Status::OK();
}

Status SourceFormatter::SerializeMeta(const std::shared_ptr<SourceDocument>& document, autil::mem_pool::Pool* pool,
                                      std::shared_ptr<SerializedSourceDocument>& serDoc)
{
    // write meta
    size_t totalMetaLen = 0;
    vector<size_t> metaLengths;
    metaLengths.resize(_groupCount);
    for (size_t i = 0; i < _groupCount; i++) {
        SourceDocument::SourceMetaIter iter = document->CreateGroupMetaIter(i);
        auto [status, len] = _metaFormatter->GetSerializeLength(iter);
        RETURN_IF_STATUS_ERROR(status, "get serialize length failed");
        metaLengths[i] = len;
        totalMetaLen += GroupFieldFormatter::GetVUInt32Length(metaLengths[i]);
        totalMetaLen += metaLengths[i];
    }

    char* metaValue = nullptr;
    if (totalMetaLen > 0) {
        metaValue = (char*)pool->allocate(totalMetaLen);
    }

    char* cursor = metaValue;
    for (size_t i = 0; i < _groupCount; i++) {
        SourceDocument::SourceMetaIter iter = document->CreateGroupMetaIter(i);
        GroupFieldFormatter::WriteVUInt32(metaLengths[i], cursor);
        auto status = _metaFormatter->SerializeFields(iter, cursor, metaLengths[i]);
        RETURN_IF_STATUS_ERROR(status, "serialize fileds failed");
        cursor += metaLengths[i];
    }
    StringView meta(metaValue, totalMetaLen);
    serDoc->SetMeta(meta);
    return Status::OK();
}

Status SourceFormatter::SerializeAccessary(const std::shared_ptr<SourceDocument>& document, autil::mem_pool::Pool* pool,
                                           std::shared_ptr<SerializedSourceDocument>& serDoc)
{
    SourceDocument::SourceMetaIter metaIter = document->CreateAccessaryMetaIter();
    auto [status, metaLen] = _accessaryFormatter->GetSerializeLength(metaIter);
    RETURN_IF_STATUS_ERROR(status, "get serialize length failed");
    metaIter.Reset();
    char* metaBuffer = nullptr;
    if (metaLen > 0) {
        metaBuffer = (char*)pool->allocate(metaLen);
    }
    status = _accessaryFormatter->SerializeFields(metaIter, metaBuffer, metaLen);
    RETURN_IF_STATUS_ERROR(status, "serialize fields failed");
    StringView meta(metaBuffer, metaLen);

    SourceDocument::SourceGroupFieldIter dataIter = document->CreateAccessaryFieldIter();
    auto [status1, len] = _accessaryFormatter->GetSerializeLength(dataIter);
    RETURN_IF_STATUS_ERROR(status1, "get serialize length failed");
    char* value = nullptr;
    if (len > 0) {
        value = (char*)pool->allocate(len);
    }
    dataIter.Reset();
    status = _accessaryFormatter->SerializeFields(dataIter, value, len);
    RETURN_IF_STATUS_ERROR(status, "SerializeFields failed ");
    StringView groupValue(value, len);
    serDoc->AddAccessaryGroupValue(meta, groupValue);
    return Status::OK();
}

void SourceFormatter::Reset()
{ // clear state in group field formatter
    _metaFormatter->Reset();
    for (auto& groupFormatter : _groupFormatterVec) {
        groupFormatter->Reset();
    }
    _accessaryFormatter->Reset();
}

}} // namespace indexlibv2::document
