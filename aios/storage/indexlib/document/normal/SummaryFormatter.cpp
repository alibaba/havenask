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
#include "indexlib/document/normal/SummaryFormatter.h"

#include "indexlib/document/normal/GroupFieldFormatter.h"
#include "indexlib/index/summary/config/SummaryGroupConfig.h"

using namespace std;
using namespace autil;
using namespace indexlib::config;
using namespace indexlib::document;

namespace indexlibv2 { namespace document {
AUTIL_LOG_SETUP(indexlib.document, SummaryFormatter);

SummaryFormatter::SummaryFormatter(const shared_ptr<config::SummaryIndexConfig>& summaryIndexConfig)
    : _summaryIndexConfig(summaryIndexConfig)
{
    if (_summaryIndexConfig && _summaryIndexConfig->NeedStoreSummary()) {
        for (index::summarygroupid_t groupId = 0; groupId < _summaryIndexConfig->GetSummaryGroupConfigCount();
             ++groupId) {
            const auto& summaryGroupConfig = _summaryIndexConfig->GetSummaryGroupConfig(groupId);
            if (summaryGroupConfig->NeedStoreSummary()) {
                _groupFormatterVec.push_back(
                    shared_ptr<SummaryGroupFormatter>(new SummaryGroupFormatter(summaryGroupConfig)));
            } else {
                _groupFormatterVec.push_back(shared_ptr<SummaryGroupFormatter>());
            }
        }
        _lengthVec.resize(_groupFormatterVec.size());
    }
}

pair<Status, size_t> SummaryFormatter::GetSerializeLength(const shared_ptr<SummaryDocument>& document)
{
    if (_groupFormatterVec.size() == 1) {
        // only default group
        assert(_groupFormatterVec[0]);
        auto [status, len] = _groupFormatterVec[0]->GetSerializeLength(document);
        RETURN2_IF_STATUS_ERROR(status, 0, "get summary document serialize length failed");
        return {Status::OK(), len};
    }
    size_t totalLength = 0;
    for (size_t i = 0; i < _groupFormatterVec.size(); ++i) {
        if (!_groupFormatterVec[i]) {
            continue;
        }
        auto [status, len] = _groupFormatterVec[i]->GetSerializeLength(document);
        RETURN2_IF_STATUS_ERROR(status, 0, "get summary document serialize length failed");
        _lengthVec[i] = len;
        totalLength += GroupFieldFormatter::GetVUInt32Length(_lengthVec[i]);
        totalLength += _lengthVec[i];
    }
    return {Status::OK(), totalLength};
}

Status SummaryFormatter::SerializeSummary(const shared_ptr<SummaryDocument>& document, char* value, size_t length) const
{
    if (_groupFormatterVec.size() == 1) {
        // only default group
        assert(_groupFormatterVec[0]);
        auto status = _groupFormatterVec[0]->SerializeSummary(document, value, length);
        RETURN_IF_STATUS_ERROR(status, "serialize summary document failed");
        return Status::OK();
    }

    char* cursor = value;
    for (size_t i = 0; i < _groupFormatterVec.size(); ++i) {
        if (!_groupFormatterVec[i]) {
            continue;
        }
        uint32_t groupLength = _lengthVec[i];
        assert(length >= (uint32_t)(cursor - value) + groupLength + GroupFieldFormatter::GetVUInt32Length(groupLength));
        GroupFieldFormatter::WriteVUInt32(groupLength, cursor);
        auto status = _groupFormatterVec[i]->SerializeSummary(document, cursor, groupLength);
        RETURN_IF_STATUS_ERROR(status, "serialize summary document failed");
        cursor += groupLength;
    }
    return Status::OK();
}

void SummaryFormatter::DeserializeSummaryDoc(const shared_ptr<SerializedSummaryDocument>& serDoc,
                                             SearchSummaryDocument* document)
{
    const char* value = serDoc->GetValue();
    size_t length = serDoc->GetLength();
    if (_groupFormatterVec.size() == 1) {
        // only default group
        assert(_groupFormatterVec[0]);
        _groupFormatterVec[0]->DeserializeSummary(document, value, length);
        return;
    }

    for (size_t i = 0; i < _groupFormatterVec.size(); ++i) {
        if (!_groupFormatterVec[i]) {
            continue;
        }
        uint32_t groupLength = GroupFieldFormatter::ReadVUInt32(value);
        _groupFormatterVec[i]->DeserializeSummary(document, value, groupLength);
        value += groupLength;
    }
}

bool SummaryFormatter::TEST_DeserializeSummary(SearchSummaryDocument* document, const char* value, size_t length) const
{
    assert(_groupFormatterVec.size() == 1 && _groupFormatterVec[0]);
    return _groupFormatterVec[0]->DeserializeSummary(document, value, length);
}

// summary doc -> serialized doc,
// bs processor use it serialize summary doc came from raw doc
Status SummaryFormatter::SerializeSummaryDoc(const shared_ptr<SummaryDocument>& doc,
                                             shared_ptr<SerializedSummaryDocument>& serDoc)
{
    auto [status, length] = GetSerializeLength(doc);
    RETURN_IF_STATUS_ERROR(status, "get doc serialize length failed");
    char* value = new char[length];

    status = SerializeSummary(doc, value, length);
    RETURN_IF_STATUS_ERROR(status, "serialize summary failed");
    if (!serDoc) {
        serDoc.reset(new SerializedSummaryDocument());
    }
    serDoc->SetSerializedDoc(value, length);
    serDoc->SetDocId(doc->GetDocId());
    return Status::OK();
}

void SummaryFormatter::TEST_DeserializeSummaryDoc(const shared_ptr<SerializedSummaryDocument>& serDoc,
                                                  SearchSummaryDocument* document)
{
    assert(document);
    TEST_DeserializeSummary(document, serDoc->GetValue(), serDoc->GetLength());
}
}} // namespace indexlibv2::document
