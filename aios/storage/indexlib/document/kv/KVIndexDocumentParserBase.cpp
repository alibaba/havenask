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
#include "indexlib/document/kv/KVIndexDocumentParserBase.h"

#include "autil/StringUtil.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/document/kv/ValueConvertor.h"
#include "indexlib/util/counter/AccumulativeCounter.h"

namespace indexlibv2::document {

KVIndexDocumentParserBase::KVIndexDocumentParserBase(
    const std::shared_ptr<indexlib::util::AccumulativeCounter>& counter)
    : _attributeConvertErrorCounter(counter)
{
}

KVIndexDocumentParserBase::~KVIndexDocumentParserBase() {}

StatusOr<std::unique_ptr<KVDocument>> KVIndexDocumentParserBase::Parse(const RawDocument& rawDoc,
                                                                       autil::mem_pool::Pool* pool) const
{
    if (MaybeIgnore(rawDoc)) {
        return {};
    }
    auto kvDoc = std::make_unique<KVDocument>(pool);
    RETURN_STATUS_DIRECTLY_IF_ERROR(Parse(rawDoc, kvDoc.get(), pool));
    return {std::move(kvDoc)};
}

Status KVIndexDocumentParserBase::Parse(const RawDocument& rawDoc, KVDocument* doc, autil::mem_pool::Pool* pool) const
{
    if (!ParseKey(rawDoc, doc)) {
        return Status::InternalError("parse key failed");
    }

    ParseTTL(rawDoc, doc);
    SetDocInfo(rawDoc, doc);

    auto opType = rawDoc.getDocOperateType();
    doc->SetDocOperateType(opType);

    if (!NeedParseValue(opType)) {
        return Status::OK();
    }

    auto result = ParseValue(rawDoc, pool);
    if (!result.success) {
        return Status::InternalError("convert value failed");
    }
    if (result.hasFormatError) {
        doc->SetFormatError();
        if (_attributeConvertErrorCounter) {
            _attributeConvertErrorCounter->Increase(1);
        }
    }
    doc->SetValue(result.convertedValue);
    return Status::OK();
}

void KVIndexDocumentParserBase::ParseTTL(const RawDocument& rawDoc, KVDocument* doc) const
{
    const auto& ttlSettings = GetTTLSettings();
    if (ttlSettings && ttlSettings->enabled && ttlSettings->ttlFromDoc) {
        uint32_t ttl = 0;
        autil::StringView ttlField = rawDoc.getField(autil::StringView(ttlSettings->ttlField));
        if (!ttlField.empty() && autil::StringUtil::strToUInt32(ttlField.data(), ttl)) {
            doc->SetTTL(ttl);
        }
    }
}

void KVIndexDocumentParserBase::SetDocInfo(const RawDocument& rawDoc, KVDocument* doc) const
{
    int64_t timestamp = rawDoc.getDocTimestamp(); // userts
    doc->SetUserTimestamp(timestamp);
    auto docInfo = doc->GetDocInfo();
    docInfo.timestamp = timestamp;
    doc->SetDocInfo(docInfo);
}

Status KVIndexDocumentParserBase::InitValueConvertor(const std::shared_ptr<config::ValueConfig>& valueConfig,
                                                     bool forcePackValue, bool parseDelete)
{
    auto r = CreateValueConvertor(valueConfig, forcePackValue, parseDelete);
    if (!r.IsOK()) {
        return r.steal_error();
    }
    _valueConvertor = std::move(r.steal_value());
    return Status::OK();
}

StatusOr<std::unique_ptr<ValueConvertor>>
KVIndexDocumentParserBase::CreateValueConvertor(const std::shared_ptr<config::ValueConfig>& valueConfig,
                                                bool forcePackValue, bool parseDelete) const
{
    auto valueConvertor = std::make_unique<ValueConvertor>(parseDelete);
    auto s = valueConvertor->Init(valueConfig, forcePackValue);
    if (!s.IsOK()) {
        return {s.steal_error()};
    }
    return {std::move(valueConvertor)};
}

bool KVIndexDocumentParserBase::NeedParseValue(DocOperateType opType) const { return opType != DELETE_DOC; }

ValueConvertor::ConvertResult KVIndexDocumentParserBase::ParseValue(const RawDocument& rawDoc,
                                                                    autil::mem_pool::Pool* pool) const
{
    return _valueConvertor->ConvertValue(rawDoc, pool);
}

} // namespace indexlibv2::document
