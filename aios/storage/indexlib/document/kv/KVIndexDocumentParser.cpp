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
#include "indexlib/document/kv/KVIndexDocumentParser.h"

#include "indexlib/document/RawDocument.h"
#include "indexlib/document/kv/KVDocument.h"
#include "indexlib/document/kv/KVKeyExtractor.h"
#include "indexlib/index/kv/config/KVIndexConfig.h"
#include "indexlib/util/DocTracer.h"
#include "indexlib/util/ErrorLogCollector.h"

namespace indexlibv2::document {

KVIndexDocumentParser::KVIndexDocumentParser(const std::shared_ptr<indexlib::util::AccumulativeCounter>& counter)
    : KVIndexDocumentParserBase(counter)
{
}

KVIndexDocumentParser::~KVIndexDocumentParser() = default;

Status KVIndexDocumentParser::Init(const std::shared_ptr<config::IIndexConfig>& indexConfig)
{
    _indexConfig = std::dynamic_pointer_cast<config::KVIndexConfig>(indexConfig);
    if (!_indexConfig) {
        return Status::InternalError("expect an KVIndexConfig, actual: [%s:%s]", indexConfig->GetIndexName().c_str(),
                                     indexConfig->GetIndexType().c_str());
    }
    _keyField = _indexConfig->GetFieldConfig();
    if (!_keyField) {
        return Status::ConfigError("no key field for [%s:%s]", indexConfig->GetIndexName().c_str(),
                                   indexConfig->GetIndexType().c_str());
    }
    _keyExtractor = std::make_unique<KVKeyExtractor>(_keyField->GetFieldType(), _indexConfig->UseNumberHash());
    return InitValueConvertor(_indexConfig->GetValueConfig(), false);
}

bool KVIndexDocumentParser::MaybeIgnore(const RawDocument& rawDoc) const
{
    autil::StringView fieldName(_keyField->GetFieldName());
    return !rawDoc.exist(fieldName) && _indexConfig->IgnoreEmptyPrimaryKey();
}

const std::shared_ptr<config::TTLSettings>& KVIndexDocumentParser::GetTTLSettings() const
{
    return _indexConfig->GetTTLSettings();
}

bool KVIndexDocumentParser::ParseKey(const RawDocument& rawDoc, KVDocument* doc) const
{
    autil::StringView fieldName(_keyField->GetFieldName());
    auto fieldValue = rawDoc.getField(autil::StringView(_keyField->GetFieldName()));
    if (_indexConfig->DenyEmptyPrimaryKey() && fieldValue.empty()) {
        const RawDocument* rawDocPtr = &rawDoc;
        ERROR_COLLECTOR_LOG(ERROR, "key value is empty!");
        IE_RAW_DOC_TRACE(rawDocPtr, "parse error: key value is empty");
        return false;
    }

    doc->SetPkField(fieldName, fieldValue);

    uint64_t keyHash = 0;
    _keyExtractor->HashKey(fieldValue, keyHash);
    doc->SetPKeyHash(keyHash);
    return true;
}

} // namespace indexlibv2::document
