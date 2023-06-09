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
#include "indexlib/document/kkv/KKVIndexDocumentParser.h"

#include "indexlib/document/RawDocument.h"
#include "indexlib/document/kkv/KKVDocument.h"
#include "indexlib/document/kkv/KKVKeysExtractor.h"
#include "indexlib/index/kkv/config/KKVIndexConfig.h"
#include "indexlib/util/DocTracer.h"
#include "indexlib/util/ErrorLogCollector.h"

namespace indexlibv2::document {
AUTIL_LOG_SETUP(indexlib.document, KKVIndexDocumentParser);

KKVIndexDocumentParser::KKVIndexDocumentParser(const std::shared_ptr<indexlib::util::AccumulativeCounter>& counter)
    : KVIndexDocumentParserBase(counter)
{
}

KKVIndexDocumentParser::~KKVIndexDocumentParser() = default;

Status KKVIndexDocumentParser::Init(const std::shared_ptr<config::IIndexConfig>& indexConfig)
{
    _indexConfig = std::dynamic_pointer_cast<config::KKVIndexConfig>(indexConfig);
    if (!_indexConfig) {
        return Status::InternalError("unexpected index:[%s:%s]", indexConfig->GetIndexName().c_str(),
                                     indexConfig->GetIndexType().c_str());
    }
    _keyExtractor = std::make_unique<KKVKeysExtractor>(_indexConfig);
    return InitValueConvertor(_indexConfig->GetValueConfig(), /* forcePackValue= */ true);
}

const std::shared_ptr<config::TTLSettings>& KKVIndexDocumentParser::GetTTLSettings() const
{
    return _indexConfig->GetTTLSettings();
}

bool KKVIndexDocumentParser::MaybeIgnore(const RawDocument& rawDoc) const
{
    const auto& pkeyFieldName = _indexConfig->GetPrefixFieldName();
    return !rawDoc.exist(autil::StringView(pkeyFieldName)) && _indexConfig->IgnoreEmptyPrimaryKey();
}

bool KKVIndexDocumentParser::ParseKey(const RawDocument& rawDoc, KVDocument* doc) const
{
    const auto& pkeyFieldName = _indexConfig->GetPrefixFieldName();

    if (pkeyFieldName.empty()) {
        ERROR_COLLECTOR_LOG(ERROR, "prefix key field is empty!");
        return true;
    }

    auto pkeyValue = rawDoc.getField(pkeyFieldName);
    if (_indexConfig->DenyEmptyPrimaryKey() && pkeyValue.empty()) {
        ERROR_COLLECTOR_LOG(ERROR, "prefix key value is empty!");
        return false;
    }

    uint64_t pkeyHash = 0;
    _keyExtractor->HashPrefixKey(pkeyValue, pkeyHash);

    const auto& skeyFieldName = _indexConfig->GetSuffixFieldName();
    auto skeyValue = rawDoc.getField(skeyFieldName);
    if (_indexConfig->DenyEmptySuffixKey() && skeyValue.empty()) {
        // delete message without skey means delete all in that pkey
        if (rawDoc.getDocOperateType() != DELETE_DOC) {
            ERROR_COLLECTOR_LOG(ERROR, "suffix key value is empty!");
            return false;
        }
    }

    uint64_t skeyHash = 0;
    bool hasSkey = false;
    if (!skeyValue.empty()) {
        _keyExtractor->HashSuffixKey(skeyValue, skeyHash);
        hasSkey = true;
    }

    auto opType = rawDoc.getDocOperateType();
    if (!hasSkey && opType == ADD_DOC) {
        ERROR_COLLECTOR_LOG(ERROR,
                            "suffix key field is empty for add doc, "
                            "which prefix key is [%s]!",
                            pkeyValue.c_str());
        return true;
    }

    // TODO(qisa.cb) seems not need now
    // if (hasSkey) {
    //     document->setIdentifier(pkValue + ":" + skeyValue);
    // } else {
    //     document->setIdentifier(pkValue);
    // }
    doc->SetPkField(autil::StringView(pkeyFieldName), autil::StringView(pkeyValue));
    doc->SetPKeyHash(pkeyHash);
    if (hasSkey) {
        doc->SetSKeyHash(skeyHash);
    }

    return true;
}

} // namespace indexlibv2::document
