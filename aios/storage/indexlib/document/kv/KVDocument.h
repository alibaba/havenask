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
#pragma once

#include "autil/Log.h"
#include "indexlib/document/IDocument.h"

namespace autil {
class DataBuffer;
}

namespace autil::mem_pool {
class Pool;
}

namespace indexlib::document {
class KVIndexDocument;
}

namespace indexlibv2::document {

class KVDocument : public IDocument
{
public:
    static constexpr uint32_t LEGACY_KV_DOCUMENT_BINARY_VERSION_THRESHOLD = 10000;
    static constexpr uint32_t KV_DOCUMENT_BINARY_VERSION = LEGACY_KV_DOCUMENT_BINARY_VERSION_THRESHOLD + 1;
    static constexpr uint32_t MULTI_INDEX_KV_DOCUMENT_BINARY_VERSION = KV_DOCUMENT_BINARY_VERSION + 1;
    static constexpr uint32_t SCHEMA_ID_KV_DOCUMENT_BINARY_VERSION = MULTI_INDEX_KV_DOCUMENT_BINARY_VERSION + 1;

public:
    explicit KVDocument(autil::mem_pool::Pool* pool);
    ~KVDocument();

    KVDocument(const KVDocument&) = delete;
    KVDocument& operator=(const KVDocument&) = delete;

    KVDocument(KVDocument&& other);
    KVDocument& operator=(KVDocument&& other);

public:
    const framework::Locator& GetLocatorV2() const override;
    void SetLocator(const framework::Locator& locator) override;
    DocInfo GetDocInfo() const override { return _docInfo; }
    void SetDocInfo(const DocInfo& docInfo) override { _docInfo = docInfo; }
    uint32_t GetTTL() const override { return _ttl; }
    docid_t GetDocId() const override;
    size_t EstimateMemory() const override;
    DocOperateType GetDocOperateType() const override { return _opType; }
    void SetDocOperateType(DocOperateType type) override { _opType = type; }
    bool HasFormatError() const override { return _hasFormatError; }
    void SetTrace(bool trace) override { _trace = trace; }
    autil::StringView GetTraceId() const override;
    autil::StringView GetSource() const override;
    int64_t GetIngestionTimestamp() const override;

public:
    void SetPKeyHash(uint64_t key) noexcept { _pkeyHash = key; }
    uint64_t GetPKeyHash() const noexcept { return _pkeyHash; }
    void SetSKeyHash(uint64_t key) noexcept
    {
        _hasSkey = true;
        _skeyHash = key;
    }
    uint64_t GetSKeyHash() const noexcept { return _skeyHash; }
    bool HasSKey() const noexcept { return _hasSkey; }
    void SetValueNoCopy(autil::StringView value) noexcept { _value = value; }
    void SetValue(autil::StringView value) noexcept;
    autil::StringView GetValue() const noexcept { return _value; }
    void SetUserTimestamp(int64_t timestamp) noexcept { _userTimestamp = timestamp; }
    int64_t GetUserTimestamp() const noexcept { return _userTimestamp; }
    void SetTTL(uint32_t ttl) noexcept { _ttl = ttl; }

    autil::mem_pool::Pool* GetPool() const { return _pool; }
    void SetPool(autil::mem_pool::Pool* pool) { _pool = pool; }
    std::unique_ptr<KVDocument> Clone(autil::mem_pool::Pool* pool) const;

    void SetPkField(const autil::StringView& name, const autil::StringView& value) noexcept;
    void SetFormatError() { _hasFormatError = true; }

    const autil::StringView& GetPkFieldName() const { return _pkFieldName; }
    const autil::StringView& GetPkFieldValue() const { return _pkFieldValue; }

    std::string GetIdentifier() const;
    void serialize(autil::DataBuffer& dataBuffer) const override;
    void deserialize(autil::DataBuffer& dataBuffer) override;
    void SerializeIndexNameHash(autil::DataBuffer& dataBuffer) const;
    void DeserializeIndexNameHash(autil::DataBuffer& dataBuffer);
    void SerializeSchemaId(autil::DataBuffer& dataBuffer) const;
    void DeserializeSchemaId(autil::DataBuffer& dataBuffer);
    bool CheckOrTrim(int64_t timestamp) const;

    uint64_t GetIndexNameHash() const { return _indexNameHash; }
    void SetIndexNameHash(uint64_t indexNameHash) { _indexNameHash = indexNameHash; }

    schemaid_t GetSchemaId() const;
    void SetSchemaId(schemaid_t schemaId);

private:
    DocOperateType _opType = ADD_DOC;
    uint64_t _pkeyHash = 0;
    uint64_t _skeyHash = 0;
    bool _hasSkey = false;
    autil::StringView _value;
    int64_t _userTimestamp = INVALID_TIMESTAMP;
    uint32_t _ttl = UNINITIALIZED_TTL;

    // serailize/deserialize in KVDocumentBatch
    uint64_t _indexNameHash = 0;
    schemaid_t _schemaId = DEFAULT_SCHEMAID;

    // do not serialize/deserialize
    autil::mem_pool::Pool* _pool = nullptr;
    autil::StringView _pkFieldName;
    autil::StringView _pkFieldValue;
    indexlibv2::framework::Locator _locator;

    DocInfo _docInfo;
    bool _hasFormatError = false;
    bool _trace = false; // used for doc trace

private:
    friend class indexlib::document::KVIndexDocument;
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::document
