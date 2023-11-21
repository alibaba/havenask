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

#include <memory>

#include "autil/DataBuffer.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/common_define.h"
#include "indexlib/document/document.h"
#include "indexlib/document/locator.h"
#include "indexlib/indexlib.h"

namespace indexlibv2::document {
class KVDocument;
}

namespace indexlib { namespace document {

// class KVIndexDocument : public document::Document
class KVIndexDocument
{
public:
    static constexpr uint32_t LEGACY_KV_DOCUMENT_BINARY_VERSION_THRESHOLD = 10000;
    static constexpr uint32_t KV_DOCUMENT_BINARY_VERSION = LEGACY_KV_DOCUMENT_BINARY_VERSION_THRESHOLD + 1;
    // store pkey version contains multi index and store pkey.
    static constexpr uint32_t STORE_PKEY_AND_MULTI_INDEX_BINARY_VERSION = KV_DOCUMENT_BINARY_VERSION + 1;
    // store tag info map
    static constexpr uint32_t STORE_TAG_INFO_MAP_VERSION = STORE_PKEY_AND_MULTI_INDEX_BINARY_VERSION + 1;

public:
    KVIndexDocument(autil::mem_pool::Pool* pool) : _indexNameHash(0), _pool(pool) {}
    ~KVIndexDocument() {}

    KVIndexDocument(const KVIndexDocument&) = delete;
    KVIndexDocument& operator=(const KVIndexDocument&) = delete;

    KVIndexDocument(KVIndexDocument&& other);
    KVIndexDocument& operator=(KVIndexDocument&& other);

public:
    // set & get
    void SetDocOperateType(DocOperateType type) noexcept { _opType = type; }
    DocOperateType GetDocOperateType() const noexcept { return _opType; }
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
    void SetValue(autil::StringView value) noexcept { _value = autil::MakeCString(value, _pool); }
    autil::StringView GetValue() const noexcept { return _value; }
    void SetRegionId(regionid_t regionId) noexcept { _regionId = regionId; }
    regionid_t GetRegionId() const noexcept { return _regionId; }
    void SetTimestamp(int64_t timestamp) noexcept { _timestamp = timestamp; }
    int64_t GetTimestamp() const noexcept { return _timestamp; }
    void SetUserTimestamp(int64_t timestamp) noexcept { _userTimestamp = timestamp; }
    int64_t GetUserTimestamp() const noexcept { return _userTimestamp; }
    void SetTTL(uint32_t ttl) noexcept { _ttl = ttl; }
    uint32_t GetTTL() const noexcept { return _ttl; }

    autil::mem_pool::Pool* GetPool() const { return _pool; }
    KVIndexDocument Clone(autil::mem_pool::Pool* pool) const;

    void SetPkField(const autil::StringView& name, const autil::StringView& value)
    {
        _pkFieldName = autil::MakeCString(name, _pool);
        _pkFieldValue = autil::MakeCString(value, _pool);
    }

    const autil::StringView& GetPkFieldName() const { return _pkFieldName; }
    const autil::StringView& GetPkFieldValue() const { return _pkFieldValue; }

    std::string GetIdentifier() const;
    void Serialize(autil::DataBuffer& dataBuffer) const;
    void Deserialize(autil::DataBuffer& dataBuffer);
    void SerializeIndexNameHash(autil::DataBuffer& dataBuffer) const;
    void DeserializeIndexNameHash(autil::DataBuffer& dataBuffer);
    void SerializePKey(autil::DataBuffer& dataBuffer) const;
    void DeserializePKey(autil::DataBuffer& dataBuffer);
    size_t EstimateMemory() const;
    bool CheckOrTrim(int64_t timestamp) const;

    uint64_t GetIndexNameHash() const { return _indexNameHash; }
    void SetIndexNameHash(uint64_t indexNameHash) { _indexNameHash = indexNameHash; }

    std::unique_ptr<indexlibv2::document::KVDocument>
    CreateKVDocumentV2(autil::mem_pool::Pool* pool, const indexlibv2::framework::Locator::DocInfo& docInfo,
                       const indexlibv2::framework::Locator& locator) const;

private:
    // serialize/deserialize
    DocOperateType _opType = ADD_DOC;
    uint64_t _pkeyHash = 0;
    uint64_t _skeyHash = 0;
    bool _hasSkey = false;
    autil::StringView _value;
    regionid_t _regionId = DEFAULT_REGIONID;
    int64_t _timestamp = INVALID_TIMESTAMP;
    int64_t _userTimestamp = INVALID_TIMESTAMP;
    uint32_t _ttl = UNINITIALIZED_TTL;

    // optional serialize/deserialize
    uint64_t _indexNameHash;
    autil::StringView _pkFieldValue;

    // do not serialize/deserialize
    autil::mem_pool::Pool* _pool = nullptr;
    autil::StringView _pkFieldName;

private:
    IE_LOG_DECLARE();
};

}} // namespace indexlib::document
