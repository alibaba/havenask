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
#include "indexlib/document/kv_document/kv_index_document.h"

#include "indexlib/document/kv/KVDocument.h"

using namespace std;

namespace indexlib { namespace document {

IE_LOG_SETUP(document, KVIndexDocument);

KVIndexDocument::KVIndexDocument(KVIndexDocument&& other)
    : _opType(other._opType)
    , _pkeyHash(other._pkeyHash)
    , _skeyHash(other._skeyHash)
    , _hasSkey(other._hasSkey)
    , _value(other._value)
    , _regionId(other._regionId)
    , _timestamp(other._timestamp)
    , _userTimestamp(other._userTimestamp)
    , _ttl(other._ttl)
    , _indexNameHash(other._indexNameHash)
    , _pkFieldValue(other._pkFieldValue)
    , _pool(other._pool)
    , _pkFieldName(other._pkFieldName)
{
}

KVIndexDocument& KVIndexDocument::operator=(KVIndexDocument&& other)
{
    if (this != &other) {
        _opType = other._opType;

        _pkeyHash = other._pkeyHash;
        _skeyHash = other._skeyHash;
        _hasSkey = other._hasSkey;
        _value = other._value;
        _regionId = other._regionId;
        _timestamp = other._timestamp;
        _userTimestamp = other._userTimestamp;
        _ttl = other._ttl;
        _pool = other._pool;
        _pkFieldName = other._pkFieldName;
        _pkFieldValue = other._pkFieldValue;
        _indexNameHash = other._indexNameHash;
    }
    return *this;
}

void KVIndexDocument::Serialize(autil::DataBuffer& dataBuffer) const
{
    dataBuffer.write(_opType);
    dataBuffer.write(_pkeyHash);
    dataBuffer.write(_skeyHash);
    dataBuffer.write(_hasSkey);
    dataBuffer.write(_value);
    dataBuffer.write(_regionId);
    dataBuffer.write(_timestamp);
    dataBuffer.write(_userTimestamp);
    dataBuffer.write(_ttl);
}

void KVIndexDocument::SerializeIndexNameHash(autil::DataBuffer& dataBuffer) const { dataBuffer.write(_indexNameHash); }

void KVIndexDocument::SerializePKey(autil::DataBuffer& dataBuffer) const { dataBuffer.write(_pkFieldValue); }

void KVIndexDocument::Deserialize(autil::DataBuffer& dataBuffer)
{
    dataBuffer.read(_opType);
    dataBuffer.read(_pkeyHash);
    dataBuffer.read(_skeyHash);
    dataBuffer.read(_hasSkey);
    dataBuffer.read(_value, _pool);
    dataBuffer.read(_regionId);
    dataBuffer.read(_timestamp);
    dataBuffer.read(_userTimestamp);
    dataBuffer.read(_ttl);
}

void KVIndexDocument::DeserializeIndexNameHash(autil::DataBuffer& dataBuffer) { dataBuffer.read(_indexNameHash); }

void KVIndexDocument::DeserializePKey(autil::DataBuffer& dataBuffer) { dataBuffer.read(_pkFieldValue, _pool); }

std::string KVIndexDocument::GetIdentifier() const { return ""; }

size_t KVIndexDocument::EstimateMemory() const { return sizeof(*this); }

bool KVIndexDocument::CheckOrTrim(int64_t timestamp) const { return GetTimestamp() >= timestamp; }

KVIndexDocument KVIndexDocument::Clone(autil::mem_pool::Pool* pool) const
{
    KVIndexDocument ret(pool);
    ret._opType = _opType;
    ret._pkeyHash = _pkeyHash;
    ret._skeyHash = _skeyHash;
    ret._hasSkey = _hasSkey;
    ret._value = autil::MakeCString(_value, pool);
    ret._regionId = _regionId;
    ret._timestamp = _timestamp;
    ret._userTimestamp = _userTimestamp;
    ret._ttl = _ttl;
    return ret;
}

std::unique_ptr<indexlibv2::document::KVDocument>
KVIndexDocument::CreateKVDocumentV2(autil::mem_pool::Pool* pool,
                                    const indexlibv2::document::IDocument::DocInfo& docInfo,
                                    const indexlibv2::framework::Locator& locator) const
{
    auto ret = make_unique<indexlibv2::document::KVDocument>(pool);
    ret->_opType = _opType;
    ret->_pkeyHash = _pkeyHash;
    ret->_skeyHash = _skeyHash;
    ret->_hasSkey = _hasSkey;
    ret->_value = autil::MakeCString(_value, pool);
    ret->_userTimestamp = _userTimestamp;
    ret->_ttl = _ttl;
    ret->_indexNameHash = _indexNameHash;
    ret->_pool = pool;
    ret->_pkFieldName = autil::MakeCString(_pkFieldName, pool);
    ret->_pkFieldValue = autil::MakeCString(_pkFieldValue, pool);
    ret->_locator = locator;
    ret->_docInfo = docInfo;
    return ret;
}

}} // namespace indexlib::document
