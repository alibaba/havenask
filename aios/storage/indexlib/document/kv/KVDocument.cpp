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
#include "indexlib/document/kv/KVDocument.h"

#include "autil/DataBuffer.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/index/kv/Constant.h"

namespace indexlibv2::document {
AUTIL_LOG_SETUP(indexlib.document, KVDocument);

KVDocument::KVDocument(autil::mem_pool::Pool* pool) : _pool(pool), _docInfo(0, INVALID_TIMESTAMP) {}

KVDocument::~KVDocument() {}

const framework::Locator& KVDocument::GetLocatorV2() const { return _locator; }
void KVDocument::SetLocator(const framework::Locator& locator) { _locator = locator; }

docid_t KVDocument::GetDocId() const
{
    assert(false);
    return INVALID_DOCID;
}
KVDocument::KVDocument(KVDocument&& other)
    : _opType(other._opType)
    , _pkeyHash(other._pkeyHash)
    , _skeyHash(other._skeyHash)
    , _hasSkey(other._hasSkey)
    , _value(other._value)
    , _userTimestamp(other._userTimestamp)
    , _ttl(other._ttl)
    , _indexNameHash(other._indexNameHash)
    , _pool(other._pool)
    , _pkFieldName(other._pkFieldName)
    , _pkFieldValue(other._pkFieldValue)
    , _locator(other._locator)
    , _docInfo(other._docInfo)
    , _trace(other._trace)
{
}

KVDocument& KVDocument::operator=(KVDocument&& other)
{
    if (this != &other) {
        _opType = other._opType;

        _pkeyHash = other._pkeyHash;
        _skeyHash = other._skeyHash;
        _hasSkey = other._hasSkey;
        _value = other._value;
        _userTimestamp = other._userTimestamp;
        _ttl = other._ttl;
        _pool = other._pool;
        _pkFieldName = other._pkFieldName;
        _pkFieldValue = other._pkFieldValue;
        _indexNameHash = other._indexNameHash;
        _locator = other._locator;
        _docInfo = other._docInfo;
        _trace = other._trace;
    }
    return *this;
}

void KVDocument::serialize(autil::DataBuffer& dataBuffer) const
{
    dataBuffer.write(_opType);
    dataBuffer.write(_pkeyHash);
    dataBuffer.write(_skeyHash);
    dataBuffer.write(_hasSkey);
    dataBuffer.write(_value);
    // to be compatible
    typedef int32_t regionid_t;
    constexpr regionid_t DEFAULT_REGIONID = (regionid_t)0;
    regionid_t regionId = DEFAULT_REGIONID;
    dataBuffer.write(regionId);
    dataBuffer.write(_docInfo.timestamp);
    dataBuffer.write(_userTimestamp);
    dataBuffer.write(_ttl);
}

void KVDocument::SerializeIndexNameHash(autil::DataBuffer& dataBuffer) const { dataBuffer.write(_indexNameHash); }

void KVDocument::deserialize(autil::DataBuffer& dataBuffer)
{
    dataBuffer.read(_opType);
    dataBuffer.read(_pkeyHash);
    dataBuffer.read(_skeyHash);
    dataBuffer.read(_hasSkey);
    dataBuffer.read(_value, _pool);
    // to be compatible
    typedef int32_t regionid_t;
    regionid_t regionId;
    dataBuffer.read(regionId);
    dataBuffer.read(_docInfo.timestamp);
    dataBuffer.read(_userTimestamp);
    dataBuffer.read(_ttl);
}

void KVDocument::DeserializeIndexNameHash(autil::DataBuffer& dataBuffer) { dataBuffer.read(_indexNameHash); }

std::string KVDocument::GetIdentifier() const { return ""; }

size_t KVDocument::EstimateMemory() const { return sizeof(*this); }

bool KVDocument::CheckOrTrim(int64_t timestamp) const { return GetDocInfo().timestamp >= timestamp; }

std::unique_ptr<KVDocument> KVDocument::Clone(autil::mem_pool::Pool* pool) const
{
    auto ret = std::make_unique<KVDocument>(pool);
    ret->_opType = _opType;
    ret->_pkeyHash = _pkeyHash;
    ret->_skeyHash = _skeyHash;
    ret->_hasSkey = _hasSkey;
    ret->_value = autil::MakeCString(_value, pool);
    ret->_userTimestamp = _userTimestamp;
    ret->_ttl = _ttl;
    ret->_indexNameHash = _indexNameHash;
    ret->_locator = _locator;
    ret->_docInfo = _docInfo;
    ret->_trace = _trace;
    ret->_pkFieldName = autil::MakeCString(_pkFieldName, pool);
    ret->_pkFieldValue = autil::MakeCString(_pkFieldValue, pool);
    return ret;
}

autil::StringView KVDocument::GetTraceId() const
{
    if (!_trace) {
        return autil::StringView::empty_instance();
    }
    return _pkFieldValue;
}

autil::StringView KVDocument::GetSource() const { return autil::StringView(); }

int64_t KVDocument::GetIngestionTimestamp() const { return INVALID_TIMESTAMP; }

void KVDocument::SetPkField(const autil::StringView& name, const autil::StringView& value) noexcept
{
    _pkFieldName = autil::MakeCString(name, _pool);
    _pkFieldValue = autil::MakeCString(value, _pool);
}

void KVDocument::SetValue(autil::StringView value) noexcept { _value = autil::MakeCString(value, _pool); }

} // namespace indexlibv2::document
