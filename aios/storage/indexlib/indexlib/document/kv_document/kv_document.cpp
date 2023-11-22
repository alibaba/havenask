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
#include "indexlib/document/kv_document/kv_document.h"

#include "autil/ConstString.h"
#include "autil/EnvUtil.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/document/kv_document/legacy/kv_index_document.h"
#include "indexlib/document/raw_document/default_raw_document.h"
#include "indexlib/util/Exception.h"

namespace indexlib { namespace document {

IE_LOG_SETUP(document, KVDocument);

namespace {

static const size_t poolChunkSize = []() {
    size_t chunkSize = 10 * 1024; // 10k
    chunkSize = autil::EnvUtil::getEnv("indexlib_kv_doc_pool_chunk", chunkSize);
    return chunkSize;
}();

}

KVDocument::KVDocument()
    : _unsafePool(new autil::mem_pool::UnsafePool(poolChunkSize))
    , _docs(DocAllocator(_unsafePool.get()))
    , _hasFormatError(false)
    , _needDeserializeFromLegacy(false)
{
    mOpType = ADD_DOC;
}

KVDocument::KVDocument(const document::KeyMapManagerPtr& keyMapManager)
    : _unsafePool(new autil::mem_pool::UnsafePool(poolChunkSize))
    , _docs(DocAllocator(_unsafePool.get()))
    , _hasFormatError(false)
    , _keyMapManager(keyMapManager)
    , _needDeserializeFromLegacy(false)
{
}

KVDocument::~KVDocument() { _docs.clear(); }

KVDocument::KVDocument(KVDocument&& other)
    : document::RawDocument(std::move(other))
    , _unsafePool(std::move(other._unsafePool))
    , _docs(std::move(other._docs))
    , _hasFormatError(other._hasFormatError)
    , _keyMapManager(std::move(other._keyMapManager))
    , _rawDoc(std::move(other._rawDoc))
    , _needDeserializeFromLegacy(other._needDeserializeFromLegacy)
{
}

KVDocument& KVDocument::operator=(KVDocument&& other)
{
    if (this != &other) {
        _rawDoc.reset();
        _docs.clear();

        _rawDoc = std::move(other._rawDoc);
        _keyMapManager = std::move(other._keyMapManager);
        _docs = std::move(other._docs);
        _hasFormatError = other._hasFormatError;
        _needDeserializeFromLegacy = other._needDeserializeFromLegacy;
        _unsafePool = std::move(other._unsafePool);
    }
    return *this;
}

bool KVDocument::Init(const document::DocumentInitParamPtr& initParam) { return true; }

void KVDocument::setField(const autil::StringView& fieldName, const autil::StringView& fieldValue)
{
    EnsureRawDoc();
    _rawDoc->setField(fieldName, fieldValue);
}

void KVDocument::setFieldNoCopy(const autil::StringView& fieldName, const autil::StringView& fieldValue)
{
    EnsureRawDoc();
    _rawDoc->setFieldNoCopy(fieldName, fieldValue);
}

const autil::StringView& KVDocument::getField(const autil::StringView& fieldName) const
{
    if (_rawDoc) {
        const auto& ret = _rawDoc->getField(fieldName);
        if (!ret.empty() || _docs.size() != 1u) {
            return ret;
        }
    }
    if (_docs.size() == 1u && fieldName == _docs[0].GetPkFieldName()) {
        return _docs[0].GetPkFieldValue();
    }
    return autil::StringView::empty_instance();
}

RawDocFieldIterator* KVDocument::CreateIterator() const
{
    if (_rawDoc) {
        return _rawDoc->CreateIterator();
    }
    return nullptr;
}

bool KVDocument::exist(const autil::StringView& fieldName) const
{
    if (_rawDoc) {
        return _rawDoc->exist(fieldName);
    }
    return false;
}

void KVDocument::eraseField(const autil::StringView& fieldName)
{
    if (_rawDoc) {
        return _rawDoc->eraseField(fieldName);
    }
}

uint32_t KVDocument::getFieldCount() const
{
    if (_rawDoc) {
        return _rawDoc->getFieldCount();
    }
    return 0;
}

void KVDocument::clear()
{
    _docs.clear();
    _unsafePool->reset();
    if (_rawDoc) {
        _rawDoc->clear();
    }
}

KVDocument* KVDocument::clone() const
{
    if (_rawDoc) {
        auto newRawDoc = _rawDoc->clone();
        auto doc = new KVDocument();
        doc->_rawDoc.reset(newRawDoc);
        return doc;
    }
    assert(false);
    return nullptr;
}

KVDocument* KVDocument::CloneKVDocument() const
{
    auto newDoc = new KVDocument();
    *static_cast<document::Document*>(newDoc) = *static_cast<const document::Document*>(this);
    *static_cast<document::RawDocument*>(newDoc) = *static_cast<const document::RawDocument*>(this);

    for (size_t i = 0; i < _docs.size(); ++i) {
        newDoc->_docs.emplace_back(_docs[i].Clone(newDoc->_unsafePool.get()));
    }
    return newDoc;
}

KVDocument* KVDocument::createNewDocument() const
{
    auto doc = new KVDocument(_keyMapManager);
    if (_rawDoc) {
        doc->EnsureRawDoc();
    }
    return doc;
}

std::string KVDocument::getIdentifier() const
{
    std::string ret;
    for (const auto& doc : _docs) {
        ret += doc.GetIdentifier() + " ";
    }
    return ret;
}

std::string KVDocument::toString() const
{
    if (_rawDoc) {
        return _rawDoc->toString();
    }
    if (!_docs.empty()) {
        const auto& pkValue = _docs[0].GetPkFieldValue();
        return std::string(pkValue.data(), pkValue.size());
    }
    return "";
}

int64_t KVDocument::GetUserTimestamp() const
{
    if (!_docs.empty()) {
        return _docs.back().GetUserTimestamp();
    }
    return INVALID_TIMESTAMP;
}

size_t KVDocument::EstimateMemory() const
{
    auto ret = sizeof(*this);
    size_t pageSize = getpagesize();
    size_t pageAlignSize = (_unsafePool->getUsedBytes() + pageSize - 1) / pageSize * pageSize;
    ret += pageAlignSize;

    if (_rawDoc) {
        ret += _rawDoc->EstimateMemory();
    }
    for (const auto& doc : _docs) {
        ret += doc.EstimateMemory();
    }
    return ret;
}

bool KVDocument::CheckOrTrim(int64_t timestamp)
{
    if (_docs.empty() || !_docs.back().CheckOrTrim(timestamp)) {
        return false;
    }

    if (_docs.begin()->CheckOrTrim(timestamp)) {
        return true;
    }
    DocAllocator allocator(_unsafePool.get());
    DocVector trimedDocs(allocator);
    trimedDocs.reserve(_docs.size());
    for (auto& doc : _docs) {
        if (doc.CheckOrTrim(timestamp)) {
            trimedDocs.push_back(std::move(doc));
        }
    }
    _docs.swap(trimedDocs);
    setDocTimestamp(_docs.back().GetTimestamp());
    return true;
}

KVIndexDocument* KVDocument::CreateOneDoc()
{
    _docs.emplace_back(_unsafePool.get());
    return &_docs.back();
}

void KVDocument::EraseLastDoc()
{
    assert(!_docs.empty());
    _docs.pop_back();
}

void KVDocument::DoSerialize(autil::DataBuffer& dataBuffer, uint32_t serializedVersion) const
{
    assert(serializedVersion >= KVIndexDocument::KV_DOCUMENT_BINARY_VERSION);

    // serialize document
    uint32_t docCount = _docs.size();
    dataBuffer.write(docCount);
    for (const auto& doc : _docs) {
        doc.Serialize(dataBuffer);
    }

    // serialize index name hash and pkey
    if (serializedVersion >= KVIndexDocument::STORE_PKEY_AND_MULTI_INDEX_BINARY_VERSION) {
        for (const auto& doc : _docs) {
            doc.SerializeIndexNameHash(dataBuffer);
        }
        dataBuffer.write(_needStorePKeyValue);
        if (_needStorePKeyValue) {
            for (const auto& doc : _docs) {
                doc.SerializePKey(dataBuffer);
            }
        }
    }
    // serialize tag
    if (serializedVersion >= KVIndexDocument::STORE_TAG_INFO_MAP_VERSION) {
        std::string source = TagInfoToString();
        dataBuffer.write(source);
    }
}

void KVDocument::DoDeserialize(autil::DataBuffer& dataBuffer, uint32_t serializedVersion)
{
    if (serializedVersion > KVIndexDocument::LEGACY_KV_DOCUMENT_BINARY_VERSION_THRESHOLD) {
        _docs.clear();

        // deserialize document
        uint32_t docCount = 0;
        dataBuffer.read(docCount);
        for (size_t i = 0; i < docCount; ++i) {
            auto kvIndexDoc = CreateOneDoc();
            kvIndexDoc->Deserialize(dataBuffer);
        }
        if (!_docs.empty()) {
            SetTimestamp(back().GetTimestamp());
            SetDocOperateType(ADD_DOC);
        }

        // deserialize index name hash and pkey
        if (serializedVersion >= KVIndexDocument::STORE_PKEY_AND_MULTI_INDEX_BINARY_VERSION) {
            for (auto& doc : _docs) {
                doc.DeserializeIndexNameHash(dataBuffer);
            }
            dataBuffer.read(_needStorePKeyValue);
            if (_needStorePKeyValue) {
                for (auto& doc : _docs) {
                    doc.DeserializePKey(dataBuffer);
                }
            }
        }
        // deserialize tag
        if (serializedVersion >= KVIndexDocument::STORE_TAG_INFO_MAP_VERSION) {
            std::string source;
            dataBuffer.read(source);
            TagInfoFromString(source);
        }
    } else {
        _needDeserializeFromLegacy = true;
    }
}

void KVDocument::extractFieldNames(std::vector<autil::StringView>& fieldNames) const
{
    assert(0);
    INDEXLIB_THROW(util::UnSupportedException, "kv document does not support extracFieldNames");
}

}} // namespace indexlib::document
