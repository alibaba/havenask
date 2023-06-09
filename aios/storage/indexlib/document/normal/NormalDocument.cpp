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
#include "indexlib/document/normal/NormalDocument.h"

#include <fstream>

#include "autil/EnvUtil.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/util/Exception.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;
using namespace indexlib::document;

namespace {
static const bool ENABLE_SIMPLE_TAG_INFO_DECODE = []() {
    return autil::EnvUtil::getEnv("indexlib_enable_simple_tag_info_decode", 1);
}();
}

namespace indexlibv2 { namespace document {
AUTIL_LOG_SETUP(indexlib.document, NormalDocument);

NormalDocument::NormalDocument() : _pool(std::make_shared<autil::mem_pool::Pool>(1024)), _docInfo(0, INVALID_TIMESTAMP)
{
    assert(_pool);
}

NormalDocument::NormalDocument(const std::shared_ptr<autil::mem_pool::Pool>& pool)
    : _pool(pool)
    , _docInfo(0, INVALID_TIMESTAMP)
{
    assert(_pool);
}

NormalDocument::~NormalDocument()
{
    _indexDocument.reset();
    _attributeDocument.reset();
    _summaryDocument.reset();
    _sourceDocument.reset();

    _pool.reset();
}

Status NormalDocument::SetSerializedVersion(uint32_t version)
{
    uint32_t docVersion = GetDocBinaryVersion();
    if (version != INVALID_DOC_VERSION && version > docVersion) {
        RETURN_IF_STATUS_ERROR(Status::InvalidArgs(),
                               "target serializedVersion [%u] bigger than doc binary version [%u]", version,
                               docVersion);
    }
    _serializedVersion = version;
    return Status::OK();
}

uint32_t NormalDocument::GetSerializedVersion() const
{
    if (_serializedVersion != INVALID_DOC_VERSION) {
        return _serializedVersion;
    }
    _serializedVersion = GetDocBinaryVersion();
    return _serializedVersion;
}

void NormalDocument::AddTag(const string& key, const string& value) { _tagInfo[key] = value; }

const string& NormalDocument::GetTag(const string& key) const
{
    auto iter = _tagInfo.find(key);
    if (iter != _tagInfo.end()) {
        return iter->second;
    }
    static string emptyString;
    return emptyString;
}

string NormalDocument::TagInfoToString() const
{
    if (_tagInfo.empty()) {
        return string("");
    }
    if (_tagInfo.size() == 1 && _tagInfo.find(DOCUMENT_SOURCE_TAG_KEY) != _tagInfo.end()) {
        // legacy format for mSource
        return GetTag(DOCUMENT_SOURCE_TAG_KEY);
    }
    return autil::legacy::FastToJsonString(_tagInfo, true);
}

void NormalDocument::TagInfoFromString(const string& str)
{
    _tagInfo.clear();
    if (str.empty()) {
        return;
    }
    if (*str.begin() == '{') {
        if (ENABLE_SIMPLE_TAG_INFO_DECODE) {
            SimpleTagInfoDecode(str, &_tagInfo);
        } else {
            autil::legacy::FastFromJsonString(_tagInfo, str);
        }
        return;
    }
    AddTag(DOCUMENT_SOURCE_TAG_KEY, str);
}

void NormalDocument::SimpleTagInfoDecode(const std::string& str, TagInfoMap* tagInfo)
{
    size_t sz = str.size();
    size_t start = 0;
    size_t end = 0;
    bool inString = false;
    std::string key;
    std::string value;
    for (size_t i = 0; i < sz; ++i) {
        char c = str[i];
        if (c == ':') {
            key = str.substr(start, end - start);
        } else if (c == ',' or c == '}') {
            value = str.substr(start, end - start);
            (*tagInfo)[std::move(key)] = std::move(value);
            key.clear();
            value.clear();
        } else if (c == '"') {
            if (!inString) {
                start = i + 1;
            } else {
                end = i;
            }
            inString = !inString;
        }
    }
}

bool NormalDocument::IsUserDoc() const
{
    return _opType == UNKNOWN_OP || _opType == ADD_DOC || _opType == DELETE_DOC || _opType == UPDATE_FIELD ||
           _opType == SKIP_DOC || _opType == DELETE_SUB_DOC;
}

void NormalDocument::serialize(autil::DataBuffer& dataBuffer) const
{
    uint32_t serializeVersion = GetSerializedVersion();
    dataBuffer.write(serializeVersion);
    DoSerialize(dataBuffer, serializeVersion);
}

void NormalDocument::deserialize(autil::DataBuffer& dataBuffer)
{
    dataBuffer.read(_serializedVersion);
    DoDeserialize(dataBuffer, _serializedVersion);
}

bool NormalDocument::operator==(const NormalDocument& doc) const
{
    if (this == &doc) {
        return true;
    }

    if (GetSerializedVersion() != doc.GetSerializedVersion()) {
        return false;
    }

    if (_docInfo.hashId != doc._docInfo.hashId || _docInfo.timestamp != doc._docInfo.timestamp) {
        return false;
    }
    if (_tagInfo != doc._tagInfo) {
        return false;
    }
    if (_locatorV2.IsFasterThan(doc._locatorV2, false) !=
            indexlibv2::framework::Locator::LocatorCompareResult::LCR_FULLY_FASTER ||
        doc._locatorV2.IsFasterThan(_locatorV2, false) !=
            indexlibv2::framework::Locator::LocatorCompareResult::LCR_FULLY_FASTER) {
        return false;
    }
    if (_opType != doc._opType) {
        return false;
    }
    if (_ttl != doc._ttl) {
        return false;
    }
    if (_originalOpType != doc._originalOpType) {
        return false;
    }

    // index document
    if (_indexDocument) {
        if (doc._indexDocument == NULL || *_indexDocument != *(doc._indexDocument)) {
            return false;
        }
    } else if (doc._indexDocument != NULL) {
        return false;
    }

    // attribute document
    if (_attributeDocument) {
        if (doc._attributeDocument == NULL || *_attributeDocument != *(doc._attributeDocument)) {
            return false;
        }
    } else if (doc._attributeDocument != NULL) {
        return false;
    }

    // summary document
    if (_summaryDocument) {
        if (doc._summaryDocument == NULL || *_summaryDocument != *(doc._summaryDocument)) {
            return false;
        }
    } else if (doc._summaryDocument != NULL) {
        return false;
    }

    if (_sourceDocument) {
        if (doc._sourceDocument == NULL || *_sourceDocument != *(doc._sourceDocument)) {
            return false;
        }
    } else if (doc._sourceDocument != NULL) {
        return false;
    }

    if (_modifiedFields.size() != doc._modifiedFields.size()) {
        return false;
    } else {
        for (size_t i = 0; i < _modifiedFields.size(); ++i) {
            if (_modifiedFields[i] != doc._modifiedFields[i]) {
                return false;
            }
        }
    }

    if (_userTimestamp != doc._userTimestamp) {
        return false;
    }
    if (_segmentIdBeforeModified != doc._segmentIdBeforeModified) {
        return false;
    }
    return true;
}

void NormalDocument::DoSerialize(DataBuffer& dataBuffer, uint32_t serializedVersion) const
{
    switch (serializedVersion) {
    case DOCUMENT_BINARY_VERSION:
        serializeVersion7(dataBuffer);
        serializeVersion8(dataBuffer);
        serializeVersion9(dataBuffer);
        serializeVersion10(dataBuffer);
        serializeVersion11(dataBuffer);
        break;
    case 10:
        serializeVersion7(dataBuffer);
        serializeVersion8(dataBuffer);
        serializeVersion9(dataBuffer);
        serializeVersion10(dataBuffer);
        break;
    case 9:
        serializeVersion7(dataBuffer);
        serializeVersion8(dataBuffer);
        serializeVersion9(dataBuffer);
        break;
    case 8:
        serializeVersion7(dataBuffer);
        serializeVersion8(dataBuffer);
        break;
    case 7:
        serializeVersion7(dataBuffer);
        break;
    case 6:
        serializeVersion6(dataBuffer);
        break;
    case 5:
        serializeVersion5(dataBuffer);
        break;
    case 4:
        serializeVersion4(dataBuffer);
        break;
    case 3:
        serializeVersion3(dataBuffer);
        break;

    default:
        INDEXLIB_THROW(indexlib::util::BadParameterException,
                       "document serialize failed, do not support serialize document with version %u",
                       _serializedVersion);
    }
}

void NormalDocument::serializeVersion3(DataBuffer& dataBuffer) const
{
    dataBuffer.write(_indexDocument);
    dataBuffer.write(_attributeDocument);
    dataBuffer.write(_summaryDocument);
    // not support sub document yet, make sure the serialized data is compatile
    DocumentVector subDocuments;
    dataBuffer.write(subDocuments);
    dataBuffer.write(_modifiedFields);
    FieldIdVector subModifiedFields;
    dataBuffer.write(subModifiedFields);
    dataBuffer.write(_docInfo.timestamp);

    string source = TagInfoToString();
    dataBuffer.write(source);
    std::string locatorStr;
    if (_locatorV2.IsValid()) {
        locatorStr = _locatorV2.Serialize();
    }
    dataBuffer.write(locatorStr);
    dataBuffer.write(_opType);
    dataBuffer.write(_originalOpType);
}

void NormalDocument::serializeVersion4(DataBuffer& dataBuffer) const
{
    // add kv index document
    dataBuffer.write(_indexDocument);
    dataBuffer.write(_attributeDocument);
    dataBuffer.write(_summaryDocument);
    // not support sub document yet, make sure the serialized data is compatile
    DocumentVector subDocuments;
    dataBuffer.write(subDocuments);
    dataBuffer.write(_modifiedFields);
    FieldIdVector subModifiedFields;
    dataBuffer.write(subModifiedFields);
    dataBuffer.write(_docInfo.timestamp);

    string source = TagInfoToString();
    dataBuffer.write(source);
    std::string locatorStr;
    if (_locatorV2.IsValid()) {
        locatorStr = _locatorV2.Serialize();
    }
    dataBuffer.write(locatorStr);
    dataBuffer.write(_opType);
    dataBuffer.write(_originalOpType);
    // write empty kv index document, to be compatible
    // use empty attribute ptr to avoid dependency of kv index document
    std::shared_ptr<AttributeDocument> null;
    dataBuffer.write(null);
}

void NormalDocument::serializeVersion5(DataBuffer& dataBuffer) const
{
    // add user timestamp
    dataBuffer.write(_indexDocument);
    dataBuffer.write(_attributeDocument);
    dataBuffer.write(_summaryDocument);
    // not support sub document yet, make sure the serialized data is compatile
    DocumentVector subDocuments;
    dataBuffer.write(subDocuments);
    dataBuffer.write(_modifiedFields);
    FieldIdVector subModifiedFields;
    dataBuffer.write(subModifiedFields);
    dataBuffer.write(_docInfo.timestamp);
    bool isValidUserTS = _userTimestamp != INVALID_TIMESTAMP;
    dataBuffer.write(isValidUserTS);
    if (isValidUserTS) {
        dataBuffer.write(_userTimestamp);
    }

    string source = TagInfoToString();
    dataBuffer.write(source);
    std::string locatorStr;
    if (_locatorV2.IsValid()) {
        locatorStr = _locatorV2.Serialize();
    }
    dataBuffer.write(locatorStr);
    dataBuffer.write(_opType);
    dataBuffer.write(_originalOpType);
    // write empty kv index document, to be compatible
    // use empty attribute ptr to avoid dependency of kv index document
    std::shared_ptr<AttributeDocument> null;
    dataBuffer.write(null);
}

void NormalDocument::serializeVersion6(DataBuffer& dataBuffer) const
{
    // add regionId
    dataBuffer.write(_indexDocument);
    dataBuffer.write(_attributeDocument);
    dataBuffer.write(_summaryDocument);
    // not support sub document yet, make sure the serialized data is compatile
    DocumentVector subDocuments;
    dataBuffer.write(subDocuments);
    dataBuffer.write(_modifiedFields);
    FieldIdVector subModifiedFields;
    dataBuffer.write(subModifiedFields);
    dataBuffer.write(_docInfo.timestamp);
    bool isValidUserTS = _userTimestamp != INVALID_TIMESTAMP;
    dataBuffer.write(isValidUserTS);
    if (isValidUserTS) {
        dataBuffer.write(_userTimestamp);
    }
    dataBuffer.write((int32_t)0); // DEFAULT_REGIONID

    string source = TagInfoToString();
    dataBuffer.write(source);
    std::string locatorStr;
    if (_locatorV2.IsValid()) {
        locatorStr = _locatorV2.Serialize();
    }
    dataBuffer.write(locatorStr);
    dataBuffer.write(_opType);
    dataBuffer.write(_originalOpType);
    // write empty kv index document, to be compatible
    // use empty attribute ptr to avoid dependency of kv index document
    std::shared_ptr<AttributeDocument> null;
    dataBuffer.write(null);
}

void NormalDocument::serializeVersion7(DataBuffer& dataBuffer) const
{
    // add schemaId & trace
    dataBuffer.write(_indexDocument);
    dataBuffer.write(_attributeDocument);
    dataBuffer.write(_summaryDocument);
    // not support sub document yet, make sure the serialized data is compatile
    DocumentVector subDocuments;
    dataBuffer.write(subDocuments);
    dataBuffer.write(_modifiedFields);
    FieldIdVector subModifiedFields;
    dataBuffer.write(subModifiedFields);
    dataBuffer.write(_docInfo.timestamp);
    bool isValidUserTS = _userTimestamp != INVALID_TIMESTAMP;
    dataBuffer.write(isValidUserTS);
    if (isValidUserTS) {
        dataBuffer.write(_userTimestamp);
    }
    dataBuffer.write((int32_t)0); // DEFAULT_REGIONID

    string source = TagInfoToString();
    dataBuffer.write(source);
    std::string locatorStr;
    if (_locatorV2.IsValid()) {
        locatorStr = _locatorV2.Serialize();
    }
    dataBuffer.write(locatorStr);
    dataBuffer.write(_opType);
    dataBuffer.write(_originalOpType);
    // write empty kv index document, to be compatible
    // use empty attribute ptr to avoid dependency of kv index document
    std::shared_ptr<AttributeDocument> null;
    dataBuffer.write(null);

    dataBuffer.write(DEFAULT_SCHEMAID);
    dataBuffer.write(_trace);
}

void NormalDocument::deserializeVersion3(DataBuffer& dataBuffer)
{
    assert(_pool);
    uint32_t documentVersion = 3;
    _indexDocument.reset(deserializeObject<IndexDocument>(dataBuffer, _pool.get(), documentVersion));
    _attributeDocument.reset(deserializeObject<AttributeDocument>(dataBuffer, _pool.get(), documentVersion));
    _summaryDocument.reset(deserializeObject<SerializedSummaryDocument>(dataBuffer, documentVersion));
    // not support sub document yet, make sure the serialized data is compatile
    DocumentVector subDocuments;
    dataBuffer.read(subDocuments);
    dataBuffer.read(_modifiedFields);
    FieldIdVector subModifiedFields;
    dataBuffer.read(subModifiedFields);
    dataBuffer.read(_docInfo.timestamp);

    string source;
    dataBuffer.read(source);
    TagInfoFromString(source);

    string locatorStr;
    dataBuffer.read(locatorStr);
    if (!locatorStr.empty() && !_locatorV2.Deserialize(locatorStr)) {
        INDEXLIB_THROW(indexlib::util::RuntimeException,
                       "document serialize failed, do not support serialize document with version %u",
                       _serializedVersion);
    }
    dataBuffer.read(_opType);
    dataBuffer.read(_originalOpType);
}

void NormalDocument::deserializeVersion4(DataBuffer& dataBuffer)
{
    assert(_pool);
    uint32_t documentVersion = 4;
    _indexDocument.reset(deserializeObject<IndexDocument>(dataBuffer, _pool.get(), documentVersion));
    _attributeDocument.reset(deserializeObject<AttributeDocument>(dataBuffer, _pool.get(), documentVersion));
    _summaryDocument.reset(deserializeObject<SerializedSummaryDocument>(dataBuffer, documentVersion));

    // not support sub document yet, make sure the serialized data is compatile
    DocumentVector subDocuments;
    dataBuffer.read(subDocuments);
    dataBuffer.read(_modifiedFields);
    FieldIdVector subModifiedFields;
    dataBuffer.read(subModifiedFields);
    dataBuffer.read(_docInfo.timestamp);

    string source;
    dataBuffer.read(source);
    TagInfoFromString(source);

    string locatorStr;
    dataBuffer.read(locatorStr);
    if (!locatorStr.empty() && !_locatorV2.Deserialize(locatorStr)) {
        INDEXLIB_THROW(indexlib::util::RuntimeException,
                       "document serialize failed, do not support serialize document with version %u",
                       _serializedVersion);
    }
    dataBuffer.read(_opType);
    dataBuffer.read(_originalOpType);
    // deserialize kv index document, which is empty, make sure compitable
    // use attribute document to avoid dependency of kv index document
    deserializeObject<AttributeDocument>(dataBuffer, _pool.get(), documentVersion);
}

void NormalDocument::deserializeVersion5(DataBuffer& dataBuffer)
{
    assert(_pool);
    uint32_t documentVersion = 5;
    _indexDocument.reset(deserializeObject<IndexDocument>(dataBuffer, _pool.get(), documentVersion));
    _attributeDocument.reset(deserializeObject<AttributeDocument>(dataBuffer, _pool.get(), documentVersion));
    _summaryDocument.reset(deserializeObject<SerializedSummaryDocument>(dataBuffer, documentVersion));
    // not support sub document yet, make sure the serialized data is compatile
    DocumentVector subDocuments;
    dataBuffer.read(subDocuments);
    dataBuffer.read(_modifiedFields);
    FieldIdVector subModifiedFields;
    dataBuffer.read(subModifiedFields);
    dataBuffer.read(_docInfo.timestamp);

    bool isValidUserTS;
    dataBuffer.read(isValidUserTS);
    if (isValidUserTS) {
        dataBuffer.read(_userTimestamp);
    }

    string source;
    dataBuffer.read(source);
    TagInfoFromString(source);

    string locatorStr;
    dataBuffer.read(locatorStr);
    if (!locatorStr.empty() && !_locatorV2.Deserialize(locatorStr)) {
        INDEXLIB_THROW(indexlib::util::RuntimeException,
                       "document serialize failed, do not support serialize document with version %u",
                       _serializedVersion);
    }
    dataBuffer.read(_opType);
    dataBuffer.read(_originalOpType);
    // deserialize kv index document, which is empty, make sure compitable
    // use attribute document to avoid dependency of kv index document
    deserializeObject<AttributeDocument>(dataBuffer, _pool.get(), documentVersion);
}

void NormalDocument::deserializeVersion6(DataBuffer& dataBuffer)
{
    assert(_pool);
    uint32_t documentVersion = 6;
    _indexDocument.reset(deserializeObject<IndexDocument>(dataBuffer, _pool.get(), documentVersion));
    _attributeDocument.reset(deserializeObject<AttributeDocument>(dataBuffer, _pool.get(), documentVersion));
    _summaryDocument.reset(deserializeObject<SerializedSummaryDocument>(dataBuffer, documentVersion));
    // not support sub document yet, make sure the serialized data is compatile
    DocumentVector subDocuments;
    dataBuffer.read(subDocuments);
    dataBuffer.read(_modifiedFields);
    FieldIdVector subModifiedFields;
    dataBuffer.read(subModifiedFields);
    dataBuffer.read(_docInfo.timestamp);

    bool isValidUserTS;
    dataBuffer.read(isValidUserTS);
    if (isValidUserTS) {
        dataBuffer.read(_userTimestamp);
    }

    int32_t id; // regionid_t
    dataBuffer.read(id);

    string source;
    dataBuffer.read(source);
    TagInfoFromString(source);

    string locatorStr;
    dataBuffer.read(locatorStr);
    if (!locatorStr.empty() && !_locatorV2.Deserialize(locatorStr)) {
        INDEXLIB_THROW(indexlib::util::RuntimeException,
                       "document serialize failed, do not support serialize document with version %u",
                       _serializedVersion);
    }
    dataBuffer.read(_opType);
    dataBuffer.read(_originalOpType);
    // deserialize kv index document, which is empty, make sure compitable
    // use attribute document to avoid dependency of kv index document
    deserializeObject<AttributeDocument>(dataBuffer, _pool.get(), documentVersion);
}

void NormalDocument::deserializeVersion7(DataBuffer& dataBuffer)
{
    assert(_pool);
    uint32_t documentVersion = 7;
    _indexDocument.reset(deserializeObject<IndexDocument>(dataBuffer, _pool.get(), documentVersion));
    _attributeDocument.reset(deserializeObject<AttributeDocument>(dataBuffer, _pool.get(), documentVersion));
    _summaryDocument.reset(deserializeObject<SerializedSummaryDocument>(dataBuffer, documentVersion));
    // not support sub document yet, make sure the serialized data is compatile
    DocumentVector subDocuments;
    dataBuffer.read(subDocuments);
    dataBuffer.read(_modifiedFields);
    FieldIdVector subModifiedFields;
    dataBuffer.read(subModifiedFields);
    dataBuffer.read(_docInfo.timestamp);

    bool isValidUserTS;
    dataBuffer.read(isValidUserTS);
    if (isValidUserTS) {
        dataBuffer.read(_userTimestamp);
    }

    int32_t id; // regionid_t
    dataBuffer.read(id);

    string source;
    dataBuffer.read(source);
    TagInfoFromString(source);

    string locatorStr;
    dataBuffer.read(locatorStr);
    if (!locatorStr.empty() && !_locatorV2.Deserialize(locatorStr)) {
        INDEXLIB_THROW(indexlib::util::RuntimeException,
                       "document serialize failed, do not support serialize document with version %u",
                       _serializedVersion);
    }
    dataBuffer.read(_opType);
    dataBuffer.read(_originalOpType);
    // deserialize kv index document, which is empty, make sure compitable
    // use attribute document to avoid dependency of kv index document
    deserializeObject<AttributeDocument>(dataBuffer, _pool.get(), documentVersion);

    uint32_t schemaId; // schemavid_t
    dataBuffer.read(schemaId);
    dataBuffer.read(_trace);
}

void NormalDocument::DoDeserialize(DataBuffer& dataBuffer, uint32_t serializedVersion)
{
    assert(_pool);
    // since version 7, all higher version can be deserialize by version7
    // fields in different version were stored in different zone
    // -----v7-----|---v8--|----v9---|
    // [f1, f2, f3][f4, f5 ][ f6, f7 ]
    if (serializedVersion >= DOCUMENT_BINARY_VERSION) {
        deserializeVersion7(dataBuffer);
        deserializeVersion8(dataBuffer);
        deserializeVersion9(dataBuffer);
        deserializeVersion10(dataBuffer);
        deserializeVersion11(dataBuffer);
        return;
    }
    switch (serializedVersion) {
    case 10:
        deserializeVersion7(dataBuffer);
        deserializeVersion8(dataBuffer);
        deserializeVersion9(dataBuffer);
        deserializeVersion10(dataBuffer);
        break;
    case 9:
        deserializeVersion7(dataBuffer);
        deserializeVersion8(dataBuffer);
        deserializeVersion9(dataBuffer);
        break;
    case 8:
        deserializeVersion7(dataBuffer);
        deserializeVersion8(dataBuffer);
        break;
    case 7:
        deserializeVersion7(dataBuffer);
        break;
    case 6:
        deserializeVersion6(dataBuffer);
        break;
    case 5:
        deserializeVersion5(dataBuffer);
        break;
    case 4:
        deserializeVersion4(dataBuffer);
        break;
    case 3:
        deserializeVersion3(dataBuffer);
        break;

    default:
        std::stringstream ss;
        ss << "document deserialize failed, expect version: " << DOCUMENT_BINARY_VERSION
           << " actual version: " << serializedVersion;
        INDEXLIB_THROW(indexlib::util::DocumentDeserializeException, "%s", ss.str().c_str());
    }
}

string NormalDocument::SerializeToString() const
{
    DataBuffer dataBuffer;
    dataBuffer.write(*this);
    return string(dataBuffer.getData(), dataBuffer.getDataLen());
}

void NormalDocument::DeserializeFromString(const string& docStr)
{
    DataBuffer dataBuffer(const_cast<char*>(docStr.data()), docStr.length());
    dataBuffer.read(*this);
}

const std::string& NormalDocument::GetPrimaryKey() const
{
    if (_indexDocument) {
        return _indexDocument->GetPrimaryKey();
    }
    static string EMPTY_STRING = "";
    return EMPTY_STRING;
}

size_t NormalDocument::GetMemoryUse() const
{
    size_t memUse = _pool->getUsedBytes();
    return memUse;
}

size_t NormalDocument::EstimateMemory() const
{
    size_t ret = sizeof(*this);
    size_t pageSize = getpagesize();
    size_t pageAlignSize = (_pool->getUsedBytes() + pageSize - 1) / pageSize * pageSize;
    ret += pageAlignSize;
    return ret;
}

bool NormalDocument::HasFormatError() const { return _attributeDocument && _attributeDocument->HasFormatError(); }

bool NormalDocument::HasIndexUpdate() const
{
    if (_indexDocument) {
        if (!_indexDocument->GetModifiedTokens().empty()) {
            return true;
        }
    }
    return false;
}

void NormalDocument::SetIngestionTimestamp(int64_t ingestionTimestamp)
{
    AddTag(DOCUMENT_INGESTIONTIMESTAMP_TAG_KEY, std::to_string(ingestionTimestamp));
}
int64_t NormalDocument::GetIngestionTimestamp() const
{
    const auto& ingestionTimestampStr = GetTag(DOCUMENT_INGESTIONTIMESTAMP_TAG_KEY);
    if (ingestionTimestampStr.empty()) {
        return INVALID_TIMESTAMP;
    }
    return autil::StringUtil::fromString<int64_t>(ingestionTimestampStr);
}

autil::StringView NormalDocument::GetTraceId() const
{
    if (!_trace) {
        return autil::StringView::empty_instance();
    }
    return GetPrimaryKey();
}

}} // namespace indexlibv2::document
