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
#include "indexlib/document/normal/IndexDocument.h"

#include "autil/StringUtil.h"
#include "indexlib/document/normal/IndexTokenizeField.h"
#include "indexlib/index/common/Constant.h"
#include "indexlib/util/ErrorLogCollector.h"
#include "indexlib/util/Exception.h"

using namespace autil;
using namespace autil::mem_pool;
using namespace std;

namespace indexlib::document {
AUTIL_LOG_SETUP(indexlib.document, IndexDocument);

IndexDocument::IndexDocument(Pool* pool)
    : _fieldCount(0)
    , _docId(INVALID_DOCID)
    , _pool(pool)
    , _payloads(_pool, HASH_MAP_INIT_ELEM_COUNT)
    , _termPayloads(_pool, HASH_MAP_INIT_ELEM_COUNT)
{
}

IndexDocument::~IndexDocument()
{
    size_t size = _fields.size();
    for (size_t i = 0; i < size; i++) {
        if (_fields[i]) {
            IE_POOL_COMPATIBLE_DELETE_CLASS(_pool, _fields[i]);
            _fields[i] = NULL;
        }
    }
    _fields.clear();

    _modifiedTokens.clear();
    _termOriginValueMap.clear();
}

Field* IndexDocument::CreateField(fieldid_t fieldId, Field::FieldTag fieldTag)
{
    assert(fieldId != INVALID_FIELDID);

    if ((fieldid_t)_fields.size() > fieldId) {
        if (NULL == _fields[fieldId]) {
            _fields[fieldId] = CreateFieldByTag(_pool, fieldTag, false);
            _fields[fieldId]->SetFieldId(fieldId);
            ++_fieldCount;
        } else if (_fields[fieldId]->GetFieldId() == INVALID_FIELDID) {
            ++_fieldCount;
            _fields[fieldId]->SetFieldId(fieldId);
        }
        return _fields[fieldId];
    }

    _fields.resize(fieldId + 1, NULL);
    Field* pField = CreateFieldByTag(_pool, fieldTag, false);
    pField->SetFieldId(fieldId);
    _fields[fieldId] = pField;
    ++_fieldCount;
    return pField;
}

void IndexDocument::ClearField(fieldid_t fieldId)
{
    if ((fieldid_t)_fields.size() <= fieldId) {
        return;
    }

    if (NULL == _fields[fieldId]) {
        return;
    }

    if (_fields[fieldId]->GetFieldId() != INVALID_FIELDID) {
        --_fieldCount;
    }
    IE_POOL_COMPATIBLE_DELETE_CLASS(_pool, _fields[fieldId]);
    _fields[fieldId] = NULL;
}

void IndexDocument::ClearData()
{
    size_t size = _fields.size();
    for (size_t i = 0; i < size; i++) {
        if (_fields[i]) {
            IE_POOL_COMPATIBLE_DELETE_CLASS(_pool, _fields[i]);
            _fields[i] = NULL;
        }
    }
    _termOriginValueMap.clear();
    _modifiedTokens.clear();

    _fields.clear();
    _fieldCount = 0;
    _docId = INVALID_DOCID;

    _sectionAttributeVec.clear();
    _payloads.Clear();
    _termPayloads.Clear();
    _primaryKey.clear();
}

bool IndexDocument::AddField(Field* field)
{
    assert(field && field->GetFieldId() != INVALID_FIELDID);

    fieldid_t fieldId = field->GetFieldId();
    if ((fieldid_t)_fields.size() <= fieldId) {
        _fields.resize(fieldId + 1, NULL);
    }

    _fields[fieldId] = field;
    ++_fieldCount;
    return true;
}

Field* IndexDocument::GetField(fieldid_t fieldId)
{
    return const_cast<Field*>(static_cast<const IndexDocument&>(*this).GetField(fieldId));
}

const Field* IndexDocument::GetField(fieldid_t fieldId) const
{
    if (fieldId >= (fieldid_t)_fields.size()) {
        return NULL;
    }
    return _fields[fieldId];
}

void IndexDocument::SetField(fieldid_t fieldId, Field* field)
{
    assert(fieldId != INVALID_FIELDID);
    if (!field) {
        return;
    }

    if ((fieldid_t)_fields.size() <= fieldId) {
        _fields.resize(fieldId + 1, NULL);
    }

    if (!_fields[fieldId]) {
        ++_fieldCount;
    } else {
        IE_POOL_COMPATIBLE_DELETE_CLASS(_pool, _fields[fieldId]);
    }

    _fields[fieldId] = field;
    field->SetFieldId(fieldId);
}

docpayload_t IndexDocument::GetDocPayload(const string& termText) const
{
    docpayload_t ans = 0;
    return _payloads.Find(HashAlgorithm::hashString64(termText.c_str()), ans);
}

docpayload_t IndexDocument::GetDocPayload(uint64_t intKey) const
{
    docpayload_t ans = 0;
    return _payloads.Find(intKey, ans);
}

termpayload_t IndexDocument::GetTermPayload(const string& termText) const
{
    uint32_t ans = 0;
    return _termPayloads.Find(HashAlgorithm::hashString64(termText.c_str()), ans);
}

termpayload_t IndexDocument::GetTermPayload(uint64_t intKey) const
{
    uint32_t ans = 0;
    return _termPayloads.Find(intKey, ans);
}

bool IndexDocument::operator==(const IndexDocument& doc) const
{
    if (this == &doc)
        return true;
    if (_docId != doc._docId || _fieldCount != doc._fieldCount || _fields.size() != doc._fields.size() ||
        _primaryKey != doc._primaryKey) {
        return false;
    }
    for (uint32_t i = 0; i < _fields.size(); ++i) {
        bool b1 = _fields[i] == NULL && doc._fields[i] == NULL;
        bool b2 = _fields[i] != NULL && doc._fields[i] != NULL && *_fields[i] == *doc._fields[i];
        if (b1 || b2)
            continue;
        return false;
    }
    return true;
}

void IndexDocument::SetTermPayload(const std::string& termText, termpayload_t payload)
{
    _termPayloads.FindAndInsert(HashAlgorithm::hashString64(termText.c_str()), payload);
}

void IndexDocument::SetTermPayload(uint64_t intKey, termpayload_t payload)
{
    _termPayloads.FindAndInsert(intKey, payload);
}

void IndexDocument::SetDocPayload(const std::string& termText, docpayload_t docPayload)
{
    _payloads.FindAndInsert(HashAlgorithm::hashString64(termText.c_str()), docPayload);
}

void IndexDocument::SetDocPayload(uint64_t intKey, docpayload_t docPayload)
{
    _payloads.FindAndInsert(intKey, docPayload);
}

docpayload_t IndexDocument::GetDocPayload(const config::PayloadConfig& payloadConfig, uint64_t intKey) const
{
    intKey = payloadConfig.RewriteTermHash(intKey);
    return GetDocPayload(intKey);
}

docpayload_t IndexDocument::GetDocPayload(const config::PayloadConfig& payloadConfig, const std::string& termText) const
{
    uint64_t intKey = payloadConfig.RewriteTermHash(HashAlgorithm::hashString64(termText.c_str()));
    return GetDocPayload(intKey);
}

void IndexDocument::SetDocPayload(const config::PayloadConfig& payloadConfig, const std::string& termText,
                                  docpayload_t docPayload)
{
    uint64_t intKey = payloadConfig.RewriteTermHash(HashAlgorithm::hashString64(termText.c_str()));
    SetDocPayload(intKey, docPayload);
}

void IndexDocument::CreateSectionAttribute(indexid_t indexId, const string& attrStr)
{
    StringView value = autil::MakeCString(attrStr, _pool);
    SetSectionAttribute(indexId, value);
}

void IndexDocument::SetSectionAttribute(indexid_t indexId, const StringView& attrStr)
{
    assert(indexId != INVALID_INDEXID);
    if (attrStr == StringView::empty_instance()) {
        return;
    }

    if ((indexid_t)_sectionAttributeVec.size() <= indexId) {
        _sectionAttributeVec.resize(indexId + 1, StringView::empty_instance());
    }
    _sectionAttributeVec[indexId] = attrStr;
}

const StringView& IndexDocument::GetSectionAttribute(indexid_t indexId) const
{
    if (indexId >= (indexid_t)_sectionAttributeVec.size()) {
        return StringView::empty_instance();
    }
    return _sectionAttributeVec[indexId];
}

IndexDocument::FieldVector::const_iterator IndexDocument::GetFieldBegin() { return _fields.begin(); }

IndexDocument::FieldVector::const_iterator IndexDocument::GetFieldEnd() { return _fields.end(); }
const std::string& IndexDocument::GetPrimaryKey() const { return _primaryKey; }
void IndexDocument::SetPrimaryKey(const std::string& primaryKey) { _primaryKey = primaryKey; }

void IndexDocument::serialize(DataBuffer& dataBuffer) const
{
    SerializeFieldVector(dataBuffer, _fields);
    dataBuffer.write(_primaryKey);
    SerializeHashMap(dataBuffer, _payloads);
    SerializeHashMap(dataBuffer, _termPayloads);
    dataBuffer.write(_sectionAttributeVec);
}

void IndexDocument::deserialize(DataBuffer& dataBuffer, mem_pool::Pool* pool, uint32_t docVersion)
{
    _fieldCount = DeserializeFieldVector(dataBuffer, _fields, _pool, (docVersion <= 4));
    dataBuffer.read(_primaryKey);
    DeserializeHashMap(dataBuffer, _payloads);
    DeserializeHashMap(dataBuffer, _termPayloads);
    dataBuffer.read(_sectionAttributeVec, _pool);
}

indexid_t IndexDocument::GetMaxIndexIdInSectionAttribute() const
{
    if (_sectionAttributeVec.empty()) {
        return INVALID_INDEXID;
    }
    return (indexid_t)(_sectionAttributeVec.size() - 1);
}

template <typename K, typename V>
void IndexDocument::SerializeHashMap(DataBuffer& dataBuffer, const util::HashMap<K, V>& hashMap)
{
    typedef typename util::HashMap<K, V> HashMapType;
    typename HashMapType::Iterator it = hashMap.CreateIterator();
    dataBuffer.write(hashMap.Size());
    while (it.HasNext()) {
        typename HashMapType::KeyValuePair& p = it.Next();
        dataBuffer.write(p.first);
        dataBuffer.write(p.second);
    }
}

template <typename K, typename V>
void IndexDocument::DeserializeHashMap(DataBuffer& dataBuffer, util::HashMap<K, V>& hashMap)
{
    size_t size;
    dataBuffer.read(size);
    hashMap.Clear();
    while (size--) {
        K k;
        V v;
        dataBuffer.read(k);
        dataBuffer.read(v);
        hashMap.FindAndInsert(k, v);
    }
}

void IndexDocument::SerializeFieldVector(DataBuffer& dataBuffer, const FieldVector& fields)
{
    uint32_t size = fields.size();
    dataBuffer.write(size);
    for (uint32_t i = 0; i < size; ++i) {
        uint8_t descriptor = GenerateFieldDescriptor(fields[i]);
        dataBuffer.write(descriptor);
        if (descriptor != 0) {
            dataBuffer.write(*(fields[i]));
        }
    }
}

uint32_t IndexDocument::DeserializeFieldVector(DataBuffer& dataBuffer, FieldVector& fields, Pool* pool, bool isLegacy)
{
    uint32_t size = 0;
    dataBuffer.read(size);
    fields.resize(size);
    uint32_t fieldCount = 0;
    for (uint32_t i = 0; i < size; ++i) {
        bool fieldExist = false;
        Field::FieldTag fieldTag = Field::FieldTag::TOKEN_FIELD;
        uint8_t descriptor = 0;
        dataBuffer.read(descriptor);
        fieldExist = (descriptor != 0);

        if (fieldExist && !isLegacy) {
            fieldTag = GetFieldTagFromFieldDescriptor(descriptor);
        }

        if (!fieldExist) {
            fields[i] = NULL;
        } else {
            fields[i] = CreateFieldByTag(pool, fieldTag, true);
            if (NULL == fields[i]) {
                INDEXLIB_FATAL_ERROR(DocumentDeserialize, "invalid fieldTag[%d]", static_cast<int8_t>(fieldTag));
            }
            dataBuffer.read(*(fields[i]));
            if (fields[i]->GetFieldId() != INVALID_FIELDID) {
                fieldCount++;
            }
        }
    }
    return fieldCount;
}

uint8_t IndexDocument::GenerateFieldDescriptor(const Field* field)
{
    if (field == NULL) {
        return 0;
    }

    int8_t fieldTagNum = static_cast<int8_t>(field->GetFieldTag());
    return Field::FIELD_DESCRIPTOR_MASK | fieldTagNum;
}

Field::FieldTag IndexDocument::GetFieldTagFromFieldDescriptor(uint8_t fieldDescriptor)
{
    return static_cast<Field::FieldTag>((~Field::FIELD_DESCRIPTOR_MASK) & fieldDescriptor);
}

Field* IndexDocument::CreateFieldByTag(autil::mem_pool::Pool* pool, Field::FieldTag fieldTag, bool fieldHasPool)
{
    auto fieldPool = fieldHasPool ? pool : nullptr;
    if (fieldTag == Field::FieldTag::TOKEN_FIELD) {
        return IE_POOL_COMPATIBLE_NEW_CLASS(pool, IndexTokenizeField, fieldPool);
    } else if (fieldTag == Field::FieldTag::RAW_FIELD) {
        return IE_POOL_COMPATIBLE_NEW_CLASS(pool, IndexRawField, fieldPool);
    } else if (fieldTag == Field::FieldTag::NULL_FIELD) {
        return IE_POOL_COMPATIBLE_NEW_CLASS(pool, NullField, fieldPool);
        return NULL;
    } else {
        AUTIL_LOG(ERROR, "invalid fieldTag:[%u]", static_cast<uint16_t>(fieldTag));
        ERROR_COLLECTOR_LOG(ERROR, "invalid fieldTag:[%u]", static_cast<uint16_t>(fieldTag));
        return NULL;
    }
}

// invalid after modifiedTokens push/set
const ModifiedTokens* IndexDocument::GetFieldModifiedTokens(fieldid_t fieldId) const
{
    assert(fieldId >= 0);
    if (fieldId >= static_cast<fieldid_t>(_modifiedTokens.size())) {
        return nullptr;
    }
    if (_modifiedTokens[fieldId].FieldId() == fieldId) {
        return &_modifiedTokens[fieldId];
    }
    return nullptr;
}

void IndexDocument::PushModifiedToken(fieldid_t fieldId, uint64_t termKey, ModifiedTokens::Operation op)
{
    assert(fieldId != INVALID_FIELDID);
    if ((fieldid_t)_modifiedTokens.size() <= fieldId) {
        _modifiedTokens.resize(fieldId + 1);
    }
    if (!_modifiedTokens[fieldId].Valid()) {
        _modifiedTokens[fieldId] = ModifiedTokens(fieldId);
    }
    _modifiedTokens[fieldId].Push(op, termKey);
}
void IndexDocument::SetNullTermModifiedOperation(fieldid_t fieldId, ModifiedTokens::Operation op)
{
    assert(fieldId != INVALID_FIELDID);
    if ((fieldid_t)_modifiedTokens.size() <= fieldId) {
        _modifiedTokens.resize(fieldId + 1);
    }
    if (!_modifiedTokens[fieldId].Valid()) {
        _modifiedTokens[fieldId] = ModifiedTokens(fieldId);
    }
    _modifiedTokens[fieldId].SetNullTermOperation(op);
}

void IndexDocument::serializeVersion10(autil::DataBuffer& dataBuffer) const { dataBuffer.write(_modifiedTokens); }

void IndexDocument::deserializeVersion10(autil::DataBuffer& dataBuffer) { dataBuffer.read(_modifiedTokens); }

void IndexDocument::serializeVersion11(autil::DataBuffer& dataBuffer) const { dataBuffer.write(_termOriginValueMap); }

void IndexDocument::deserializeVersion11(autil::DataBuffer& dataBuffer) { dataBuffer.read(_termOriginValueMap); }

void IndexDocument::AddTermOriginValue(const TermOriginValueMap& termOriginValueMap)
{
    for (const auto& [indexName, newTermHashMap] : termOriginValueMap) {
        auto it = _termOriginValueMap.find(indexName);
        if (it == _termOriginValueMap.end()) {
            _termOriginValueMap.insert(it, std::make_pair(indexName, newTermHashMap));
        } else {
            auto& currentHashMap = it->second;
            currentHashMap.insert(newTermHashMap.begin(), newTermHashMap.end());
        }
    }
    return;
}
} // namespace indexlib::document
