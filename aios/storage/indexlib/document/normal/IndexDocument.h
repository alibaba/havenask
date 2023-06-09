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

#include "autil/ConstString.h"
#include "autil/DataBuffer.h"
#include "indexlib/base/Status.h"
#include "indexlib/document/normal/Field.h"
#include "indexlib/document/normal/IndexRawField.h"
#include "indexlib/document/normal/ModifiedTokens.h"
#include "indexlib/document/normal/NullField.h"
#include "indexlib/index/common/Types.h"
#include "indexlib/index/inverted_index/config/PayloadConfig.h"
#include "indexlib/util/HashMap.h"

namespace indexlib::document {

class IndexDocument
{
public:
    typedef std::vector<Field*> FieldVector;
    typedef std::vector<autil::StringView> SectionAttributeVector;
    // map<indexName, map<hash, termStr>>
    typedef std::map<std::string, std::unordered_map<uint64_t, std::string>> TermOriginValueMap;

public:
    IndexDocument(autil::mem_pool::Pool* pool);
    ~IndexDocument();

public:
    class Iterator
    {
    public:
        Iterator(const IndexDocument& doc) : _document(doc)
        {
            _cursor = doc._fields.begin();
            MoveToNext();
        }

    public:
        bool HasNext() const { return _cursor != _document._fields.end(); }

        Field* Next()
        {
            if (_cursor != _document._fields.end()) {
                Field* field = *_cursor;
                ++_cursor;
                MoveToNext();
                return field;
            }
            return NULL;
        }

    private:
        void MoveToNext()
        {
            while (_cursor != _document._fields.end() &&
                   ((*_cursor) == NULL || (*_cursor)->GetFieldId() == INVALID_FIELDID)) {
                ++_cursor;
            }
        }

    private:
        const IndexDocument& _document;
        FieldVector::const_iterator _cursor;
    };

public:
    friend class Iterator;

public:
    // CreateField() and Reset() are for reusing Field object
    Field* CreateField(fieldid_t fieldId, Field::FieldTag fieldTag = Field::FieldTag::TOKEN_FIELD);

    // field should be allocated by _pool
    bool AddField(Field* field);
    Field* GetField(fieldid_t fieldId);
    const Field* GetField(fieldid_t fieldId) const;
    void ClearField(fieldid_t fieldId);

    void SetField(fieldid_t fieldId, Field* field);
    docid_t GetDocId() const { return _docId; }

    void SetDocId(docid_t docId) { _docId = docId; }
    uint32_t GetFieldCount() const { return _fieldCount; }

    indexid_t GetMaxIndexIdInSectionAttribute() const;
    void CreateSectionAttribute(indexid_t indexId, const std::string& attrStr);

    // attrStr should be allocated by _pool
    void SetSectionAttribute(indexid_t indexId, const autil::StringView& attrStr);
    const autil::StringView& GetSectionAttribute(indexid_t indexId) const;

    FieldVector::const_iterator GetFieldBegin();
    FieldVector::const_iterator GetFieldEnd();
    const std::string& GetPrimaryKey() const;
    void SetPrimaryKey(const std::string& primaryKey);
    void serialize(autil::DataBuffer& dataBuffer) const;

    void deserialize(autil::DataBuffer& dataBuffer, autil::mem_pool::Pool* pool,
                     uint32_t docVersion = DOCUMENT_BINARY_VERSION);

    void serializeVersion10(autil::DataBuffer& dataBuffer) const;
    void deserializeVersion10(autil::DataBuffer& dataBuffer);

    // for output term's orgin-value
    void serializeVersion11(autil::DataBuffer& dataBuffer) const;
    void deserializeVersion11(autil::DataBuffer& dataBuffer);

    // the following four functions are used to get/set global idf for each term
    termpayload_t GetTermPayload(const std::string& termText) const;
    termpayload_t GetTermPayload(uint64_t intKey) const;
    void SetTermPayload(const std::string& termText, termpayload_t payload);
    void SetTermPayload(uint64_t intKey, termpayload_t payload);

    docpayload_t GetDocPayload(const std::string& termText) const;
    docpayload_t GetDocPayload(uint64_t intKey) const;
    void SetDocPayload(uint64_t intKey, docpayload_t docPayload);
    void SetDocPayload(const std::string& termText, docpayload_t docPayload);
    docpayload_t GetDocPayload(const config::PayloadConfig& payloadConfig, uint64_t intKey) const;
    docpayload_t GetDocPayload(const config::PayloadConfig& payloadConfig, const std::string& termText) const;
    void SetDocPayload(const config::PayloadConfig& payloadConfig, const std::string& termText,
                       docpayload_t docPayload);

    const std::vector<ModifiedTokens>& GetModifiedTokens() const { return _modifiedTokens; }
    std::vector<ModifiedTokens> StealModifiedTokens() const { return std::move(_modifiedTokens); }
    void SetModifiedTokens(std::vector<ModifiedTokens> tokens) { _modifiedTokens = std::move(tokens); }
    // invalid after modifiedTokens push/set
    const ModifiedTokens* GetFieldModifiedTokens(fieldid_t fieldId) const;
    void PushModifiedToken(fieldid_t fieldId, uint64_t termKey, ModifiedTokens::Operation op);
    void SetNullTermModifiedOperation(fieldid_t fieldId, ModifiedTokens::Operation op);

    void Reserve(uint32_t fieldNum) { _fields.reserve(fieldNum); }

    Iterator CreateIterator() const { return Iterator(*this); }

    bool operator==(const IndexDocument& doc) const;
    bool operator!=(const IndexDocument& doc) const { return !(*this == doc); }

    void ClearData();
    autil::mem_pool::Pool* GetMemPool() { return _pool; }
    const TermOriginValueMap& GetTermOriginValueMap() const { return _termOriginValueMap; }
    void AddTermOriginValue(const TermOriginValueMap& termOriginValueMap);

private:
    static uint8_t GenerateFieldDescriptor(const Field* field);
    static Field::FieldTag GetFieldTagFromFieldDescriptor(uint8_t fieldDescriptor);
    static Field* CreateFieldByTag(autil::mem_pool::Pool* pool, Field::FieldTag fieldTag, bool fieldHasPool);
    template <typename K, typename V>
    static void SerializeHashMap(autil::DataBuffer& dataBuffer, const util::HashMap<K, V>& hashMap);
    template <typename K, typename V>
    static void DeserializeHashMap(autil::DataBuffer& dataBuffer, util::HashMap<K, V>& hashMap);

    static void SerializeFieldVector(autil::DataBuffer& dataBuffer, const FieldVector& fields);
    static uint32_t DeserializeFieldVector(autil::DataBuffer& dataBuffer, FieldVector& fields,
                                           autil::mem_pool::Pool* pool, bool isLegacy);

private:
    static const uint32_t HASH_MAP_CHUNK_SIZE = 4096;
    static const uint32_t HASH_MAP_INIT_ELEM_COUNT = 128;

private:
    uint32_t _fieldCount;
    docid_t _docId;

    FieldVector _fields;
    std::string _primaryKey;

    autil::mem_pool::Pool* _pool;

    typedef util::HashMap<uint64_t, docpayload_t> DocPayloadMap;
    typedef util::HashMap<uint64_t, termpayload_t> TermPayloadMap;

    DocPayloadMap _payloads;
    TermPayloadMap _termPayloads;
    SectionAttributeVector _sectionAttributeVec;

    std::vector<ModifiedTokens> _modifiedTokens;
    TermOriginValueMap _termOriginValueMap;

private:
    friend class DocumentTest;
    friend class IndexDocumentTest;
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::document
