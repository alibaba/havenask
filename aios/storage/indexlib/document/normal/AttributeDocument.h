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
#include <set>

#include "indexlib/base/Define.h"
#include "indexlib/document/normal/SummaryDocument.h"
#include "indexlib/index/attribute/Constant.h"

namespace indexlib::document {

class AttributeDocument
{
public:
    // NormalAttributeDocument has no pack attributes
    typedef SummaryDocument NormalAttributeDocument;
    typedef SummaryDocument::Iterator Iterator;
    typedef SummaryDocument::FieldVector FieldVector;

public:
    AttributeDocument(autil::mem_pool::Pool* pool = NULL) : _fieldFormatError(false) {}

    ~AttributeDocument() {}

public:
    Iterator CreateIterator() const { return _normalAttrDoc.CreateIterator(); }

    void SetDocId(docid_t docId) { _normalAttrDoc.SetDocId(docId); }

    docid_t GetDocId() const { return _normalAttrDoc.GetDocId(); }

    void SetField(fieldid_t id, const autil::StringView& value) { _normalAttrDoc.SetField(id, value); }

    void SetField(fieldid_t id, const autil::StringView& value, bool isNull)
    {
        if (isNull) {
            SetNullField(id);
            return;
        }
        SetField(id, value);
    }

    void SetNullField(const fieldid_t id) { _nullFields.insert(id); }

    bool IsNullField(fieldid_t id) { return _nullFields.find(id) != _nullFields.end(); }

    void ClearFields(const std::vector<fieldid_t>& fieldIds) { _normalAttrDoc.ClearFields(fieldIds); }

    bool HasField(fieldid_t id) const { return _normalAttrDoc.HasField(id); }

    const autil::StringView& GetField(fieldid_t id) const { return _normalAttrDoc.GetField(id); }

    const autil::StringView& GetField(fieldid_t id, bool& isNull) const
    {
        auto iter = _nullFields.find(id);
        if (iter != _nullFields.end()) {
            isNull = true;
            return autil::StringView::empty_instance();
        }
        isNull = false;
        return _normalAttrDoc.GetField(id);
    }

    uint32_t GetNotEmptyFieldCount() const { return _normalAttrDoc.GetNotEmptyFieldCount(); }

    uint32_t GetNullFieldCount() const { return _nullFields.size(); }

    size_t GetPackFieldCount() const { return _packFields.size(); }

    void Reserve(size_t size) { _normalAttrDoc.Reserve(size); }

    void GetNullFieldIds(std::vector<fieldid_t>& fids) const
    {
        fids.insert(fids.end(), _nullFields.begin(), _nullFields.end());
    }

    void Reset()
    {
        _normalAttrDoc.Reset();
        ResetPackFields();
        _fieldFormatError = false;
    }

    void serialize(autil::DataBuffer& dataBuffer) const;
    inline void serializeVersion8(autil::DataBuffer& dataBuffer) const;
    void deserialize(autil::DataBuffer& dataBuffer, autil::mem_pool::Pool* pool,
                     uint32_t docVersion = DOCUMENT_BINARY_VERSION);
    inline void deserializeVersion8(autil::DataBuffer& dataBuffer);

    bool operator==(const AttributeDocument& right) const;
    bool operator!=(const AttributeDocument& right) const;

    void SetPackField(packattrid_t packId, const autil::StringView& value);
    const autil::StringView& GetPackField(packattrid_t packId) const;

    void SetFormatError(bool hasError) { _fieldFormatError = hasError; }
    bool& GetFormatErrorLable() { return _fieldFormatError; }
    bool HasFormatError() const { return _fieldFormatError; }

private:
    void ResetPackFields() { _packFields.clear(); }

private:
    NormalAttributeDocument _normalAttrDoc;
    FieldVector _packFields;
    std::set<fieldid_t> _nullFields;
    bool _fieldFormatError;

private:
    AUTIL_LOG_DECLARE();

private:
    friend class DocumentTest;
};

inline void AttributeDocument::serialize(autil::DataBuffer& dataBuffer) const
{
    // version 7 and belows
    _normalAttrDoc.serialize(dataBuffer);
    dataBuffer.write(_packFields);
}

inline void __ALWAYS_INLINE AttributeDocument::serializeVersion8(autil::DataBuffer& dataBuffer) const
{
    dataBuffer.write(_nullFields);
}

inline void AttributeDocument::deserialize(autil::DataBuffer& dataBuffer, autil::mem_pool::Pool* pool,
                                           uint32_t docVersion)
{
    // version 7 and belows
    ResetPackFields();
    _normalAttrDoc.deserialize(dataBuffer, pool);
    dataBuffer.read(_packFields, pool);
}

inline void __ALWAYS_INLINE AttributeDocument::deserializeVersion8(autil::DataBuffer& dataBuffer)
{
    dataBuffer.read(_nullFields);
}

inline bool AttributeDocument::operator==(const AttributeDocument& right) const
{
    return _packFields == right._packFields && _normalAttrDoc == right._normalAttrDoc &&
           _nullFields == right._nullFields;
}

inline bool AttributeDocument::operator!=(const AttributeDocument& right) const { return !(operator==(right)); }

inline void AttributeDocument::SetPackField(packattrid_t packId, const autil::StringView& value)
{
    if (packId >= (packattrid_t)_packFields.size()) {
        _packFields.resize(packId + 1);
    }
    _packFields[packId] = value;
}

inline const autil::StringView& AttributeDocument::GetPackField(packattrid_t packId) const
{
    if (packId != INVALID_ATTRID && packId < (packattrid_t)_packFields.size()) {
        return _packFields[size_t(packId)];
    }
    return autil::StringView::empty_instance();
}
} // namespace indexlib::document
