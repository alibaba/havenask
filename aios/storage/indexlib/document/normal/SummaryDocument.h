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
#include <string>
#include <vector>

#include "autil/ConstString.h"
#include "autil/DataBuffer.h"
#include "indexlib/base/Constant.h"

namespace indexlib::document {

class SummaryDocument
{
public:
    typedef std::vector<autil::StringView> FieldVector;

public:
    class Iterator
    {
    public:
        Iterator(const FieldVector& fieldVec, fieldid_t firstNotEmptyFieldId = INVALID_FIELDID) : _fields(fieldVec)
        {
            _cursor = _fields.begin();
            if (firstNotEmptyFieldId != INVALID_FIELDID) {
                assert((size_t)firstNotEmptyFieldId < _fields.size());
                _fieldId = firstNotEmptyFieldId;
                _cursor += _fieldId;
            } else {
                _fieldId = 0;
                MoveToNext();
            }
        }

    public:
        bool HasNext() const { return _cursor != _fields.end(); }

        const autil::StringView& Next(fieldid_t& fieldId)
        {
            if (_cursor != _fields.end()) {
                const autil::StringView& value = *_cursor;
                fieldId = _fieldId;
                ++_cursor;
                ++_fieldId;
                MoveToNext();
                return value;
            }
            return autil::StringView::empty_instance();
        }

    private:
        void MoveToNext()
        {
            while (_cursor != _fields.end() && _cursor->empty()) {
                ++_cursor;
                ++_fieldId;
            }
        }

    private:
        const FieldVector& _fields;
        FieldVector::const_iterator _cursor;
        fieldid_t _fieldId;
    };

public:
    SummaryDocument(autil::mem_pool::Pool* pool = NULL)
        : _notEmptyFieldCount(0)
        , _firstNotEmptyFieldId(INVALID_FIELDID)
        , _docId(INVALID_DOCID)
    {
    }

    ~SummaryDocument() {}

public:
    void SetDocId(docid_t docId) { _docId = docId; }
    docid_t GetDocId() const { return _docId; }
    // caller: BS::ClassifiedDocument
    void SetField(fieldid_t id, const autil::StringView& value);
    void ClearFields(const std::vector<fieldid_t>& fieldIds);
    bool HasField(fieldid_t id) const;
    const autil::StringView& GetField(fieldid_t id) const;

    uint32_t GetNotEmptyFieldCount() const { return _notEmptyFieldCount; }
    void Reserve(size_t size) { _fields.resize(size); }
    void Reset()
    {
        _fields.clear();
        _notEmptyFieldCount = 0;
        _firstNotEmptyFieldId = INVALID_FIELDID;
    }

    Iterator CreateIterator() const { return Iterator(_fields, _firstNotEmptyFieldId); }

    void serialize(autil::DataBuffer& dataBuffer) const;
    void deserialize(autil::DataBuffer& dataBuffer, autil::mem_pool::Pool* pool);

    void deserializeLegacyFormat(autil::DataBuffer& dataBuffer, autil::mem_pool::Pool* pool, uint32_t docVersion);

    bool operator==(const SummaryDocument& right) const;
    bool operator!=(const SummaryDocument& right) const;

private:
    bool NeedPartialSerialize() const;
    void DeserializeAllFields(autil::DataBuffer& dataBuffer, autil::mem_pool::Pool* pool);
    void DeserializeNotEmptyFields(autil::DataBuffer& dataBuffer, autil::mem_pool::Pool* pool);

private:
    FieldVector _fields;
    uint32_t _notEmptyFieldCount;
    fieldid_t _firstNotEmptyFieldId;
    docid_t _docId;

private:
    AUTIL_LOG_DECLARE();

private:
    friend class SummaryDocumentTest;
    friend class DocumentTest;
};

//////////////////////////////////////////////////////////////////
inline bool SummaryDocument::HasField(fieldid_t id) const
{
    if (id < (fieldid_t)_fields.size()) {
        return !_fields[id].empty();
    }

    return false;
}

inline void SummaryDocument::SetField(fieldid_t id, const autil::StringView& value)
{
    if (id >= (fieldid_t)_fields.size()) {
        _fields.resize(id + 1);
    }

    if (_fields[id].empty() && !value.empty()) {
        ++_notEmptyFieldCount;
        if (_firstNotEmptyFieldId == INVALID_FIELDID || id < _firstNotEmptyFieldId) {
            _firstNotEmptyFieldId = id;
        }
    }
    _fields[id] = value;
}

inline bool SummaryDocument::NeedPartialSerialize() const
{
    if (_fields.size() == (size_t)_notEmptyFieldCount) {
        // all fields not empty
        return false;
    }

    size_t maxVintSize = 0;
    uint32_t maxFieldId = _fields.size();
    while (maxFieldId > 0x7F) {
        maxFieldId >>= 7;
        ++maxVintSize;
    }
    ++maxVintSize;

    // each empty field use 1 byte
    size_t emptyFieldSize = _fields.size() - _notEmptyFieldCount;
    return emptyFieldSize > (maxVintSize * _notEmptyFieldCount);
}
} // namespace indexlib::document
