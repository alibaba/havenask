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
#include "indexlib/document/normal/IndexTokenizeField.h"

using namespace std;

namespace indexlib { namespace document {
AUTIL_LOG_SETUP(indexlib.document, IndexTokenizeField);

IndexTokenizeField::IndexTokenizeField(autil::mem_pool::Pool* pool)
    : Field(pool, FieldTag::TOKEN_FIELD)
    , _selfPool(1024)
    , _sections(Alloc(pool == NULL ? &_selfPool : pool))
    , _sectionUsed(0)
{
}

IndexTokenizeField::~IndexTokenizeField()
{
    size_t size = _sections.size();
    for (size_t i = 0; i < size; i++) {
        IE_POOL_COMPATIBLE_DELETE_CLASS(_pool, _sections[i]);
        _sections[i] = NULL;
    }
    _sections.clear();
}

void IndexTokenizeField::Reset()
{
    _sectionUsed = 0;
    size_t size = _sections.size();
    for (size_t i = 0; i < size; i++) {
        _sections[i]->Reset();
    }
    _fieldId = INVALID_FIELDID;
}

void IndexTokenizeField::serialize(autil::DataBuffer& dataBuffer) const
{
    uint32_t size = _sections.size();
    dataBuffer.write(size);
    for (size_t i = 0; i < size; ++i) {
        bool isNull = _sections[i];
        dataBuffer.write(isNull);
        if (isNull) {
            dataBuffer.write(*(_sections[i]));
        }
    }
    dataBuffer.write(_fieldId);
    dataBuffer.write(_sectionUsed);
}

void IndexTokenizeField::deserialize(autil::DataBuffer& dataBuffer)
{
    uint32_t size = 0;
    dataBuffer.read(size);
    _sections.resize(size);
    for (uint32_t i = 0; i < size; ++i) {
        bool isNull;
        dataBuffer.read(isNull);
        if (isNull) {
            Section* pSection = IE_POOL_COMPATIBLE_NEW_CLASS(_pool, Section, Section::DEFAULT_TOKEN_NUM, _pool);
            dataBuffer.read(*pSection);
            _sections[i] = pSection;
        } else {
            _sections[i] = NULL;
        }
    }
    dataBuffer.read(_fieldId);
    dataBuffer.read(_sectionUsed);
}

Section* IndexTokenizeField::CreateSection(uint32_t tokenCount)
{
    if (_sectionUsed < MAX_SECTION_PER_FIELD) {
        if (_sections.size() > _sectionUsed) {
            return _sections[_sectionUsed++];
        } else {
            Section* pSection = IE_POOL_COMPATIBLE_NEW_CLASS(_pool, Section, tokenCount);
            pSection->SetSectionId(_sectionUsed++);
            _sections.push_back(pSection);
            return pSection;
        }
    }
    return NULL;
}

void IndexTokenizeField::AddSection(Section* section)
{
    assert(section);
    // TODO: FIXME: to uncomment
    // if (_sectionUsed >= MAX_SECTION_PER_FIELD)
    // {
    //     INDEXLIB_THROW(util::OutOfRangeException,
    //                  "Number of section per field exceed 256");
    // }
    section->SetSectionId(_sections.size());
    _sections.push_back(section);
    _sectionUsed++;
}

Section* IndexTokenizeField::GetSection(sectionid_t sectionId) const
{
    if (sectionId >= _sectionUsed) {
        return NULL;
    }
    return _sections[sectionId];
}

bool IndexTokenizeField::operator==(const Field& field) const
{
    if (!Field::operator==(field)) {
        return false;
    }

    const IndexTokenizeField* typedOtherField = dynamic_cast<const IndexTokenizeField*>(&field);
    if (typedOtherField == nullptr) {
        return false;
    }

    if (_sectionUsed != typedOtherField->_sectionUsed)
        return false;

    for (uint32_t i = 0; i < _sectionUsed; ++i) {
        if (*_sections[i] != *(typedOtherField->_sections[i]))
            return false;
    }
    return true;
}
}} // namespace indexlib::document
