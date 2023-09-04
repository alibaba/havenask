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
#include "suez/turing/expression/util/SectionReaderWrapper.h"

#include <assert.h>
#include <memory>

#include "autil/Log.h"
#include "indexlib/index/inverted_index/SectionAttributeReader.h"

namespace indexlib {
namespace index {
AUTIL_LOG_SETUP(ha3, SectionReaderWrapper);

SectionReaderWrapper::SectionReaderWrapper(const SectionAttributeReader *sectionReader)
    : _sectionReader(sectionReader) {}

SectionReaderWrapper::~SectionReaderWrapper() {}

void SectionReaderWrapper::loadSection(docid_t docId) { _inDocSectionMetaPtr = _sectionReader->GetSection(docId); }

const InDocSectionMetaPtr &SectionReaderWrapper::getInDocSectionMeta() { return _inDocSectionMetaPtr; }

section_len_t SectionReaderWrapper::getSectionLength(fieldid_t fieldId, sectionid_t sectionId) const {
    assert(_inDocSectionMetaPtr);
    return _inDocSectionMetaPtr->GetSectionLen(fieldId, sectionId);
}

section_weight_t SectionReaderWrapper::getSectionWeight(fieldid_t fieldId, sectionid_t sectionId) const {
    assert(_inDocSectionMetaPtr);
    return _inDocSectionMetaPtr->GetSectionWeight(fieldId, sectionId);
}

field_len_t SectionReaderWrapper::getFieldLength(fieldid_t fieldId) const {
    assert(_inDocSectionMetaPtr);
    return _inDocSectionMetaPtr->GetFieldLen(fieldId);
}

field_len_t SectionReaderWrapper::getFieldLengthWithoutDelimiter(fieldid_t fieldId) const {
    assert(_inDocSectionMetaPtr);
    field_len_t fieldLen = _inDocSectionMetaPtr->GetFieldLen(fieldId);
    uint32_t sectionCount = _inDocSectionMetaPtr->GetSectionCountInField(fieldId);
    assert(fieldLen >= sectionCount);
    return fieldLen - sectionCount;
}

uint32_t SectionReaderWrapper::getSectionCountInField(fieldid_t fieldId) const {
    assert(_inDocSectionMetaPtr);
    return _inDocSectionMetaPtr->GetSectionCountInField(fieldId);
}

} // namespace index
} // namespace indexlib
