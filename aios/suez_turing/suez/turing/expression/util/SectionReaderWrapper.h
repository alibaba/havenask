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
#include <stdint.h>

#include "autil/Log.h" // IWYU pragma: keep
#include "indexlib/index/inverted_index/InDocSectionMeta.h"
#include "indexlib/indexlib.h"
#include "indexlib/misc/common.h"

namespace indexlib {
namespace index {
class SectionAttributeReader;
} // namespace index
} // namespace indexlib

namespace indexlib {
namespace index {

class SectionReaderWrapper {
public:
    SectionReaderWrapper(const SectionAttributeReader *sectionReader);
    virtual ~SectionReaderWrapper();

public:
    /* load section for your given 'docId'.
     * WARN: all the getSectionXXX() fuction should be called after called loadSection() */
    void loadSection(docid_t docId);

    const InDocSectionMetaPtr &getInDocSectionMeta();

    virtual section_len_t getSectionLength(fieldid_t fieldId, sectionid_t sectionId) const;

    virtual section_weight_t getSectionWeight(fieldid_t fieldId, sectionid_t sectionId) const;

    virtual field_len_t getFieldLength(fieldid_t fieldId) const;

    virtual field_len_t getFieldLengthWithoutDelimiter(fieldid_t fieldId) const;

    virtual uint32_t getSectionCountInField(fieldid_t fieldId) const;

private:
    const SectionAttributeReader *_sectionReader;
    InDocSectionMetaPtr _inDocSectionMetaPtr;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<SectionReaderWrapper> SectionReaderWrapperPtr;

} // namespace index
} // namespace indexlib
