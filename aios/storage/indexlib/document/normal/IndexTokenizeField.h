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

#include "autil/mem_pool/pool_allocator.h"
#include "indexlib/document/normal/Field.h"
#include "indexlib/document/normal/Section.h"

namespace indexlib { namespace document {

#pragma pack(push, 2)

class IndexTokenizeField : public Field
{
public:
    const static size_t MAX_SECTION_PER_FIELD = 256;

public:
    IndexTokenizeField(autil::mem_pool::Pool* pool = NULL);
    ~IndexTokenizeField();

public:
    typedef autil::mem_pool::pool_allocator<Section*> Alloc;
    typedef std::vector<Section*, Alloc> SectionVector;
    typedef SectionVector::const_iterator Iterator;

public:
    void Reset() override;
    void serialize(autil::DataBuffer& dataBuffer) const override;
    void deserialize(autil::DataBuffer& dataBuffer) override;
    bool operator==(const Field& field) const override;
    bool operator!=(const Field& field) const override { return !(*this == field); }

public:
    // used for reusing section object
    Section* CreateSection(uint32_t tokenCount = Section::DEFAULT_TOKEN_NUM);

    void AddSection(Section* section);
    Section* GetSection(sectionid_t sectionId) const;

    size_t GetSectionCount() const { return _sectionUsed; }
    void SetSectionCount(sectionid_t sectionCount) { _sectionUsed = sectionCount; }

    Iterator Begin() const { return _sections.begin(); }
    Iterator End() const { return _sections.end(); }

private:
    autil::mem_pool::Pool _selfPool;
    SectionVector _sections;
    sectionid_t _sectionUsed;

private:
    AUTIL_LOG_DECLARE();
};

#pragma pack(pop)

}} // namespace indexlib::document
