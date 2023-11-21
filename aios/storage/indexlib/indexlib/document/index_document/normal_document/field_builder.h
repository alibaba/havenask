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

#include "indexlib/common_define.h"
#include "indexlib/document/index_document/normal_document/normal_document.h"
#include "indexlib/document/index_document/normal_document/section.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(document, IndexTokenizeField);

namespace indexlib { namespace document {

class FieldBuilder
{
public:
    static const uint32_t MAX_SECTION_PER_FIELD = Field::MAX_SECTION_PER_FIELD;

public:
    FieldBuilder(indexlib::document::IndexTokenizeField* field, autil::mem_pool::Pool* pool);
    ~FieldBuilder();

public:
    const indexlib::document::Token* AddToken(uint64_t hashKey, pospayload_t posPayload = 0);

    const indexlib::document::Token* AddTokenWithPosition(uint64_t hashKey, pos_t pos, pospayload_t posPayload = 0);
    bool AddPlaceHolder();
    void BeginSection(section_weight_t secWeight);
    section_len_t EndSection();
    uint32_t EndField();

    void SetMaxSectionLen(section_len_t maxSectionLen)
    {
        if (maxSectionLen == 0 || maxSectionLen > Section::MAX_SECTION_LENGTH) {
            mMaxSectionLen = Section::MAX_SECTION_LENGTH;
        } else {
            mMaxSectionLen = maxSectionLen;
        }
    }

    section_len_t GetMaxSectionLen() const { return mMaxSectionLen; }

    void SetMaxSectionPerField(uint32_t maxSectionPerField);

    uint32_t GetMaxSectionPerField() const { return mMaxSectionPerField; }

protected:
    bool GetNewSection();

private:
    IndexTokenizeField* mField;
    Section* mCurrSection;
    section_len_t mCurrSectionLen;
    section_len_t mMaxSectionLen;
    pos_t mLastTokenPos;
    pos_t mPos;
    section_weight_t mSectionWeight;
    pos_t mNextPos;
    uint32_t mMaxSectionPerField;
    autil::mem_pool::Pool* mPool;

private:
    IE_LOG_DECLARE();
};

typedef std::shared_ptr<FieldBuilder> FieldBuilderPtr;
}} // namespace indexlib::document
