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
#include "indexlib/document/index_document/normal_document/field_builder.h"

#include "indexlib/document/index_document/normal_document/index_tokenize_field.h"
#include "indexlib/util/Exception.h"

using namespace std;
using namespace autil::legacy;

namespace indexlib { namespace document {
IE_LOG_SETUP(document, FieldBuilder);

FieldBuilder::FieldBuilder(IndexTokenizeField* field, autil::mem_pool::Pool* pool)
    : mField(field)
    , mCurrSection(NULL)
    , mCurrSectionLen(0)
    , mMaxSectionLen(Section::MAX_SECTION_LENGTH)
    , mLastTokenPos(0)
    , mPos(0)
    , mSectionWeight(0)
    , mNextPos(0)
    , mMaxSectionPerField(MAX_SECTION_PER_FIELD)
    , mPool(pool)
{
    assert(mField);
    (void)mPool;
}

FieldBuilder::~FieldBuilder() {}

bool FieldBuilder::GetNewSection()
{
    if (mCurrSectionLen == mMaxSectionLen - 1 || mCurrSection == NULL) {
        if (mCurrSectionLen > 0 && mCurrSection) {
            EndSection();
        }
        if (mField->GetSectionCount() >= mMaxSectionPerField) {
            return false;
        }
        mCurrSection = mField->CreateSection();
        if (!mCurrSection) // exceed max section number per field
        {
            return false;
        }
    }
    return true;
}

const Token* FieldBuilder::AddTokenWithPosition(uint64_t hashKey, pos_t pos, pospayload_t posPayload)
{
    assert(pos >= mNextPos);
    uint32_t placeHolderCount = pos - mNextPos;
    for (uint32_t i = 0; i < placeHolderCount; ++i) {
        if (!AddPlaceHolder())
            return NULL;
    }
    mNextPos = pos + 1;
    return AddToken(hashKey, posPayload);
}

const Token* FieldBuilder::AddToken(uint64_t hashKey, pospayload_t posPayload)
{
    if (!GetNewSection()) {
        return NULL;
    }
    Token* token = mCurrSection->CreateToken(hashKey, 0, 0);
    assert(token);

    token->SetPosPayload(posPayload);
    token->SetPosIncrement(mPos - mLastTokenPos);
    ++mCurrSectionLen;
    mLastTokenPos = mPos;
    ++mPos;
    return token;
}

bool FieldBuilder::AddPlaceHolder()
{
    if (!GetNewSection()) {
        return false;
    }

    ++mPos;
    ++mCurrSectionLen;
    return true;
}

void FieldBuilder::BeginSection(section_weight_t secWeight) { mSectionWeight = secWeight; }

section_len_t FieldBuilder::EndSection()
{
    section_len_t ret = mCurrSectionLen;
    if (mCurrSectionLen > 0) {
        ++mPos; // add section gap
        mCurrSection->SetLength(mCurrSectionLen + 1);
        mCurrSection->SetWeight(mSectionWeight);
        mCurrSectionLen = 0;
        mCurrSection = NULL;
        return ret + 1;
    }
    return ret;
}

uint32_t FieldBuilder::EndField()
{
    assert(mField);
    EndSection();

    if (mCurrSection && mCurrSection->GetLength() == 0) {
        mField->SetSectionCount(mField->GetSectionCount() - 1);
    }

    uint32_t fieldLen = 0;
    for (auto it = mField->Begin(); it != mField->End(); ++it) {
        const Section* section = (*it);
        if (section->GetLength() == 0) {
            INDEXLIB_THROW(RuntimeError, "Section Length == 0 !!!");
        }
        fieldLen += section->GetLength();
    }
    if ((pos_t)fieldLen != mPos) {
        INDEXLIB_THROW(RuntimeError, "Total Section Length Mismatch");
    }
    return fieldLen;
}

void FieldBuilder::SetMaxSectionPerField(uint32_t maxSectionPerField)
{
    mMaxSectionPerField = maxSectionPerField > 1 ? maxSectionPerField : 1;
    mMaxSectionPerField = mMaxSectionPerField > MAX_SECTION_PER_FIELD ? MAX_SECTION_PER_FIELD : mMaxSectionPerField;
}
}} // namespace indexlib::document
