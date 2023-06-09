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
#include "indexlib/document/raw_document/default_raw_doc_field_iterator.h"

using namespace std;

namespace indexlib { namespace document {
IE_LOG_SETUP(document, DefaultRawDocFieldIterator);

DefaultRawDocFieldIterator::DefaultRawDocFieldIterator(const DefaultRawDocument::FieldVec* fieldsKeyPrimary,
                                                       const DefaultRawDocument::FieldVec* fieldsValuePrimary,
                                                       const DefaultRawDocument::FieldVec* fieldsKeyIncrement,
                                                       const DefaultRawDocument::FieldVec* fieldsValueIncrement)
    : mFieldsKeyPrimary(fieldsKeyPrimary)
    , mFieldsValuePrimary(fieldsValuePrimary)
    , mFieldsKeyIncrement(fieldsKeyIncrement)
    , mFieldsValueIncrement(fieldsValueIncrement)
    , mCurCount(0)
{
}

bool DefaultRawDocFieldIterator::IsValid() const
{
    size_t totalCount = 0;
    if (mFieldsKeyIncrement) {
        totalCount += mFieldsKeyIncrement->size();
    }
    if (mFieldsKeyPrimary) {
        totalCount += mFieldsKeyPrimary->size();
    }
    return mCurCount < totalCount;
}

void DefaultRawDocFieldIterator::MoveToNext()
{
    if (IsValid()) {
        ++mCurCount;
    }
}
autil::StringView DefaultRawDocFieldIterator::GetFieldName() const
{
    if (!IsValid()) {
        return autil::StringView::empty_instance();
    }
    size_t primaryCount = 0;
    if (mFieldsKeyPrimary && mCurCount < mFieldsKeyPrimary->size()) {
        return mFieldsKeyPrimary->at(mCurCount);
    }
    if (mFieldsKeyPrimary) {
        primaryCount = mFieldsKeyPrimary->size();
    }
    return mFieldsKeyIncrement->at(mCurCount - primaryCount);
}

autil::StringView DefaultRawDocFieldIterator::GetFieldValue() const
{
    if (!IsValid()) {
        return autil::StringView::empty_instance();
    }
    return mCurCount < mFieldsValuePrimary->size() ? mFieldsValuePrimary->at(mCurCount)
                                                   : mFieldsValueIncrement->at(mCurCount - mFieldsValuePrimary->size());
}
}} // namespace indexlib::document
