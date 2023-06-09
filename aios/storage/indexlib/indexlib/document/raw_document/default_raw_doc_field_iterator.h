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
#ifndef __INDEXLIB_DEFAULT_RAW_DOC_FIELD_ITERATOR_H
#define __INDEXLIB_DEFAULT_RAW_DOC_FIELD_ITERATOR_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/document/raw_doc_field_iterator.h"
#include "indexlib/document/raw_document/default_raw_document.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace document {

class DefaultRawDocFieldIterator : public RawDocFieldIterator
{
public:
    DefaultRawDocFieldIterator(const DefaultRawDocument::FieldVec* fieldsKeyPrimary,
                               const DefaultRawDocument::FieldVec* fieldsValuePrimary,
                               const DefaultRawDocument::FieldVec* fieldsKeyIncrement,
                               const DefaultRawDocument::FieldVec* fieldsValueIncrement);
    ~DefaultRawDocFieldIterator() {}

public:
    virtual bool IsValid() const override;
    virtual void MoveToNext() override;
    virtual autil::StringView GetFieldName() const override;
    virtual autil::StringView GetFieldValue() const override;

private:
    const DefaultRawDocument::FieldVec* mFieldsKeyPrimary;
    const DefaultRawDocument::FieldVec* mFieldsValuePrimary;
    const DefaultRawDocument::FieldVec* mFieldsKeyIncrement;
    const DefaultRawDocument::FieldVec* mFieldsValueIncrement;
    uint32_t mCurCount;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DefaultRawDocFieldIterator);
}} // namespace indexlib::document

#endif //__INDEXLIB_DEFAULT_RAW_DOC_FIELD_ITERATOR_H
