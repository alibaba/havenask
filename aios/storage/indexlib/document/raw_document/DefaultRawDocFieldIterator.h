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

#include "indexlib/document/RawDocFieldIterator.h"

namespace indexlibv2 { namespace document {

class DefaultRawDocFieldIterator : public RawDocFieldIterator
{
public:
    typedef std::vector<autil::StringView> FieldVec;

public:
    DefaultRawDocFieldIterator(const FieldVec* fieldsKeyPrimary, const FieldVec* fieldsValuePrimary,
                               const FieldVec* fieldsKeyIncrement, const FieldVec* fieldsValueIncrement);
    ~DefaultRawDocFieldIterator() {}

public:
    bool IsValid() const override;
    void MoveToNext() override;
    autil::StringView GetFieldName() const override;
    autil::StringView GetFieldValue() const override;

private:
    const FieldVec* _fieldsKeyPrimary;
    const FieldVec* _fieldsValuePrimary;
    const FieldVec* _fieldsKeyIncrement;
    const FieldVec* _fieldsValueIncrement;

private:
    uint32_t _curCount;
    AUTIL_LOG_DECLARE();
};

}} // namespace indexlibv2::document
