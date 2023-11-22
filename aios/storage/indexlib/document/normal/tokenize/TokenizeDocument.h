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
#include <vector>

#include "autil/Log.h"
#include "indexlib/document/normal/tokenize/TokenizeField.h"

namespace indexlib { namespace document {

class TokenizeDocument
{
public:
    typedef std::vector<std::shared_ptr<TokenizeField>> FieldVector;
    typedef FieldVector::iterator Iterator;

public:
    TokenizeDocument();
    ~TokenizeDocument();

private:
    TokenizeDocument(const TokenizeDocument&);
    TokenizeDocument& operator=(const TokenizeDocument&);

public:
    const std::shared_ptr<TokenizeField>& getField(fieldid_t fieldId) const;
    void eraseField(fieldid_t fieldId);
    size_t getFieldCount() const { return _fields.size(); }
    const std::shared_ptr<TokenizeField>& createField(fieldid_t fieldId);

    void reserve(uint32_t fieldNum) { _fields.reserve(fieldNum); }

    void clear();

private:
    FieldVector _fields;
    std::shared_ptr<TokenNodeAllocator> _tokenNodeAllocatorPtr;

private:
    AUTIL_LOG_DECLARE();
};

}} // namespace indexlib::document
