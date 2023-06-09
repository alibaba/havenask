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
#include "indexlib/document/normal/tokenize/TokenizeDocument.h"

namespace indexlib { namespace document {
AUTIL_LOG_SETUP(indexlib.document, TokenizeDocument);

TokenizeDocument::TokenizeDocument() { _tokenNodeAllocatorPtr = TokenNodeAllocatorPool::getAllocator(); }

TokenizeDocument::~TokenizeDocument() {}

const std::shared_ptr<TokenizeField>& TokenizeDocument::createField(fieldid_t fieldId)
{
    assert(fieldId >= 0);

    if ((size_t)fieldId >= _fields.size()) {
        _fields.resize(fieldId + 1, std::shared_ptr<TokenizeField>());
    }

    if (_fields[fieldId] == NULL) {
        auto field = std::make_shared<TokenizeField>(_tokenNodeAllocatorPtr);
        field->setFieldId(fieldId);
        _fields[fieldId] = field;
    }
    return _fields[fieldId];
}

void TokenizeDocument::eraseField(fieldid_t fieldId)
{
    assert(fieldId >= 0);
    if (fieldId < _fields.size()) {
        _fields[fieldId].reset();
    }
}

const std::shared_ptr<TokenizeField>& TokenizeDocument::getField(fieldid_t fieldId) const
{
    if ((size_t)fieldId >= _fields.size()) {
        static std::shared_ptr<TokenizeField> tokenizeFieldPtr;
        return tokenizeFieldPtr;
    }
    return _fields[fieldId];
}
}} // namespace indexlib::document
