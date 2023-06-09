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
#include "indexlib/document/raw_document/DefaultRawDocFieldIterator.h"

using namespace std;

namespace indexlibv2 { namespace document {
AUTIL_LOG_SETUP(indexlib.document, DefaultRawDocFieldIterator);

DefaultRawDocFieldIterator::DefaultRawDocFieldIterator(const FieldVec* fieldsKeyPrimary,
                                                       const FieldVec* fieldsValuePrimary,
                                                       const FieldVec* fieldsKeyIncrement,
                                                       const FieldVec* fieldsValueIncrement)
    : _fieldsKeyPrimary(fieldsKeyPrimary)
    , _fieldsValuePrimary(fieldsValuePrimary)
    , _fieldsKeyIncrement(fieldsKeyIncrement)
    , _fieldsValueIncrement(fieldsValueIncrement)
    , _curCount(0)
{
}

bool DefaultRawDocFieldIterator::IsValid() const
{
    size_t totalCount = 0;
    if (_fieldsKeyIncrement) {
        totalCount += _fieldsKeyIncrement->size();
    }
    if (_fieldsKeyPrimary) {
        totalCount += _fieldsKeyPrimary->size();
    }
    return _curCount < totalCount;
}

void DefaultRawDocFieldIterator::MoveToNext()
{
    if (IsValid()) {
        ++_curCount;
    }
}
autil::StringView DefaultRawDocFieldIterator::GetFieldName() const
{
    if (!IsValid()) {
        return autil::StringView::empty_instance();
    }
    size_t primaryCount = 0;
    if (_fieldsKeyPrimary && _curCount < _fieldsKeyPrimary->size()) {
        return _fieldsKeyPrimary->at(_curCount);
    }
    if (_fieldsKeyPrimary) {
        primaryCount = _fieldsKeyPrimary->size();
    }
    return _fieldsKeyIncrement->at(_curCount - primaryCount);
}

autil::StringView DefaultRawDocFieldIterator::GetFieldValue() const
{
    if (!IsValid()) {
        return autil::StringView::empty_instance();
    }
    return _curCount < _fieldsValuePrimary->size() ? _fieldsValuePrimary->at(_curCount)
                                                   : _fieldsValueIncrement->at(_curCount - _fieldsValuePrimary->size());
}
}} // namespace indexlibv2::document
