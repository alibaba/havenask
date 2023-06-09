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
#include "indexlib/document/normal/tokenize/TokenizeField.h"

namespace indexlib { namespace document {
AUTIL_LOG_SETUP(indexlib.document, TokenizeField);

TokenizeField::TokenizeField(const std::shared_ptr<TokenNodeAllocator>& tokenNodeAllocatorPtr)
    : _fieldId(0)
    , _isNull(false)
    , _tokenNodeAllocatorPtr(tokenNodeAllocatorPtr)
{
}

TokenizeField::~TokenizeField()
{
    for (SectionVector::iterator it = _sectionVector.begin(); it < _sectionVector.end(); ++it) {
        if (NULL != *it) {
            delete *it;
            *it = NULL;
        }
    }
}

TokenizeSection* TokenizeField::getNewSection()
{
    TokenizeSection* section = new TokenizeSection(_tokenNodeAllocatorPtr);
    _sectionVector.push_back(section);
    return section;
}

bool TokenizeField::isEmpty() const
{
    if (_sectionVector.size() == 0) {
        return true;
    }
    Iterator it = createIterator();
    while (!it.isEnd()) {
        if ((*it) && (*it)->getBasicLength() != 0) {
            return false;
        }
        it.next();
    }
    return true;
}
///////////////////// Iterator /////
TokenizeField::Iterator::Iterator(const TokenizeField& tokenizeField)
{
    _curIterator = tokenizeField._sectionVector.begin();
    _endIterator = tokenizeField._sectionVector.end();
}

bool TokenizeField::Iterator::next()
{
    if (_curIterator < _endIterator) {
        ++_curIterator;
        return true;
    }
    return false;
}
}} // namespace indexlib::document
