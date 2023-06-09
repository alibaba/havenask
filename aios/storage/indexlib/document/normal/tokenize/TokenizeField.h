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

#include <vector>

#include "autil/Log.h"
#include "indexlib/base/Types.h"
#include "indexlib/document/normal/tokenize/TokenizeSection.h"

namespace indexlib { namespace document {

class TokenizeSection;

class TokenizeField
{
public:
    class Iterator;

public:
    TokenizeField(const std::shared_ptr<TokenNodeAllocator>& tokenNodeAllocatorPtr);
    ~TokenizeField();

public:
    fieldid_t getFieldId() const { return _fieldId; }
    void setFieldId(fieldid_t fieldId) { _fieldId = fieldId; }

    TokenizeSection* getNewSection();

    uint32_t getSectionCount() const { return _sectionVector.size(); }

    void erase(Iterator& iterator)
    {
        if (iterator._curIterator < iterator._endIterator) {
            if (*iterator) {
                delete *iterator;
            }
            iterator._curIterator = _sectionVector.erase(iterator._curIterator);
            iterator._endIterator = _sectionVector.end();
        }
    }

    void setNull(bool flag) { _isNull = flag; }
    bool isNull() const { return _isNull && isEmpty(); }

    bool isEmpty() const;
    Iterator createIterator() const { return Iterator(*this); }

private:
    typedef std::vector<TokenizeSection*> SectionVector;

public:
    class Iterator
    {
    public:
        Iterator() {};
        Iterator(const TokenizeField& tokenizeField);

    public:
        bool next();
        bool isEnd() { return _curIterator == _endIterator; }

        TokenizeSection* operator*() const
        {
            if (_curIterator < _endIterator) {
                return *_curIterator;
            }
            return NULL;
        }

    private:
        SectionVector::const_iterator _curIterator;
        SectionVector::const_iterator _endIterator;

    private:
        friend class TokenizeField;
    };

private:
    fieldid_t _fieldId;
    bool _isNull;
    SectionVector _sectionVector;
    std::shared_ptr<TokenNodeAllocator> _tokenNodeAllocatorPtr;

private:
    friend class Iterator;

private:
    AUTIL_LOG_DECLARE();
};

}} // namespace indexlib::document
