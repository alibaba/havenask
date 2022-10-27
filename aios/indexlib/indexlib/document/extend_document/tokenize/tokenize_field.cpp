#include "indexlib/document/extend_document/tokenize/tokenize_field.h"

IE_NAMESPACE_BEGIN(document);
IE_LOG_SETUP(document, TokenizeField);

TokenizeField::TokenizeField(const TokenNodeAllocatorPtr &tokenNodeAllocatorPtr)
{
    _fieldId = 0;
    _tokenNodeAllocatorPtr = tokenNodeAllocatorPtr;
}

TokenizeField::~TokenizeField()
{
    for (SectionVector::iterator it = _sectionVector.begin();
        it < _sectionVector.end(); ++it)
    {
        if (NULL != *it) {
            delete *it;
            *it = NULL;
        }
    }
}

TokenizeSection* TokenizeField::getNewSection()
{
    TokenizeSection *section = new TokenizeSection(_tokenNodeAllocatorPtr);
    _sectionVector.push_back(section);
    return section;
}

bool TokenizeField::isEmpty() {
    if (_sectionVector.size() == 0) {
        return true;
    }
    Iterator it = createIterator();
    while(!it.isEnd()) {
        if ((*it) && (*it)->getBasicLength() != 0) {
            return false;
        }
        it.next();
    }
    return true;
}
///////////////////// Iterator /////
TokenizeField::Iterator::Iterator(TokenizeField &tokenizeField) {
    _curIterator = tokenizeField._sectionVector.begin();
    _endIterator = tokenizeField._sectionVector.end();
}

bool TokenizeField::Iterator::next() {
    if (_curIterator < _endIterator) {
        ++_curIterator;
        return true;
    }
    return false;
}

IE_NAMESPACE_END(document);
