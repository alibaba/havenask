#ifndef __INDEXLIB_TOKENIZEFIELD_H
#define __INDEXLIB_TOKENIZEFIELD_H

#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/document/extend_document/tokenize/tokenize_section.h"

IE_NAMESPACE_BEGIN(document);

class TokenizeField
{
public:
    class Iterator;

public:
    TokenizeField(const TokenNodeAllocatorPtr &tokenNodeAllocatorPtr);
    ~TokenizeField();
public:
    fieldid_t getFieldId() const { return _fieldId; }
    void setFieldId(fieldid_t fieldId) { _fieldId = fieldId; }

    TokenizeSection* getNewSection();

    uint32_t getSectionCount() const{
        return _sectionVector.size();
    }

    void erase(Iterator &iterator) {
        if(iterator._curIterator < iterator._endIterator)
        {
            if (*iterator) {
                delete *iterator;
            }
            iterator._curIterator = _sectionVector.erase(iterator._curIterator);
            iterator._endIterator = _sectionVector.end();
        }
    }

    bool isEmpty();
    Iterator createIterator() {return Iterator(*this);}

private:
    typedef std::vector<TokenizeSection *> SectionVector;

public:
    class Iterator{
    public:
        Iterator(){};
        Iterator(TokenizeField &tokenizeField);

    public:
        bool next();
        bool isEnd() {
            return _curIterator == _endIterator;

        }

        TokenizeSection* operator *() const {
            if (_curIterator < _endIterator) {
                return *_curIterator;
            }
            return NULL;
        }

    private:
        SectionVector::iterator _curIterator;
        SectionVector::iterator _endIterator;
    private:
        friend class TokenizeField;
    };

private:
    fieldid_t _fieldId;
    SectionVector _sectionVector;
    TokenNodeAllocatorPtr _tokenNodeAllocatorPtr;
private:
    friend class Iterator;
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TokenizeField);

IE_NAMESPACE_END(document);

#endif //__INDEXLIB_TOKENIZEFIELD_H
