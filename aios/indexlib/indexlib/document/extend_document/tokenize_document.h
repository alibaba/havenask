#ifndef __INDEXLIB_TOKENIZEDOCUMENT_H
#define __INDEXLIB_TOKENIZEDOCUMENT_H

#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/document/extend_document/tokenize/tokenize_field.h"

IE_NAMESPACE_BEGIN(document);

class TokenizeDocument
{
public:
    typedef std::vector<TokenizeFieldPtr> FieldVector;
    typedef FieldVector::iterator Iterator;
public:
    TokenizeDocument();
    ~TokenizeDocument();
private:
    TokenizeDocument(const TokenizeDocument &);
    TokenizeDocument& operator = (const TokenizeDocument &);

public:
    const TokenizeFieldPtr &getField(fieldid_t fieldId) const;
    size_t getFieldCount() const { return _fields.size();}
    const TokenizeFieldPtr &createField(fieldid_t fieldId);

    void reserve(uint32_t fieldNum) { _fields.reserve(fieldNum); }

private:
    FieldVector _fields;
    TokenNodeAllocatorPtr _tokenNodeAllocatorPtr;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TokenizeDocument);

IE_NAMESPACE_END(document);

#endif //__INDEXLIB_TOKENIZEDOCUMENT_H
