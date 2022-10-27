#include "indexlib/document/extend_document/tokenize_document.h"

IE_NAMESPACE_BEGIN(document);
IE_LOG_SETUP(document, TokenizeDocument);

TokenizeDocument::TokenizeDocument()
{
    _tokenNodeAllocatorPtr = TokenNodeAllocatorPool::getAllocator();
}

TokenizeDocument::~TokenizeDocument()
{
}

const TokenizeFieldPtr &TokenizeDocument::createField(fieldid_t fieldId)
{
    assert(fieldId >= 0);

    if ((size_t)fieldId >= _fields.size())
    {
        _fields.resize(fieldId + 1, TokenizeFieldPtr());
    }

    if (_fields[fieldId] == NULL)
    {
        TokenizeFieldPtr field(new TokenizeField(_tokenNodeAllocatorPtr));
        field->setFieldId(fieldId);
        _fields[fieldId] = field;
    }
    return _fields[fieldId];
}

const TokenizeFieldPtr &TokenizeDocument::getField(fieldid_t fieldId) const
{
    if ((size_t)fieldId >= _fields.size())
    {
        static TokenizeFieldPtr tokenizeFieldPtr;
        return tokenizeFieldPtr;
    }
    return _fields[fieldId];
}

IE_NAMESPACE_END(document);
