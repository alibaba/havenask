#ifndef __INDEXLIB_ATTRIBUTE_DOCUMENT_H
#define __INDEXLIB_ATTRIBUTE_DOCUMENT_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/document/index_document/normal_document/summary_document.h"

IE_NAMESPACE_BEGIN(document);

class AttributeDocument
{
public:
    // NormalAttributeDocument has no pack attributes
    typedef SummaryDocument NormalAttributeDocument;
    typedef SummaryDocument::Iterator Iterator;
    typedef SummaryDocument::FieldVector FieldVector;
public:
    AttributeDocument(autil::mem_pool::Pool* pool = NULL)
        : mFieldFormatError(false)
    {}
    
    ~AttributeDocument() {}
public:
    Iterator CreateIterator() const
    {
        return mNormalAttrDoc.CreateIterator();
    }
    
    void SetDocId(docid_t docId){ mNormalAttrDoc.SetDocId(docId); }
    
    docid_t GetDocId() const { return mNormalAttrDoc.GetDocId(); }
    
    void SetField(fieldid_t id, const autil::ConstString& value)
    { mNormalAttrDoc.SetField(id, value); }
    
    void ClearFields(const std::vector<fieldid_t>& fieldIds)
    { mNormalAttrDoc.ClearFields(fieldIds); }
    
    bool HasField(fieldid_t id) const
    { return mNormalAttrDoc.HasField(id); }
    const autil::ConstString& GetField(fieldid_t id) const
    { return mNormalAttrDoc.GetField(id); }

    uint32_t GetNotEmptyFieldCount() const
    {
        return mNormalAttrDoc.GetNotEmptyFieldCount();
    }

    size_t GetPackFieldCount() const
    {
        return mPackFields.size();
    }
    
    void Reserve(size_t size)
    { mNormalAttrDoc.Reserve(size); }
    
    void Reset()
    {
        mNormalAttrDoc.Reset();
        ResetPackFields();
        mFieldFormatError = false;
    }

    void serialize(autil::DataBuffer &dataBuffer) const;
    void deserialize(autil::DataBuffer &dataBuffer, autil::mem_pool::Pool *pool,
                     uint32_t docVersion = DOCUMENT_BINARY_VERSION);
    
    bool operator == (const AttributeDocument& right) const;
    bool operator != (const AttributeDocument& right) const;

    void SetPackField(packattrid_t packId, const autil::ConstString& value);
    const autil::ConstString& GetPackField(packattrid_t packId) const;

    void SetFormatError(bool hasError) { mFieldFormatError = hasError; }
    bool& GetFormatErrorLable() { return mFieldFormatError; }
    bool HasFormatError() const { return mFieldFormatError; }
    
private:
    void ResetPackFields()
    { 
        mPackFields.clear();
    }
    
private:
    NormalAttributeDocument mNormalAttrDoc;
    FieldVector mPackFields;
    bool mFieldFormatError;

private:
    IE_LOG_DECLARE();
    
private:
    friend class DocumentTest;
};

DEFINE_SHARED_PTR(AttributeDocument);

inline void AttributeDocument::serialize(autil::DataBuffer &dataBuffer) const
{
    mNormalAttrDoc.serialize(dataBuffer);
    dataBuffer.write(mPackFields);
}

inline void AttributeDocument::deserialize(
        autil::DataBuffer &dataBuffer, autil::mem_pool::Pool *pool, uint32_t docVersion)
{
    ResetPackFields();
    mNormalAttrDoc.deserialize(dataBuffer, pool);
    dataBuffer.read(mPackFields, pool);
}

inline bool AttributeDocument::operator == (const AttributeDocument& right) const 
{
    return mPackFields == right.mPackFields
        && mNormalAttrDoc == right.mNormalAttrDoc;
}

inline bool AttributeDocument::operator != (const AttributeDocument& right) const 
{
    return !( operator == (right));
}

inline void AttributeDocument::SetPackField(
    packattrid_t packId, const autil::ConstString& value)
{
    if (packId >= (packattrid_t)mPackFields.size())
    {
        mPackFields.resize(packId + 1);
    }
    mPackFields[packId] = value;
}

inline const autil::ConstString& AttributeDocument::GetPackField(
    packattrid_t packId) const
{
    if (packId != INVALID_ATTRID && packId < (packattrid_t)mPackFields.size())
    {
        return mPackFields[size_t(packId)];
    }
    return autil::ConstString::EMPTY_STRING;
}

IE_NAMESPACE_END(document);

#endif //__INDEXLIB_ATTRIBUTE_DOCUMENT_H
