#ifndef __INDEXLIB_ATTR_FIELD_VALUE_H
#define __INDEXLIB_ATTR_FIELD_VALUE_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/util/mem_buffer.h"
#include "indexlib/index_define.h"
#include <autil/ConstString.h>

IE_NAMESPACE_BEGIN(index);

class AttrFieldValue
{
public:
    union AttrIdentifier
    {
        fieldid_t fieldId;
        packattrid_t packAttrId;
    };
    
public:
    AttrFieldValue();
    ~AttrFieldValue();

private:
    AttrFieldValue(const AttrFieldValue& other);

public:
    uint8_t* Data() const { return (uint8_t*)mBuffer.GetBuffer(); }

    const autil::ConstString* GetConstStringData() const
    { return &mConstStringValue; }

    void SetDataSize(size_t size)
    { 
        mSize = size; 
        mConstStringValue.reset(mBuffer.GetBuffer(), mSize);
    }

    size_t GetDataSize() const { return mSize; }

    void ReserveBuffer(size_t size)
    { mBuffer.Reserve(size); }

    size_t BufferLength() const
    { return mBuffer.GetBufferSize(); }

    void SetFieldId(fieldid_t fieldId) { mIdentifier.fieldId = fieldId; }
    fieldid_t GetFieldId() const { return mIdentifier.fieldId; }

    void SetPackAttrId(packattrid_t packAttrId) { mIdentifier.packAttrId = packAttrId; }
    packattrid_t GetPackAttrId() const { return mIdentifier.packAttrId; }

    void SetIsPackAttr(bool isPacked) { mIsPackAttr = isPacked; }
    bool IsPackAttr() const { return mIsPackAttr; }

    void SetDocId(docid_t docId) { mDocId = docId; }
    docid_t GetDocId() const { return mDocId; }

    void SetIsSubDocId(bool isSubDocId) { mIsSubDocId = isSubDocId; }
    bool IsSubDocId() const { return mIsSubDocId; }

    void Reset();
private:
    autil::ConstString mConstStringValue;
    size_t mSize;
    util::MemBuffer mBuffer;
    AttrIdentifier mIdentifier;
    docid_t mDocId;
    bool mIsSubDocId;
    bool mIsPackAttr;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(AttrFieldValue);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_ATTR_FIELD_VALUE_H
