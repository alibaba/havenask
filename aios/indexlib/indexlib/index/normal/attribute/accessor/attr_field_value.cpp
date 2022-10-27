#include "indexlib/index/normal/attribute/accessor/attr_field_value.h"

using namespace std;

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, AttrFieldValue);

AttrFieldValue::AttrFieldValue() 
{
    Reset();
}

AttrFieldValue::~AttrFieldValue() 
{}

void AttrFieldValue::Reset()
{
    mIdentifier.fieldId = INVALID_FIELDID;
    mSize = 0;
    mDocId = INVALID_DOCID;
    mIsSubDocId = false;
    mConstStringValue.reset(mBuffer.GetBuffer(), mSize);
    mIsPackAttr = false;
}

AttrFieldValue::AttrFieldValue(const AttrFieldValue& other)
{
    ReserveBuffer(other.mBuffer.GetBufferSize());
    mIdentifier = other.mIdentifier;
    mSize = other.mSize;
    if (mBuffer.GetBuffer() != NULL)
    {
        memcpy(mBuffer.GetBuffer(), other.mBuffer.GetBuffer(), other.mSize);
    }
    mDocId = other.mDocId;
    mIsSubDocId = other.mIsSubDocId;
    mConstStringValue.reset((char*)mBuffer.GetBuffer(), mSize);
    mIsPackAttr = other.mIsPackAttr;
}


IE_NAMESPACE_END(index);

