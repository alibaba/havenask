#include "indexlib/document/index_document/normal_document/index_raw_field.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

IE_NAMESPACE_BEGIN(document);
IE_LOG_SETUP(document, IndexRawField);

IndexRawField::IndexRawField(Pool* pool)
    : Field(pool, FieldTag::RAW_FIELD)
{
}

IndexRawField::~IndexRawField() 
{
}

void IndexRawField::Reset()
{
    assert(false);
}

void IndexRawField::serialize(autil::DataBuffer &dataBuffer) const
{
    dataBuffer.write(mData);
    dataBuffer.write(mFieldId);
}

void IndexRawField::deserialize(autil::DataBuffer &dataBuffer)
{
    ConstString value;
    dataBuffer.read(value, mPool);
    mData = value;
    dataBuffer.read(mFieldId);
}

bool IndexRawField::operator==(const Field& field) const
{
    if (!Field::operator == (field)) {
        return false;
    }
    const IndexRawField* typedOtherField =
        dynamic_cast<const IndexRawField*>(&field);
    if (typedOtherField == nullptr) {
        return false;
    }

    return mData == typedOtherField->mData;
}

IE_NAMESPACE_END(document);

