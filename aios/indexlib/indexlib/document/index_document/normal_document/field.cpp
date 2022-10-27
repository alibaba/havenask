
#include "indexlib/document/index_document/normal_document/field.h"
#include "indexlib/misc/exception.h"

using namespace std;

IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(document);

Field::Field(autil::mem_pool::Pool* pool, FieldTag fieldTag)
    : mPool(pool)
    , mFieldId(INVALID_FIELDID)
    , mFieldTag(fieldTag)
{
}

Field::~Field()
{
}

bool Field::operator==(const Field& field) const
{
    if (this == & field)
        return true;
    if (mFieldId != field.mFieldId || mFieldTag != field.mFieldTag)
        return false;
    return true;
}

IE_NAMESPACE_END(document);

