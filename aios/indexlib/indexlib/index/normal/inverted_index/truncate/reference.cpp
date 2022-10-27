#include "indexlib/index/normal/inverted_index/truncate/reference.h"

using namespace std;

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, Reference);

Reference::Reference(size_t offset, FieldType fieldType) 
    : mOffset(offset)
    , mFieldType(fieldType)
{
}

Reference::~Reference() 
{
}

size_t Reference::GetOffset() const 
{
    return mOffset; 
}

FieldType Reference::GetFieldType() const 
{ 
    return mFieldType; 
}


IE_NAMESPACE_END(index);

