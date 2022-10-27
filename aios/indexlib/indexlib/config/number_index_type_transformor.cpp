#include "indexlib/config/number_index_type_transformor.h"
#include "indexlib/config/field_config.h"
using namespace std;

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, NumberIndexTypeTransformor);
using namespace config;

NumberIndexTypeTransformor::NumberIndexTypeTransformor() 
{
}

NumberIndexTypeTransformor::~NumberIndexTypeTransformor() 
{
}

void NumberIndexTypeTransformor::Transform(IndexConfigPtr indexConfig, IndexType& type)
{
    //single field in number index
    assert(indexConfig->GetFieldCount() == 1);
    IndexConfig::Iterator iter = indexConfig->CreateIterator();
    FieldConfigPtr fieldConfig = iter.Next();
    type = TransformFieldTypeToIndexType(fieldConfig->GetFieldType());
}

IndexType NumberIndexTypeTransformor::TransformFieldTypeToIndexType(FieldType fieldType)
{
    IndexType type = it_unknown;
    switch(fieldType) 
    {
    case ft_int8: 
        type = it_number_int8; 
        break;
    case ft_uint8:
        type = it_number_uint8; 
        break;
    case ft_int16:
        type = it_number_int16; 
        break;
    case ft_uint16: 
        type = it_number_uint16; 
        break;
    case ft_integer: 
        type = it_number_int32; 
        break;
    case ft_uint32: 
        type = it_number_uint32; 
        break;
    case ft_long: 
        type = it_number_int64; 
        break;
    case ft_uint64: 
        type = it_number_uint64; 
        break;
    default: 
        assert(false); 
        break;
    }
    return type;
}

IE_NAMESPACE_END(config);

