#include "indexlib/util/key_hasher_factory.h"

using namespace std;

IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(util);
IE_LOG_SETUP(index, KeyHasherFactory);

KeyHasherFactory::KeyHasherFactory() 
{
}

KeyHasherFactory::~KeyHasherFactory() 
{
}

KeyHasher* KeyHasherFactory::CreatePrimaryKeyHasher(
        FieldType fieldType, PrimaryKeyHashType hashType)
{
    switch (hashType)
    {
    case pk_default_hash:
        return CreateByFieldType(ft_string);
    case pk_number_hash:
        return CreateByFieldType(fieldType);
    case pk_murmur_hash:
        return new MurmurHasher();
    default:
        return NULL;
    }
}

KeyHasher* KeyHasherFactory::Create(IndexType indexType) 
{
    switch (indexType)
    {
    case it_number_int8:
        return new Int8NumberHasher();
    case it_number_uint8:
        return new UInt8NumberHasher();
    case it_number_int16:
        return new Int16NumberHasher();
    case it_number_uint16:
        return new UInt16NumberHasher();
    case it_number_int32:
        return new Int32NumberHasher();
    case it_number_uint32:
        return new UInt32NumberHasher();
    case it_number_int64:
        return new Int64NumberHasher();
    case it_number_uint64:
    case it_spatial:
        return new UInt64NumberHasher();
    case it_unknown:
        assert(false);
        return NULL;
    default:
        return new DefaultHasher();
    }
    return NULL;
}

KeyHasher* KeyHasherFactory::CreateByFieldType(FieldType fieldType) {
    switch (fieldType)
    {
    case ft_int8:
        return new Int8NumberHasher();
    case ft_uint8:
        return new UInt8NumberHasher();
    case ft_int16:
        return new Int16NumberHasher();
    case ft_uint16:
        return new UInt16NumberHasher();
    case ft_int32:
        return new Int32NumberHasher();
    case ft_uint32:
        return new UInt32NumberHasher();
    case ft_int64:
        return new Int64NumberHasher();
    case ft_uint64:
    case ft_location:
        return new UInt64NumberHasher();
    case ft_unknown:
        assert(false);
        return NULL;
    default:
        return new DefaultHasher();
    }
    return NULL;
}

KeyHasher* KeyHasherFactory::Create(FieldType fieldType, bool useNumberHash)
{
    if (!useNumberHash)
    {
        return new MurmurHasher;
    }
    switch(fieldType)
    {
    case ft_int8:
    case ft_int16:
    case ft_int32:
    case ft_int64:
        return new Int64NumberHasher;
    case ft_uint8:
    case ft_uint16:
    case ft_uint32:
    case ft_uint64:
        return new UInt64NumberHasher;
    default:
        return new MurmurHasher;
    }
}

IE_NAMESPACE_END(util);

