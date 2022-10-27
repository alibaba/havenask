#include "indexlib/config/primary_key_index_config.h"
#include "indexlib/config/impl/primary_key_index_config_impl.h"
#include "indexlib/config/configurator_define.h"
#include "indexlib/config/config_define.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, PrimaryKeyIndexConfigImpl);

PrimaryKeyIndexConfigImpl::PrimaryKeyIndexConfigImpl(const string& indexName,
                                                     IndexType indexType)
    : SingleFieldIndexConfigImpl(indexName, indexType)
    , mPkIndexType(pk_sort_array)
    , mPkHashType(pk_default_hash)
{
    mPkLoadParam.reset(new PrimaryKeyLoadStrategyParam());
}

PrimaryKeyIndexConfigImpl::PrimaryKeyIndexConfigImpl(const PrimaryKeyIndexConfigImpl& other)
    : SingleFieldIndexConfigImpl(other)
    , mPkLoadParam(other.mPkLoadParam)
    , mPkIndexType(other.mPkIndexType)
    , mPkHashType(other.mPkHashType)
{}

PrimaryKeyIndexConfigImpl::~PrimaryKeyIndexConfigImpl()
{}

IndexConfigImpl* PrimaryKeyIndexConfigImpl::Clone() const
{
    return new PrimaryKeyIndexConfigImpl(*this);
}

void PrimaryKeyIndexConfigImpl::Jsonize(Jsonizable::JsonWrapper& json)
{
    SingleFieldIndexConfigImpl::Jsonize(json);
    if (json.GetMode() == TO_JSON) 
    {
        string pkIndexTypeStr = PkIndexTypeToString(mPkIndexType);
        json.Jsonize(PRIMARY_KEY_STORAGE_TYPE, pkIndexTypeStr);

        string pkHashTypeStr = PkHashTypeToString(mPkHashType);
        json.Jsonize("pk_hash_type", pkHashTypeStr);
    }
    else
    {
        string pkIndexTypeStr = "sort_array";
        json.Jsonize(PRIMARY_KEY_STORAGE_TYPE, pkIndexTypeStr, pkIndexTypeStr);
        mPkIndexType = StringToPkIndexType(pkIndexTypeStr);

        string pkHashTypeStr = "default_hash";
        json.Jsonize("pk_hash_type", pkHashTypeStr, pkHashTypeStr);
        mPkHashType = StringToPkHashType(pkHashTypeStr);
        
        bool useNumberPkHash = false;
        json.Jsonize(USE_NUMBER_PK_HASH, useNumberPkHash, false);
        if (useNumberPkHash)
        {
            mPkHashType = pk_number_hash;
        }
    }
}

void PrimaryKeyIndexConfigImpl::AssertEqual(const IndexConfigImpl& other) const
{
    SingleFieldIndexConfigImpl::AssertEqual(other);
    const PrimaryKeyIndexConfigImpl& other2 = (const PrimaryKeyIndexConfigImpl&)other;
    IE_CONFIG_ASSERT_EQUAL(mPkIndexType, other2.mPkIndexType, 
                           "mPkIndexType not equal");
    IE_CONFIG_ASSERT_EQUAL(mPkHashType, other2.mPkHashType, 
                           "mPkHashType not equal");
    mPkLoadParam->AssertEqual(*other2.mPkLoadParam);
}

void PrimaryKeyIndexConfigImpl::AssertCompatible(const IndexConfigImpl& other) const
{
    SingleFieldIndexConfigImpl::AssertEqual(other);
    const PrimaryKeyIndexConfigImpl& other2 = (const PrimaryKeyIndexConfigImpl&)other;
    IE_CONFIG_ASSERT_EQUAL(mPkIndexType, other2.mPkIndexType, 
                           "mPkIndexType not equal");
    IE_CONFIG_ASSERT_EQUAL(mPkHashType, other2.mPkHashType, 
                           "mPkHashType not equal");
}

void PrimaryKeyIndexConfigImpl::Check() const
{
    SingleFieldIndexConfigImpl::Check();
    if (!IsPrimaryKeyIndex())
    {
        INDEXLIB_FATAL_ERROR(Schema, "wrong pk index type");
    }
    if (mPkHashType == pk_number_hash)
    {
        if (mFieldConfig->IsMultiValue() ||
            !FieldConfig::IsIntegerType(mFieldConfig->GetFieldType()))
        {
            INDEXLIB_FATAL_ERROR(Schema, 
                    "enable number pk hash, but pk field type not number"
                    " or multivalue");
        }
    }
}

bool PrimaryKeyIndexConfigImpl::IsPrimaryKeyIndex() const
{
    return mIndexType == it_primarykey64 || 
        mIndexType == it_primarykey128;
}

void PrimaryKeyIndexConfigImpl::SetPrimaryKeyLoadParam(
    PrimaryKeyLoadStrategyParam::PrimaryKeyLoadMode loadMode,
    bool lookupReverse, const string& param)
{
    mPkLoadParam.reset(new PrimaryKeyLoadStrategyParam(
                           loadMode, lookupReverse));
    mPkLoadParam->FromString(param);
}

PrimaryKeyIndexType PrimaryKeyIndexConfigImpl::StringToPkIndexType(
    const string& strPkIndexType)
{
    PrimaryKeyIndexType type = pk_sort_array;
    if (strPkIndexType == "hash_table")
    {
        type = pk_hash_table;
    }
    else if (strPkIndexType == "sort_array")
    {
        type = pk_sort_array;
    }
    else
    {
        INDEXLIB_FATAL_ERROR(Schema, 
                             "unsupported pk storage type: %s",
                             strPkIndexType.c_str());
    }
    return type;
}

string PrimaryKeyIndexConfigImpl::PkIndexTypeToString(const PrimaryKeyIndexType type)
{
    switch(type)
    {
    case pk_hash_table: return string("hash_table");
    case pk_sort_array: return string("sort_array");
    default:
        INDEXLIB_FATAL_ERROR(UnSupported, "unknown pk type[%d]!", type);
    }
    return string("");
}

PrimaryKeyHashType PrimaryKeyIndexConfigImpl::StringToPkHashType(
    const string& strPkHashType)
{
    PrimaryKeyHashType type = pk_default_hash;
    if (strPkHashType == "default_hash")
    {
        type = pk_default_hash;
    }
    else if (strPkHashType == "murmur_hash")
    {
        type = pk_murmur_hash;
    }
    else if (strPkHashType == "number_hash")
    {
        type = pk_number_hash;
    }
    else
    {
        INDEXLIB_FATAL_ERROR(Schema, 
                             "unsupported pk hash type: %s",
                             strPkHashType.c_str());
    }
    return type;
}

string PrimaryKeyIndexConfigImpl::PkHashTypeToString(const PrimaryKeyHashType type)
{
    switch(type)
    {
    case pk_default_hash: return string("default_hash");
    case pk_murmur_hash: return string("murmur_hash");
    case pk_number_hash: return string("number_hash");
    default:
        INDEXLIB_FATAL_ERROR(UnSupported, "unknown pk hash type[%d]!", type);
    }
    return string("");
}

bool PrimaryKeyIndexConfigImpl::CheckFieldType(FieldType ft) const
{
    return true;
}
IE_NAMESPACE_END(config);

