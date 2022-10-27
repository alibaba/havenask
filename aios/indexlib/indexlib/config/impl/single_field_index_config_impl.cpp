#include <autil/legacy/any.h>
#include <autil/legacy/jsonizable.h>
#include <autil/legacy/json.h>
#include "indexlib/misc/exception.h"
#include "indexlib/config/configurator_define.h"
#include "indexlib/config/config_define.h"
#include "indexlib/config/schema_configurator.h"
#include "indexlib/config/impl/single_field_index_config_impl.h"

using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;
IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(config);
IE_LOG_SETUP(config, SingleFieldIndexConfigImpl);

SingleFieldIndexConfigImpl::SingleFieldIndexConfigImpl(
    const string& indexName, IndexType indexType)
    : IndexConfigImpl(indexName, indexType)
    , mHasPrimaryKeyAttribute(false)
{}

SingleFieldIndexConfigImpl::SingleFieldIndexConfigImpl(
    const SingleFieldIndexConfigImpl& other)
    : IndexConfigImpl(other)
    , mFieldConfig(other.mFieldConfig)
    , mHasPrimaryKeyAttribute(other.mHasPrimaryKeyAttribute)
{}

void SingleFieldIndexConfigImpl::SetFieldConfig(const FieldConfigPtr& fieldConfig)
{
    FieldType fieldType = fieldConfig->GetFieldType();
    if (!CheckFieldType(fieldType))
    {
        stringstream ss;
        ss << "IndexType " << IndexConfig::IndexTypeToStr(mIndexType)
           << " and FieldType " << FieldConfig::FieldTypeToStr(fieldType)
           << " Mismatch.";
        INDEXLIB_FATAL_ERROR(Schema, "%s", ss.str().c_str());
    }

    mFieldConfig = fieldConfig;
    mAnalyzer = mFieldConfig->GetAnalyzerName();
}

bool SingleFieldIndexConfigImpl::CheckFieldType(FieldType ft) const
{
    #define DO_CHECK(flag) if (flag) return true;

    IndexType it = mIndexType;
    DO_CHECK (it == it_trie);

    DO_CHECK (ft == ft_text && it == it_text);
    DO_CHECK (ft == ft_uint64 && it == it_date);
    DO_CHECK (ft == ft_string && (it == it_string));
    DO_CHECK ((it == it_number_int8 && ft == ft_int8)
              || (it == it_number_uint8 && ft == ft_uint8)
              || (it == it_number_int16 && ft == ft_int16)
              || (it == it_number_uint16 && ft == ft_uint16)
              || (it == it_number_int32 && ft == ft_int32)
              || (it == it_number_uint32 && ft == ft_uint32)
              || (it == it_number_int64 && ft == ft_int64)
              || (it == it_number_uint64 && ft == ft_uint64));
    DO_CHECK ((it == it_number || it == it_range)&&
              (ft == ft_int8 || ft == ft_uint8 ||
               ft == ft_int16 || ft == ft_uint16 ||
               ft == ft_integer || ft == ft_uint32 ||
               ft == ft_long || ft == ft_uint64 )
              );
    return false;
}

bool SingleFieldIndexConfigImpl::IsInIndex(fieldid_t fieldId) const
{
    return (mFieldConfig->GetFieldId() == fieldId);
}

void SingleFieldIndexConfigImpl::AssertEqual(const IndexConfigImpl& other) const
{
    IndexConfigImpl::AssertEqual(other);
    const SingleFieldIndexConfigImpl& other2 = (const SingleFieldIndexConfigImpl&)other;
    IE_CONFIG_ASSERT_EQUAL(mHasPrimaryKeyAttribute, other2.mHasPrimaryKeyAttribute, 
                           "mHasPrimaryKeyAttribute not equal");
    if (mFieldConfig)
    {
        mFieldConfig->AssertEqual(*(other2.mFieldConfig));        
    }
}

void SingleFieldIndexConfigImpl::AssertCompatible(const IndexConfigImpl& other) const
{
    AssertEqual(other);
}

void SingleFieldIndexConfigImpl::Jsonize(autil::legacy::Jsonizable::JsonWrapper& json)
{
    if (mVirtual)
    {
        return;
    }
    IndexConfigImpl::Jsonize(json);
    if (json.GetMode() == TO_JSON) 
    {
        if (mFieldConfig.get() != NULL)
        {
            string fieldName = mFieldConfig->GetFieldName();
            json.Jsonize(INDEX_FIELDS, fieldName);
        }

        if (mHasPrimaryKeyAttribute)
        {
            json.Jsonize(HAS_PRIMARY_KEY_ATTRIBUTE, mHasPrimaryKeyAttribute);
        }
    }
    else
    {
        // parse HasPrimaryKeyAttribute
        if (mIndexType == it_primarykey64 || mIndexType == it_primarykey128)
        {
            bool flag = false;
            json.Jsonize(HAS_PRIMARY_KEY_ATTRIBUTE, flag, flag);
            SetPrimaryKeyAttributeFlag(flag);
        }
    }
}

void SingleFieldIndexConfigImpl::CheckWhetherIsVirtualField() const
{
    bool valid = (mVirtual && mFieldConfig->IsVirtual()) || 
                 (!mVirtual && !mFieldConfig->IsVirtual());
    if (!valid)
    {
        INDEXLIB_FATAL_ERROR(Schema, 
                             "Index is virtual but field is not virtual or"
                             "Index is not virtual but field is virtual");
    }
}

IndexConfigImpl* SingleFieldIndexConfigImpl::Clone() const
{
    return new SingleFieldIndexConfigImpl(*this);
}

void SingleFieldIndexConfigImpl::Check() const
{
    IndexConfigImpl::Check();
    CheckWhetherIsVirtualField();
}

IE_NAMESPACE_END(config);

