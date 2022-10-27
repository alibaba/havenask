#include "indexlib/index/normal/attribute/format/var_num_attribute_data_formatter.h"
#include "indexlib/config/attribute_config.h"
#include "indexlib/config/pack_attribute_config.h"
#include "indexlib/config/field_type_traits.h"

using namespace std;
IE_NAMESPACE_USE(config);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, VarNumAttributeDataFormatter);

VarNumAttributeDataFormatter::VarNumAttributeDataFormatter() 
    : mIsMultiString(false)
    , mFieldSizeShift(0)
    , mFixedValueCount(-1)
    , mFixedValueLength(0)
{
}

VarNumAttributeDataFormatter::~VarNumAttributeDataFormatter() 
{
}

void VarNumAttributeDataFormatter::Init(const AttributeConfigPtr& attrConfig)
{
    mFixedValueCount = attrConfig->GetFieldConfig()->GetFixedMultiValueCount();
    if (mFixedValueCount != -1)
    {
        mFixedValueLength = config::PackAttributeConfig::GetFixLenFieldSize(
                attrConfig->GetFieldConfig());
    }
    
    if (attrConfig->GetFieldType() != ft_string)
    {
        size_t fieldTypeSize = SizeOfFieldType(attrConfig->GetFieldType());
        switch (fieldTypeSize)
        {
        case 1: mFieldSizeShift = 0; break;
        case 2: mFieldSizeShift = 1; break;
        case 4: mFieldSizeShift = 2; break;
        case 8: mFieldSizeShift = 3; break;

        default: 
            assert(false);
            mFieldSizeShift = 0;
        }
        return;
    }

    if (attrConfig->IsMultiString())
    {
        mIsMultiString = true;
    }
    else
    {
        mFieldSizeShift = 0;
    }
}

IE_NAMESPACE_END(index);

