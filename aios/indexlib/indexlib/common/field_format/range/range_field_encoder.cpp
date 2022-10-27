#include "indexlib/common/field_format/range/range_field_encoder.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/range_index_config.h"

using namespace std;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(common);

IE_NAMESPACE_BEGIN(common);

IE_LOG_SETUP(common, RangeFieldEncoder);

RangeFieldEncoder::RangeFieldEncoder(const config::IndexPartitionSchemaPtr& schema)
{
    Init(schema);
}

RangeFieldEncoder::~RangeFieldEncoder() 
{
}

void RangeFieldEncoder::Init(const IndexPartitionSchemaPtr& schema)
{
    assert(schema);
    IndexSchemaPtr indexSchema = schema->GetIndexSchema();
    assert(indexSchema);
    auto indexConfigs = indexSchema->CreateIterator(false);
    for (auto iter = indexConfigs->Begin(); iter != indexConfigs->End(); iter++)
    {
        const IndexConfigPtr& indexConfig = *iter;
        if (indexConfig->GetIndexType() != it_range)
        {
            continue;
        }

        RangeIndexConfigPtr rangeConfig =
            DYNAMIC_POINTER_CAST(RangeIndexConfig, indexConfig);
        fieldid_t fieldId = rangeConfig->GetFieldConfig()->GetFieldId();
        FieldType fieldType = rangeConfig->GetFieldConfig()->GetFieldType();
        RangeFieldType rangeType = GetRangeFieldType(fieldType);
        if ((size_t)fieldId >= mFieldVec.size())
        {
            mFieldVec.resize(fieldId + 1, UNKOWN);
            mFieldVec[fieldId] = rangeType;
        }
        else
        {
            mFieldVec[fieldId] = rangeType;
        }
    }
}

RangeFieldEncoder::RangeFieldType RangeFieldEncoder::GetRangeFieldType(FieldType fieldType)
{
    if (fieldType == ft_int8 || fieldType == ft_int16 ||
        fieldType == ft_int32 || fieldType == ft_int64)
    {
        return INT;
    }

    if (fieldType == ft_uint8 || fieldType == ft_uint16 ||
        fieldType == ft_uint32 || fieldType == ft_uint64)
    {
        return UINT;
    }
    return UNKOWN;
}


IE_NAMESPACE_END(common);
