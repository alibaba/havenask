#include "indexlib/common/field_format/date/date_field_encoder.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(common);
IE_LOG_SETUP(common, DateFieldEncoder);




DateFieldEncoder::DateFieldEncoder(const config::IndexPartitionSchemaPtr& schema)
{
    Init(schema);
}

DateFieldEncoder::~DateFieldEncoder() 
{
}

void DateFieldEncoder::Init(const IndexPartitionSchemaPtr& schema)
{
    assert(schema);
    IndexSchemaPtr indexSchema = schema->GetIndexSchema();
    assert(indexSchema);
    auto indexConfigs = indexSchema->CreateIterator(false);
    for (auto iter = indexConfigs->Begin(); iter != indexConfigs->End(); iter++)
    {
        const IndexConfigPtr& indexConfig = *iter;
        if (indexConfig->GetIndexType() != it_date)
        {
            continue;
        }
        DateIndexConfigPtr dateConfig = DYNAMIC_POINTER_CAST(
                DateIndexConfig, indexConfig);
        assert(dateConfig);
        fieldid_t fieldId = dateConfig->GetFieldConfig()->GetFieldId();
        if (fieldId >= (fieldid_t)mDateDescriptions.size())
        {
            mDateDescriptions.resize(fieldId + 1,
                    std::make_pair(DateLevelFormat::GU_UNKOWN,
                            DateLevelFormat()));
        }
        mDateDescriptions[fieldId] = std::make_pair(dateConfig->GetBuildGranularity(),
                dateConfig->GetDateLevelFormat());
    }
}

bool DateFieldEncoder::IsDateIndexField(fieldid_t fieldId)
{
    if (fieldId >= (fieldid_t)mDateDescriptions.size())
    {
        return false;
    }

    return mDateDescriptions[fieldId].first != DateLevelFormat::GU_UNKOWN;
}

IE_NAMESPACE_END(common);
