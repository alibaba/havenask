#include "indexlib/common/field_format/spatial/spatial_field_encoder.h"
#include "indexlib/util/key_hasher_factory.h"
#include "indexlib/config/spatial_index_config.h"

using namespace std;
IE_NAMESPACE_USE(config);

IE_NAMESPACE_USE(util);
IE_NAMESPACE_BEGIN(common);
IE_LOG_SETUP(common, SpatialFieldEncoder);

SpatialFieldEncoder::SpatialFieldEncoder(const IndexPartitionSchemaPtr& schema) 
{
    Init(schema);
}

SpatialFieldEncoder::~SpatialFieldEncoder() 
{
}

void SpatialFieldEncoder::Init(const IndexPartitionSchemaPtr& schema)
{
    assert(schema);
    IndexSchemaPtr indexSchema = schema->GetIndexSchema();
    assert(indexSchema);
    auto indexConfigs = indexSchema->CreateIterator(false);
    for (auto iter = indexConfigs->Begin(); iter != indexConfigs->End(); iter++)
    {
        const IndexConfigPtr& indexConfig = *iter;
        if (indexConfig->GetIndexType() != it_spatial)
        {
            continue;
        }

        SpatialIndexConfigPtr spatialConfig = DYNAMIC_POINTER_CAST(
                SpatialIndexConfig, indexConfig);
        assert(spatialConfig);
        fieldid_t fieldId = spatialConfig->GetFieldConfig()->GetFieldId();
        FieldType fieldType = spatialConfig->GetFieldConfig()->GetFieldType();
        assert(fieldType == ft_location
               || fieldType == ft_line
               || fieldType == ft_polygon);
        if (fieldId >= (fieldid_t)mSpatialLevels.size())
        {
            mSpatialLevels.resize(fieldId + 1, make_pair(-1, -1));
            mFieldType.resize(fieldId + 1, ft_unknown);
            mDistanceLoss.resize(fieldId + 1, SpatialIndexConfig::DEFAULT_DIST_LOSS_ACCURACY);
        }
        int8_t topLevel = GeoHashUtil::DistanceToLevel(spatialConfig->GetMaxSearchDist());
        int8_t detailLevel = GeoHashUtil::DistanceToLevel(spatialConfig->GetMaxDistError());
        mSpatialLevels[fieldId] = make_pair(topLevel, detailLevel);
        mFieldType[fieldId] = fieldType;
        mDistanceLoss[fieldId] = spatialConfig->GetDistanceLoss();
    }
}

IE_NAMESPACE_END(common);

