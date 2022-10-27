#ifndef __INDEXLIB_SPATIAL_FIELD_ENCODER_H
#define __INDEXLIB_SPATIAL_FIELD_ENCODER_H

#include <tr1/memory>
#include <map>
#include <autil/MultiValueType.h>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/common/field_format/spatial/shape/shape_creator.h"
#include "indexlib/common/field_format/spatial/geo_hash/geo_hash_util.h"
#include "indexlib/common/field_format/spatial/shape/point.h"
#include "indexlib/common/field_format/spatial/shape/line.h"
#include "indexlib/common/field_format/spatial/shape/polygon.h"
#include "indexlib/common/field_format/spatial/cell.h"
#include "indexlib/common/field_format/spatial/shape/shape_coverer.h"
#include "indexlib/common/field_format/spatial/shape_index_query_strategy.h"

IE_NAMESPACE_BEGIN(common);

class SpatialFieldEncoder
{
public:
    SpatialFieldEncoder(const config::IndexPartitionSchemaPtr& schema);
    ~SpatialFieldEncoder();

public:
    bool IsSpatialIndexField(fieldid_t fieldId);
    void Encode(fieldid_t fieldId, const std::string& fieldValue,
                std::vector<dictkey_t>& dictKeys);

private:
    void Init(const config::IndexPartitionSchemaPtr& schema);
    void EncodeShape(fieldid_t fieldId,
                     const std::string& fieldValue,
                     std::vector<dictkey_t>& dictKeys);
    void EncodePoint(fieldid_t fieldId, const std::string& fieldValue,
                     std::vector<dictkey_t>& dictKeys);
    void AppendToDictKeys(const std::vector<dictkey_t>& srcKeys,
                          std::vector<dictkey_t>& destKeys);
    int8_t GetTopLevel(fieldid_t fieldId)
    { return mSpatialLevels[fieldId].first; }
    int8_t GetDetailLevel(fieldid_t fieldId)
    { return mSpatialLevels[fieldId].second; }

private:
    typedef std::pair<int8_t, int8_t> SpatialLevelPair;

private:
    std::vector<SpatialLevelPair> mSpatialLevels;
    std::vector<FieldType> mFieldType;
    std::vector<double> mDistanceLoss;
private:
    friend class SpatialFieldEncoderTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SpatialFieldEncoder);
///////////////////////////////////////////////////////
inline bool SpatialFieldEncoder::IsSpatialIndexField(fieldid_t fieldId)
{
    if (fieldId >= (fieldid_t)mSpatialLevels.size())
    {
        return false;
    }
    if (GetTopLevel(fieldId) == -1)
    {
        return false;
    }

    return true;
}

inline void SpatialFieldEncoder::Encode(fieldid_t fieldId,
                                        const std::string& fieldValue,
                                        std::vector<dictkey_t>& dictKeys)
{
    dictKeys.clear();
    if (!IsSpatialIndexField(fieldId))
    {
        return;
    }

    if (fieldValue.empty())
    {
        IE_LOG(DEBUG, "field [%d] value is empty.", fieldId);
        return;
    }

    std::vector<std::string> shapeStrs;
    autil::StringUtil::fromString(
        fieldValue, shapeStrs, std::string(1, MULTI_VALUE_SEPARATOR));
    FieldType type = mFieldType[fieldId];
    if (type == ft_location)
    {
        for (size_t i = 0; i < shapeStrs.size(); i++)
        {
            std::vector<dictkey_t> pointKeys;
            EncodePoint(fieldId, shapeStrs[i], pointKeys);
            AppendToDictKeys(pointKeys, dictKeys);
        }
        return;
    }
    for (size_t i = 0; i < shapeStrs.size(); i++)
    {
        std::vector<dictkey_t> keys;
        EncodeShape(fieldId, shapeStrs[i], keys);
        AppendToDictKeys(keys, dictKeys);
    }
}

inline void SpatialFieldEncoder::EncodeShape(fieldid_t fieldId,
        const std::string& fieldValue,
        std::vector<dictkey_t>& dictKeys)
{
    FieldType type = mFieldType[fieldId];
    ShapePtr shape;
    if (type == ft_line)
    {
        shape = Line::FromString(fieldValue);
    }
    else if (type == ft_polygon)
    {
        shape = Polygon::FromString(fieldValue);
    }
    else
    {
        assert(false);
        return;
    }
    
    if (!shape)
    {
        return;
    }
    uint8_t detailLevel =
        ShapeIndexQueryStrategy::CalculateDetailSearchLevel(
            shape, mDistanceLoss[fieldId], mSpatialLevels[fieldId].first,
            mSpatialLevels[fieldId].second);
    ShapeCoverer shapeCoverer(mSpatialLevels[fieldId].first,
                              mSpatialLevels[fieldId].second,
                              false);
    shapeCoverer.GetShapeCoveredCells(
        shape, 30000,
        detailLevel, dictKeys);
}

inline void SpatialFieldEncoder::EncodePoint(fieldid_t fieldId,
        const std::string& fieldValue,
        std::vector<dictkey_t>& dictKeys)
{
    PointPtr point = Point::FromString(fieldValue);
    if (!point)
    {
        return;
    }
    GeoHashUtil::Encode(point->GetX(), point->GetY(), dictKeys, 
                        GetTopLevel(fieldId), GetDetailLevel(fieldId));
}

inline void SpatialFieldEncoder::AppendToDictKeys(const std::vector<dictkey_t>& srcKeys,
        std::vector<dictkey_t>& destKeys)
{
    for (size_t i = 0; i < srcKeys.size(); i++)
    {
        destKeys.push_back(srcKeys[i]);
    }
}


IE_NAMESPACE_END(common);

#endif //__INDEXLIB_SPATIAL_FIELD_ENCODER_H
