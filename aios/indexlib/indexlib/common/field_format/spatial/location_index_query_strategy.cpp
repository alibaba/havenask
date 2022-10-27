#include <autil/StringUtil.h>
#include "indexlib/common/field_format/spatial/location_index_query_strategy.h"
#include "indexlib/common/field_format/spatial/shape/circle.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_BEGIN(common);
IE_LOG_SETUP(common, LocationIndexQueryStrategy);

LocationIndexQueryStrategy::LocationIndexQueryStrategy(
        int8_t topLevel, int8_t detailLevel, const string& indexName,
        double distanceLoss, size_t maxSearchTerms)
    : QueryStrategy(topLevel, detailLevel, indexName, distanceLoss,
                    maxSearchTerms, true)
{
}

LocationIndexQueryStrategy::~LocationIndexQueryStrategy() 
{
}

void LocationIndexQueryStrategy::CalculateDetailSearchLevel(
        Shape::ShapeType shapeType,
        double distance, uint8_t& detailLevel) 
{
    if (shapeType == Shape::LINE || shapeType == Shape::POLYGON)
    {
        detailLevel = GeoHashUtil::DistanceToLevel(distance * mDistanceLoss);
    }
    else
    {
        detailLevel = GeoHashUtil::DistanceToLevel(distance * 1.2) + 1;
    }
    
    if (detailLevel > mDetailLevel)
    {
        detailLevel = mDetailLevel;
    }

    if (detailLevel < mTopLevel)
    {
        detailLevel = mTopLevel;
    }
}

void LocationIndexQueryStrategy::CalculateTerms(
        const ShapePtr& shape, vector<dictkey_t>& cells)
{
    std::vector<dictkey_t> coverCells;
    GetShapeCoverCells(shape, coverCells);
    for (size_t i = 0; i < coverCells.size(); i++)
    {
        Cell::RemoveLeafTag(coverCells[i]);
        cells.push_back(coverCells[i]);
    }

    if (cells.size() > 1000)
    {
        IE_LOG(DEBUG, "query shape [%s] term num [%lu] too much, "
               "index level [%u,%u]", 
               shape->ToString().c_str(), cells.size(), 
               mTopLevel, mDetailLevel);
    }
}

IE_NAMESPACE_END(common);
