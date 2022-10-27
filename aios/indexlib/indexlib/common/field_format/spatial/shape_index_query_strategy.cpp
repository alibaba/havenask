#include <autil/StringUtil.h>
#include "indexlib/common/field_format/spatial/shape_index_query_strategy.h"

using namespace std;
using namespace autil;

IE_NAMESPACE_BEGIN(common);
IE_LOG_SETUP(common, ShapeIndexQueryStrategy);

ShapeIndexQueryStrategy::ShapeIndexQueryStrategy(
        int8_t topLevel, int8_t detailLevel,
        const std::string& indexName,
        double distanceLoss, size_t maxSearchTerms)
    : QueryStrategy(topLevel, detailLevel, indexName, distanceLoss,
                    maxSearchTerms, false)
{
}

ShapeIndexQueryStrategy::~ShapeIndexQueryStrategy() 
{
}

void ShapeIndexQueryStrategy::CalculateDetailSearchLevel(
        Shape::ShapeType type, double distance, uint8_t& detailLevel)
{
    detailLevel = GeoHashUtil::DistanceToLevel(distance * mDistanceLoss);
    if (detailLevel > mDetailLevel)
    {
        detailLevel = mDetailLevel;
    }

    if (detailLevel < mTopLevel)
    {
        detailLevel = mTopLevel;
    }
}

void ShapeIndexQueryStrategy::CalculateTerms(
        const ShapePtr& shape, vector<dictkey_t>& cells)
{
    assert(shape);

    std::vector<uint64_t> coverCells;
    GetShapeCoverCells(shape, coverCells);

    for (size_t i = 0; i < coverCells.size(); i++)
    {
        if (Cell::IsLeafCell(coverCells[i]))
        {
            cells.push_back(coverCells[i]);
            Cell::RemoveLeafTag(coverCells[i]);
            cells.push_back(coverCells[i]);
        }
        else
        {
            Cell::SetLeafTag(coverCells[i]);
            cells.push_back(coverCells[i]);
        }
    }

    if (cells.size() > 1000)
    {
        IE_LOG(DEBUG, "query shape [%s] term num [%lu] too much, "
               "index level [%u,%u]", 
               shape->ToString().c_str(), cells.size(), 
               mTopLevel, mDetailLevel);
    }
}

uint8_t ShapeIndexQueryStrategy::CalculateDetailSearchLevel(
        const ShapePtr& shape, double distanceLoss,
        uint8_t topLevel, uint8_t detailLevel)
{
    double distance = shape->GetBoundingBox()->CalculateDiagonalLength();
    uint8_t searchLevel = GeoHashUtil::DistanceToLevel(distance * distanceLoss);
    IE_LOG(DEBUG, "distance %lf: level %u", distance, searchLevel);
    if (searchLevel > detailLevel)
    {
        searchLevel = detailLevel;
    }

    if (searchLevel < topLevel)
    {
        searchLevel = topLevel;
    }
    return searchLevel;
}


IE_NAMESPACE_END(common);

