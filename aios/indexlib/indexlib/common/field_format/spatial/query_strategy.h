#ifndef __INDEXLIB_QUERY_STRATEGY_H
#define __INDEXLIB_QUERY_STRATEGY_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/common/field_format/spatial/shape/shape.h"
#include "indexlib/common/field_format/spatial/shape/shape_coverer.h"
#include "indexlib/common/field_format/spatial/shape/circle.h"

IE_NAMESPACE_BEGIN(common);

class QueryStrategy
{
public:
    QueryStrategy(
        int8_t topLevel, int8_t detailLevel,
        const std::string& indexName, double distanceLoss,
        size_t maxSearchTerms, bool shapeCoverOnlyGetLeafCell)
        : mIndexName(indexName)
        , mMaxSearchTerms(maxSearchTerms)
        , mShapeCover(topLevel, detailLevel, shapeCoverOnlyGetLeafCell)
        , mTopLevel(topLevel)
        , mDetailLevel(detailLevel)
        , mDistanceLoss(distanceLoss)
    {}
    virtual ~QueryStrategy() {}
    friend class QueryStrategyTest;
    
public:
    virtual void CalculateTerms(const ShapePtr& shape, std::vector<dictkey_t>& terms) = 0;

protected:
    virtual void GetPointCoveredCells(const PointPtr& point,
                                      std::vector<uint64_t>& coverCells) = 0;
    virtual void CalculateDetailSearchLevel(Shape::ShapeType type,
                                            double distance, uint8_t& detailLevel) = 0;
    void GetShapeCoverCells(const ShapePtr& shape, std::vector<dictkey_t>& cells);

protected:
    std::string mIndexName;
    size_t mMaxSearchTerms;
    ShapeCoverer mShapeCover;
    int8_t mTopLevel;
    int8_t mDetailLevel;
    double mDistanceLoss;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(QueryStrategy);

/////////////////////////////////////////////////////////
inline void QueryStrategy::GetShapeCoverCells(
    const ShapePtr& shape, std::vector<dictkey_t>& coverCells)
{
    assert(shape);
    coverCells.clear();
    Shape::ShapeType type = shape->GetType();
    uint8_t detailLevel = mDetailLevel;
    if (type == Shape::POINT)
    {
        PointPtr point = DYNAMIC_POINTER_CAST(Point, shape);
        GetPointCoveredCells(point, coverCells);
    }
    else if (type == Shape::CIRCLE)
    {
        CirclePtr circle = DYNAMIC_POINTER_CAST(Circle, shape);
        assert(circle);
        CalculateDetailSearchLevel(
            Shape::CIRCLE, circle->GetRadius() * 2, detailLevel);
        ShapePtr rectangle = shape->GetBoundingBox();
        assert(rectangle);
        mShapeCover.GetShapeCoveredCells(rectangle, mMaxSearchTerms,
                detailLevel, coverCells);
    }
    else
    {
        RectanglePtr rectangle = shape->GetBoundingBox();
        assert(rectangle);
        CalculateDetailSearchLevel(shape->GetType(),
            rectangle->CalculateDiagonalLength(), detailLevel);
        mShapeCover.GetShapeCoveredCells(shape, mMaxSearchTerms,
                detailLevel, coverCells);
    }

}

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_QUERY_STRATEGY_H
