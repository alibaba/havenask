#ifndef __INDEXLIB_SHAPE_INDEX_QUERY_STRATEGY_H
#define __INDEXLIB_SHAPE_INDEX_QUERY_STRATEGY_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/common/field_format/spatial/query_strategy.h"

IE_NAMESPACE_BEGIN(common);

class ShapeIndexQueryStrategy : public QueryStrategy
{
public:
    ShapeIndexQueryStrategy(
        int8_t topLevel, int8_t detailLevel,
        const std::string& indexName,
        double distanceLoss,
        size_t maxSearchTerms = 3000);
    ~ShapeIndexQueryStrategy();
public:
    void CalculateTerms(const ShapePtr& shape, std::vector<dictkey_t>& terms) override;
    static uint8_t CalculateDetailSearchLevel(
        const ShapePtr& shape, double distanceLoss,
        uint8_t topLevel, uint8_t detailLevel);

protected:
    void GetPointCoveredCells(const PointPtr& point,
                              std::vector<uint64_t>& coverCells) override
    {
        mShapeCover.GetPointCoveredCells(point, coverCells, false);
    }
    void CalculateDetailSearchLevel(
        Shape::ShapeType type, double distance, uint8_t& detailLevel) override;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(ShapeIndexQueryStrategy);

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_SHAPE_INDEX_QUERY_STRATEGY_H
