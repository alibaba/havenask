#ifndef __INDEXLIB_LOCATION_INDEX_QUERY_STRATEGY_H
#define __INDEXLIB_LOCATION_INDEX_QUERY_STRATEGY_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/common/field_format/spatial/query_strategy.h"

IE_NAMESPACE_BEGIN(common);

class LocationIndexQueryStrategy : public QueryStrategy
{
public:
    LocationIndexQueryStrategy(int8_t topLevel, int8_t detailLevel,
                               const std::string& indexName,
                               double distanceLoss,
                         size_t maxSearchTerms = 3000);
    ~LocationIndexQueryStrategy();

public:
    void CalculateDetailSearchLevel(Shape::ShapeType shapeType,
                                    double distance, uint8_t& detailLevel) override;
    void CalculateTerms(const ShapePtr& shape, std::vector<dictkey_t>& terms) override;
    void GetPointCoveredCells(const PointPtr& point, std::vector<uint64_t>& coverCells) override
    {
        mShapeCover.GetPointCoveredCells(point, coverCells, true);
    }
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(LocationIndexQueryStrategy);

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_LOCATION_INDEX_QUERY_STRATEGY_H
